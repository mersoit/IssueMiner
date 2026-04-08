# -*- coding: utf-8 -*-
"""
Emergent Phase 4: Per-taxonomy-branch incident analysis.

For each (Product, TopicKey) in emergent_taxonomy:
  - Fetch threads assigned to that branch
  - Fetch existing EmergentIncidents for that branch
  - Call GPT-5.2 in sequential batches (within a branch) so each batch
    sees the growing incident list from previous batches
  - Model creates new incidents or assigns threads to existing ones
  - Model sets incident status based on temporal + content signals

SEQUENTIAL within a branch (batches see previous results).
PARALLEL across branches (branches don't share incidents).

Reads from:
  - dbo.emergent_taxonomy
  - dbo.thread_enrichment  (threads with EmergentTaxonomyID set)
  - dbo.EmergentIncidents  (existing incidents for the branch)

Writes:
  - dbo.EmergentIncidents       (upsert)
  - dbo.EmergentIncidentThreads (link)
"""

import os
import json
import logging
import time
import threading
import pyodbc
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import azure.functions as func
from openai import AzureOpenAI

from aoai_helpers import (
    make_nano_client,
    get_nano_deployment,
    make_mini_client,
    get_mini_deployment,
    call_aoai_with_retry,
    estimate_tokens,
    get_rate_limiter,
    sql_connect as _aoai_sql_connect,
)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

_BATCH_SIZE = int(os.getenv("EMRG_EXEC_BATCH_SIZE", "12"))
_MAX_BRANCHES_PARALLEL = int(os.getenv("EMRG_EXEC_MAX_BRANCHES", "4"))

# Incident statuses the model can set
_VALID_STATUSES = {
    "active",
    "investigating",
    "mitigated",
    "resolved",
    "by-design",
    "false-positive",
    "archived",
}


def sql_connect() -> pyodbc.Connection:
    return _aoai_sql_connect(autocommit=False)


# ---------------------------------------------------------
# SQL: Read
# ---------------------------------------------------------

def fetch_taxonomy_branches(cnx: pyodbc.Connection) -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    cur.execute(
        """
        SELECT TaxonomyID, Product, TopicKey, TopicSignature
        FROM dbo.emergent_taxonomy
        WHERE IsActive = 1
        ORDER BY Product, TopicKey
        """
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_threads_for_branch(
    cnx: pyodbc.Connection,
    taxonomy_id: int,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    cur.execute(
        """
        SELECT TOP (?)
            thread_id,
            product,
            signature_text,
            resolution_signature_text,
            emergent_signal,
            source_created_at,
            solution_usefulness,
            classification
        FROM dbo.thread_enrichment
        WHERE EmergentTaxonomyID = ?
          AND (classification = 'emergent_issue' OR is_emergent = 1)
          AND ISNULL(emergent_signal, 0.0) >= 0.5
        ORDER BY source_created_at DESC, thread_id DESC
        """,
        int(limit),
        int(taxonomy_id),
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_incidents_for_branch(
    cnx: pyodbc.Connection,
    product: str,
    topic_key: str,
) -> List[Dict[str, Any]]:
    """Fetch existing incidents scoped to a taxonomy branch via TopicKey stored in RawJson."""
    cur = cnx.cursor()
    cur.execute(
        """
        SELECT
            IncidentID, IncidentKey, Product, IncidentType,
            Title, Summary, AffectedComponent, Status,
            Confidence, ThreadCount,
            FirstThreadSeenUtc, LastThreadSeenUtc, TopicKey
        FROM dbo.EmergentIncidents
        WHERE Product = ?
          AND TopicKey = ?
          AND Status NOT IN ('archived')
        ORDER BY LastThreadSeenUtc DESC
        """,
        str(product),
        str(topic_key),
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---------------------------------------------------------
# SQL: Write
# ---------------------------------------------------------

def upsert_incident(
    cnx: pyodbc.Connection,
    product: str,
    topic_key: str,
    inc: Dict[str, Any],
    thread_timestamps: Dict[str, Any],
) -> Optional[int]:
    """Upsert a single incident and link its threads. Returns IncidentID or None."""
    cur = cnx.cursor()

    inc_key = (inc.get("incident_key") or "").strip()
    inc_type = (inc.get("incident_type") or "unknown").strip()
    title = (inc.get("title") or "")[:500]
    summary = (inc.get("summary") or "")[:4000]
    component = (inc.get("affected_component") or "")[:200]
    region = (inc.get("affected_region") or None)
    if isinstance(region, str):
        region = region.strip()[:100] or None
    period = (inc.get("detected_period") or None)
    if isinstance(period, str):
        period = period.strip()[:20] or None
    confidence = float(inc.get("confidence") or 0.0)
    status = (inc.get("status") or "active").strip().lower()
    thread_ids = [str(t) for t in (inc.get("thread_ids") or []) if t]
    raw = json.dumps(inc, ensure_ascii=False)

    if not inc_key or not thread_ids:
        return None

    if status not in _VALID_STATUSES:
        status = "active"

    ts_list = [
        thread_timestamps[tid]
        for tid in thread_ids
        if tid in thread_timestamps and thread_timestamps[tid]
    ]
    first_ts = min(ts_list) if ts_list else None
    last_ts = max(ts_list) if ts_list else None

    cur.execute(
        """
        MERGE dbo.EmergentIncidents WITH (HOLDLOCK) AS t
        USING (SELECT ? AS IncidentKey) AS s ON t.IncidentKey = s.IncidentKey
        WHEN MATCHED THEN
            UPDATE SET
                LastUpdatedUtc    = SYSUTCDATETIME(),
                Status            = ?,
                Confidence        = CASE WHEN ? > t.Confidence THEN ? ELSE t.Confidence END,
                LastThreadSeenUtc = CASE WHEN ? > t.LastThreadSeenUtc THEN ? ELSE t.LastThreadSeenUtc END,
                ThreadCount       = (SELECT COUNT(*) FROM dbo.EmergentIncidentThreads eit WHERE eit.IncidentID = t.IncidentID) + ?,
                Summary           = CASE WHEN LEN(?) > LEN(ISNULL(t.Summary, '')) THEN ? ELSE t.Summary END,
                AffectedRegion    = COALESCE(t.AffectedRegion, ?),
                DetectedPeriod    = COALESCE(t.DetectedPeriod, ?)
        WHEN NOT MATCHED THEN
            INSERT (IncidentKey, Product, TopicKey, IncidentType, Title, Summary,
                    AffectedComponent, AffectedRegion, DetectedPeriod, Status, Confidence,
                    FirstThreadSeenUtc, LastThreadSeenUtc,
                    FirstDetectedUtc, LastUpdatedUtc, ThreadCount,
                    ModelVersion, RawJson)
            VALUES (?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    COALESCE(?, SYSUTCDATETIME()), COALESCE(?, SYSUTCDATETIME()),
                    SYSUTCDATETIME(), SYSUTCDATETIME(), ?,
                    'mini', ?);
        """,
        # USING
        inc_key,
        # WHEN MATCHED
        status,
        confidence, confidence,
        last_ts, last_ts,
        len(thread_ids),
        summary, summary,
        region, period,
        # WHEN NOT MATCHED
        inc_key, product, topic_key, inc_type, title, summary,
        component, region, period, status, confidence,
        first_ts, last_ts,
        len(thread_ids), raw,
    )

    cur.execute(
        "SELECT IncidentID FROM dbo.EmergentIncidents WHERE IncidentKey = ?",
        inc_key,
    )
    row = cur.fetchone()
    if not row:
        return None

    inc_id = int(row[0])

    for tid in thread_ids:
        ts = thread_timestamps.get(tid)
        try:
            cur.execute(
                """
                IF NOT EXISTS (
                    SELECT 1 FROM dbo.EmergentIncidentThreads
                    WHERE IncidentID = ? AND ThreadID = ?
                )
                INSERT INTO dbo.EmergentIncidentThreads
                    (IncidentID, ThreadID, ThreadCreatedAt, Confidence)
                VALUES (?, ?, ?, ?)
                """,
                inc_id, tid,
                inc_id, tid, ts, confidence,
            )
        except Exception:
            pass

    return inc_id


# ---------------------------------------------------------
# Mini: Per-branch incident analysis
# ---------------------------------------------------------

def _call_mini_analyze_branch(
    client: AzureOpenAI,
    deployment: str,
    product: str,
    topic_key: str,
    threads: List[Dict[str, Any]],
    existing_incidents: List[Dict[str, Any]],
) -> Dict[str, Any]:
    system_prompt = (
        "You are an incident aggregator for Azure support.\n"
        "You analyze emergent-flagged threads within a specific taxonomy branch and:\n"
        "  1) Assign threads to EXISTING incidents when they match.\n"
        "  2) Create NEW incidents when threads represent a genuinely new issue.\n"
        "  3) Set status on each incident based on temporal and content signals.\n\n"

        "INCIDENT STATUSES:\n"
        "  active         — new reports still appearing, no fix known.\n"
        "  investigating  — enough signal to warrant human review (thread_count >= 5 or high confidence).\n"
        "  mitigated      — a workaround exists in the thread content but root cause not fixed.\n"
        "  resolved       — root cause fixed; threads contain confirmed fix and no new reports.\n"
        "  by-design      — official response confirms intentional behavior.\n"
        "  false-positive  — not actually a platform issue after review.\n\n"

        "STATUS RULES:\n"
        "  - Set 'mitigated' if threads contain workaround steps but issue persists.\n"
        "  - Set 'resolved' only if threads confirm a fix AND no recent threads report the same issue.\n"
        "  - Set 'by-design' only if an official Microsoft response confirms this.\n"
        "  - Set 'false-positive' if all threads turn out to be customer config errors.\n"
        "  - Default to 'active' when unsure.\n\n"

        "TEMPORAL AWARENESS:\n"
        "  - Threads have source_created_at timestamps. Use these to detect:\n"
        "    * Clustering in time (many threads in same week = likely incident)\n"
        "    * Recency (no threads in last 30 days = possibly resolved)\n"
        "    * Regression (gap then new reports = reopened)\n\n"

        "DEDUPLICATION RULES:\n"
        "  - Assign to an existing incident if: same symptom AND same region AND\n"
        "    the thread's source_created_at is within 14 days of existing last_seen.\n"
        "  - Create a NEW incident if:\n"
        "    * Same symptom but DIFFERENT region → new key with region suffix.\n"
        "    * Same symptom but thread is 14+ days after existing last_seen → new key with period suffix.\n"
        "  - Use the EXACT incident_key from existing_incidents when assigning.\n\n"

        "TEMPORAL SPLIT RULES (CRITICAL):\n"
        "  - Compare each thread's source_created_at against the existing incident's last_seen.\n"
        "  - Gap > 14 days = treat as a SEPARATE incident event, even if symptoms are identical.\n"
        "  - Within the current batch: if the gap between the EARLIEST and LATEST thread exceeds\n"
        "    21 days AND there is a visible silence gap of 7+ days in the middle, split into\n"
        "    multiple incidents within this call.\n"
        "  - Do NOT expand an existing incident's time window by more than 21 days total.\n\n"

        "INCIDENT KEY RULES:\n"
        "  - kebab-case. No raw dates, GUIDs, or thread IDs.\n"
        "  - Same symptom + same region + within 14 days → reuse existing key.\n"
        "  - Same symptom + different region → append region slug: 'outage-quota-east-us'.\n"
        "  - Same symptom + 14+ day gap → append period (month of first thread): 'outage-quota-apr-2026'.\n"
        "  - Same symptom + different region + different period → both: 'outage-quota-east-us-apr-2026'.\n"
        "  - For bugs/by-design (not time-bound): keep key stable with no qualifier.\n\n"

        "GROUPING RULES:\n"
        "  - Prefer fewer incidents — but NOT if threads span different regions or different time windows.\n"
        "  - A single thread with emergent_signal >= 0.8 in a new region warrants its own incident.\n\n"

        "Output JSON only:\n"
        "{\n"
        "  \"incidents\": [\n"
        "    {\n"
        "      \"incident_key\": \"string (reuse existing or new per key rules above)\",\n"
        "      \"incident_type\": \"regression|outage|bug|perf\",\n"
        "      \"title\": \"short (<80 chars)\",\n"
        "      \"summary\": \"2-4 sentences\",\n"
        "      \"affected_component\": \"string\",\n"
        "      \"affected_region\": \"region slug or null if global (e.g. 'east-us', 'west-eu')\",\n"
        "      \"detected_period\": \"'mmm-yyyy' of first thread or null if not time-bound (e.g. 'apr-2026')\",\n"
        "      \"status\": \"active|investigating|mitigated|resolved|by-design|false-positive\",\n"
        "      \"confidence\": 0.0-1.0,\n"
        "      \"thread_ids\": [\"...\"]\n"
        "    }\n"
        "  ],\n"
        "  \"notes\": \"reasoning summary\"\n"
        "}\n\n"
        "Rules:\n"
        "  - Every input thread_id must appear in exactly one incident's thread_ids.\n"
        "  - confidence >= 0.6 required to create a new incident.\n"
    )

    slim_threads = [
        {
            "thread_id": str(t.get("thread_id") or ""),
            "signature_text": (t.get("signature_text") or "")[:600],
            "resolution_signature_text": (t.get("resolution_signature_text") or "")[:400],
            "emergent_signal": float(t["emergent_signal"]) if t.get("emergent_signal") is not None else None,
            "source_created_at": str(t.get("source_created_at") or ""),
            "solution_usefulness": float(t["solution_usefulness"]) if t.get("solution_usefulness") is not None else None,
        }
        for t in threads
    ]

    slim_existing = [
        {
            "incident_key": e.get("IncidentKey"),
            "incident_type": e.get("IncidentType"),
            "title": e.get("Title"),
            "status": e.get("Status"),
            "thread_count": int(e["ThreadCount"]) if e.get("ThreadCount") is not None else None,
            "first_seen": str(e.get("FirstThreadSeenUtc") or ""),
            "last_seen": str(e.get("LastThreadSeenUtc") or ""),
            "affected_component": e.get("AffectedComponent"),
        }
        for e in existing_incidents
    ]

    user_content = json.dumps({
        "product": product,
        "topic_key": topic_key,
        "threads": slim_threads,
        "existing_incidents": slim_existing,
    }, ensure_ascii=False)

    resp = call_aoai_with_retry(
        client,
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        response_format={"type": "json_object"},
        estimated_prompt_tokens=estimate_tokens(system_prompt) + estimate_tokens(user_content),
        rate_limiter=get_rate_limiter("mini"),
        caller_tag="phase3b_emergent",
    )
    return json.loads(resp.choices[0].message.content or "{}")


# ---------------------------------------------------------
# Branch processor (sequential within branch)
# ---------------------------------------------------------

def _process_branch(
    branch: Dict[str, Any],
    batch_size: int,
) -> Dict[str, Any]:
    """Process one taxonomy branch: sequential batches, growing incident list."""
    product = str(branch.get("Product") or "").strip()
    topic_key = str(branch.get("TopicKey") or "").strip()
    taxonomy_id = int(branch["TaxonomyID"])

    client = make_mini_client()
    deployment = get_mini_deployment()

    with sql_connect() as cnx:
        all_threads = fetch_threads_for_branch(cnx, taxonomy_id, limit=500)
        existing_incidents = fetch_incidents_for_branch(cnx, product, topic_key)

    if not all_threads:
        return {
            "product": product, "topic_key": topic_key,
            "threads": 0, "incidents_before": len(existing_incidents),
            "incidents_after": len(existing_incidents), "batches": 0,
        }

    thread_timestamps = {
        str(t.get("thread_id")): t.get("source_created_at")
        for t in all_threads
    }

    total_incidents_created = 0
    total_threads_assigned = 0
    batches_run = 0

    for i0 in range(0, len(all_threads), batch_size):
        batch = all_threads[i0:i0 + batch_size]
        if not batch:
            continue

        out = _call_mini_analyze_branch(
            client, deployment, product, topic_key,
            threads=batch,
            existing_incidents=existing_incidents,
        )

        incidents_out = out.get("incidents") or []
        batches_run += 1

        with sql_connect() as cnx:
            for inc in incidents_out:
                inc_id = upsert_incident(cnx, product, topic_key, inc, thread_timestamps)
                if inc_id is not None:
                    total_incidents_created += 1
                    total_threads_assigned += len(inc.get("thread_ids") or [])

            cnx.commit()

            # Refresh existing incidents for next batch (sequential awareness)
            existing_incidents = fetch_incidents_for_branch(cnx, product, topic_key)

    return {
        "product": product,
        "topic_key": topic_key,
        "threads": len(all_threads),
        "incidents_before": len(existing_incidents) - total_incidents_created,
        "incidents_after": len(existing_incidents),
        "incidents_upserted": total_incidents_created,
        "threads_assigned": total_threads_assigned,
        "batches": batches_run,
    }


# ---------------------------------------------------------
# HTTP Entry Point
# ---------------------------------------------------------

def run_phase3b_emergent(req: func.HttpRequest) -> func.HttpResponse:
    """
    Phase 3B: Per-branch emergent incident analysis.
    Sequential within branch, parallel across branches.

    Query params:
      batch_size    (default 12, max 20)
      max_branches  (default 4, max 12) — parallel branch workers
      product       (optional) — process only one product
      topic_key     (optional) — process only one branch
    """
    try:
        batch_size = max(1, min(int(req.params.get("batch_size", str(_BATCH_SIZE))), 20))
        max_branches = max(1, min(int(req.params.get("max_branches", str(_MAX_BRANCHES_PARALLEL))), 12))
        product_filter = req.params.get("product", "").strip() or None
        topic_filter = req.params.get("topic_key", "").strip() or None

        with sql_connect() as cnx:
            branches = fetch_taxonomy_branches(cnx)

        if product_filter:
            branches = [b for b in branches if str(b.get("Product") or "").strip() == product_filter]
        if topic_filter:
            branches = [b for b in branches if str(b.get("TopicKey") or "").strip() == topic_filter]

        if not branches:
            return func.HttpResponse(
                json.dumps({"status": "ok", "branches": 0, "note": "no matching branches"}),
                mimetype="application/json",
            )

        logging.info(
            "phase3b_emergent starting branches=%d batch_size=%d max_branches=%d",
            len(branches), batch_size, max_branches,
        )

        results: List[Dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=max_branches) as ex:
            futures = {
                ex.submit(_process_branch, branch, batch_size): branch
                for branch in branches
            }

            for f in as_completed(futures):
                branch = futures[f]
                try:
                    result = f.result()
                    results.append(result)
                    logging.info(
                        "phase3b_emergent branch_done product=%s topic=%s threads=%d incidents=%d",
                        result.get("product"), result.get("topic_key"),
                        result.get("threads", 0), result.get("incidents_upserted", 0),
                    )
                except Exception as e:
                    logging.exception("phase3b_emergent branch_failed product=%s topic=%s",
                                      branch.get("Product"), branch.get("TopicKey"))
                    results.append({
                        "product": branch.get("Product"),
                        "topic_key": branch.get("TopicKey"),
                        "status": "error",
                        "error": str(e),
                    })

        total_incidents = sum(r.get("incidents_upserted", 0) for r in results)
        total_threads = sum(r.get("threads_assigned", 0) for r in results)

        return func.HttpResponse(
            json.dumps({
                "status": "ok",
                "branches_processed": len(results),
                "total_incidents_upserted": total_incidents,
                "total_threads_assigned": total_threads,
                "batch_size": batch_size,
                "details": results,
            }, default=str),
            mimetype="application/json",
        )

    except Exception as e:
        logging.exception("phase3b_emergent failed")
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}),
            status_code=500,
            mimetype="application/json",
        )
