# -*- coding: utf-8 -*-
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
    call_aoai_with_retry,
    estimate_tokens,
    sql_connect as _aoai_sql_connect,
)


# ---------------------------------------------------------
# Configuration & Shared Clients
# ---------------------------------------------------------

def sql_connect() -> pyodbc.Connection:
    return _aoai_sql_connect(autocommit=False)


def make_aoai_client() -> AzureOpenAI:
    """Backwards-compatible alias."""
    return make_nano_client()


def _get_aoai_deployment() -> str:
    return get_nano_deployment()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _short_tid(tid: Any) -> str:
    s = str(tid or "")
    return s if len(s) <= 10 else (s[:6] + "..." + s[-3:])


# Hard cap concurrent AOAI calls (prevents host instability/rate limit storms)
_AOAI_MAX_CONCURRENCY = int(os.getenv("AOAI_MAX_CONCURRENCY", "4"))
_aoai_sema = threading.BoundedSemaphore(_AOAI_MAX_CONCURRENCY)

# DB batching knobs
_P2F_DB_EXECUTEMANY_BATCH = int(os.getenv("P2F_DB_EXECUTEMANY_BATCH", "500"))


# ---------------------------------------------------------
# SQL: Work Claiming (Emergent-only)
# ---------------------------------------------------------

def force_reset_threads_for_emergent(cnx: pyodbc.Connection, max_rows: int) -> int:
    cur = cnx.cursor()
    cur.execute("""
        ;WITH cte AS (
            SELECT TOP (?)
                thread_id
            FROM dbo.thread_enrichment WITH (ROWLOCK, READPAST, UPDLOCK)
            WHERE CatalogCheckedUtc IS NOT NULL
              AND product IS NOT NULL AND LTRIM(RTRIM(product)) <> ''
              AND signature_text IS NOT NULL AND LTRIM(RTRIM(signature_text)) <> ''
              AND (classification = 'emergent_issue' OR is_emergent = 1)
              AND ISNULL(emergent_signal, 0.0) >= 0.5
            ORDER BY CatalogCheckedUtc DESC, ingested_at DESC
        )
        UPDATE te
        SET EmergentStartedUtc = NULL,
            EmergentCompletedUtc = NULL
        FROM dbo.thread_enrichment te
        JOIN cte ON cte.thread_id = te.thread_id;
    """, int(max_rows))
    return int(cur.rowcount if cur.rowcount is not None else 0)


def claim_threads_for_emergent(
    cnx: pyodbc.Connection,
    batch_size: int,
    include_completed: bool
) -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    cur.execute("""
        ;WITH cte AS (
            SELECT TOP (?)
                thread_id
            FROM dbo.thread_enrichment WITH (ROWLOCK, READPAST, UPDLOCK)
            WHERE CatalogCheckedUtc IS NOT NULL
              AND product IS NOT NULL AND LTRIM(RTRIM(product)) <> ''
              AND signature_text IS NOT NULL AND LTRIM(RTRIM(signature_text)) <> ''
              AND (classification = 'emergent_issue' OR is_emergent = 1)
              AND ISNULL(emergent_signal, 0.0) >= 0.5
              AND (? = 1 OR EmergentCompletedUtc IS NULL)
              AND EmergentStartedUtc IS NULL
            ORDER BY CatalogCheckedUtc DESC, ingested_at DESC
        )
        UPDATE te
        SET EmergentStartedUtc = SYSUTCDATETIME()
        OUTPUT INSERTED.thread_id,
               INSERTED.product,
               INSERTED.signature_text,
               INSERTED.resolution_signature_text,
               INSERTED.emergent_signal,
               INSERTED.source_created_at,
               INSERTED.CatalogCheckedUtc
        FROM dbo.thread_enrichment te
        JOIN cte ON cte.thread_id = te.thread_id;
    """, int(batch_size), 1 if include_completed else 0)

    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def mark_emergent_completed(cnx: pyodbc.Connection, thread_ids: List[str]) -> None:
    if not thread_ids:
        return
    cur = cnx.cursor()
    placeholders = ",".join("?" for _ in thread_ids)
    cur.execute(
        f"""
        UPDATE dbo.thread_enrichment
        SET EmergentCompletedUtc = SYSUTCDATETIME()
        WHERE thread_id IN ({placeholders})
        """,
        *[str(t) for t in thread_ids]
    )


def release_emergent_claim(cnx: pyodbc.Connection, thread_ids: List[str]) -> None:
    if not thread_ids:
        return
    cur = cnx.cursor()
    placeholders = ",".join("?" for _ in thread_ids)
    cur.execute(
        f"""
        UPDATE dbo.thread_enrichment
        SET EmergentStartedUtc = NULL
        WHERE thread_id IN ({placeholders})
          AND EmergentCompletedUtc IS NULL
        """,
        *[str(t) for t in thread_ids]
    )


def clear_stale_emergent_claims(cnx: pyodbc.Connection, older_than_minutes: int = 60) -> int:
    cur = cnx.cursor()
    cur.execute("""
        UPDATE dbo.thread_enrichment
        SET EmergentStartedUtc = NULL
        WHERE EmergentStartedUtc IS NOT NULL
          AND EmergentCompletedUtc IS NULL
          AND EmergentStartedUtc < DATEADD(MINUTE, -?, SYSUTCDATETIME());
    """, int(older_than_minutes))
    return int(cur.rowcount if cur.rowcount is not None else 0)


# ---------------------------------------------------------
# Nano: Emergent grouping/detection
# ---------------------------------------------------------

def _call_nano_detect_emergent(
    client: AzureOpenAI,
    deployment: str,
    product: str,
    threads: List[Dict[str, Any]],
    trace_id: str
) -> Dict[str, Any]:
    system_prompt = (
        "You are an incident triage model for Azure technical support.\n"
        "You are given a batch of threads already flagged as potential emergent issues by a classifier.\n"
        "Your job is to:\n"
        "  1. Re-validate each thread: is it a genuine platform incident signal or a false positive?\n"
        "  2. Group genuine signals into incident candidates.\n\n"

        "EMERGENT ISSUE DEFINITION (apply strictly):\n"
        "A genuine emergent issue requires ALL of the following:\n"
        "  A) Platform-side causation: root cause is in the Azure service, not customer config or code.\n"
        "     FALSE POSITIVE signals: wrong SKU, missing config, customer code bug, DNS misconfiguration.\n"
        "     TRUE SIGNAL: HTTP 5xx from service, service timeout with no customer change, API behavior change.\n"
        "  B) Multi-user potential: symptom is generic enough to affect multiple independent customers.\n"
        "     FALSE POSITIVE: problem tied to one resource name, one tenant ID, one specific deployment.\n"
        "     TRUE SIGNAL: error reproducible with standard setup, no unique customer identifiers in root cause.\n"
        "  C) Not already documented: issue is not a known limitation, FAQ, or documented breaking change.\n"
        "     FALSE POSITIVE: error code is in official docs with a known fix.\n"
        "     TRUE SIGNAL: no public resolution exists, or behavior contradicts documentation.\n\n"

        "INCIDENT TYPES (use in incident_key prefix):\n"
        "  'regression-' : behavior changed from a previously working state.\n"
        "  'outage-'     : service is unavailable or returning errors at scale.\n"
        "  'bug-'        : incorrect behavior that is reproducible but not a regression.\n"
        "  'perf-'       : performance degradation with service-side evidence.\n\n"

        "Output MUST be valid JSON only:\n"
        "{\n"
        "  \"incidents\": [\n"
        "     {\n"
        "       \"incident_key\": \"<type_prefix>-<product>-<symptom-tokens>\",\n"
        "       \"incident_type\": \"regression|outage|bug|perf\",\n"
        "       \"title\": \"short (<80 chars)\",\n"
        "       \"summary\": \"2-3 sentences: what breaks, how it manifests, why it is platform-side\",\n"
        "       \"affected_component\": \"specific service component or API\",\n"
        "       \"confidence\": 0.0-1.0,\n"
        "       \"thread_ids\": [\"...\"]\n"
        "     }\n"
        "  ],\n"
        "  \"thread_judgements\": [\n"
        "     {\n"
        "       \"thread_id\": \"...\",\n"
        "       \"label\": \"true_incident|false_positive\",\n"
        "       \"false_positive_reason\": \"config_error|customer_code|known_issue|single_tenant|other\",\n"
        "       \"confidence\": 0.0-1.0,\n"
        "       \"reason\": \"one sentence\"\n"
        "     }\n"
        "  ]\n"
        "}\n\n"
        "Rules:\n"
        "  - Include exactly one judgement per input thread_id.\n"
        "  - For false_positive threads: false_positive_reason is required; incident assignment is not.\n"
        "  - For true_incident threads: assign to exactly one incident (create one if none matches).\n"
        "  - Prefer fewer, broader incident keys over many narrow ones (merge near-duplicates).\n"
        "  - incident_key must be stable: no timestamps, IDs, or subscription-specific tokens.\n"
        "  - confidence >= 0.7 required to create a new incident; otherwise merge or mark false_positive.\n"
        "Return JSON only.\n"
    )

    slim_threads = [{
        "thread_id": str(t.get("thread_id")),
        "signature_text": (t.get("signature_text") or "")[:900],
        "resolution_signature_text": (t.get("resolution_signature_text") or "")[:900],
        "emergent_signal": float(t["emergent_signal"]) if t.get("emergent_signal") is not None else None,
        "source_created_at": str(t.get("source_created_at") or ""),
    } for t in threads]

    user_payload = {
        "product": product,
        "threads": slim_threads,
    }

    t0 = _now_ms()
    logging.info("[2F:%s] nano_start threads=%d", trace_id, len(threads))
    try:
        user_content = json.dumps(user_payload, ensure_ascii=False)
        with _aoai_sema:
            resp = call_aoai_with_retry(
                client,
                model=deployment,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
                response_format={"type": "json_object"},
                estimated_prompt_tokens=estimate_tokens(system_prompt) + estimate_tokens(user_content),
                caller_tag="phase2f",
            )
        return json.loads(resp.choices[0].message.content)
    finally:
        logging.info("[2F:%s] nano_end ms=%d", trace_id, _now_ms() - t0)


def _validate_detect_output(
    input_threads: List[Dict[str, Any]],
    out: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Dict[str, Any]]]:
    exp = {str(t.get("thread_id") or "") for t in input_threads if t.get("thread_id")}
    judgements = out.get("thread_judgements")
    if not isinstance(judgements, list):
        return False, "missing_thread_judgements", {}

    by_tid: Dict[str, Dict[str, Any]] = {}
    extras = []
    for j in judgements:
        if not isinstance(j, dict):
            continue
        tid = str(j.get("thread_id") or "")
        if not tid:
            continue
        if tid not in exp:
            extras.append(tid)
            continue
        by_tid[tid] = j

    missing = [t for t in exp if t not in by_tid]
    if missing:
        return False, f"missing_judgements:{len(missing)}", by_tid
    if extras:
        return False, f"extra_judgements:{len(extras)}", by_tid
    return True, "ok", by_tid

def _upsert_emergent_incidents(
    cnx: pyodbc.Connection,
    product: str,
    incidents: List[Dict[str, Any]],
    thread_timestamps: Dict[str, Any],  # thread_id -> source_created_at
) -> Dict[str, int]:
    """
    Upsert each incident into EmergentIncidents and link confirmed threads.
    Returns mapping of incident_key -> IncidentID.
    """
    cur = cnx.cursor()
    key_to_id: Dict[str, int] = {}

    for inc in incidents:
        inc_key = (inc.get("incident_key") or "").strip()
        inc_type = (inc.get("incident_type") or "unknown").strip()
        title = (inc.get("title") or "")[:500]
        summary = inc.get("summary") or ""
        component = (inc.get("affected_component") or "")[:200]
        confidence = float(inc.get("confidence") or 0.0)
        thread_ids = [str(t) for t in (inc.get("thread_ids") or []) if t]
        raw = json.dumps(inc, ensure_ascii=False)

        if not inc_key or not thread_ids:
            continue

        # Compute thread time bounds from available timestamps
        ts_list = [
            thread_timestamps[tid]
            for tid in thread_ids
            if tid in thread_timestamps and thread_timestamps[tid]
        ]
        first_ts = min(ts_list) if ts_list else None
        last_ts = max(ts_list) if ts_list else None

        # Upsert incident
        cur.execute("""
            MERGE dbo.EmergentIncidents WITH (HOLDLOCK) AS t
            USING (SELECT ? AS IncidentKey) AS s ON t.IncidentKey = s.IncidentKey
            WHEN MATCHED THEN
                UPDATE SET
                    LastUpdatedUtc   = SYSUTCDATETIME(),
                    Confidence       = CASE WHEN ? > t.Confidence THEN ? ELSE t.Confidence END,
                    LastThreadSeenUtc = CASE WHEN ? > t.LastThreadSeenUtc THEN ? ELSE t.LastThreadSeenUtc END,
                    ThreadCount      = (SELECT COUNT(*) FROM dbo.EmergentIncidentThreads eit WHERE eit.IncidentID = t.IncidentID) + ?
            WHEN NOT MATCHED THEN
                INSERT (IncidentKey, Product, IncidentType, Title, Summary,
                        AffectedComponent, Status, Confidence,
                        FirstThreadSeenUtc, LastThreadSeenUtc,
                        FirstDetectedUtc, LastUpdatedUtc, ThreadCount,
                        ModelVersion, RawJson)
                VALUES (?, ?, ?, ?, ?,
                        ?, 'active', ?,
                        COALESCE(?, SYSUTCDATETIME()), COALESCE(?, SYSUTCDATETIME()),
                        SYSUTCDATETIME(), SYSUTCDATETIME(), ?,
                        'nano', ?);
        """,
            inc_key,
            # WHEN MATCHED UPDATE
            confidence, confidence,
            last_ts, last_ts,
            len(thread_ids),
            # WHEN NOT MATCHED INSERT
            inc_key, product, inc_type, title, summary,
            component, confidence,
            first_ts, last_ts,
            len(thread_ids), raw,
        )

        # Get IncidentID
        cur.execute("SELECT IncidentID FROM dbo.EmergentIncidents WHERE IncidentKey = ?", inc_key)
        row = cur.fetchone()
        if row:
            inc_id = int(row[0])
            key_to_id[inc_key] = inc_id

            # Link threads (ignore duplicates)
            for tid in thread_ids:
                ts = thread_timestamps.get(tid)
                try:
                    cur.execute("""
                        IF NOT EXISTS (
                            SELECT 1 FROM dbo.EmergentIncidentThreads
                            WHERE IncidentID = ? AND ThreadID = ?
                        )
                        INSERT INTO dbo.EmergentIncidentThreads
                            (IncidentID, ThreadID, ThreadCreatedAt, Confidence)
                        VALUES (?, ?, ?, ?)
                    """, inc_id, tid, inc_id, tid, ts, confidence)
                except Exception:
                    pass  # duplicate on race; safe to ignore

    return key_to_id

# ---------------------------------------------------------
# DB Persistence (bulk)
# ---------------------------------------------------------

def _executemany_in_chunks(cur: pyodbc.Cursor, sql: str, rows: List[tuple], chunk_size: int) -> None:
    if not rows:
        return
    for i0 in range(0, len(rows), chunk_size):
        chunk = rows[i0:i0 + chunk_size]
        cur.fast_executemany = True
        cur.executemany(sql, chunk)


def bulk_apply_emergent_judgements(
    cnx: pyodbc.Connection,
    judgements: List[Tuple[float, str, str]]
) -> None:
    """
    Bulk writes into thread_enrichment:
      emergent_signal = COALESCE(emergent_signal, confidence)
      emergent_signal_text = reason (truncated)
    judgements rows: (confidence, reason, thread_id)
    """
    if not judgements:
        return
    cur = cnx.cursor()
    _executemany_in_chunks(cur, """
        UPDATE dbo.thread_enrichment
        SET emergent_signal = CASE
                WHEN emergent_signal IS NULL THEN ?
                ELSE emergent_signal
            END,
            emergent_signal_text = ?
        WHERE thread_id = ?;
    """, judgements, chunk_size=_P2F_DB_EXECUTEMANY_BATCH)


# ---------------------------------------------------------
# Worker
# ---------------------------------------------------------

def process_emergent_batch(product: str, threads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    client = make_aoai_client()
    deployment = _get_aoai_deployment()
    trace_id = f"{product}:{_short_tid(threads[0].get('thread_id'))}" if threads else product

    try:
        out = _call_nano_detect_emergent(client, deployment, product, threads, trace_id=trace_id)
    except Exception as e:
        logging.error("[2F:%s] nano call failed: %s", trace_id, str(e)[:300])
        with sql_connect() as cnx:
            release_emergent_claim(cnx, [str(t.get("thread_id")) for t in threads])
            cnx.commit()
        return [{"status": "nano_error", "error": str(e)}]

    ok, msg, by_tid = _validate_detect_output(threads, out)

    if not ok:
        logging.warning("[2F:%s] validation failed: %s — releasing claims", trace_id, msg)
        with sql_connect() as cnx:
            release_emergent_claim(cnx, [str(t.get("thread_id")) for t in threads])
            cnx.commit()
        return [{"status": "nano_validation_failed", "error": msg, "nano_output": out}]

    results: List[Dict[str, Any]] = []
    to_write: List[Tuple[float, str, str]] = []

    for t in threads:
        tid = str(t.get("thread_id"))
        j = by_tid.get(tid) or {}
        label = str(j.get("label") or "")
        conf = float(j.get("confidence") or 0.0)
        reason = str(j.get("reason") or "")[:900]

        to_write.append((conf, reason, tid))

        results.append({
            "thread_id": tid,
            "status": "judged",
            "label": label,
            "confidence": conf,
        })

    # Build timestamp map for incident persistence
    thread_timestamps = {
        str(t.get("thread_id")): t.get("source_created_at")
        for t in threads
    }

    incidents = out.get("incidents") or []

    with sql_connect() as cnx:
        try:
            bulk_apply_emergent_judgements(cnx, to_write)
            _upsert_emergent_incidents(cnx, product, incidents, thread_timestamps)
            mark_emergent_completed(cnx, [str(t.get("thread_id")) for t in threads])
            cnx.commit()
            return results
        except Exception as e:
            cnx.rollback()
            release_emergent_claim(cnx, [str(t.get("thread_id")) for t in threads])
            cnx.commit()
            return [{"status": "db_error", "error": str(e)}]


# ---------------------------------------------------------
# HTTP Function Entry Point
# ---------------------------------------------------------

def run_phase2f_detect_emergent(req: func.HttpRequest) -> func.HttpResponse:
    """
    Phase 2F: detect/emergent triage pass (separate from Phase 2E common assignment).
    """
    try:
        batch_size = int(req.params.get("batch_size", "10"))
        max_batches = int(req.params.get("max_batches", "10"))
        max_workers = int(req.params.get("max_workers", "2"))
        force = (req.params.get("force", "0") == "1")

        stale_claim_minutes = int(req.params.get("stale_claim_minutes", "60"))
        reset_rows_override = req.params.get("reset_rows")
        reset_rows_override_i = int(reset_rows_override) if reset_rows_override not in (None, "") else None

        batch_size = max(1, min(batch_size, 25))
        max_batches = max(1, min(max_batches, 200))
        max_workers = max(1, min(max_workers, 12))

        include_completed = force

        with sql_connect() as cnx:
            cleared = clear_stale_emergent_claims(cnx, older_than_minutes=stale_claim_minutes)
            cnx.commit()
        logging.info("[2F] cleared_stale_claims=%d", cleared)

        reset_info = {"enabled": force, "reset_rows": 0, "affected": 0}
        if force:
            reset_rows = reset_rows_override_i if reset_rows_override_i is not None else (batch_size * max_batches)
            reset_rows = max(1, min(reset_rows, 20000))
            with sql_connect() as cnx:
                n = force_reset_threads_for_emergent(cnx, max_rows=reset_rows)
                cnx.commit()
            reset_info = {"enabled": True, "reset_rows": reset_rows, "affected": int(n)}
            logging.info("[2F] force_reset rows=%d window=%d", int(n), reset_rows)

        all_results: List[Dict[str, Any]] = []
        batches_submitted = 0
        futures = []
        claimed_total = 0

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            while batches_submitted < max_batches:
                while len([f for f in futures if not f.done()]) >= (max_workers * 2):
                    done = [f for f in futures if f.done()]
                    if not done:
                        time.sleep(0.05)
                        continue
                    for f in done:
                        futures.remove(f)
                        try:
                            all_results.extend(f.result())
                        except Exception as e:
                            all_results.append({"status": "worker_failed", "error": str(e)})

                with sql_connect() as cnx:
                    threads = claim_threads_for_emergent(cnx, batch_size=batch_size, include_completed=include_completed)
                    cnx.commit()

                if not threads:
                    logging.info("[2F] no_threads_claimed batches_submitted=%d", batches_submitted)
                    break

                batches_submitted += 1
                claimed_total += len(threads)

                tids = [str(t.get("thread_id")) for t in threads]
                by_prod: Dict[str, List[Dict[str, Any]]] = {}
                for t in threads:
                    prod = str(t.get("product") or "Other")
                    by_prod.setdefault(prod, []).append(t)

                logging.info(
                    "[2F] batch=%d claimed=%d products=%s tid_sample=%s..%s",
                    batches_submitted,
                    len(threads),
                    sorted(by_prod.keys()),
                    tids[0] if tids else "",
                    tids[-1] if tids else ""
                )

                for prod, group in by_prod.items():
                    futures.append(ex.submit(process_emergent_batch, prod, group))

            for f in as_completed(futures):
                try:
                    all_results.extend(f.result())
                except Exception as e:
                    all_results.append({"status": "worker_failed", "error": str(e)})

        summary_by_status: Dict[str, int] = {}
        for r in all_results:
            s = str(r.get("status") or "unknown")
            summary_by_status[s] = summary_by_status.get(s, 0) + 1

        return func.HttpResponse(
            json.dumps({
                "status": "ok",
                "mode": "emergent_only",
                "processed": len(all_results),
                "claimed_total": claimed_total,
                "batch_size": batch_size,
                "max_batches": max_batches,
                "max_workers": max_workers,
                "include_completed": include_completed,
                "force_reset": reset_info,
                "aoai_max_concurrency": _AOAI_MAX_CONCURRENCY,
                "db_batching": {"executemany_batch": _P2F_DB_EXECUTEMANY_BATCH},
                "summary_by_status": summary_by_status,
                "details": all_results
            }, default=str),
            mimetype="application/json"
        )

    except Exception as e:
        logging.exception("Phase 2F Fatal Error")
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )