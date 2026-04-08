# -*- coding: utf-8 -*-
"""phase1c_emergent_cluster

Phase 1C: EMERGENT catalog maintainer (incidents/regressions/outages).
Uses Nano model to propose/refine emergent catalog nodes across catalog slices.

This file uses centralized AOAI helpers for:
  - shared nano client
  - deployment resolution
  - rate-limiting + retry
  - shared SQL connection
"""

import os
import json
import logging
import hashlib
from typing import List, Dict, Any, Optional

import azure.functions as func
import pyodbc
from openai import AzureOpenAI

from aoai_helpers import (
    make_nano_client,
    get_nano_deployment,
    call_aoai_with_retry,
    estimate_tokens,
    sql_connect as _aoai_sql_connect,
)


_P1C_KEEP_IDS_MIN_FRACTION = float(os.getenv("P1C_KEEP_IDS_MIN_FRACTION", "0.85"))

_P1C_LOG_FULL_PROMPT = (os.getenv("P1C_LOG_FULL_PROMPT", "0") == "1")
_P1C_LOG_MAX_PROMPT_CHARS = int(os.getenv("P1C_LOG_MAX_PROMPT_CHARS", "500"))
_P1C_LOG_MAX_JSON_CHARS = int(os.getenv("P1C_LOG_MAX_JSON_CHARS", "2000"))


# ---------------------------------------------------------
# Small log helpers
# ---------------------------------------------------------

def _sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()[:12]


def _truncate(s: str, n: int) -> str:
    s = s or ""
    return s if len(s) <= n else (s[:n] + f"...(+{len(s) - n} chars)")


def _safe_json_preview(obj: Any, max_chars: int) -> str:
    try:
        s = json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        s = str(obj)
    return _truncate(s, max_chars)


def _log_nano_call(
    kind: str,
    slice_index: int,
    slices_total: int,
    threads: List[Dict[str, Any]],
    catalog_slice: List[Dict[str, Any]],
    system_prompt: str,
    user_payload: Dict[str, Any],
) -> None:
    tid_sample = [str(t.get("thread_id")) for t in threads if t.get("thread_id")][:10]
    prod_sample = sorted({str(t.get("product") or "").strip() for t in threads if str(t.get("product") or "").strip()})[:5]

    prompt_hash = _sha1(system_prompt)
    prompt_preview = system_prompt if _P1C_LOG_FULL_PROMPT else _truncate(system_prompt, _P1C_LOG_MAX_PROMPT_CHARS)

    logging.warning(
        "phase1c nano_%s request slice=%d/%d threads=%d thread_ids=%s thread_products=%s catalog_slice=%d prompt_sha1=%s prompt=%s user_payload_preview=%s",
        kind,
        slice_index,
        slices_total,
        len(threads),
        tid_sample,
        prod_sample,
        len(catalog_slice),
        prompt_hash,
        prompt_preview,
        _safe_json_preview(
            {
                "slice_index": user_payload.get("slice_index"),
                "slices_total": user_payload.get("slices_total"),
                "threads_count": len(user_payload.get("threads") or []),
                "existing_catalog_slice_count": len(user_payload.get("existing_catalog_slice") or []),
                "prior_candidates_count": len(user_payload.get("prior_candidates") or []),
            },
            300,
        ),
    )


def _log_nano_response(kind: str, resp_text: str, parsed: Dict[str, Any]) -> None:
    notes = parsed.get("notes")

    if kind == "propose":
        cands = parsed.get("candidates") or []
        sample = cands[:5] if isinstance(cands, list) else []
        logging.warning(
            "phase1c nano_%s response raw_len=%d notes=%s candidates=%d sample=%s raw_preview=%s",
            kind,
            len(resp_text or ""),
            _truncate(str(notes or ""), 300),
            len(cands) if isinstance(cands, list) else -1,
            _safe_json_preview(sample, 900),
            _truncate(resp_text or "", _P1C_LOG_MAX_JSON_CHARS),
        )
        return

    if kind == "refine":
        keep_ids = parsed.get("keep_candidate_ids") or []
        drops = parsed.get("drop") or []
        adds = parsed.get("add") or []
        logging.warning(
            "phase1c nano_%s response raw_len=%d notes=%s keep_ids=%d drop=%d add=%d keep_sample=%s drop_sample=%s add_sample=%s raw_preview=%s",
            kind,
            len(resp_text or ""),
            _truncate(str(notes or ""), 300),
            len(keep_ids) if isinstance(keep_ids, list) else -1,
            len(drops) if isinstance(drops, list) else -1,
            len(adds) if isinstance(adds, list) else -1,
            _safe_json_preview(keep_ids[:10] if isinstance(keep_ids, list) else [], 700),
            _safe_json_preview(drops[:5] if isinstance(drops, list) else [], 900),
            _safe_json_preview(adds[:3] if isinstance(adds, list) else [], 900),
            _truncate(resp_text or "", _P1C_LOG_MAX_JSON_CHARS),
        )
        return

    logging.warning(
        "phase1c nano_%s response raw_len=%d raw_preview=%s",
        kind,
        len(resp_text or ""),
        _truncate(resp_text or "", _P1C_LOG_MAX_JSON_CHARS),
    )


# ---------------------------------------------------------
# Shared Clients
# ---------------------------------------------------------

def sql_connect() -> pyodbc.Connection:
    return _aoai_sql_connect(autocommit=False)


def make_aoai_client() -> AzureOpenAI:
    """Backwards-compatible alias – returns the shared nano client."""
    return make_nano_client()


# ---------------------------------------------------------
# SQL
# ---------------------------------------------------------

def fetch_emergent_threads(cnx: pyodbc.Connection, limit: int, force: bool, batch_id: Optional[str] = None) -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    batch_filter = "AND batch_id = ?" if batch_id else ""
    batch_args = [batch_id] if batch_id else []
    cur.execute(
        f"""
        SELECT TOP (?)
            thread_id,
            product,
            topic_cluster_key,
            scenario_cluster_key,
            variant_cluster_key,
            signature_text,
            resolution_signature_text,
            emergent_signal,
            ingested_at
        FROM dbo.thread_enrichment
        WHERE (classification = 'emergent_issue' OR is_emergent = 1)
          AND emergent_signal >= 0.5
          AND product IS NOT NULL AND LTRIM(RTRIM(product)) <> ''
          AND signature_text IS NOT NULL AND LTRIM(RTRIM(signature_text)) <> ''
          AND (? = 1 OR CatalogCheckedUtc IS NULL)
          {batch_filter}
        ORDER BY ingested_at DESC, thread_id DESC
        """,
        int(limit),
        1 if force else 0,
        *batch_args,
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def mark_catalog_checked(cnx: pyodbc.Connection, thread_ids: List[str]) -> None:
    if not thread_ids:
        return
    cur = cnx.cursor()
    placeholders = ",".join("?" for _ in thread_ids)
    cur.execute(
        f"""
        UPDATE dbo.thread_enrichment
        SET CatalogCheckedUtc = SYSUTCDATETIME()
        WHERE thread_id IN ({placeholders})
        """,
        *[str(t) for t in thread_ids],
    )


def fetch_emergent_catalog(cnx: pyodbc.Connection, max_rows: int = 8000) -> List[Dict[str, Any]]:
    """Read existing emergent taxonomy (Product + TopicKey) from dedicated table."""
    cur = cnx.cursor()
    cur.execute(
        """
        SELECT TOP (?)
            TaxonomyID,
            Product,
            TopicKey,
            TopicSignature,
            ThreadCount
        FROM dbo.emergent_taxonomy
        WHERE IsActive = 1
        ORDER BY Product, ThreadCount DESC, TopicKey
        """,
        int(max_rows),
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _upsert_taxonomy_topics(
    cnx: pyodbc.Connection,
    candidates: Dict[str, Dict[str, Any]],
) -> int:
    """
    Write finalized topic candidates into dbo.emergent_taxonomy.
    Returns number of rows upserted.
    """
    if not candidates:
        return 0
    cur = cnx.cursor()
    upserted = 0
    for c in candidates.values():
        prod = str(c.get("product") or "").strip()
        topic_key = str(c.get("topic_key") or "").strip()
        sig = str(c.get("signature_text") or "").strip()
        if not prod or not topic_key:
            continue
        cur.execute(
            """
            MERGE dbo.emergent_taxonomy AS t
            USING (SELECT ? AS Product, ? AS TopicKey) AS s
              ON t.Product = s.Product AND t.TopicKey = s.TopicKey
            WHEN MATCHED THEN
                UPDATE SET
                    TopicSignature = CASE WHEN LEN(?) > LEN(ISNULL(t.TopicSignature, '')) THEN ? ELSE t.TopicSignature END,
                    LastUpdatedUtc = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (Product, TopicKey, TopicSignature, ThreadCount, IsActive)
                VALUES (?, ?, ?, 0, 1);
            """,
            prod, topic_key,
            sig, sig,
            prod, topic_key, sig,
        )
        upserted += 1
    return upserted


# ---------------------------------------------------------
# Candidate helpers
# ---------------------------------------------------------

def _catalog_window(catalog_rows: List[Dict[str, Any]], start: int, size: int) -> List[Dict[str, Any]]:
    if not catalog_rows or size <= 0:
        return []
    n = len(catalog_rows)
    start = start % n
    end = start + min(size, n)
    if end <= n:
        return catalog_rows[start:end]
    return catalog_rows[start:] + catalog_rows[: (end - n)]


def _should_apply_keep_list(keep_ids: set, prior_count: int) -> bool:
    if prior_count <= 0 or not keep_ids:
        return False
    return len(keep_ids) >= int(prior_count * _P1C_KEEP_IDS_MIN_FRACTION)


def _build_slim_catalog_slice(catalog_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build slim taxonomy slice for the AI prompt — Product + TopicKey only."""
    slim: List[Dict[str, Any]] = []
    for r in catalog_rows:
        slim.append(
            {
                "product": r.get("Product") or r.get("product"),
                "topic_key": r.get("TopicKey") or r.get("cluster_key"),
                "sig": (r.get("TopicSignature") or r.get("cluster_signature_text") or "")[:200],
            }
        )
    return slim


# ---------------------------------------------------------
# Nano calls (rate-limit + retry via aoai_helpers)
# ---------------------------------------------------------

def call_nano_emergent_propose(
    client: AzureOpenAI,
    deployment: str,
    threads: List[Dict[str, Any]],
    catalog_slice: List[Dict[str, Any]],
    slice_index: int,
    slices_total: int,
) -> Dict[str, Any]:
    system_prompt = (
        "You maintain a 2-level EMERGENT TAXONOMY for grouping platform incidents by product and topic.\n\n"

        "EMERGENT ISSUE DEFINITION:\n"
        "Only include issues that are platform-side (Azure service behavior), affect multiple customers,\n"
        "and are NOT explained by customer misconfiguration, known limitations, or documented breaking changes.\n\n"

        "TAXONOMY LEVELS:\n"
        "  Product = the Azure service (provided per thread, do NOT change it).\n"
        "  Topic   = the broad technical area/component within that product where incidents occur.\n"
        "            Examples: 'custom-domain', 'networking', 'deployment', 'authentication', 'gateway'.\n"
        "            A topic groups many specific incidents; it is NOT an incident itself.\n\n"

        "TOPIC KEY RULES:\n"
        "  - kebab-case, 2-4 tokens, broad and reusable.\n"
        "  - Do NOT include the product name in the topic key.\n"
        "  - Do NOT include incident-specific details (error codes, timestamps, operation names).\n"
        "  - A topic should be broad enough to contain 3-10 distinct incident types.\n\n"

        "YOU WILL BE SHOWN THE EXISTING TAXONOMY IN MULTIPLE SLICES.\n"
        "Do not assume the slice contains everything.\n"
        f"This is slice {slice_index} of {slices_total}.\n\n"

        "PROPOSE RULES:\n"
        "  - Prefer reusing existing topic_keys from the provided taxonomy slice.\n"
        "  - Only propose a NEW topic if no existing topic covers the thread's component area.\n"
        "  - Each thread should map to exactly one topic.\n"
        "  - Reject threads where emergent_signal < 0.5 or symptoms indicate customer error.\n\n"

        "OUTPUT JSON ONLY: { \"candidates\": [...], \"notes\": \"...\" }\n"
        "Candidate schema:\n"
        "{\n"
        "  \"product\": \"string\",\n"
        "  \"topic_key\": \"string\",\n"
        "  \"signature_text\": \"short description of what this topic covers\"\n"
        "}\n"
    )

    def _jsonable_number(x: Any) -> Optional[float]:
        if x is None:
            return None
        try:
            return float(x)
        except Exception:
            return None

    slim_threads = [
        {
            "thread_id": str(t.get("thread_id") or ""),
            "product": (t.get("product") or ""),
            "emergent_signal": _jsonable_number(t.get("emergent_signal")),
            "signature_text": (t.get("signature_text") or "")[:700],
            "resolution_signature_text": (t.get("resolution_signature_text") or "")[:700],
            "hints": {
                "topic_cluster_key": t.get("topic_cluster_key"),
                "scenario_cluster_key": t.get("scenario_cluster_key"),
                "variant_cluster_key": t.get("variant_cluster_key"),
            },
        }
        for t in threads
    ]

    user_payload = {
        "slice_index": slice_index,
        "slices_total": slices_total,
        "threads": slim_threads,
        "existing_catalog_slice": catalog_slice,
    }

    _log_nano_call(
        kind="propose",
        slice_index=slice_index,
        slices_total=slices_total,
        threads=threads,
        catalog_slice=catalog_slice,
        system_prompt=system_prompt,
        user_payload=user_payload,
    )

    user_content = json.dumps(user_payload, ensure_ascii=False)
    resp = call_aoai_with_retry(
        client,
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        response_format={"type": "json_object"},
        estimated_prompt_tokens=estimate_tokens(system_prompt) + estimate_tokens(user_content),
        caller_tag="phase1c_propose",
    )

    resp_text = resp.choices[0].message.content or ""
    parsed = json.loads(resp_text)

    _log_nano_response("propose", resp_text, parsed)
    return parsed


def call_nano_emergent_refine(
    client: AzureOpenAI,
    deployment: str,
    threads: List[Dict[str, Any]],
    catalog_slice: List[Dict[str, Any]],
    prior_candidates: List[Dict[str, Any]],
    slice_index: int,
    slices_total: int,
) -> Dict[str, Any]:
    system_prompt = (
        "You are maintaining an EMERGENT-ISSUE catalog (incidents/regressions/outages).\n"
        "You are shown the existing EMERGENT catalog in MULTIPLE SLICES across calls.\n\n"
        f"This is slice {slice_index} of {slices_total}.\n\n"
        "Task for THIS call:\n"
        "1) Review the catalog slice.\n"
        "2) Review the current candidate pool.\n"
        "3) Drop any candidate that is not needed because an existing node in this slice already covers it.\n"
        "4) Add new candidates if this slice reveals gaps.\n\n"
        "Drop rules (STRICT):\n"
        "- If an existing node in this slice is semantically suitable, DROP the candidate.\n"
        "- If two candidates are near-duplicates, keep the better/general one.\n\n"
        "Output JSON only:\n"
        "{\n"
        "  \"keep_candidate_ids\": [\"...\"],\n"
        "  \"drop\": [{\"candidate_id\":\"...\",\"reason\":\"...\"}],\n"
        "  \"add\": [ {candidate...} ],\n"
        "  \"notes\": \"...\"\n"
        "}\n"
    )

    def _jsonable_number(x: Any) -> Optional[float]:
        if x is None:
            return None
        try:
            return float(x)
        except Exception:
            return None

    slim_threads = [
        {
            "thread_id": str(t.get("thread_id") or ""),
            "product": (t.get("product") or ""),
            "emergent_signal": _jsonable_number(t.get("emergent_signal")),
            "signature_text": (t.get("signature_text") or "")[:700],
            "resolution_signature_text": (t.get("resolution_signature_text") or "")[:700],
        }
        for t in threads
    ]

    user_payload = {
        "slice_index": slice_index,
        "slices_total": slices_total,
        "threads": slim_threads,
        "existing_catalog_slice": catalog_slice,
        "prior_candidates": prior_candidates,
    }

    _log_nano_call(
        kind="refine",
        slice_index=slice_index,
        slices_total=slices_total,
        threads=threads,
        catalog_slice=catalog_slice,
        system_prompt=system_prompt,
        user_payload=user_payload,
    )

    user_content = json.dumps(user_payload, ensure_ascii=False)
    resp = call_aoai_with_retry(
        client,
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        response_format={"type": "json_object"},
        estimated_prompt_tokens=estimate_tokens(system_prompt) + estimate_tokens(user_content),
        caller_tag="phase1c_refine",
    )

    resp_text = resp.choices[0].message.content or ""
    parsed = json.loads(resp_text)

    _log_nano_response("refine", resp_text, parsed)
    return parsed


# ---------------------------------------------------------
# Function Entry Point (required by function_app import)
# ---------------------------------------------------------

def run_phase1c_emergent_cluster(req: func.HttpRequest) -> func.HttpResponse:
    """Route handler for /api/phase1c_emergent_cluster."""
    try:
        limit = int(req.params.get("limit", "20"))
        batch_size = int(req.params.get("batch_size", "10"))
        force = req.params.get("force", "0") == "1"
        batch_id: Optional[str] = (req.params.get("batch_id") or "").strip() or None

        slice_limit = int(req.params.get("slice_limit", "800"))
        max_slices = int(req.params.get("max_slices", "4"))

        limit = max(1, min(limit, 500))
        batch_size = max(1, min(batch_size, 25))
        slice_limit = max(50, min(slice_limit, 2500))
        max_slices = max(1, min(max_slices, 8))

        client = make_aoai_client()
        deployment = get_nano_deployment()

        processed = 0
        nano_calls = 0

        with sql_connect() as cnx:
            catalog_rows = fetch_emergent_catalog(cnx, max_rows=8000)
            rows = fetch_emergent_threads(cnx, limit=limit, force=force, batch_id=batch_id)

            if not rows:
                return func.HttpResponse(
                    json.dumps({"status": "ok", "processed": 0, "nano_calls": 0, "details": []}, ensure_ascii=False),
                    mimetype="application/json",
                )

            # Build slices over emergent catalog (if any)
            effective_slices = 1 if len(catalog_rows) <= slice_limit else max_slices

            # Track all topics generated across batches so subsequent batches
            # see prior results and avoid creating near-duplicate topics.
            cross_batch_topics: List[Dict[str, Any]] = []

            # Work in batches of threads
            details: List[Dict[str, Any]] = []
            for i0 in range(0, len(rows), batch_size):
                batch = rows[i0 : i0 + batch_size]
                if not batch:
                    continue

                candidates: Dict[str, Dict[str, Any]] = {}
                prev_keys: Optional[set] = None
                catalog_slice_start = 0

                for slice_index in range(1, effective_slices + 1):
                    window = _catalog_window(catalog_rows, start=catalog_slice_start, size=slice_limit)
                    slim_slice = _build_slim_catalog_slice(window)
                    # Include topics from prior batches so Nano can reuse them
                    if cross_batch_topics:
                        slim_slice = cross_batch_topics + slim_slice

                    if slice_index > 1 and len(catalog_rows) > slice_limit:
                        catalog_slice_start = (catalog_slice_start + slice_limit) % max(1, len(catalog_rows))

                    if slice_index == 1:
                        out = call_nano_emergent_propose(
                            client,
                            deployment,
                            threads=batch,
                            catalog_slice=slim_slice,
                            slice_index=slice_index,
                            slices_total=effective_slices,
                        )
                        nano_calls += 1
                        proposed = out.get("candidates") or []
                        # Keep for summary only
                        candidates = {str(i): (c if isinstance(c, dict) else {}) for i, c in enumerate(proposed)}
                        prev_keys = set(candidates.keys())
                    else:
                        out = call_nano_emergent_refine(
                            client,
                            deployment,
                            threads=batch,
                            catalog_slice=slim_slice,
                            prior_candidates=list(candidates.values()),
                            slice_index=slice_index,
                            slices_total=effective_slices,
                        )
                        nano_calls += 1
                        keep_ids = set(str(x) for x in (out.get("keep_candidate_ids") or []) if x)
                        if keep_ids and _should_apply_keep_list(keep_ids, prior_count=len(candidates)):
                            candidates = {k: v for k, v in candidates.items() if k in keep_ids}

                        cur_keys = set(candidates.keys())
                        if prev_keys is not None and cur_keys == prev_keys:
                            break
                        prev_keys = cur_keys

                # Write finalized topic candidates to emergent_taxonomy
                upserted = _upsert_taxonomy_topics(cnx, candidates)
                if upserted > 0:
                    logging.warning(
                        "phase1c upserted %d taxonomy topics from batch %d",
                        upserted, i0 // batch_size + 1,
                    )
                    # Collect finalized topics for cross-batch dedup
                    for c in candidates.values():
                        if isinstance(c, dict) and c.get("topic_key"):
                            cross_batch_topics.append({
                                "product": c.get("product") or "",
                                "topic_key": c.get("topic_key") or "",
                                "sig": (c.get("signature_text") or "")[:200],
                            })

                processed += len(batch)
                details.append({
                    "batch_threads": [str(t.get("thread_id")) for t in batch],
                    "candidate_count": len(candidates),
                    "topics_upserted": upserted,
                })

            mark_catalog_checked(cnx, [str(r.get("thread_id")) for r in rows if r.get("thread_id")])
            cnx.commit()

        return func.HttpResponse(
            json.dumps(
                {
                    "status": "ok",
                    "processed": processed,
                    "nano_calls": nano_calls,
                    "slice_limit": slice_limit,
                    "max_slices": max_slices,
                    "details": details,
                },
                ensure_ascii=False,
            ),
            mimetype="application/json",
        )

    except Exception as e:
        logging.exception("phase1c failed")
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}, ensure_ascii=False),
            status_code=500,
            mimetype="application/json",
        )