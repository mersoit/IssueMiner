# -*- coding: utf-8 -*-
import os
import json
import logging
import time
import threading
import hashlib
import pyodbc
from typing import List, Dict, Any, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

import azure.functions as func
from openai import AzureOpenAI

from aoai_helpers import (
    make_nano_client,
    get_nano_deployment,
    call_aoai_with_retry,
    estimate_tokens,
    get_rate_limiter,
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

# Fan-out guardrails: staged assignment can explode into many calls
_P2E_MAX_TOPIC_GROUPS = int(os.getenv("P2E_MAX_TOPIC_GROUPS", "4"))
_P2E_MAX_SCENARIO_GROUPS = int(os.getenv("P2E_MAX_SCENARIO_GROUPS", "6"))

# Default stage payload caps (per-call).
_P2E_MAX_TOPICS = int(os.getenv("P2E_MAX_TOPICS", "200"))
_P2E_MAX_SCENARIOS = int(os.getenv("P2E_MAX_SCENARIOS", "250"))
_P2E_MAX_VARIANTS = int(os.getenv("P2E_MAX_VARIANTS", "250"))
_P2E_MAX_LEAVES = int(os.getenv("P2E_MAX_LEAVES", "350"))

# DB batching knobs
_P2E_DB_EXECUTEMANY_BATCH = int(os.getenv("P2E_DB_EXECUTEMANY_BATCH", "500"))

# Orphan auto-create (new in this revision)
_P2E_ORPHAN_AUTOCREATE_ENABLED = (os.getenv("P2E_ORPHAN_AUTOCREATE_ENABLED", "1") == "1")
_P2E_ORPHAN_USEFULNESS_MIN = float(os.getenv("P2E_ORPHAN_USEFULNESS_MIN", "0.4"))
_P2E_ORPHAN_MAX_THREADS_PER_BATCH = int(os.getenv("P2E_ORPHAN_MAX_THREADS_PER_BATCH", "12"))
_P2E_ORPHAN_CREATED_BY_PHASE = os.getenv("P2E_ORPHAN_CREATED_BY_PHASE", "phase2e_orphan_autocreate")
_P2E_ORPHAN_INSERT_BATCH = int(os.getenv("P2E_ORPHAN_INSERT_BATCH", "200"))


# ---------------------------------------------------------
# SQL: Catalog + Work Claiming
# ---------------------------------------------------------

def fetch_catalog(cnx: pyodbc.Connection, max_rows: int = 15000) -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    cur.execute(
        """
        SELECT TOP (?)
            cluster_id,
            cluster_level,
            product,
            cluster_key,
            parent_cluster_id,
            cluster_signature_text,
            resolution_signature_text
        FROM dbo.issue_cluster
        WHERE is_active = 1
          AND cluster_level IN (1,2,3,4)
        ORDER BY product, cluster_level, cluster_id
        """,
        int(max_rows),
    )

    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def force_reset_threads_for_assignment(
    cnx: pyodbc.Connection,
    max_rows: int,
    mode: str = "all"
) -> int:
    """
    mode:
      - all: reset ALL eligible common threads (bounded by max_rows)
      - recent: reset most-recent slice (legacy behavior)
    Phase 2E is COMMON-ONLY: excludes classification='emergent_issue'.
    """
    cur = cnx.cursor()

    if mode == "recent":
        cur.execute(
            """
            ;WITH cte AS (
                SELECT TOP (?)
                    thread_id
                FROM dbo.thread_enrichment WITH (ROWLOCK, READPAST, UPDLOCK)
                WHERE CatalogCheckedUtc IS NOT NULL
                  AND product IS NOT NULL AND LTRIM(RTRIM(product)) <> '' AND product <> 'Other'
                  AND signature_text IS NOT NULL AND LTRIM(RTRIM(signature_text)) <> ''
                  AND (classification IS NULL OR classification NOT IN ('emergent_issue', 'not_usable', 'learn_microsoft'))
                  AND ISNULL(solution_usefulness, 0.0) >= 0.3
                ORDER BY CatalogCheckedUtc DESC, ingested_at DESC
            )
            UPDATE te
            SET AssignmentStartedUtc = NULL,
                AssignmentCompletedUtc = NULL
            FROM dbo.thread_enrichment te
            JOIN cte ON cte.thread_id = te.thread_id;
            """,
            int(max_rows),
        )
    else:
        # "all" (bounded): include both completed + uncompleted. Ensures force=1 truly re-runs.
        # Order is stable-ish but not important; we just want to sweep.
        cur.execute(
            """
            ;WITH cte AS (
                SELECT TOP (?)
                    thread_id
                FROM dbo.thread_enrichment WITH (ROWLOCK, READPAST, UPDLOCK)
                WHERE CatalogCheckedUtc IS NOT NULL
                  AND product IS NOT NULL AND LTRIM(RTRIM(product)) <> '' AND product <> 'Other'
                  AND signature_text IS NOT NULL AND LTRIM(RTRIM(signature_text)) <> ''
                  AND (classification IS NULL OR classification NOT IN ('emergent_issue', 'not_usable', 'learn_microsoft'))
                  AND ISNULL(solution_usefulness, 0.0) >= 0.3
                ORDER BY thread_id DESC
            )
            UPDATE te
            SET AssignmentStartedUtc = NULL,
                AssignmentCompletedUtc = NULL
            FROM dbo.thread_enrichment te
            JOIN cte ON cte.thread_id = te.thread_id;
            """,
            int(max_rows),
        )

    return int(cur.rowcount if cur.rowcount is not None else 0)


def fetch_next_product_for_assignment(
    cnx: pyodbc.Connection,
    include_completed: bool,
    exclude_products: Set[str],
) -> Optional[str]:
    cur = cnx.cursor()

    if exclude_products:
        placeholders = ",".join("?" for _ in exclude_products)
        cur.execute(
            f"""
            SELECT TOP (1) product
            FROM dbo.thread_enrichment
            WHERE CatalogCheckedUtc IS NOT NULL
              AND product IS NOT NULL AND LTRIM(RTRIM(product)) <> '' AND product <> 'Other'
              AND signature_text IS NOT NULL AND LTRIM(RTRIM(signature_text)) <> ''
              AND (classification IS NULL OR classification NOT IN ('emergent_issue', 'not_usable', 'learn_microsoft'))
              AND ISNULL(solution_usefulness, 0.0) >= 0.3
              AND (? = 1 OR AssignmentCompletedUtc IS NULL)
              AND AssignmentStartedUtc IS NULL
              AND product NOT IN ({placeholders})
            ORDER BY CatalogCheckedUtc DESC, ingested_at DESC, thread_id DESC
            """,
            1 if include_completed else 0,
            *[p for p in exclude_products],
        )
    else:
        cur.execute(
            """
            SELECT TOP (1) product
            FROM dbo.thread_enrichment
            WHERE CatalogCheckedUtc IS NOT NULL
              AND product IS NOT NULL AND LTRIM(RTRIM(product)) <> '' AND product <> 'Other'
              AND signature_text IS NOT NULL AND LTRIM(RTRIM(signature_text)) <> ''
              AND (classification IS NULL OR classification NOT IN ('emergent_issue', 'not_usable', 'learn_microsoft'))
              AND ISNULL(solution_usefulness, 0.0) >= 0.3
              AND (? = 1 OR AssignmentCompletedUtc IS NULL)
              AND AssignmentStartedUtc IS NULL
            ORDER BY CatalogCheckedUtc DESC, ingested_at DESC, thread_id DESC
            """,
            1 if include_completed else 0,
        )

    row = cur.fetchone()
    if not row:
        return None
    return str(row[0] or "").strip() or None


def claim_threads_for_assignment_product(
    cnx: pyodbc.Connection,
    batch_size: int,
    include_completed: bool,
    product: str,
) -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    cur.execute(
        """
        ;WITH cte AS (
            SELECT TOP (?)
                thread_id
            FROM dbo.thread_enrichment WITH (ROWLOCK, READPAST, UPDLOCK)
            WHERE CatalogCheckedUtc IS NOT NULL
              AND product = ?
              AND signature_text IS NOT NULL AND LTRIM(RTRIM(signature_text)) <> ''
              AND (classification IS NULL OR classification NOT IN ('emergent_issue', 'not_usable', 'learn_microsoft'))
              AND ISNULL(solution_usefulness, 0.0) >= 0.3
              AND (? = 1 OR AssignmentCompletedUtc IS NULL)
              AND AssignmentStartedUtc IS NULL
            ORDER BY CatalogCheckedUtc DESC, ingested_at DESC, thread_id DESC
        )
        UPDATE te
        SET AssignmentStartedUtc = SYSUTCDATETIME()
        OUTPUT INSERTED.thread_id,
               INSERTED.product,
               INSERTED.signature_text,
               INSERTED.resolution_signature_text,
               INSERTED.solution_usefulness,
               INSERTED.TopicClusterID,
               INSERTED.ScenarioClusterID,
               INSERTED.VariantClusterID,
               INSERTED.ResolutionLeafClusterID,
               INSERTED.CatalogCheckedUtc
        FROM dbo.thread_enrichment te
        JOIN cte ON cte.thread_id = te.thread_id;
        """,
        int(batch_size),
        str(product),
        1 if include_completed else 0,
    )

    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def mark_assignment_completed(cnx: pyodbc.Connection, thread_ids: List[str]) -> None:
    if not thread_ids:
        return
    cur = cnx.cursor()
    placeholders = ",".join("?" for _ in thread_ids)
    cur.execute(
        f"""
        UPDATE dbo.thread_enrichment
        SET AssignmentCompletedUtc = SYSUTCDATETIME()
        WHERE thread_id IN ({placeholders})
        """,
        *[str(t) for t in thread_ids],
    )


def release_assignment_claim(cnx: pyodbc.Connection, thread_ids: List[str]) -> None:
    if not thread_ids:
        return
    cur = cnx.cursor()
    placeholders = ",".join("?" for _ in thread_ids)
    cur.execute(
        f"""
        UPDATE dbo.thread_enrichment
        SET AssignmentStartedUtc = NULL
        WHERE thread_id IN ({placeholders})
          AND AssignmentCompletedUtc IS NULL
        """,
        *[str(t) for t in thread_ids],
    )


# ---------------------------------------------------------
# Catalog Projection (for Nano)
# ---------------------------------------------------------

def _build_parent_key_index(catalog_rows: List[Dict[str, Any]]) -> Dict[int, str]:
    by_id: Dict[int, Dict[str, Any]] = {}
    for r in catalog_rows:
        cid = r.get("cluster_id")
        if cid is None:
            continue
        by_id[int(cid)] = r

    parent_key_by_id: Dict[int, str] = {}
    for cid, r in by_id.items():
        pid = r.get("parent_cluster_id")
        if pid is None:
            continue
        parent = by_id.get(int(pid))
        if parent:
            parent_key_by_id[cid] = str(parent.get("cluster_key") or "")
    return parent_key_by_id


def build_catalog_payload_by_product(catalog_rows: List[Dict[str, Any]], max_per_level: int) -> Dict[str, Dict[str, Any]]:
    parent_key_by_id = _build_parent_key_index(catalog_rows)

    out: Dict[str, Dict[str, Any]] = {}
    for r in catalog_rows:
        prod = str(r.get("product") or "Other")
        lvl = int(r.get("cluster_level") or 0)
        if lvl not in (1, 2, 3, 4):
            continue

        key = str(r.get("cluster_key") or "")
        parent_key = parent_key_by_id.get(int(r["cluster_id"])) if r.get("cluster_id") is not None else None

        node = {
            "key": key,
            "parent_key": parent_key,
            "sig": (str(r.get("cluster_signature_text") or ""))[:160],
            "res_sig": (str(r.get("resolution_signature_text") or ""))[:160],
        }

        if prod not in out:
            out[prod] = {
                "topics": [],
                "scenarios": [],
                "variants": [],
                "leaves": [],
                "scenario_by_topic": {},
                "variant_by_scenario": {},
                "leaf_by_variant": {},
            }

        if lvl == 1:
            out[prod]["topics"].append(node)
        elif lvl == 2:
            out[prod]["scenarios"].append(node)
            if parent_key:
                out[prod]["scenario_by_topic"].setdefault(parent_key, []).append(key)
        elif lvl == 3:
            out[prod]["variants"].append(node)
            if parent_key:
                out[prod]["variant_by_scenario"].setdefault(parent_key, []).append(key)
        elif lvl == 4:
            out[prod]["leaves"].append(node)
            if parent_key:
                out[prod]["leaf_by_variant"].setdefault(parent_key, []).append(key)

    for prod in list(out.keys()):
        out[prod]["topics"] = out[prod]["topics"][:max_per_level]
        out[prod]["scenarios"] = out[prod]["scenarios"][:max_per_level]
        out[prod]["variants"] = out[prod]["variants"][:max_per_level]
        out[prod]["leaves"] = out[prod]["leaves"][:max_per_level]

    return out


# ---------------------------------------------------------
# Assignment validation / DB apply
# ---------------------------------------------------------

def build_id_indexes(
    catalog_rows: List[Dict[str, Any]]
) -> Tuple[Dict[Tuple[str, int, str, Optional[int]], int], Dict[int, Optional[int]]]:
    id_by_key: Dict[Tuple[str, int, str, Optional[int]], int] = {}
    parent_by_id: Dict[int, Optional[int]] = {}

    for r in catalog_rows:
        cid = r.get("cluster_id")
        if cid is None:
            continue
        cid_i = int(cid)

        prod = str(r.get("product") or "Other")
        lvl = int(r.get("cluster_level") or 0)
        key = str(r.get("cluster_key") or "")
        pid = r.get("parent_cluster_id")
        pid_i = int(pid) if pid is not None else None

        id_by_key[(prod, lvl, key, pid_i)] = cid_i
        parent_by_id[cid_i] = pid_i

    return id_by_key, parent_by_id


def _executemany_in_chunks(cur: pyodbc.Cursor, sql: str, rows: List[tuple], chunk_size: int) -> None:
    if not rows:
        return
    for i0 in range(0, len(rows), chunk_size):
        chunk = rows[i0:i0 + chunk_size]
        cur.fast_executemany = True
        cur.executemany(sql, chunk)


def bulk_upsert_memberships(cnx: pyodbc.Connection, membership_rows: List[Tuple[str, int]]) -> None:
    if not membership_rows:
        return

    values_sql = ",".join("(?,?)" for _ in membership_rows)
    params: List[Any] = []
    for tid, leaf_id in membership_rows:
        params.extend([str(tid), int(leaf_id)])

    cur = cnx.cursor()
    cur.execute(
        f"""
        SET NOCOUNT ON;

        DECLARE @in TABLE (
            thread_id NVARCHAR(128) NOT NULL,
            leaf_id BIGINT NOT NULL
        );

        INSERT INTO @in (thread_id, leaf_id)
        VALUES {values_sql};

        UPDATE m
        SET m.resolution_leaf_cluster_id = i.leaf_id,
            m.assigned_at = SYSUTCDATETIME()
        FROM dbo.thread_cluster_membership m
        JOIN @in i ON i.thread_id = m.thread_id;

        INSERT INTO dbo.thread_cluster_membership (thread_id, resolution_leaf_cluster_id, assigned_at)
        SELECT i.thread_id, i.leaf_id, SYSUTCDATETIME()
        FROM @in i
        LEFT JOIN dbo.thread_cluster_membership m ON m.thread_id = i.thread_id
        WHERE m.thread_id IS NULL;
        """,
        *params,
    )


def bulk_update_thread_cluster_ids(
    cnx: pyodbc.Connection,
    update_rows: List[Tuple[int, Optional[int], Optional[int], Optional[int], str]],
) -> None:
    if not update_rows:
        return

    cur = cnx.cursor()
    _executemany_in_chunks(
        cur,
        """
        UPDATE dbo.thread_enrichment
        SET TopicClusterID = ?,
            ScenarioClusterID = ?,
            VariantClusterID = ?,
            ResolutionLeafClusterID = ?,
            ClusteredUtc = SYSUTCDATETIME()
        WHERE thread_id = ?;
        """,
        update_rows,
        chunk_size=_P2E_DB_EXECUTEMANY_BATCH,
    )


def validate_and_resolve_assignment(
    id_by_key: Dict[Tuple[str, int, str, Optional[int]], int],
    assignment: Dict[str, Any],
    product: str,
) -> Tuple[Dict[str, Any], Tuple[int, Optional[int], Optional[int], Optional[int]]]:
    tid = str(assignment.get("thread_id") or "")
    t_key = str(assignment.get("topic_key") or "").strip()
    s_key = str(assignment.get("scenario_key") or "").strip()
    v_key = str(assignment.get("variant_key") or "").strip()
    l_key = str(assignment.get("leaf_key") or "").strip()

    if not tid or not t_key:
        return ({"thread_id": tid, "status": "bad_assignment"}, (0, None, None, None))

    if s_key == "NO_MATCH" or v_key == "NO_MATCH":
        return ({"thread_id": tid, "status": "bad_assignment", "reason": "NO_MATCH_only_allowed_for_leaf"}, (0, None, None, None))

    topic_id = id_by_key.get((product, 1, t_key, None))
    if not topic_id:
        return ({"thread_id": tid, "status": "missing_topic", "topic_key": t_key}, (0, None, None, None))

    if not s_key:
        return ({"thread_id": tid, "status": "assigned_topic_only", "topic_id": topic_id}, (topic_id, None, None, None))

    scenario_id = id_by_key.get((product, 2, s_key, topic_id))
    if not scenario_id:
        return ({"thread_id": tid, "status": "missing_scenario", "scenario_key": s_key, "topic_key": t_key}, (topic_id, None, None, None))

    if not v_key:
        return ({"thread_id": tid, "status": "assigned_topic_scenario", "topic_id": topic_id, "scenario_id": scenario_id}, (topic_id, scenario_id, None, None))

    variant_id = id_by_key.get((product, 3, v_key, scenario_id))
    if not variant_id:
        return ({"thread_id": tid, "status": "missing_variant", "variant_key": v_key, "scenario_key": s_key}, (topic_id, scenario_id, None, None))

    if not l_key or l_key == "NO_MATCH":
        return ({"thread_id": tid, "status": "no_leaf_match", "topic_id": topic_id, "scenario_id": scenario_id, "variant_id": variant_id}, (topic_id, scenario_id, variant_id, None))

    leaf_id = id_by_key.get((product, 4, l_key, variant_id))
    if not leaf_id:
        return ({"thread_id": tid, "status": "missing_leaf", "leaf_key": l_key}, (topic_id, scenario_id, variant_id, None))

    return (
        {"thread_id": tid, "status": "assigned", "leaf_id": leaf_id, "confidence": assignment.get("confidence")},
        (topic_id, scenario_id, variant_id, leaf_id),
    )


def _apply_assignments_bulk(
    cnx: pyodbc.Connection,
    id_by_key: Dict[Tuple[str, int, str, Optional[int]], int],
    product: str,
    threads: List[Dict[str, Any]],
    assignments_by_tid: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    update_rows: List[Tuple[int, Optional[int], Optional[int], Optional[int], str]] = []
    membership_rows: List[Tuple[str, int]] = []

    for t in threads:
        tid = str(t.get("thread_id") or "")
        a = assignments_by_tid.get(tid) or {"thread_id": tid}

        res, ids = validate_and_resolve_assignment(id_by_key, a, product)
        results.append(res)

        topic_id, scenario_id, variant_id, leaf_id = ids
        if topic_id:
            update_rows.append((topic_id, scenario_id, variant_id, leaf_id, tid))
            if leaf_id is not None and res.get("status") == "assigned":
                membership_rows.append((tid, int(leaf_id)))

    bulk_upsert_memberships(cnx, membership_rows)
    bulk_update_thread_cluster_ids(cnx, update_rows)

    return results


def validate_batch_assignments(input_threads: List[Dict[str, Any]], output: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Dict[str, Any]]]:
    expected = {str(t.get("thread_id")) for t in input_threads if t.get("thread_id")}
    assignments = output.get("assignments")

    if not isinstance(assignments, list):
        return False, "missing_assignments_array", {}

    by_tid: Dict[str, Dict[str, Any]] = {}
    extras = []
    for a in assignments:
        tid = str(a.get("thread_id")) if isinstance(a, dict) else ""
        if not tid:
            continue
        if tid not in expected:
            extras.append(tid)
            continue
        by_tid[tid] = a

    missing = [t for t in expected if t not in by_tid]
    if missing:
        return False, f"missing_assignments:{len(missing)}", by_tid
    if extras:
        return False, f"extra_assignments:{len(extras)}", by_tid

    return True, "ok", by_tid


# ---------------------------------------------------------
# Catalog slicing for staged calls
# ---------------------------------------------------------

def _filter_catalog_for_topic_stage(catalog_payload: Dict[str, Any], max_topics: int) -> Dict[str, Any]:
    return {"topics": (catalog_payload.get("topics") or [])[:max_topics], "scenarios": [], "variants": [], "leaves": [], "scenario_by_topic": {}, "variant_by_scenario": {}, "leaf_by_variant": {}}


def _filter_catalog_for_scenario_stage(catalog_payload: Dict[str, Any], topic_key: str, max_scenarios: int) -> Dict[str, Any]:
    scenario_keys = set((catalog_payload.get("scenario_by_topic") or {}).get(topic_key) or [])
    scenarios = [s for s in (catalog_payload.get("scenarios") or []) if s.get("key") in scenario_keys]
    scenario_by_topic = {topic_key: [s.get("key") for s in scenarios if s.get("key")]}

    return {"topics": [{"key": topic_key, "parent_key": None, "sig": "", "res_sig": ""}], "scenarios": scenarios[:max_scenarios], "variants": [], "leaves": [], "scenario_by_topic": scenario_by_topic, "variant_by_scenario": {}, "leaf_by_variant": {}}


def _filter_catalog_for_variant_leaf_stage(catalog_payload: Dict[str, Any], scenario_key: str, max_variants: int, max_leaves: int) -> Dict[str, Any]:
    variant_keys = set((catalog_payload.get("variant_by_scenario") or {}).get(scenario_key) or [])
    variants = [v for v in (catalog_payload.get("variants") or []) if v.get("key") in variant_keys]
    variants = variants[:max_variants]

    kept_variant_keys = [v.get("key") for v in variants if v.get("key")]
    leaf_by_variant_full = catalog_payload.get("leaf_by_variant") or {}

    leaf_keys = set()
    leaf_by_variant: Dict[str, List[str]] = {}
    for vk in kept_variant_keys:
        keys = list(leaf_by_variant_full.get(vk) or [])
        leaf_by_variant[vk] = keys
        leaf_keys.update(keys)

    leaves = [l for l in (catalog_payload.get("leaves") or []) if l.get("key") in leaf_keys]
    leaves = leaves[:max_leaves]

    kept_leaf_keys = set(l.get("key") for l in leaves if l.get("key"))
    for vk in list(leaf_by_variant.keys()):
        leaf_by_variant[vk] = [lk for lk in leaf_by_variant[vk] if lk in kept_leaf_keys]

    return {"topics": [], "scenarios": [], "variants": variants, "leaves": leaves, "scenario_by_topic": {}, "variant_by_scenario": {scenario_key: kept_variant_keys}, "leaf_by_variant": leaf_by_variant}


# ---------------------------------------------------------
# Nano call (assignment)
# ---------------------------------------------------------

def _call_nano_assign(
    client: AzureOpenAI,
    deployment: str,
    product: str,
    threads: List[Dict[str, Any]],
    catalog_payload: Dict[str, Any],
    trace_id: str,
    stage: str,
) -> Dict[str, Any]:
    stage_rules = ""
    if stage == "topic":
        stage_rules = (
            "STAGE RULES:\n"
            "- You are ONLY assigning topic_key.\n"
            "- You MUST set scenario_key=\"\", variant_key=\"\", leaf_key=\"\" for every assignment.\n"
        )
    elif stage == "scenario":
        stage_rules = (
            "STAGE RULES:\n"
            "- You are assigning scenario_key under the chosen topic_key.\n"
            "- You MUST set variant_key=\"\", leaf_key=\"\" for every assignment.\n"
            "- topic_key MUST be set and must be valid.\n"
        )

    system_prompt = (
        stage_rules
        + "You assign support threads to an EXISTING hierarchy catalog.\n"
        "Catalog Levels:\n"
        "L1 Topic (may be standalone)\n"
        "L2 Scenario (must have Topic parent; may be standalone under Topic)\n"
        "L3 Variant (must have Scenario parent)\n"
        "L4 Leaf (must have Variant parent; specific fix pattern)\n\n"
        "HARD RULES (must follow):\n"
        "1) Output MUST be valid JSON: {\"assignments\":[...]}.\n"
        "2) Output MUST contain exactly ONE assignment per input thread_id.\n"
        "3) For every assignment you MUST include ALL fields:\n"
        "   thread_id, topic_key, scenario_key, variant_key, leaf_key, confidence, reason.\n"
        "4) Keys MUST be strings. Do not output null.\n"
        "5) Parentless must never happen:\n"
        "   - scenario_key cannot be set unless topic_key is set.\n"
        "   - variant_key cannot be set unless scenario_key is set.\n"
        "   - leaf_key cannot be set unless variant_key is set.\n\n"
        "Selection rules:\n"
        "- topic_key MUST be one of catalog.topics[].key\n"
        "- If you set scenario_key, it MUST be one of catalog.scenario_by_topic[topic_key]\n"
        "- If you set variant_key, it MUST be one of catalog.variant_by_scenario[scenario_key]\n"
        "- If you set leaf_key and leaf_key != \"NO_MATCH\", it MUST be one of catalog.leaf_by_variant[variant_key]\n"
        "- \"NO_MATCH\" is ONLY allowed for leaf_key.\n\n"
        "confidence must be 0.0-1.0.\n"
        "Return JSON only. No markdown.\n"
        "If unsure, prefer topic/scenario only.\n"
    )

    slim_threads = [
        {
            "thread_id": str(t.get("thread_id")),
            "signature_text": (t.get("signature_text") or "")[:900],
            "resolution_signature_text": (t.get("resolution_signature_text") or "")[:900],
        }
        for t in threads
    ]

    user_payload = {"product": product, "threads": slim_threads, "catalog": catalog_payload}

    t0 = _now_ms()
    logging.info(
        "[2E:%s] nano_call_start stage=%s product=%s threads=%d catalog_sizes={t:%d,s:%d,v:%d,l:%d}",
        trace_id,
        stage,
        product,
        len(threads),
        len(catalog_payload.get("topics") or []),
        len(catalog_payload.get("scenarios") or []),
        len(catalog_payload.get("variants") or []),
        len(catalog_payload.get("leaves") or []),
    )

    with _aoai_sema:
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
            rate_limiter=get_rate_limiter("nano"),
            caller_tag=f"phase2e_{stage}",
        )

    logging.info("[2E:%s] nano_call_end stage=%s ms=%d", trace_id, stage, _now_ms() - t0)
    return json.loads(resp.choices[0].message.content or "{}")


# ---------------------------------------------------------
# Orphan auto-create + insert (adapted from Phase 1B patterns)
# ---------------------------------------------------------

def _norm_key(k: str) -> str:
    k = (k or "").strip().lower()
    k = k.replace(" ", "-")
    return k[:360]


def _norm_level(v: Any) -> int:
    try:
        i = int(v)
        return i if i in (1, 2, 3, 4) else 0
    except Exception:
        return 0


def _candidate_id(prod: str, lvl: int, topic_key: str, scenario_key: str, variant_key: str, leaf_key: str) -> str:
    raw = f"{prod}|{lvl}|{topic_key}|{scenario_key}|{variant_key}|{leaf_key}".lower().strip()
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _catalog_l1_keys_for_product(catalog_rows: List[Dict[str, Any]], product: str) -> Set[str]:
    prod = (product or "").strip()
    if not prod:
        return set()
    return {str(r.get("cluster_key") or "").strip().lower() for r in catalog_rows if str(r.get("product") or "").strip() == prod and int(r.get("cluster_level") or 0) == 1}


def _build_bulk_nodes_from_candidates(candidates: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    lvl_order = {1: 0, 2: 1, 3: 2, 4: 3}
    vals = sorted(candidates.values(), key=lambda x: lvl_order.get(int(x.get("level") or 0), 99))

    for c in vals:
        prod = str(c.get("product") or "").strip()
        lvl = int(c.get("level") or 0)

        topic_key = str(c.get("topic_key") or "").strip()
        scenario_key = str(c.get("scenario_key") or "").strip()
        variant_key = str(c.get("variant_key") or "").strip()
        leaf_key = str(c.get("leaf_key") or "").strip()

        sig = str(c.get("signature_text") or "").strip()
        res_sig = c.get("resolution_signature_text")

        if lvl == 1:
            out.append({"product": prod, "level": 1, "key": topic_key, "parent_key": None, "signature_text": sig, "resolution_signature_text": None})
        elif lvl == 2:
            out.append({"product": prod, "level": 2, "key": scenario_key, "parent_key": topic_key, "signature_text": sig, "resolution_signature_text": None})
        elif lvl == 3:
            out.append({"product": prod, "level": 3, "key": variant_key, "parent_key": scenario_key, "signature_text": sig, "resolution_signature_text": None})
        elif lvl == 4:
            out.append({"product": prod, "level": 4, "key": leaf_key, "parent_key": variant_key, "signature_text": sig or leaf_key, "resolution_signature_text": res_sig})

    seen = set()
    deduped: List[Dict[str, Any]] = []
    for n in out:
        tup = (n["product"], int(n["level"]), n["key"], n.get("parent_key") or "")
        if tup in seen:
            continue
        seen.add(tup)
        deduped.append(n)

    return deduped


def bulk_get_or_create_issue_clusters_common(
    cnx: pyodbc.Connection,
    nodes: List[Dict[str, Any]],
    created_by_phase: str,
) -> List[Dict[str, Any]]:
    if not nodes:
        return []

    norm: List[Dict[str, Any]] = []
    for n in nodes:
        prod = str(n.get("product") or "").strip()
        lvl = int(n.get("level") or 0)
        key = str(n.get("key") or "").strip()

        pkey = n.get("parent_key")
        pkey_s = str(pkey).strip() if pkey is not None else None

        sig = str(n.get("signature_text") or "").strip()
        res_sig = n.get("resolution_signature_text")
        res_sig_s = str(res_sig).strip() if isinstance(res_sig, str) else (None if res_sig is None else str(res_sig))

        if not prod or lvl not in (1, 2, 3, 4) or not key:
            continue

        if lvl == 1:
            pkey_s = None
        elif not pkey_s:
            continue

        norm.append({"product": prod, "level": lvl, "key": key, "parent_key": pkey_s, "signature_text": sig, "resolution_signature_text": res_sig_s})

    if not norm:
        return []

    values_sql = ",".join("(?,?,?,?,?,?)" for _ in norm)
    params: List[Any] = []
    for n in norm:
        params.extend([int(n["level"]), n["product"], n["key"], n["parent_key"], n["signature_text"], n["resolution_signature_text"]])

    cur = cnx.cursor()
    cur.execute(
        f"""
        SET NOCOUNT ON;

        DECLARE @in TABLE (
            rownum INT IDENTITY(1,1) PRIMARY KEY,
            lvl TINYINT NOT NULL,
            prod NVARCHAR(64) NOT NULL,
            k NVARCHAR(360) NOT NULL,
            parent_k NVARCHAR(360) NULL,
            sig NVARCHAR(1200) NULL,
            res_sig NVARCHAR(1200) NULL
        );

        DECLARE @resolved TABLE (
            rownum INT NOT NULL PRIMARY KEY,
            lvl TINYINT NOT NULL,
            prod NVARCHAR(64) NOT NULL,
            k NVARCHAR(360) NOT NULL,
            parent_key NVARCHAR(360) NULL,
            parent_id BIGINT NULL,
            sig NVARCHAR(1200) NULL,
            res_sig NVARCHAR(1200) NULL
        );

        INSERT INTO @in (lvl, prod, k, parent_k, sig, res_sig)
        VALUES {values_sql};

        INSERT INTO @resolved (rownum, lvl, prod, k, parent_key, parent_id, sig, res_sig)
        SELECT
            i.rownum,
            i.lvl,
            i.prod,
            i.k,
            i.parent_k AS parent_key,
            CASE
                WHEN i.lvl = 1 THEN NULL
                ELSE COALESCE(p2.cluster_id, p0.cluster_id, p1.cluster_id)
            END AS parent_id,
            i.sig,
            i.res_sig
        FROM @in i
        OUTER APPLY (
            SELECT TOP 1 p.cluster_id
            FROM dbo.issue_cluster p
            WHERE p.is_active = 1
              AND p.product = i.prod
              AND p.cluster_level = i.lvl - 1
              AND p.cluster_key = i.parent_k
              AND p.created_by_phase = ?
            ORDER BY p.cluster_id DESC
        ) p2
        OUTER APPLY (
            SELECT TOP 1 p.cluster_id
            FROM dbo.issue_cluster p
            WHERE p.is_active = 1
              AND p.product = i.prod
              AND p.cluster_level = i.lvl - 1
              AND p.cluster_key = i.parent_k
              AND p.created_by_phase IS NULL
            ORDER BY p.cluster_id DESC
        ) p0
        OUTER APPLY (
            SELECT TOP 1 p.cluster_id
            FROM dbo.issue_cluster p
            WHERE p.is_active = 1
              AND p.product = i.prod
              AND p.cluster_level = i.lvl - 1
              AND p.cluster_key = i.parent_k
            ORDER BY p.cluster_id DESC
        ) p1;

        BEGIN TRY
            INSERT INTO dbo.issue_cluster
                (cluster_level, product, cluster_key, parent_cluster_id,
                 cluster_signature_text, resolution_signature_text,
                 last_seen_at, member_count, is_active, created_by_phase)
            SELECT
                r.lvl,
                r.prod,
                r.k,
                CASE WHEN r.lvl = 1 THEN NULL ELSE r.parent_id END,
                r.sig,
                r.res_sig,
                SYSUTCDATETIME(),
                0,
                1,
                ?
            FROM @resolved r
            WHERE (r.lvl = 1 OR r.parent_id IS NOT NULL)
              AND NOT EXISTS (
                  SELECT 1
                  FROM dbo.issue_cluster ic
                  WHERE ic.is_active = 1
                    AND ic.product = r.prod
                    AND ic.cluster_level = r.lvl
                    AND ic.cluster_key = r.k
              );
        END TRY
        BEGIN CATCH
            IF ERROR_NUMBER() NOT IN (2601, 2627)
                THROW;
        END CATCH;

        SELECT
            r.prod AS product,
            r.lvl AS level,
            r.k AS [key],
            r.parent_key AS parent_key,
            CASE WHEN r.lvl = 1 THEN NULL ELSE r.parent_id END AS parent_cluster_id,
            ic.cluster_id,
            r.sig AS signature_text,
            r.res_sig AS resolution_signature_text
        FROM @resolved r
        JOIN dbo.issue_cluster ic
          ON ic.is_active = 1
         AND ic.product = r.prod
         AND ic.cluster_level = r.lvl
         AND ic.cluster_key = r.k
        WHERE (r.lvl = 1 OR r.parent_id IS NOT NULL)
        ORDER BY r.rownum ASC;
        """,
        created_by_phase,
        created_by_phase,
        *params,
    )

    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def call_nano_orphan_propose_nodes(
    client: AzureOpenAI,
    deployment: str,
    product: str,
    orphan_threads: List[Dict[str, Any]],
) -> Dict[str, Any]:
    system_prompt = (
        "You maintain a 4-level COMMON-ISSUE catalog.\n"
        "Task: propose NEW catalog nodes ONLY to cover orphan threads (no leaf match).\n\n"
        "STRICT:\n"
        "- Prefer proposing ONLY L4 leaves under existing variants.\n"
        "- Do NOT create one leaf per thread if same fix pattern.\n"
        "- Keys must be kebab-case, stable.\n\n"
        "OUTPUT JSON ONLY:\n"
        "{ \"candidates\": [ ... ], \"notes\": \"...\" }\n\n"
        "Candidate schema:\n"
        "{\n"
        "  \"product\": \"string\",\n"
        "  \"level\": 1|2|3|4,\n"
        "  \"topic_key\": \"string\",\n"
        "  \"scenario_key\": \"string\",\n"
        "  \"variant_key\": \"string\",\n"
        "  \"leaf_key\": \"string\",\n"
        "  \"signature_text\": \"short normalized\",\n"
        "  \"resolution_signature_text\": \"required for L4\"\n"
        "}\n"
    )

    slim_threads = [
        {
            "thread_id": str(t.get("thread_id")),
            "product": product,
            "signature_text": (t.get("signature_text") or "")[:900],
            "resolution_signature_text": (t.get("resolution_signature_text") or "")[:900],
            "current_assignment": {
                "topic_key": t.get("topic_key") or "",
                "scenario_key": t.get("scenario_key") or "",
                "variant_key": t.get("variant_key") or "",
                "leaf_key": t.get("leaf_key") or "",
            },
            "solution_usefulness": t.get("solution_usefulness", 0.0),
        }
        for t in orphan_threads
    ]

    user_payload = {"product": product, "threads": slim_threads}

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
            caller_tag="phase2e_orphan_propose",
        )
    return json.loads(resp.choices[0].message.content or "{}")


def _select_orphan_threads_for_autocreate(threads: List[Dict[str, Any]], product: str, results: List[Dict[str, Any]], assignments: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_tid_res = {str(r.get("thread_id") or ""): r for r in results}
    out: List[Dict[str, Any]] = []

    for t in threads:
        tid = str(t.get("thread_id") or "")
        r = by_tid_res.get(tid) or {}
        status = str(r.get("status") or "")

        usefulness = t.get("solution_usefulness")
        try:
            usefulness_f = float(usefulness) if usefulness is not None else 0.0
        except Exception:
            usefulness_f = 0.0

        if usefulness_f <= _P2E_ORPHAN_USEFULNESS_MIN:
            continue

        if status not in ("no_leaf_match", "missing_leaf"):
            continue

        a = assignments.get(tid) or {}
        out.append({
            "thread_id": tid,
            "product": product,
            "signature_text": t.get("signature_text") or "",
            "resolution_signature_text": t.get("resolution_signature_text") or "",
            "solution_usefulness": usefulness_f,
            "status": status,
            "topic_key": a.get("topic_key") or "",
            "scenario_key": a.get("scenario_key") or "",
            "variant_key": a.get("variant_key") or "",
            "leaf_key": a.get("leaf_key") or "",
        })

    out.sort(key=lambda x: (-float(x.get("solution_usefulness") or 0.0), str(x.get("thread_id") or "")))
    return out[: max(0, _P2E_ORPHAN_MAX_THREADS_PER_BATCH)]


def _refresh_indexes_for_product(
    cnx: pyodbc.Connection,
    product: str,
    max_catalog: int,
    max_per_level: int,
) -> Tuple[List[Dict[str, Any]], Dict[Tuple[str, int, str, Optional[int]], int], Dict[str, Any]]:
    catalog_rows = fetch_catalog(cnx, max_rows=max_catalog)
    id_by_key, _ = build_id_indexes(catalog_rows)
    payload_by_product = build_catalog_payload_by_product(catalog_rows, max_per_level=max_per_level)
    return catalog_rows, id_by_key, (payload_by_product.get(product) or {"topics": [], "scenarios": [], "variants": [], "leaves": [], "scenario_by_topic": {}, "variant_by_scenario": {}, "leaf_by_variant": {}})


# ---------------------------------------------------------
# Worker: Process One Product Batch (staged + orphan autocreate + auto-assign)
# ---------------------------------------------------------

def process_product_batch(
    catalog_payload: Dict[str, Any],
    product: str,
    threads: List[Dict[str, Any]],
    id_by_key: Dict[Tuple[str, int, str, Optional[int]], int],
) -> List[Dict[str, Any]]:
    logging.info("[2E] worker_start product=%s threads=%d", product, len(threads))

    client = make_aoai_client()
    deployment = _get_aoai_deployment()

    trace_id = f"{product}:{_short_tid(threads[0].get('thread_id'))}" if threads else product
    logging.info("[2E:%s] product_batch_start threads=%d", trace_id, len(threads))

    # ---------- Stage A: Topic ----------
    topic_catalog = _filter_catalog_for_topic_stage(catalog_payload, max_topics=_P2E_MAX_TOPICS)
    out_topic = _call_nano_assign(client, deployment, product, threads, topic_catalog, trace_id=trace_id, stage="topic")

    ok, msg, by_tid_topic = validate_batch_assignments(threads, out_topic)
    if not ok:
        out_full = _call_nano_assign(client, deployment, product, threads, catalog_payload, trace_id=trace_id, stage="full")
        ok2, msg2, by_tid_full = validate_batch_assignments(threads, out_full)
        if not ok2:
            return [{"status": "fallback_full_validation_failed", "error": msg2, "nano_output": out_full}]
        by_tid_topic = by_tid_full

    # seed assignments
    assignments: Dict[str, Dict[str, Any]] = {}
    for t in threads:
        tid = str(t.get("thread_id"))
        a = dict(by_tid_topic.get(tid) or {})
        a.setdefault("thread_id", tid)
        a["scenario_key"] = ""
        a["variant_key"] = ""
        a["leaf_key"] = ""
        assignments[tid] = a

    # group by topic
    by_topic: Dict[str, List[Dict[str, Any]]] = {}
    for t in threads:
        tid = str(t.get("thread_id"))
        tkey = str(assignments[tid].get("topic_key") or "").strip()
        by_topic.setdefault(tkey, []).append(t)

    if len(by_topic) > _P2E_MAX_TOPIC_GROUPS:
        out_full = _call_nano_assign(client, deployment, product, threads, catalog_payload, trace_id=trace_id, stage="full")
        ok3, msg3, by_tid_full = validate_batch_assignments(threads, out_full)
        if not ok3:
            return [{"status": "full_validation_failed", "error": msg3, "nano_output": out_full}]
        with sql_connect() as cnx:
            results = _apply_assignments_bulk(cnx, id_by_key, product, threads, by_tid_full)
            mark_assignment_completed(cnx, [str(t.get("thread_id")) for t in threads])
            cnx.commit()
            return results

    # ---------- Stage B: Scenario ----------
    for topic_key, group in by_topic.items():
        if not topic_key:
            continue
        scenario_catalog = _filter_catalog_for_scenario_stage(catalog_payload, topic_key, max_scenarios=_P2E_MAX_SCENARIOS)
        out_scn = _call_nano_assign(client, deployment, product, group, scenario_catalog, trace_id=trace_id, stage="scenario")
        ok4, msg4, by_tid_scn = validate_batch_assignments(group, out_scn)
        if not ok4:
            continue
        for t in group:
            tid = str(t.get("thread_id"))
            a2 = by_tid_scn.get(tid) or {}
            assignments[tid]["scenario_key"] = str(a2.get("scenario_key") or "").strip()
            assignments[tid]["variant_key"] = ""
            assignments[tid]["leaf_key"] = ""

    # group by scenario
    by_scenario: Dict[str, List[Dict[str, Any]]] = {}
    for t in threads:
        tid = str(t.get("thread_id"))
        skey = str(assignments[tid].get("scenario_key") or "").strip()
        if skey:
            by_scenario.setdefault(skey, []).append(t)

    if len(by_scenario) > _P2E_MAX_SCENARIO_GROUPS:
        out_full = _call_nano_assign(client, deployment, product, threads, catalog_payload, trace_id=trace_id, stage="full")
        ok5, msg5, by_tid_full = validate_batch_assignments(threads, out_full)
        if not ok5:
            return [{"status": "full_validation_failed", "error": msg5, "nano_output": out_full}]
        with sql_connect() as cnx:
            results = _apply_assignments_bulk(cnx, id_by_key, product, threads, by_tid_full)
            mark_assignment_completed(cnx, [str(t.get("thread_id")) for t in threads])
            cnx.commit()
            return results

    # ---------- Stage C: Variant + Leaf ----------
    for scenario_key, group in by_scenario.items():
        stage_catalog = _filter_catalog_for_variant_leaf_stage(catalog_payload, scenario_key=scenario_key, max_variants=_P2E_MAX_VARIANTS, max_leaves=_P2E_MAX_LEAVES)
        out_full = _call_nano_assign(client, deployment, product, group, stage_catalog, trace_id=trace_id, stage="full")
        ok6, msg6, by_tid_full = validate_batch_assignments(group, out_full)
        if not ok6:
            continue
        for t in group:
            tid = str(t.get("thread_id"))
            a3 = by_tid_full.get(tid) or {}
            assignments[tid]["variant_key"] = str(a3.get("variant_key") or "").strip()
            assignments[tid]["leaf_key"] = str(a3.get("leaf_key") or "").strip()
            assignments[tid]["confidence"] = a3.get("confidence")
            assignments[tid]["reason"] = a3.get("reason")

    # ---------- DB apply + orphan autocreate + immediate reassign ----------
    with sql_connect() as cnx:
        try:
            # First apply
            results = _apply_assignments_bulk(cnx, id_by_key, product, threads, assignments)

            created_nodes: List[Dict[str, Any]] = []
            reassigned_results: Optional[List[Dict[str, Any]]] = None

            if _P2E_ORPHAN_AUTOCREATE_ENABLED:
                orphans = _select_orphan_threads_for_autocreate(threads, product, results, assignments)
                if orphans:
                    logging.warning("[2E:%s] orphan_autocreate start orphans=%d usefulness_min=%.2f", trace_id, len(orphans), _P2E_ORPHAN_USEFULNESS_MIN)

                    propose_out = call_nano_orphan_propose_nodes(client, deployment, product, orphans)
                    proposed = propose_out.get("candidates") or []
                    logging.warning("[2E:%s] orphan_autocreate proposed_candidates=%d", trace_id, len(proposed))
                    # Refresh server-side view (catalog + indexes) for guardrails
                    catalog_rows_live = fetch_catalog(cnx, max_rows=20000)
                    existing_l1 = _catalog_l1_keys_for_product(catalog_rows_live, product)
                    proposed_l1 = {
                        _norm_key(n.get("topic_key") or "")
                        for n in proposed
                        if _norm_level(n.get("level")) == 1 and (str(n.get("product") or "").strip() in ("", product))
                    }

                    candidates: Dict[str, Dict[str, Any]] = {}
                    for n in proposed:
                        prod = str(n.get("product") or "").strip() or product
                        if prod != product:
                            continue

                        lvl = _norm_level(n.get("level"))
                        topic_key = _norm_key(n.get("topic_key") or "")
                        scenario_key = _norm_key(n.get("scenario_key") or "")
                        variant_key = _norm_key(n.get("variant_key") or "")
                        leaf_key = _norm_key(n.get("leaf_key") or "")

                        sig = str(n.get("signature_text") or "").strip()
                        res_sig = str(n.get("resolution_signature_text") or "").strip() if n.get("resolution_signature_text") else None

                        if lvl not in (1, 2, 3, 4):
                            continue
                        if lvl == 4 and not res_sig:
                            continue

                        if (lvl == 1 and not topic_key) or \
                           (lvl == 2 and (not topic_key or not scenario_key)) or \
                           (lvl == 3 and (not topic_key or not scenario_key or not variant_key)) or \
                           (lvl == 4 and (not topic_key or not scenario_key or not variant_key or not leaf_key)):
                            continue

                        if lvl in (2, 3, 4) and (topic_key not in existing_l1 and topic_key not in proposed_l1):
                            continue

                        cid = _candidate_id(prod, lvl, topic_key, scenario_key, variant_key, leaf_key)
                        candidates[cid] = {
                            "candidate_id": cid,
                            "product": prod,
                            "level": lvl,
                            "topic_key": topic_key,
                            "scenario_key": scenario_key,
                            "variant_key": variant_key,
                            "leaf_key": leaf_key,
                            "signature_text": sig,
                            "resolution_signature_text": res_sig,
                        }

                    bulk_nodes = _build_bulk_nodes_from_candidates(candidates)

                    # Parents first
                    nodes_by_level: Dict[int, List[Dict[str, Any]]] = {1: [], 2: [], 3: [], 4: []}
                    for n in bulk_nodes:
                        lvl = int(n.get("level") or 0)
                        if lvl in nodes_by_level:
                            nodes_by_level[lvl].append(n)

                    for lvl in (1, 2, 3, 4):
                        lvl_nodes = nodes_by_level[lvl]
                        if not lvl_nodes:
                            continue
                        for i0 in range(0, len(lvl_nodes), _P2E_ORPHAN_INSERT_BATCH):
                            chunk = lvl_nodes[i0 : i0 + _P2E_ORPHAN_INSERT_BATCH]
                            created_nodes.extend(bulk_get_or_create_issue_clusters_common(cnx, chunk, created_by_phase=_P2E_ORPHAN_CREATED_BY_PHASE))

                    # ---- IMMEDIATE REASSIGN (same run) ----
                    # Refresh catalog payload + id indexes, then re-run ONLY the orphan group through leaf stage
                    _, id_by_key_live, catalog_payload_live = _refresh_indexes_for_product(
                        cnx, product=product, max_catalog=20000, max_per_level=500
                    )

                    # Build a small assignment dict for just orphan tids; keep previous topic/scenario/variant if present,
                    # but allow model to pick leaf from newly-created leaves.
                    orphan_threads_for_assign: List[Dict[str, Any]] = []
                    for o in orphans:
                        tid = str(o.get("thread_id") or "")
                        # find the original thread object
                        t = next((x for x in threads if str(x.get("thread_id") or "") == tid), None)
                        if t:
                            orphan_threads_for_assign.append(t)

                    if orphan_threads_for_assign:
                        out_re = _call_nano_assign(
                            client,
                            deployment,
                            product,
                            orphan_threads_for_assign,
                            catalog_payload_live,
                            trace_id=trace_id,
                            stage="full",
                        )
                        okr, msgr, by_tid_re = validate_batch_assignments(orphan_threads_for_assign, out_re)
                        if okr:
                            # Apply reassigned (only affects those tids)
                            reassigned_results = _apply_assignments_bulk(cnx, id_by_key_live, product, orphan_threads_for_assign, by_tid_re)
                            # Merge reassigned statuses back into results list
                            by_tid_rr = {str(r.get("thread_id") or ""): r for r in reassigned_results}
                            for r in results:
                                tid = str(r.get("thread_id") or "")
                                if tid in by_tid_rr:
                                    r.update(by_tid_rr[tid])
                                    r["status"] = by_tid_rr[tid].get("status", r.get("status"))
                                    r["leaf_id"] = by_tid_rr[tid].get("leaf_id", r.get("leaf_id"))
                            logging.warning("[2E:%s] orphan_autocreate reassigned=%d", trace_id, len(reassigned_results))
                        else:
                            logging.warning("[2E:%s] orphan_autocreate reassignment_failed msg=%s", trace_id, msgr)

            mark_assignment_completed(cnx, [str(t.get("thread_id")) for t in threads])
            cnx.commit()

            if created_nodes:
                for r in results:
                    r.setdefault("autocreated_nodes_sample", created_nodes[:25])

            return results

        except Exception as e:
            cnx.rollback()
            release_assignment_claim(cnx, [str(t.get("thread_id")) for t in threads])
            cnx.commit()
            return [{"status": "db_error", "error": str(e)}]


# ---------------------------------------------------------
# Counts Recompute
# ---------------------------------------------------------

def recompute_catalog_member_counts(cnx: pyodbc.Connection) -> None:
    cur = cnx.cursor()
    cur.execute(
        """
        ;WITH leaf_counts AS (
            SELECT
                m.resolution_leaf_cluster_id AS leaf_id,
                COUNT(*) AS cnt
            FROM dbo.thread_cluster_membership m
            WHERE m.resolution_leaf_cluster_id IS NOT NULL
            GROUP BY m.resolution_leaf_cluster_id
        ),
        l4 AS (
            SELECT
                ic.cluster_id,
                CAST(ISNULL(lc.cnt, 0) AS INT) AS cnt
            FROM dbo.issue_cluster ic
            LEFT JOIN leaf_counts lc ON lc.leaf_id = ic.cluster_id
            WHERE ic.is_active = 1 AND ic.cluster_level = 4
        ),
        l3 AS (
            SELECT
                v.cluster_id,
                CAST(SUM(ISNULL(l4.cnt, 0)) AS INT) AS cnt
            FROM dbo.issue_cluster v
            LEFT JOIN dbo.issue_cluster l ON l.parent_cluster_id = v.cluster_id AND l.is_active = 1 AND l.cluster_level = 4
            LEFT JOIN l4 ON l4.cluster_id = l.cluster_id
            WHERE v.is_active = 1 AND v.cluster_level = 3
            GROUP BY v.cluster_id
        ),
        l2 AS (
            SELECT
                s.cluster_id,
                CAST(SUM(ISNULL(l3.cnt, 0)) AS INT) AS cnt
            FROM dbo.issue_cluster s
            LEFT JOIN dbo.issue_cluster v ON v.parent_cluster_id = s.cluster_id AND v.is_active = 1 AND v.cluster_level = 3
            LEFT JOIN l3 ON l3.cluster_id = v.cluster_id
            WHERE s.is_active = 1 AND s.cluster_level = 2
            GROUP BY s.cluster_id
        ),
        l1 AS (
            SELECT
                t.cluster_id,
                CAST(SUM(ISNULL(l2.cnt, 0)) AS INT) AS cnt
            FROM dbo.issue_cluster t
            LEFT JOIN dbo.issue_cluster s ON s.parent_cluster_id = t.cluster_id AND s.is_active = 1 AND s.cluster_level = 2
            LEFT JOIN l2 ON l2.cluster_id = s.cluster_id
            WHERE t.is_active = 1 AND t.cluster_level = 1
            GROUP BY t.cluster_id
        ),
        all_counts AS (
            SELECT cluster_id, cnt FROM l1
            UNION ALL SELECT cluster_id, cnt FROM l2
            UNION ALL SELECT cluster_id, cnt FROM l3
            UNION ALL SELECT cluster_id, cnt FROM l4
        )
        UPDATE ic
        SET member_count = ac.cnt
        FROM dbo.issue_cluster ic
        JOIN all_counts ac ON ac.cluster_id = ic.cluster_id
        WHERE ic.is_active = 1
          AND ic.cluster_level IN (1,2,3,4);
        """
    )


def clear_stale_assignment_claims(cnx: pyodbc.Connection, older_than_minutes: int = 60) -> int:
    cur = cnx.cursor()
    cur.execute(
        """
        UPDATE dbo.thread_enrichment
        SET AssignmentStartedUtc = NULL
        WHERE AssignmentStartedUtc IS NOT NULL
          AND AssignmentCompletedUtc IS NULL
          AND AssignmentStartedUtc < DATEADD(MINUTE, -?, SYSUTCDATETIME());
        """,
        int(older_than_minutes),
    )
    return int(cur.rowcount if cur.rowcount is not None else 0)


# ---------------------------------------------------------
# Function Entry Point
# ---------------------------------------------------------

def run_phase2e_assign_leaf(req: func.HttpRequest) -> func.HttpResponse:
    try:
        batch_size = int(req.params.get("batch_size", "5"))
        max_batches = int(req.params.get("max_batches", "10"))
        max_workers = int(req.params.get("max_workers", "2"))
        force = (req.params.get("force", "0") == "1")

        max_catalog = int(req.params.get("max_catalog", "15000"))
        max_per_level = int(req.params.get("max_per_level", "150"))

        stale_claim_minutes = int(req.params.get("stale_claim_minutes", "60"))
        reset_rows_override = req.params.get("reset_rows")
        reset_rows_override_i = int(reset_rows_override) if reset_rows_override not in (None, "") else None

        batch_size = max(1, min(batch_size, 25))
        max_batches = max(1, min(max_batches, 200))
        max_workers = max(1, min(max_workers, 12))

        include_completed = force

        # ---- Pre-run diagnostics + cleanup stale claims (common-only) ----
        pre_stats: Dict[str, int] = {}
        with sql_connect() as cnx:
            cleared = clear_stale_assignment_claims(cnx, older_than_minutes=stale_claim_minutes)

            cur = cnx.cursor()
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total_common,
                    SUM(CASE WHEN AssignmentCompletedUtc IS NULL THEN 1 ELSE 0 END) AS uncompleted_common,
                    SUM(CASE WHEN AssignmentCompletedUtc IS NULL AND AssignmentStartedUtc IS NOT NULL THEN 1 ELSE 0 END) AS uncompleted_inprogress_common,
                    SUM(CASE WHEN AssignmentCompletedUtc IS NULL AND CatalogCheckedUtc IS NULL THEN 1 ELSE 0 END) AS uncompleted_no_catalogchecked_common,
                    SUM(CASE WHEN AssignmentCompletedUtc IS NULL AND (product IS NULL OR LTRIM(RTRIM(product)) = '') THEN 1 ELSE 0 END) AS uncompleted_no_product_common,
                    SUM(CASE WHEN AssignmentCompletedUtc IS NULL AND (signature_text IS NULL OR LTRIM(RTRIM(signature_text)) = '') THEN 1 ELSE 0 END) AS uncompleted_no_sig_common
                FROM dbo.thread_enrichment
                WHERE (classification IS NULL OR classification <> 'emergent_issue');
                """
            )
            r = cur.fetchone()
            pre_stats = {
                "total_common": int(r[0] or 0),
                "uncompleted_common": int(r[1] or 0),
                "uncompleted_inprogress_common": int(r[2] or 0),
                "uncompleted_no_catalogchecked_common": int(r[3] or 0),
                "uncompleted_no_product_common": int(r[4] or 0),
                "uncompleted_no_sig_common": int(r[5] or 0),
                "cleared_stale_claims": int(cleared),
            }
            cnx.commit()

        logging.info("[2E] pre_stats=%s", pre_stats)

        # ---- Force reset ONCE per invocation (bounded window, common-only) ----
        reset_info: Dict[str, Any] = {"enabled": force, "reset_rows": 0, "affected": 0}
        if force:
            # If the user didn't override, use a big sweep cap so "force=1" behaves like "rerun all".
            reset_rows = reset_rows_override_i if reset_rows_override_i is not None else 20000
            reset_rows = max(1, min(reset_rows, 200000))  # safety cap
            with sql_connect() as cnx:
                n = force_reset_threads_for_assignment(cnx, max_rows=reset_rows, mode="all")
                cnx.commit()
            reset_info = {"enabled": True, "reset_rows": reset_rows, "affected": int(n)}
            logging.info("[2E] force_reset(common,all) rows=%d cap=%d", int(n), reset_rows)

        # ---- Load catalog ----
        with sql_connect() as cnx:
            catalog_rows = fetch_catalog(cnx, max_rows=max_catalog)

        if not catalog_rows:
            return func.HttpResponse(
                json.dumps({"status": "error", "error": "catalog_empty(issue_cluster levels 1-4)"}),
                status_code=500,
                mimetype="application/json",
            )

        id_by_key, _ = build_id_indexes(catalog_rows)
        catalog_payload_by_product = build_catalog_payload_by_product(catalog_rows, max_per_level=max_per_level)

        # ---- Main work loop (drain one product fully) ----
        all_results: List[Dict[str, Any]] = []
        batches_submitted = 0
        futures = []
        claimed_total = 0

        chosen_product: Optional[str] = None
        drained_products: Set[str] = set()

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
                    if chosen_product is None:
                        chosen_product = fetch_next_product_for_assignment(
                            cnx,
                            include_completed=include_completed,
                            exclude_products=drained_products if force else set(),
                        )
                        if not chosen_product:
                            cnx.commit()
                            logging.info("[2E] no_products_available batches_submitted=%d", batches_submitted)
                            break
                        logging.info("[2E] chosen_product=%s", chosen_product)

                    threads = claim_threads_for_assignment_product(
                        cnx,
                        batch_size=batch_size,
                        include_completed=include_completed,
                        product=chosen_product,
                    )
                    cnx.commit()

                if not threads:
                    logging.info("[2E] product_drained product=%s", chosen_product)
                    if force and chosen_product:
                        drained_products.add(chosen_product)
                    chosen_product = None
                    continue

                batches_submitted += 1
                claimed_total += len(threads)

                prod = str(threads[0].get("product") or "Other")

                payload = catalog_payload_by_product.get(prod)
                if not payload:
                    with sql_connect() as cnx:
                        mark_assignment_completed(cnx, [str(x.get("thread_id")) for x in threads])
                        cnx.commit()
                    all_results.extend([{"thread_id": x.get("thread_id"), "status": "no_catalog_for_product", "product": prod} for x in threads])
                    continue

                futures.append(ex.submit(process_product_batch, payload, prod, threads, id_by_key))

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
            json.dumps(
                {
                    "status": "ok",
                    "mode": "common_only",
                    "processed": len(all_results),
                    "claimed_total": claimed_total,
                    "batch_size": batch_size,
                    "max_batches": max_batches,
                    "max_workers": max_workers,
                    "include_completed": include_completed,
                    "orphan_autocreate": {
                        "enabled": _P2E_ORPHAN_AUTOCREATE_ENABLED,
                        "usefulness_min": _P2E_ORPHAN_USEFULNESS_MIN,
                        "max_threads_per_batch": _P2E_ORPHAN_MAX_THREADS_PER_BATCH,
                        "created_by_phase": _P2E_ORPHAN_CREATED_BY_PHASE,
                    },
                    "summary_by_status": summary_by_status,
                    "details": all_results,
                },
                default=str,
            ),
            mimetype="application/json",
        )

    except Exception as e:
        logging.exception("Phase 2E Fatal Error")
        return func.HttpResponse(json.dumps({"status": "error", "error": str(e)}), status_code=500, mimetype="application/json")