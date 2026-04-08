import os
import json
import logging
import hashlib
from typing import List, Dict, Any, Optional, Tuple, Set

import azure.functions as func
import pyodbc
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

# Limits SQL statement size + lock footprint
_P1B_INSERT_BATCH = int(os.getenv("P1B_INSERT_BATCH", "200"))
_P1B_KEEP_IDS_MIN_FRACTION = float(os.getenv("P1B_KEEP_IDS_MIN_FRACTION", "0.85"))

_P1B_LOG_FULL_PROMPT = (os.getenv("P1B_LOG_FULL_PROMPT", "0") == "1")
_P1B_LOG_MAX_PROMPT_CHARS = int(os.getenv("P1B_LOG_MAX_PROMPT_CHARS", "500"))
_P1B_LOG_MAX_JSON_CHARS = int(os.getenv("P1B_LOG_MAX_JSON_CHARS", "2000"))


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
    prod_sample = sorted(
        {str(t.get("product") or "").strip() for t in threads if str(t.get("product") or "").strip()}
    )[:5]
    cat_prod_sample = sorted(
        {str(r.get("product") or "").strip() for r in catalog_slice if str(r.get("product") or "").strip()}
    )[:5]
    cat_key_sample = [str(r.get("key") or "") for r in catalog_slice[:10]]

    prompt_hash = _sha1(system_prompt)
    prompt_preview = system_prompt if _P1B_LOG_FULL_PROMPT else _truncate(system_prompt, _P1B_LOG_MAX_PROMPT_CHARS)

    logging.warning(
        "phase1b nano_%s request slice=%d/%d threads=%d thread_ids=%s thread_products=%s catalog_slice=%d "
        "catalog_products=%s catalog_keys=%s prompt_sha1=%s prompt=%s user_payload_preview=%s",
        kind,
        slice_index,
        slices_total,
        len(threads),
        tid_sample,
        prod_sample,
        len(catalog_slice),
        cat_prod_sample,
        cat_key_sample,
        prompt_hash,
        prompt_preview,
        _safe_json_preview(
            {
                "slice_index": user_payload.get("slice_index"),
                "slices_total": user_payload.get("slices_total"),
                "threads_count": len(user_payload.get("threads") or []),
                "existing_catalog_slice_count": len(user_payload.get("existing_catalog_slice") or []),
            },
            max_chars=300,
        ),
    )


def _log_nano_response(kind: str, resp_text: str, parsed: Dict[str, Any]) -> None:
    notes = parsed.get("notes")
    if kind == "propose":
        cands = parsed.get("candidates") or []
        sample = cands[:5] if isinstance(cands, list) else []
        logging.warning(
            "phase1b nano_%s response raw_len=%d notes=%s candidates=%d sample=%s raw_preview=%s",
            kind,
            len(resp_text or ""),
            _truncate(str(notes or ""), 300),
            len(cands) if isinstance(cands, list) else -1,
            _safe_json_preview(sample, 900),
            _truncate(resp_text or "", _P1B_LOG_MAX_JSON_CHARS),
        )
        return

    if kind == "refine":
        keep_ids = parsed.get("keep_candidate_ids") or []
        drops = parsed.get("drop") or []
        adds = parsed.get("add") or []
        logging.warning(
            "phase1b nano_%s response raw_len=%d notes=%s keep_ids=%d drop=%d add=%d keep_sample=%s drop_sample=%s "
            "add_sample=%s raw_preview=%s",
            kind,
            len(resp_text or ""),
            _truncate(str(notes or ""), 300),
            len(keep_ids) if isinstance(keep_ids, list) else -1,
            len(drops) if isinstance(drops, list) else -1,
            len(adds) if isinstance(adds, list) else -1,
            _safe_json_preview(keep_ids[:10] if isinstance(keep_ids, list) else [], 700),
            _safe_json_preview(drops[:5] if isinstance(drops, list) else [], 900),
            _safe_json_preview(adds[:3] if isinstance(adds, list) else [], 900),
            _truncate(resp_text or "", _P1B_LOG_MAX_JSON_CHARS),
        )
        return

    logging.warning(
        "phase1b nano_%s response raw_len=%d raw_preview=%s",
        kind,
        len(resp_text or ""),
        _truncate(resp_text or "", _P1B_LOG_MAX_JSON_CHARS),
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
# SQL: Thread selection
# ---------------------------------------------------------

def _debug_log_enrichment_state(cnx: pyodbc.Connection, force: bool) -> None:
    cur = cnx.cursor()
    cur.execute(
        """
        SELECT COUNT(*) AS eligible_rows
        FROM dbo.thread_enrichment
        WHERE (? = 1 OR CatalogCheckedUtc IS NULL)
          AND product IS NOT NULL AND LTRIM(RTRIM(product)) <> ''
          AND signature_text IS NOT NULL AND LTRIM(RTRIM(signature_text)) <> ''
          AND (classification IS NULL OR classification NOT IN ('emergent_issue', 'not_usable', 'learn_microsoft'))
          AND product <> 'Other'
          AND ISNULL(solution_usefulness, 0.0) >= 0.4
        """,
        1 if force else 0,
    )
    eligible = int(cur.fetchone()[0])
    logging.warning("phase1b eligible_rows(force=%s)=%d", force, eligible)


def _log_db_identity(cnx: pyodbc.Connection) -> None:
    cur = cnx.cursor()
    cur.execute("SELECT @@SERVERNAME AS server_name, DB_NAME() AS database_name")
    row = cur.fetchone()
    logging.warning("phase1b SQL identity: server=%s db=%s", row[0], row[1])


def fetch_next_common_product(cnx: pyodbc.Connection, force: bool, exclude_products: Set[str]) -> Optional[str]:
    """
    Choose the next product to process by looking at the newest eligible row.
    Excluding products prevents the loop from re-picking the same product when force=1
    or when one product dominates the newest rows.
    """
    cur = cnx.cursor()

    if exclude_products:
        placeholders = ",".join("?" for _ in exclude_products)
        cur.execute(
            f"""
            SELECT TOP (1) product
            FROM dbo.thread_enrichment
            WHERE (? = 1 OR CatalogCheckedUtc IS NULL)
              AND product IS NOT NULL AND LTRIM(RTRIM(product)) <> '' AND product <> 'Other'
              AND signature_text IS NOT NULL AND LTRIM(RTRIM(signature_text)) <> ''
              AND (classification IS NULL OR classification NOT IN ('emergent_issue', 'not_usable', 'learn_microsoft'))
              AND ISNULL(solution_usefulness, 0.0) >= 0.4
              AND product NOT IN ({placeholders})
            ORDER BY ingested_at DESC, thread_id DESC
            """,
            1 if force else 0,
            *[p for p in exclude_products],
        )
    else:
        cur.execute(
            """
            SELECT TOP (1) product
            FROM dbo.thread_enrichment
            WHERE (? = 1 OR CatalogCheckedUtc IS NULL)
              AND product IS NOT NULL AND LTRIM(RTRIM(product)) <> '' AND product <> 'Other'
              AND signature_text IS NOT NULL AND LTRIM(RTRIM(signature_text)) <> ''
              AND (classification IS NULL OR classification NOT IN ('emergent_issue', 'not_usable', 'learn_microsoft'))
              AND ISNULL(solution_usefulness, 0.0) >= 0.4
            ORDER BY ingested_at DESC, thread_id DESC
            """,
            1 if force else 0,
        )

    row = cur.fetchone()
    if not row:
        return None
    return str(row[0] or "").strip() or None


def fetch_unclustered_enrichments(
    cnx: pyodbc.Connection,
    limit: int,
    force: bool,
    cursor_ingested_at: Optional[Any] = None,
    cursor_thread_id: Optional[str] = None,
    product_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Phase 1B: COMMON-ONLY catalog build. Excludes emergent issue threads.
    Uses keyset pagination on (ingested_at DESC, thread_id DESC) so that force=1
    does not re-read the same TOP rows each batch.

    If product_filter is provided, the page is homogeneous for that product.
    """
    cursor = cnx.cursor()
    prod = (product_filter or "").strip() or None

    if cursor_ingested_at is None or cursor_thread_id is None:
        if prod is None:
            cursor.execute(
                """
                SELECT TOP (?)
                    thread_id,
                    product,
                    topic_cluster_key,
                    scenario_cluster_key,
                    variant_cluster_key,
                    resolution_leaf_key,
                    signature_text,
                    resolution_signature_text,
                    solution_usefulness,
                    ingested_at
                FROM dbo.thread_enrichment
                WHERE (? = 1 OR CatalogCheckedUtc IS NULL)
                  AND product IS NOT NULL AND LTRIM(RTRIM(product)) <> '' AND product <> 'Other'
                  AND signature_text IS NOT NULL AND LTRIM(RTRIM(signature_text)) <> ''
                  AND (classification IS NULL OR classification NOT IN ('emergent_issue', 'not_usable', 'learn_microsoft'))
                  AND ISNULL(solution_usefulness, 0.0) >= 0.4
                ORDER BY ingested_at DESC, thread_id DESC
                """,
                int(limit),
                1 if force else 0,
            )
        else:
            cursor.execute(
                """
                SELECT TOP (?)
                    thread_id,
                    product,
                    topic_cluster_key,
                    scenario_cluster_key,
                    variant_cluster_key,
                    resolution_leaf_key,
                    signature_text,
                    resolution_signature_text,
                    solution_usefulness,
                    ingested_at
                FROM dbo.thread_enrichment
                WHERE (? = 1 OR CatalogCheckedUtc IS NULL)
                  AND product = ?
                  AND signature_text IS NOT NULL AND LTRIM(RTRIM(signature_text)) <> ''
                  AND (classification IS NULL OR classification NOT IN ('emergent_issue', 'not_usable', 'learn_microsoft'))
                  AND ISNULL(solution_usefulness, 0.0) >= 0.4
                ORDER BY ingested_at DESC, thread_id DESC
                """,
                int(limit),
                1 if force else 0,
                prod,
            )
    else:
        if prod is None:
            cursor.execute(
                """
                SELECT TOP (?)
                    thread_id,
                    product,
                    topic_cluster_key,
                    scenario_cluster_key,
                    variant_cluster_key,
                    resolution_leaf_key,
                    signature_text,
                    resolution_signature_text,
                    solution_usefulness,
                    ingested_at
                FROM dbo.thread_enrichment
                WHERE (? = 1 OR CatalogCheckedUtc IS NULL)
                  AND product IS NOT NULL AND LTRIM(RTRIM(product)) <> '' AND product <> 'Other'
                  AND signature_text IS NOT NULL AND LTRIM(RTRIM(signature_text)) <> ''
                  AND (classification IS NULL OR classification NOT IN ('emergent_issue', 'not_usable', 'learn_microsoft'))
                  AND ISNULL(solution_usefulness, 0.0) >= 0.4
                  AND (
                        ingested_at < ?
                     OR (ingested_at = ? AND thread_id < ?)
                  )
                ORDER BY ingested_at DESC, thread_id DESC
                """,
                int(limit),
                1 if force else 0,
                cursor_ingested_at,
                cursor_ingested_at,
                str(cursor_thread_id),
            )
        else:
            cursor.execute(
                """
                SELECT TOP (?)
                    thread_id,
                    product,
                    topic_cluster_key,
                    scenario_cluster_key,
                    variant_cluster_key,
                    resolution_leaf_key,
                    signature_text,
                    resolution_signature_text,
                    solution_usefulness,
                    ingested_at
                FROM dbo.thread_enrichment
                WHERE (? = 1 OR CatalogCheckedUtc IS NULL)
                  AND product = ?
                  AND signature_text IS NOT NULL AND LTRIM(RTRIM(signature_text)) <> ''
                  AND (classification IS NULL OR classification NOT IN ('emergent_issue', 'not_usable', 'learn_microsoft'))
                  AND ISNULL(solution_usefulness, 0.0) >= 0.4
                  AND (
                        ingested_at < ?
                     OR (ingested_at = ? AND thread_id < ?)
                  )
                ORDER BY ingested_at DESC, thread_id DESC
                """,
                int(limit),
                1 if force else 0,
                prod,
                cursor_ingested_at,
                cursor_ingested_at,
                str(cursor_thread_id),
            )

    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def mark_catalog_checked(cnx: pyodbc.Connection, thread_ids: List[str]) -> None:
    if not thread_ids:
        return
    cursor = cnx.cursor()
    placeholders = ",".join("?" for _ in thread_ids)
    cursor.execute(
        f"""
        UPDATE dbo.thread_enrichment
        SET CatalogCheckedUtc = SYSUTCDATETIME()
        WHERE thread_id IN ({placeholders})
        """,
        *[str(t) for t in thread_ids],
    )


def fetch_area_path_catalog(cnx: pyodbc.Connection, max_rows: int = 8000) -> List[Dict[str, Any]]:
    cursor = cnx.cursor()
    cursor.execute(
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
        WHERE cluster_level IN (1,2,3,4) AND is_active = 1
        ORDER BY product, cluster_level, member_count DESC, last_seen_at DESC
        """,
        int(max_rows),
    )
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


# ---------------------------------------------------------
# Helpers: normalization + ids
# ---------------------------------------------------------

def _catalog_window(catalog_rows: List[Dict[str, Any]], start: int, size: int) -> List[Dict[str, Any]]:
    """
    Non-mutating circular window over catalog_rows.
    Prevents cross-product interference caused by rotating the shared catalog list.
    """
    if not catalog_rows or size <= 0:
        return []
    n = len(catalog_rows)
    start = start % n
    end = start + min(size, n)
    if end <= n:
        return catalog_rows[start:end]
    return catalog_rows[start:] + catalog_rows[: (end - n)]


def _should_apply_keep_list(keep_ids: set, prior_count: int) -> bool:
    """
    keep_candidate_ids is often incomplete. Only treat it as authoritative
    when it appears to include most/all prior candidates.
    """
    if prior_count <= 0 or not keep_ids:
        return False
    # Apply if model kept almost everything (configurable). Default 85%.
    return len(keep_ids) >= int(prior_count * _P1B_KEEP_IDS_MIN_FRACTION)


def _norm_key(k: str) -> str:
    k = (k or "").strip().lower()
    k = k.replace(" ", "-")
    return k[:360]


def _norm_level(v: Any) -> int:
    if v is None:
        return 0
    if isinstance(v, int):
        return v if v in (1, 2, 3, 4) else 0
    if isinstance(v, float):
        i = int(v)
        return i if i in (1, 2, 3, 4) else 0

    s = str(v).strip().lower()
    if not s:
        return 0
    if s in ("topic", "l1", "level1", "1"):
        return 1
    if s in ("scenario", "l2", "level2", "2"):
        return 2
    if s in ("variant", "l3", "level3", "3"):
        return 3
    if s in ("leaf", "l4", "level4", "4"):
        return 4

    digits = "".join(ch for ch in s if ch.isdigit())
    if digits:
        i = int(digits)
        return i if i in (1, 2, 3, 4) else 0
    return 0


def _candidate_id(prod: str, lvl: int, topic_key: str, scenario_key: str, variant_key: str, leaf_key: str) -> str:
    raw = f"{prod}|{lvl}|{topic_key}|{scenario_key}|{variant_key}|{leaf_key}".lower().strip()
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _most_common_product(rows: List[Dict[str, Any]]) -> str:
    counts: Dict[str, int] = {}
    for r in rows:
        p = str(r.get("product") or "").strip()
        if not p:
            continue
        counts[p] = counts.get(p, 0) + 1
    if not counts:
        return "Other"
    return max(counts.items(), key=lambda kv: kv[1])[0]


def _catalog_l1_keys_for_product(catalog_rows: List[Dict[str, Any]], product: str) -> Set[str]:
    prod = (product or "").strip()
    if not prod:
        return set()
    return {
        str(r.get("cluster_key") or "").strip().lower()
        for r in catalog_rows
        if str(r.get("product") or "").strip() == prod and int(r.get("cluster_level") or 0) == 1
    }


# ---------------------------------------------------------
# Catalog slicing for Nano
# ---------------------------------------------------------

def _select_catalog_slice_for_threads(
    threads: List[Dict[str, Any]],
    catalog_rows: List[Dict[str, Any]],
    catalog_limit: int,
) -> List[Dict[str, Any]]:
    want = {str(t.get("product") or "").strip() for t in threads}
    want = {p for p in want if p}

    primary = [r for r in catalog_rows if str(r.get("product") or "").strip() in want]
    return primary[:catalog_limit]


def _build_slim_catalog_slice(catalog_rows: List[Dict[str, Any]], capped_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_id = {int(r["cluster_id"]): r for r in catalog_rows if r.get("cluster_id") is not None}

    slim_catalog: List[Dict[str, Any]] = []
    for r in capped_rows:
        parent_id = r.get("parent_cluster_id")
        parent_key = None
        if parent_id is not None and int(parent_id) in by_id:
            parent_key = by_id[int(parent_id)]["cluster_key"]

        slim_catalog.append(
            {
                "product": r.get("product"),
                "level": int(r.get("cluster_level")),
                "key": r.get("cluster_key"),
                "parent_key": parent_key,
                "sig": (r.get("cluster_signature_text") or "")[:160],
                "res_sig": (r.get("resolution_signature_text") or "")[:160],
            }
        )
    return slim_catalog


# ---------------------------------------------------------
# Synthesize L4 leaf candidates from enrichment hints
# ---------------------------------------------------------

def _synthesize_leaf_candidates_from_hints(
    batch_rows: List[Dict[str, Any]],
    product: str,
    existing_candidates: Dict[str, Dict[str, Any]],
    catalog_rows: List[Dict[str, Any]],
) -> int:
    """
    Create L4 leaf candidates directly from enrichment hint data (resolution_leaf_key,
    variant_cluster_key, etc.) for threads that have resolution info.

    This ensures L4 nodes get into the catalog even when the LLM doesn't propose them.
    Only creates leaves that attach to existing or proposed L3 variants.
    Returns count of synthesized candidates added.
    """
    added = 0
    existing_l1 = _catalog_l1_keys_for_product(catalog_rows, product)

    # Also consider L1s that are already in the candidate pool
    candidate_l1 = {
        str(c.get("topic_key") or "").strip().lower()
        for c in existing_candidates.values()
        if int(c.get("level") or 0) == 1
    }
    all_l1 = existing_l1 | candidate_l1

    # Deduplicate by leaf_key to avoid creating one leaf per thread
    seen_leaf_keys: Set[str] = set()

    for t in batch_rows:
        res_leaf_key = _norm_key(t.get("resolution_leaf_key") or "")
        variant_key = _norm_key(t.get("variant_cluster_key") or "")
        scenario_key = _norm_key(t.get("scenario_cluster_key") or "")
        topic_key = _norm_key(t.get("topic_cluster_key") or "")
        res_sig = str(t.get("resolution_signature_text") or "").strip()
        sig = str(t.get("signature_text") or "").strip()

        # Need at minimum: topic_key, scenario_key, variant_key, and leaf_key
        if not topic_key or not scenario_key or not variant_key or not res_leaf_key:
            continue

        # Skip if topic not in existing or proposed L1
        if topic_key not in all_l1:
            continue

        # Skip duplicates within this batch
        if res_leaf_key in seen_leaf_keys:
            continue
        seen_leaf_keys.add(res_leaf_key)

        # Use resolution_signature_text, fall back to signature_text
        leaf_res_sig = res_sig if res_sig else sig
        if not leaf_res_sig:
            continue

        # Check if we already have this leaf candidate
        cid = _candidate_id(product, 4, topic_key, scenario_key, variant_key, res_leaf_key)
        if cid in existing_candidates:
            continue

        existing_candidates[cid] = {
            "candidate_id": cid,
            "product": product,
            "level": 4,
            "topic_key": topic_key,
            "scenario_key": scenario_key,
            "variant_key": variant_key,
            "leaf_key": res_leaf_key,
            "signature_text": sig[:300] if sig else f"leaf: {res_leaf_key}",
            "resolution_signature_text": leaf_res_sig[:300],
        }
        added += 1

    return added


# ---------------------------------------------------------
# Nano calls
# ---------------------------------------------------------

def call_nano_catalog_propose(
    client: AzureOpenAI,
    deployment: str,
    threads: List[Dict[str, Any]],
    catalog_slice: List[Dict[str, Any]],
    slice_index: int,
    slices_total: int,
) -> Dict[str, Any]:
    system_prompt = (
        "You maintain a 4-level COMMON-ISSUE catalog (NOT emergent incidents/outages).\n"
        "\n"

        "CATALOG LEVELS (STRICT):\n"
        "L1 Topic    = durable technical subsystem or support domain (short, broad areas like custom-domain, networking, identity, deployment).\n"
        "L2 Scenario = recurring failure mode, support situation, process, operation, or symptom family within the topic.\n"
        "L3 Variant  = stable diagnostic branch under a Scenario, typically distinguished by an error/signal token, environment condition, or branching clue.\n"
        "L4 Leaf     = concrete symptom-to-resolution pattern under a Variant.\n"
        "Lower levels must always be more specific than their parents.\n"
        "\n"

        "LEVEL DECISION RULES (CRITICAL):\n"
        "- L1 Topic = the TECHNICAL AREA or SERVICE COMPONENT within the product where the issue occurs.\n"
        "  Think: 'custom-domain', 'networking', 'authentication', 'developer-portal', 'policy', 'migration'.\n"
        "  A topic must be broad enough to contain at least 3-5 distinct failure modes as scenarios.\n"
        "  NEVER include the product name or product alias in the topic key.\n"
        "  The product is stored separately — do NOT repeat it.\n"
        "  BAD L1 for product=APIM: 'api-management-custom-domain', 'apim-networking'\n"
        "  GOOD L1 for product=APIM: 'custom-domain', 'networking'\n"
        "  BAD L1 for product=Static Web Apps: 'static-web-apps-custom-domain'\n"
        "  GOOD L1 for product=Static Web Apps: 'custom-domain'\n"
        "- Do NOT create L1 for a single operation, single error, single workaround, or narrow edge case.\n"
        "- Do NOT create L1 whose key is the product name, product alias, or product slug.\n"
        "- L2 Scenario key format: <operation-or-failure-mode>[-<constraint>]\n"
        "  It must NOT start with the topic key repeated, and must NOT contain the product name.\n"
        "  BAD: scenario_key='custom-domain-validation-stuck' under topic='custom-domain' (repeats topic)\n"
        "  GOOD: scenario_key='validation-stuck' under topic='custom-domain'\n"
        "- L3 Variant must add a distinctive diagnostic signal token. Must NOT equal scenario_key.\n"
        "- Prefer fewer, stronger L1 Topics. When uncertain, nest narrower items under existing broader topics.\n"
        "- A candidate should only be L1 if a support engineer would navigate to it before knowing the exact symptom.\n\n"
        "\n"

        "YOU WILL BE SHOWN THE EXISTING CATALOG IN MULTIPLE SLICES.\n"
        "Do NOT assume the slice contains everything.\n"
        f"This is slice {slice_index} of {slices_total}.\n"
        "Input includes forum threads which must be mapped into catalog nodes.\n"
        "\n"

        "PARENT-FIT / REUSE RULES (CRITICAL):\n"
        "- Prefer reuse of existing nodes in the provided slice.\n"
        "- Before proposing a new Topic, check whether threads fit naturally under an existing Topic.\n"
        "- If the difference is mainly operation, symptom, or error token, it likely belongs under an existing Topic as L2/L3/L4.\n"
        "- Avoid parallel nodes that differ only by wording or minor context.\n"
        "\n"

        "PRODUCT RULES (STRICT):\n"
        "- All candidates MUST use product exactly matching threads[].product.\n"
        "- Do NOT invent or switch products.\n"
        "- Do NOT encode product into keys (no '<product>-...' prefixes).\n"
        "\n"

        "KEY RULES (STRICT):\n"
        "- kebab-case\n"
        "- stable and reusable\n"
        "- do NOT include tenant IDs, GUIDs, timestamps, or thread-specific values\n"
        "- keep keys concise\n"
        "- max 120 characters per key segment\n"
        "- topic_key must be short (<= 32 chars)\n"
        "- topic_key must NOT equal product name\n"
        "- scenario_key must NOT equal topic_key\n"
        "- variant_key must NOT equal scenario_key\n"
        "- scenario_key/variant_key/leaf_key MUST NOT contain the product name or product slug\n"
        "\n"

        "PARENT-CHAIN RULE (CRITICAL):\n"
        "- If proposing any L4, you MUST also propose or attach its L3, L2, and L1 parents.\n"
        "- If proposing any L3, you MUST also propose or attach its L2 and L1 parents.\n"
        "- If proposing any L2, you MUST also propose or attach its L1 parent.\n"
        "- Parentless nodes are invalid.\n"
        "\n"

        "L4 LEAF CREATION RULES (IMPORTANT):\n"
        "- L4 Leaf nodes represent specific fix/resolution patterns.\n"
        "- Threads with resolution_signature_text containing 'rc:' and 'fix:' (not 'unknown') SHOULD usually generate L4 leaves.\n"
        "- hints.resolution_leaf_key provides suggested leaf guidance.\n"
        "- Leaf format for resolved threads: '<variant_key>__rc-<rootcause>__fix-<fix>'\n"
        "- Group similar fixes into a single leaf.\n"
        "- Do NOT create one leaf per thread unless the fixes differ.\n"
        "- L4 leaves REQUIRE resolution_signature_text.\n"
        "\n"

        "CONSERVATIVE PROPOSAL RULES:\n"
        "- Prefer reuse of existing nodes.\n"
        "- Prefer fewer, broader nodes that cover multiple threads.\n"
        "- Avoid thread-specific wording.\n"
        "- BUT propose L4 leaves when clear resolution patterns exist.\n"
        "\n"

        "OUTPUT FORMAT (JSON ONLY):\n"
        "{ \"candidates\": [ ... ], \"notes\": \"...\" }\n"
        "\n"

        "CANDIDATE SCHEMA:\n"
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
        "\n"

        "REQUIRED FIELDS BY LEVEL:\n"
        "- L1: product, level=1, topic_key, signature_text\n"
        "- L2: product, level=2, topic_key, scenario_key, signature_text\n"
        "- L3: product, level=3, topic_key, scenario_key, variant_key, signature_text\n"
        "- L4: product, level=4, topic_key, scenario_key, variant_key, leaf_key, signature_text, resolution_signature_text\n"
    )

    slim_threads = [
        {
            "thread_id": t.get("thread_id"),
            "product": t.get("product"),
            "signature_text": (t.get("signature_text") or "")[:700],
            "resolution_signature_text": (t.get("resolution_signature_text") or "")[:700],
            "hints": {
                "topic_cluster_key": t.get("topic_cluster_key"),
                "scenario_cluster_key": t.get("scenario_cluster_key"),
                "variant_cluster_key": t.get("variant_cluster_key"),
                "resolution_leaf_key": t.get("resolution_leaf_key"),
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

    # Use Mini for propose (better key quality + multi-constraint reasoning)
    mini_client = make_mini_client()
    mini_deployment = get_mini_deployment()

    user_content = json.dumps(user_payload, ensure_ascii=False)
    resp = call_aoai_with_retry(
        mini_client,
        model=mini_deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        response_format={"type": "json_object"},
        estimated_prompt_tokens=estimate_tokens(system_prompt) + estimate_tokens(user_content),
        rate_limiter=get_rate_limiter("mini"),
        caller_tag="phase1b_propose",
    )

    resp_text = resp.choices[0].message.content or ""
    parsed = json.loads(resp_text)

    _log_nano_response("propose", resp_text, parsed)
    return parsed


def call_nano_catalog_refine(
    client: AzureOpenAI,
    deployment: str,
    threads: List[Dict[str, Any]],
    catalog_slice: List[Dict[str, Any]],
    prior_candidates: List[Dict[str, Any]],
    slice_index: int,
    slices_total: int,
) -> Dict[str, Any]:
    system_prompt = (
        "You are maintaining a 4-level COMMON-ISSUE catalog (NOT emergent incidents).\n"
        "You are shown the existing catalog in MULTIPLE SLICES across calls.\n\n"
        f"This is slice {slice_index} of {slices_total}.\n\n"
        "Task for THIS call:\n"
        "1) Review the provided catalog slice.\n"
        "2) Review the current candidate pool.\n"
        "3) Drop any candidate that is not needed because an existing node in this slice already covers it.\n"
        "4) You may add new candidates if this slice reveals gaps.\n\n"
        "LEVEL SEMANTICS (STRICT):\n"
        "- Product is already provided in threads[].product and in the catalog row's product field.\n"
        "  Do NOT encode product again into keys.\n"
        "- topic_key: component/family only.\n"
        "- scenario_key: topic + context/operation/symptom. MUST NOT equal topic_key.\n"
        "- variant_key: add a distinctive error/signal token. MUST NOT equal scenario_key.\n"
        "- leaf_key: specific fix pattern; short.\n\n"
        "Drop rules (STRICT):\n"
        "- If an existing node in this slice is semantically suitable, DROP the candidate.\n"
        "- If two candidates are near-duplicates, drop the weaker one and keep the better, more general key.\n\n"
        "Output JSON only:\n"
        "{\n"
        "  \"keep_candidate_ids\": [\"...\"],\n"
        "  \"drop\": [{\"candidate_id\":\"...\",\"reason\":\"...\"}],\n"
        "  \"add\": [ {candidate...} ],\n"
        "  \"notes\": \"...\"\n"
        "}\n"
    )

    slim_threads = [
        {
            "thread_id": t.get("thread_id"),
            "product": t.get("product"),
            "signature_text": (t.get("signature_text") or "")[:700],
            "resolution_signature_text": (t.get("resolution_signature_text") or "")[:700],
        }
        for t in threads
    ]

    # Strip sig/res_sig from catalog slice — refine only needs keys for dedup/keep/drop decisions
    slim_catalog_slice = [
        {
            "product": r.get("product"),
            "level": r.get("level"),
            "key": r.get("key"),
            "parent_key": r.get("parent_key"),
        }
        for r in catalog_slice
    ]

    user_payload = {
        "slice_index": slice_index,
        "slices_total": slices_total,
        "threads": slim_threads,
        "existing_catalog_slice": slim_catalog_slice,
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

    # Refine stays on Nano (simpler keep/drop task)
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
        caller_tag="phase1b_refine",
    )

    resp_text = resp.choices[0].message.content or ""
    parsed = json.loads(resp_text)

    _log_nano_response("refine", resp_text, parsed)
    return parsed


# ---------------------------------------------------------
# Bulk DB: get-or-create nodes (correct parent linkage)
# ---------------------------------------------------------

def bulk_get_or_create_issue_clusters(
    cnx: pyodbc.Connection,
    nodes: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Bulk get-or-create issue_cluster rows for a set of nodes in ONE round-trip.

    Product-aware behavior:
      - Parent resolution is within SAME product.
      - Existence checks are within SAME product.
      - Return join is within SAME product.
    """
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
        res_sig_s = str(res_sig).strip() if res_sig is not None else None

        if not prod or lvl not in (1, 2, 3, 4) or not key:
            continue

        if lvl == 1:
            pkey_s = None
        elif not pkey_s:
            continue

        norm.append(
            {
                "product": prod,
                "level": lvl,
                "key": key,
                "parent_key": pkey_s,
                "signature_text": sig,
                "resolution_signature_text": res_sig_s,
            }
        )

    if not norm:
        return []

    values_sql = ",".join("(?,?,?,?,?,?)" for _ in norm)
    params: List[Any] = []
    for n in norm:
        params.extend(
            [
                int(n["level"]),
                n["product"],
                n["key"],
                n["parent_key"],
                n["signature_text"],
                n["resolution_signature_text"],
            ]
        )

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

        ------------------------------------------------------------------
        -- Resolve parent IDs (product-aware + lane-isolated)
        ------------------------------------------------------------------
        INSERT INTO @resolved (rownum, lvl, prod, k, parent_key, parent_id, sig, res_sig)
        SELECT
            i.rownum,
            i.lvl,
            i.prod,
            i.k,
            i.parent_k AS parent_key,
            CASE
                WHEN i.lvl = 1 THEN NULL
                ELSE COALESCE(p1.cluster_id, p0.cluster_id)
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
              AND p.created_by_phase = 'phase1b_catalog'
            ORDER BY p.cluster_id DESC
        ) p1
        OUTER APPLY (
            SELECT TOP 1 p.cluster_id
            FROM dbo.issue_cluster p
            WHERE p.is_active = 1
              AND p.product = i.prod
              AND p.cluster_level = i.lvl - 1
              AND p.cluster_key = i.parent_k
              AND p.created_by_phase IS NULL
            ORDER BY p.cluster_id DESC
        ) p0;

        ------------------------------------------------------------------
        -- Insert missing rows (product-aware)
        ------------------------------------------------------------------
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
                'phase1b_catalog'
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

        ------------------------------------------------------------------
        -- Touch/backfill (product-aware + lane-isolated)
        ------------------------------------------------------------------
        UPDATE ic
        SET
            last_seen_at = SYSUTCDATETIME(),
            cluster_signature_text = CASE
                WHEN (ic.cluster_signature_text IS NULL OR ic.cluster_signature_text = '')
                     AND (r.sig IS NOT NULL AND r.sig <> '') THEN r.sig
                ELSE ic.cluster_signature_text
            END,
            resolution_signature_text = CASE
                WHEN (ic.resolution_signature_text IS NULL OR ic.resolution_signature_text = '')
                     AND (r.res_sig IS NOT NULL AND r.res_sig <> '') THEN r.res_sig
                ELSE ic.resolution_signature_text
            END
        FROM dbo.issue_cluster ic
        JOIN @resolved r
          ON ic.is_active = 1
         AND ic.product = r.prod
         AND ic.cluster_level = r.lvl
         AND ic.cluster_key = r.k
        WHERE (r.lvl = 1 OR r.parent_id IS NOT NULL)
          AND (ic.created_by_phase = 'phase1b_catalog' OR ic.created_by_phase IS NULL);

        ------------------------------------------------------------------
        -- Return IDs (product-aware)
        ------------------------------------------------------------------
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
        *params,
    )

    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _ensure_parent_scaffolding(candidates: Dict[str, Dict[str, Any]]) -> None:
    """
    Ensure that for any L2/L3/L4 candidate, its parent chain exists in the candidate set.
    Note: this can manufacture L1s if topic_key is wrong, so upstream validation is required.
    """
    if not candidates:
        return

    items = list(candidates.values())

    for c in items:
        prod = str(c.get("product") or "").strip()
        lvl = int(c.get("level") or 0)

        topic_key = str(c.get("topic_key") or "").strip()
        scenario_key = str(c.get("scenario_key") or "").strip()
        variant_key = str(c.get("variant_key") or "").strip()

        if not prod or not topic_key:
            continue

        if lvl in (2, 3, 4):
            cid1 = _candidate_id(prod, 1, topic_key, "", "", "")
            candidates.setdefault(
                cid1,
                {
                    "candidate_id": cid1,
                    "product": prod,
                    "level": 1,
                    "topic_key": topic_key,
                    "scenario_key": "",
                    "variant_key": "",
                    "leaf_key": "",
                    "signature_text": f"{topic_key} (auto-parent)",
                    "resolution_signature_text": None,
                },
            )

        if lvl in (3, 4) and scenario_key:
            cid2 = _candidate_id(prod, 2, topic_key, scenario_key, "", "")
            candidates.setdefault(
                cid2,
                {
                    "candidate_id": cid2,
                    "product": prod,
                    "level": 2,
                    "topic_key": topic_key,
                    "scenario_key": scenario_key,
                    "variant_key": "",
                    "leaf_key": "",
                    "signature_text": f"{scenario_key} (auto-parent)",
                    "resolution_signature_text": None,
                },
            )

        if lvl == 4 and scenario_key and variant_key:
            cid3 = _candidate_id(prod, 3, topic_key, scenario_key, variant_key, "")
            candidates.setdefault(
                cid3,
                {
                    "candidate_id": cid3,
                    "product": prod,
                    "level": 3,
                    "topic_key": topic_key,
                    "scenario_key": scenario_key,
                    "variant_key": variant_key,
                    "leaf_key": "",
                    "signature_text": f"{variant_key} (auto-parent)",
                    "resolution_signature_text": None,
                },
            )


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
            out.append(
                {
                    "product": prod,
                    "level": 1,
                    "key": topic_key,
                    "parent_key": None,
                    "signature_text": sig,
                    "resolution_signature_text": None,
                }
            )
        elif lvl == 2:
            out.append(
                {
                    "product": prod,
                    "level": 2,
                    "key": scenario_key,
                    "parent_key": topic_key,
                    "signature_text": sig,
                    "resolution_signature_text": None,
                }
            )
        elif lvl == 3:
            out.append(
                {
                    "product": prod,
                    "level": 3,
                    "key": variant_key,
                    "parent_key": scenario_key,
                    "signature_text": sig,
                    "resolution_signature_text": None,
                }
            )
        elif lvl == 4:
            leaf_sig = sig or f"{prod} leaf: {leaf_key}"
            out.append(
                {
                    "product": prod,
                    "level": 4,
                    "key": leaf_key,
                    "parent_key": variant_key,
                    "signature_text": leaf_sig,
                    "resolution_signature_text": res_sig,
                }
            )

    seen = set()
    deduped: List[Dict[str, Any]] = []
    for n in out:
        tup = (n["product"], int(n["level"]), n["key"], n.get("parent_key") or "")
        if tup in seen:
            continue
        seen.add(tup)
        deduped.append(n)

    return deduped


# ---------------------------------------------------------
# In-memory catalog cache update
# ---------------------------------------------------------

def _append_inserted_to_catalog_rows(
    catalog_rows: List[Dict[str, Any]],
    inserted: List[Dict[str, Any]],
    by_level_key_parent: Dict[tuple, Dict[str, Any]],
) -> None:
    """
    Keep in-memory catalog usable for subsequent slices by preserving sig/res_sig from inserted payload.

    Supports inserted shapes:
      - bulk:   {product, level, key, parent_key, cluster_id, signature_text, resolution_signature_text}
      - legacy: {product, level, topic_key/scenario_key/variant_key/leaf_key, cluster_id, signature_text, resolution_signature_text}
    """
    def _add_row(row: Dict[str, Any]) -> None:
        k = (
            str(row.get("product") or ""),
            int(row.get("cluster_level") or 0),
            str(row.get("cluster_key") or ""),
            row.get("parent_cluster_id"),
        )
        if k in by_level_key_parent:
            return
        by_level_key_parent[k] = row
        catalog_rows.append(row)

    id_by_level_key_parent = {
        (
            str(r.get("product") or ""),
            int(r.get("cluster_level") or 0),
            str(r.get("cluster_key") or ""),
            r.get("parent_cluster_id"),
        ): int(r["cluster_id"])
        for r in catalog_rows
        if r.get("cluster_id") is not None
    }

    id_by_level_key_any_parent: Dict[Tuple[str, int, str], int] = {}
    for r in catalog_rows:
        if r.get("cluster_id") is None:
            continue
        prod = str(r.get("product") or "")
        lvl = int(r.get("cluster_level") or 0)
        key = str(r.get("cluster_key") or "")
        if prod and lvl in (1, 2, 3, 4) and key:
            id_by_level_key_any_parent[(prod, lvl, key)] = int(r["cluster_id"])

    def _get_id(prod: str, lvl: int, key: str, parent_id: Optional[int]) -> Optional[int]:
        return id_by_level_key_parent.get((prod, lvl, key, parent_id))

    def _get_id_any_parent(prod: str, lvl: int, key: str) -> Optional[int]:
        return id_by_level_key_any_parent.get((prod, lvl, key))

    for ins in inserted:
        prod = str(ins.get("product") or "").strip()
        lvl = _norm_level(ins.get("level"))
        cid = ins.get("cluster_id")
        if not prod or lvl not in (1, 2, 3, 4) or cid is None:
            continue

        sig = (ins.get("signature_text") or "").strip()
        res_sig = ins.get("resolution_signature_text")
        if isinstance(res_sig, str):
            res_sig = res_sig.strip() or None

        if "key" in ins:
            key = str(ins.get("key") or "").strip()
            parent_key = ins.get("parent_key")
            parent_key_s = str(parent_key).strip() if parent_key is not None else None

            if not key:
                continue

            if lvl == 1:
                parent_id = None
            else:
                parent_id = _get_id_any_parent(prod, lvl - 1, parent_key_s or "")
                if parent_id is None:
                    continue

            _add_row(
                {
                    "cluster_id": int(cid),
                    "cluster_level": int(lvl),
                    "product": prod,
                    "cluster_key": key,
                    "parent_cluster_id": int(parent_id) if parent_id is not None else None,
                    "cluster_signature_text": sig,
                    "resolution_signature_text": res_sig,
                }
            )

            id_by_level_key_parent[(prod, lvl, key, int(parent_id) if parent_id is not None else None)] = int(cid)
            id_by_level_key_any_parent[(prod, lvl, key)] = int(cid)
            continue

        if lvl == 1:
            key = str(ins.get("topic_key") or "")
            parent_id = None
        elif lvl == 2:
            key = str(ins.get("scenario_key") or "")
            topic_key = str(ins.get("topic_key") or "")
            parent_id = _get_id(prod, 1, topic_key, None)
        elif lvl == 3:
            key = str(ins.get("variant_key") or "")
            scenario_key = str(ins.get("scenario_key") or "")
            topic_key = str(ins.get("topic_key") or "")
            topic_id = _get_id(prod, 1, topic_key, None)
            parent_id = _get_id(prod, 2, scenario_key, topic_id)
        else:
            key = str(ins.get("leaf_key") or "")
            variant_key = str(ins.get("variant_key") or "")
            scenario_key = str(ins.get("scenario_key") or "")
            topic_key = str(ins.get("topic_key") or "")
            topic_id = _get_id(prod, 1, topic_key, None)
            scenario_id = _get_id(prod, 2, scenario_key, topic_id)
            parent_id = _get_id(prod, 3, variant_key, scenario_id)

        _add_row(
            {
                "cluster_id": int(cid),
                "cluster_level": int(lvl),
                "product": prod,
                "cluster_key": key,
                "parent_cluster_id": int(parent_id) if parent_id is not None else None,
                "cluster_signature_text": sig,
                "resolution_signature_text": res_sig,
            }
        )

        id_by_level_key_parent[(prod, lvl, key, int(parent_id) if parent_id is not None else None)] = int(cid)
        id_by_level_key_any_parent[(prod, lvl, key)] = int(cid)

# ---------------------------------------------------------
# Product alias normalization for key validation
# ---------------------------------------------------------

# Map from product name (as stored in DB) to its known aliases/slugs
# that must NOT appear in cluster keys.
_PRODUCT_KEY_ALIASES: Dict[str, List[str]] = {
    "APIM": ["api-management", "apim", "azure-api-management"],
    "Static Web Apps": ["static-web-apps", "swa", "staticwebapp"],
    "Web Apps": ["web-apps", "webapp", "app-service-web"],
    "App Service": ["app-service", "appservice"],
    "Functions": ["functions", "azure-functions", "function-app"],
    "Redis": ["redis", "azure-cache-redis", "redis-cache"],
    "Storage Account": ["storage-account", "storage", "azure-storage"],
    "Logic Apps": ["logic-apps", "logicapps", "logic-app"],
    "Service Bus": ["service-bus", "servicebus"],
    "Event Grid": ["event-grid", "eventgrid"],
    "Event Hub": ["event-hub", "eventhub"],
    "DevOps": ["devops", "azure-devops"],
    "VNet": ["vnet", "virtual-network"],
    "Monitor": ["monitor", "azure-monitor"],
    "Ai Search": ["ai-search", "cognitive-search", "azure-search"],
    "ARM": ["arm", "resource-manager", "azure-resource-manager"],
}


def _get_product_aliases(product: str) -> List[str]:
    """Return all known key-slug aliases for a product that must not appear in cluster keys."""
    prod = (product or "").strip()
    direct = _PRODUCT_KEY_ALIASES.get(prod, [])
    # Also add a naive slug of the product name itself
    naive_slug = prod.lower().replace(" ", "-").replace("_", "-")
    return list({naive_slug} | set(direct))


def _key_contains_product_alias(key: str, product: str) -> bool:
    """Return True if key starts with or contains a product alias as a prefix segment."""
    key_lower = (key or "").strip().lower()
    for alias in _get_product_aliases(product):
        # Check if key starts with alias (as a prefix segment: alias- or alias exactly)
        if key_lower == alias or key_lower.startswith(alias + "-"):
            return True
    return False


def _strip_product_prefix(key: str, product: str) -> str:
    """
    Attempt to strip a leading product alias from a key.
    e.g. 'api-management-custom-domain' with product='APIM' -> 'custom-domain'
    Returns the stripped key, or the original if no alias matched.
    """
    key_lower = (key or "").strip().lower()
    for alias in sorted(_get_product_aliases(product), key=len, reverse=True):
        prefix = alias + "-"
        if key_lower.startswith(prefix):
            stripped = key[len(prefix):]
            if stripped:
                return stripped
    return key

# ---------------------------------------------------------
# Function Entry Point
# ---------------------------------------------------------

def run_phase1b_cluster(req: func.HttpRequest) -> func.HttpResponse:
    """
    Phase 1B common catalog maintainer:
      - Excludes emergent issues: classification <> 'emergent_issue'
      - Uses Nano across multiple catalog slices:
          propose candidates on slice 1
          refine candidate pool over slices 2..N
          insert only final candidates
      - Uses bulk get-or-create for DB efficiency.
    """
    try:
        limit = int(req.params.get("limit", "100000"))       # default: process all eligible
        batch_size = int(req.params.get("batch_size", "10")) # rows per Nano call
        max_batches = int(req.params.get("max_batches", "0"))
        force = req.params.get("force", "0") == "1"

        slice_limit = int(req.params.get("slice_limit", "800"))
        max_slices = int(req.params.get("max_slices", "4"))

        limit = max(1, min(limit, 100000))
        batch_size = max(1, min(batch_size, 25))
        slice_limit = max(200, min(slice_limit, 2500))
        max_slices = max(1, min(max_slices, 16))  # raised from 8 → 16

        if max_batches <= 0:
            max_batches = (limit + batch_size - 1) // batch_size
        max_batches = max(1, min(max_batches, 200))

        client = make_aoai_client()
        deployment = get_nano_deployment()

        processed_total = 0
        nano_calls = 0

        inserted_total: List[Dict[str, Any]] = []
        skipped_total: List[Dict[str, Any]] = []
        dropped_total: List[Dict[str, Any]] = []
        synthesized_total = 0

        with sql_connect() as cnx:
            _log_db_identity(cnx)
            _debug_log_enrichment_state(cnx, force)
            catalog_rows = fetch_area_path_catalog(cnx, max_rows=8000)
            by_level_key_parent: Dict[tuple, Dict[str, Any]] = {
                (str(r.get("product") or ""), int(r.get("cluster_level") or 0), str(r.get("cluster_key") or ""), r.get("parent_cluster_id")): r
                for r in catalog_rows
            }

            cursor_ingested_at = None
            cursor_thread_id = None

            chosen_product: Optional[str] = None
            seen_products: Set[str] = set()
            catalog_slice_start = 0

            for _ in range(max_batches):
                remaining = limit - processed_total
                if remaining <= 0:
                    break

                if chosen_product is None:
                    chosen_product = fetch_next_common_product(cnx, force, exclude_products=seen_products)
                    if not chosen_product:
                        break
                    cursor_ingested_at = None
                    cursor_thread_id = None
                    catalog_slice_start = 0
                    logging.warning("phase1b chosen_product=%s", chosen_product)

                rows = fetch_unclustered_enrichments(
                    cnx,
                    min(batch_size, remaining),
                    force,
                    cursor_ingested_at=cursor_ingested_at,
                    cursor_thread_id=cursor_thread_id,
                    product_filter=chosen_product,
                )

                if not rows:
                    logging.warning("phase1b product_drained=%s", chosen_product)
                    if chosen_product:
                        seen_products.add(chosen_product)
                    chosen_product = None
                    continue

                last = rows[-1]
                cursor_ingested_at = last.get("ingested_at")
                cursor_thread_id = str(last.get("thread_id") or "")

                logging.warning(
                    "phase1b batch: got=%d remaining=%d product=%s thread_ids=%s",
                    len(rows),
                    remaining,
                    chosen_product,
                    [str(r.get("thread_id")) for r in rows][:10],
                )

                rows_by_product: Dict[str, List[Dict[str, Any]]] = {}
                for r in rows:
                    prod = str(r.get("product") or "").strip() or "Other"
                    rows_by_product.setdefault(prod, []).append(r)

                inserted_batch: List[Dict[str, Any]] = []

                for batch_product, batch_rows in rows_by_product.items():
                    default_prod = batch_product
                    catalog_slice_start = 0

                    candidates: Dict[str, Dict[str, Any]] = {}

                    # Pre-filter catalog to this product only, so slice_limit
                    # applies to product-relevant rows rather than the full mixed catalog.
                    product_catalog = [
                        r for r in catalog_rows
                        if str(r.get("product") or "").strip() == default_prod
                    ]

                    effective_slices = 1 if len(product_catalog) <= slice_limit else max_slices
                    prev_candidate_keys: Optional[set] = None

                    for slice_index in range(1, effective_slices + 1):
                        # Window and slim directly over the product-scoped catalog
                        window = _catalog_window(product_catalog, start=catalog_slice_start, size=slice_limit)
                        slim_slice = _build_slim_catalog_slice(product_catalog, window)

                        if slice_index > 1 and len(product_catalog) > slice_limit:
                            catalog_slice_start = (catalog_slice_start + slice_limit) % max(1, len(product_catalog))

                        if slice_index == 1:
                            out = call_nano_catalog_propose(
                                client,
                                deployment,
                                threads=batch_rows,
                                catalog_slice=slim_slice,
                                slice_index=slice_index,
                                slices_total=effective_slices,
                            )
                            nano_calls += 1

                            proposed = out.get("candidates") or []

                            existing_l1 = _catalog_l1_keys_for_product(catalog_rows, default_prod)
                            proposed_l1 = {
                                _norm_key(n.get("topic_key") or "")
                                for n in proposed
                                if _norm_level(n.get("level")) == 1
                                and (str(n.get("product") or "").strip() in ("", default_prod))
                            }

                            for n in proposed:
                                prod = str(n.get("product") or "").strip() or default_prod

                                if prod != default_prod:
                                    skipped_total.append(
                                        {
                                            "product": prod,
                                            "level": n.get("level"),
                                            "why": f"cross_product_candidate_in_{default_prod}_batch",
                                        }
                                    )
                                    continue

                                lvl = _norm_level(n.get("level"))

                                topic_key = _norm_key(n.get("topic_key") or "")
                                scenario_key = _norm_key(n.get("scenario_key") or "")
                                variant_key = _norm_key(n.get("variant_key") or "")
                                leaf_key = _norm_key(n.get("leaf_key") or "")

                                # --- Auto-repair: strip leading product alias from all keys ---
                                topic_key_repaired = _strip_product_prefix(topic_key, prod)
                                scenario_key_repaired = _strip_product_prefix(scenario_key, prod)
                                variant_key_repaired = _strip_product_prefix(variant_key, prod)
                                leaf_key_repaired = _strip_product_prefix(leaf_key, prod)

                                # Log when repair changes something (helps tune Phase 1A prompt)
                                if topic_key_repaired != topic_key:
                                    logging.warning(
                                        "phase1b key_repair product=%s level=%d "
                                        "topic_key: '%s' -> '%s'",
                                        prod, lvl, topic_key, topic_key_repaired,
                                    )
                                if scenario_key_repaired != scenario_key:
                                    logging.warning(
                                        "phase1b key_repair product=%s level=%d "
                                        "scenario_key: '%s' -> '%s'",
                                        prod, lvl, scenario_key, scenario_key_repaired,
                                    )

                                topic_key = topic_key_repaired
                                scenario_key = scenario_key_repaired
                                variant_key = variant_key_repaired
                                leaf_key = leaf_key_repaired
                                # --- End auto-repair ---

                                # Reject if topic key is still a product alias after repair
                                if _key_contains_product_alias(topic_key, prod):
                                    skipped_total.append({
                                        "product": prod,
                                        "level": lvl,
                                        "why": "topic_key_is_product_alias",
                                        "topic_key": topic_key,
                                    })
                                    logging.warning(
                                        "phase1b rejected product_alias_topic product=%s "
                                        "topic_key='%s'",
                                        prod, topic_key,
                                    )
                                    continue

                                sig = (n.get("signature_text") or "").strip()
                                res_sig = (n.get("resolution_signature_text") or "").strip() if n.get("resolution_signature_text") else None

                                if not prod or lvl not in (1, 2, 3, 4):
                                    skipped_total.append({"product": prod, "level": lvl, "why": "bad_product_or_level"})
                                    continue

                                if lvl == 4 and not res_sig:
                                    res_sig = sig

                                if (lvl == 1 and not topic_key) or \
                                   (lvl == 2 and (not topic_key or not scenario_key)) or \
                                   (lvl == 3 and (not topic_key or not scenario_key or not variant_key)) or \
                                   (lvl == 4 and (not topic_key or not scenario_key or not variant_key or not leaf_key)):
                                    skipped_total.append({"product": prod, "level": lvl, "why": "missing_required_keys"})
                                    continue

                                if lvl in (2, 3, 4):
                                    if topic_key not in existing_l1 and topic_key not in proposed_l1:
                                        skipped_total.append(
                                            {
                                                "product": prod,
                                                "level": lvl,
                                                "why": "topic_key_not_in_existing_or_proposed_l1",
                                                "topic_key": topic_key,
                                            }
                                        )
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

                            if not candidates:
                                break

                            prev_candidate_keys = set(candidates.keys())

                        else:
                            # Skip refine slice if there are no catalog rows for this product
                            # in the current window — it would be a no-op Nano call.
                            if not slim_slice:
                                logging.warning(
                                    "phase1b skipping empty refine slice=%d/%d product=%s",
                                    slice_index, effective_slices, default_prod,
                                )
                                continue

                            prior_candidates = list(candidates.values())
                            out = call_nano_catalog_refine(
                                client,
                                deployment,
                                threads=batch_rows,
                                catalog_slice=slim_slice,
                                prior_candidates=prior_candidates,
                                slice_index=slice_index,
                                slices_total=effective_slices,
                            )
                            nano_calls += 1

                            keep_ids = set(str(x) for x in (out.get("keep_candidate_ids") or []) if x)
                            drops = out.get("drop") or []
                            adds = out.get("add") or []

                            for d in drops:
                                did = str(d.get("candidate_id") or "")
                                if did and did in candidates:
                                    dropped_total.append({"candidate_id": did, "reason": d.get("reason")})
                                    candidates.pop(did, None)

                            if _should_apply_keep_list(keep_ids, prior_count=len(prior_candidates)):
                                for did in list(candidates.keys()):
                                    if did not in keep_ids:
                                        dropped_total.append({"candidate_id": did, "reason": "not_in_keep_list"})
                                        candidates.pop(did, None)

                            existing_l1 = _catalog_l1_keys_for_product(catalog_rows, default_prod)
                            adds_l1 = {
                                _norm_key(n.get("topic_key") or "")
                                for n in adds
                                if _norm_level(n.get("level")) == 1
                                and (str(n.get("product") or "").strip() in ("", default_prod))
                            }

                            for n in adds:
                                prod = str(n.get("product") or "").strip() or default_prod
                                if prod != default_prod:
                                    continue

                                lvl = _norm_level(n.get("level"))

                                topic_key = _norm_key(n.get("topic_key") or "")
                                scenario_key = _norm_key(n.get("scenario_key") or "")
                                variant_key = _norm_key(n.get("variant_key") or "")
                                leaf_key = _norm_key(n.get("leaf_key") or "")

                                # --- Auto-repair: strip leading product alias from all keys ---
                                topic_key_repaired = _strip_product_prefix(topic_key, prod)
                                scenario_key_repaired = _strip_product_prefix(scenario_key, prod)
                                variant_key_repaired = _strip_product_prefix(variant_key, prod)
                                leaf_key_repaired = _strip_product_prefix(leaf_key, prod)

                                if topic_key_repaired != topic_key:
                                    logging.warning(
                                        "phase1b key_repair(adds) product=%s level=%d "
                                        "topic_key: '%s' -> '%s'",
                                        prod, lvl, topic_key, topic_key_repaired,
                                    )
                                if scenario_key_repaired != scenario_key:
                                    logging.warning(
                                        "phase1b key_repair(adds) product=%s level=%d "
                                        "scenario_key: '%s' -> '%s'",
                                        prod, lvl, scenario_key, scenario_key_repaired,
                                    )

                                topic_key = topic_key_repaired
                                scenario_key = scenario_key_repaired
                                variant_key = variant_key_repaired
                                leaf_key = leaf_key_repaired
                                # --- End auto-repair ---

                                # Reject if topic key is still a product alias after repair
                                if _key_contains_product_alias(topic_key, prod):
                                    skipped_total.append({
                                        "product": prod,
                                        "level": lvl,
                                        "why": "topic_key_is_product_alias",
                                        "topic_key": topic_key,
                                    })
                                    logging.warning(
                                        "phase1b rejected product_alias_topic(adds) product=%s "
                                        "topic_key='%s'",
                                        prod, topic_key,
                                    )
                                    continue

                                sig = (n.get("signature_text") or "").strip()
                                res_sig = (n.get("resolution_signature_text") or "").strip() if n.get("resolution_signature_text") else None

                                if not prod or lvl not in (1, 2, 3, 4):
                                    continue

                                if lvl == 4 and not res_sig:
                                    res_sig = sig

                                if (lvl == 1 and not topic_key) or \
                                   (lvl == 2 and (not topic_key or not scenario_key)) or \
                                   (lvl == 3 and (not topic_key or not scenario_key or not variant_key)) or \
                                   (lvl == 4 and (not topic_key or not scenario_key or not variant_key or not leaf_key)):
                                    continue

                                if lvl in (2, 3, 4):
                                    if topic_key not in existing_l1 and topic_key not in adds_l1:
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

                            cur_keys = set(candidates.keys())
                            if prev_candidate_keys is not None and cur_keys == prev_candidate_keys:
                                break
                            prev_candidate_keys = cur_keys

                    # Synthesize L4 leaf candidates from enrichment hints
                    # (ensures leaves get created even if model didn't propose them)
                    synth_count = _synthesize_leaf_candidates_from_hints(
                        batch_rows, default_prod, candidates, catalog_rows,
                    )
                    if synth_count > 0:
                        logging.warning(
                            "phase1b synthesized_leaf_candidates product=%s count=%d",
                            default_prod, synth_count,
                        )
                    synthesized_total += synth_count

                    _ensure_parent_scaffolding(candidates)
                    bulk_nodes = _build_bulk_nodes_from_candidates(candidates)
                    logging.warning(
                        "phase1b bulk_nodes product=%s count=%d levels=%s sample=%s",
                        default_prod,
                        len(bulk_nodes),
                        sorted({int(n.get("level") or 0) for n in bulk_nodes}),
                        bulk_nodes[:5],
                    )

                    nodes_by_level: Dict[int, List[Dict[str, Any]]] = {1: [], 2: [], 3: [], 4: []}
                    for n in bulk_nodes:
                        lvl = int(n.get("level") or 0)
                        if lvl in nodes_by_level:
                            nodes_by_level[lvl].append(n)

                    for lvl in (1, 2, 3, 4):
                        lvl_nodes = nodes_by_level[lvl]
                        if not lvl_nodes:
                            continue

                        for i0 in range(0, len(lvl_nodes), _P1B_INSERT_BATCH):
                            chunk = lvl_nodes[i0 : i0 + _P1B_INSERT_BATCH]
                            out_rows = bulk_get_or_create_issue_clusters(cnx, chunk)

                            for r in out_rows:
                                prod = str(r.get("product") or "").strip()
                                out_lvl = int(r.get("level") or 0)
                                key = str(r.get("key") or "").strip()
                                parent_key = r.get("parent_key")
                                parent_key_s = str(parent_key).strip() if parent_key is not None else None
                                cid = r.get("cluster_id")

                                if not prod or out_lvl not in (1, 2, 3, 4) or not key or cid is None:
                                    continue

                                sig = str(r.get("signature_text") or "").strip()
                                res_sig = r.get("resolution_signature_text")
                                if isinstance(res_sig, str):
                                    res_sig = res_sig.strip() or None

                                inserted_batch.append(
                                    {
                                        "product": prod,
                                        "level": out_lvl,
                                        "key": key,
                                        "parent_key": parent_key_s,
                                        "cluster_id": int(cid),
                                        "signature_text": sig,
                                        "resolution_signature_text": res_sig,
                                    }
                                )

                mark_catalog_checked(cnx, [str(r.get("thread_id")) for r in rows if r.get("thread_id")])
                cnx.commit()

                _append_inserted_to_catalog_rows(catalog_rows, inserted_batch, by_level_key_parent)

                processed_total += len(rows)
                inserted_total.extend(inserted_batch)

        return func.HttpResponse(
            json.dumps(
                {
                    "status": "ok",
                    "mode": "common_only",
                    "processed": processed_total,
                    "nano_calls": nano_calls,
                    "max_slices": max_slices,
                    "slice_limit": slice_limit,
                    "insert_batch": _P1B_INSERT_BATCH,
                    "new_nodes_inserted": len(inserted_total),
                    "dropped_candidates": len(dropped_total),
                    "skipped_candidates": len(skipped_total),
                    "synthesized_candidates": synthesized_total,
                    "details": inserted_total,
                    "drop_samples": dropped_total[:20],
                    "skip_samples": skipped_total[:20],
                },
                ensure_ascii=False,
            ),
            mimetype="application/json",
        )

    except Exception as e:
        logging.exception("Critical error in run_phase1b_cluster")
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}, ensure_ascii=False),
            status_code=500,
            mimetype="application/json",
        )