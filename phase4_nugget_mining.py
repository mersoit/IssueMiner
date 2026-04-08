import os
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple
import azure.functions as func
import pyodbc

from phase3_phase3_functions import (
    sql_connect,
    _search_kb_articles,
)

from aoai_helpers import (
    call_aoai_with_retry,
    get_rate_limiter,
    get_choice_text,
    make_mini_client,
    get_mini_deployment,
    make_nano_client,
    get_nano_deployment,
)


# ---------------------------------------------------------------------------
# Reasoning-model token budget
# ---------------------------------------------------------------------------
# gpt-5-nano is a reasoning model: max_completion_tokens is shared between
# internal chain-of-thought (reasoning_tokens) and visible output.  The old
# value of 1600 was entirely consumed by reasoning, leaving 0 for output.
# These defaults give ample room; override via env vars if needed.

_PASS1_MAX_COMPLETION_TOKENS = int(os.environ.get("PHASE4_PASS1_MAX_TOKENS", "16000"))
_PASS2_MAX_COMPLETION_TOKENS = int(os.environ.get("PHASE4_PASS2_MAX_TOKENS", "8000"))

# Parallelism settings
# Nano has 5k RPM / 5M TPM — 20 workers is a safe default.
_MAX_WORKERS = int(os.environ.get("PHASE4_MAX_WORKERS", "20"))


# -------------------------
# SQL helpers
# -------------------------

def fetch_threads_for_nuggets(cnx, limit: int, product: str = "", batch_id: str = "") -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    product = (product or "").strip()
    batch_filter = "AND t.batch_id = ?" if batch_id else ""
    batch_args = [batch_id] if batch_id else []
    if product:
        cur.execute(
            f"""
            SELECT TOP (?)
                t.thread_id,
                t.product,
                t.topic_cluster_key,
                t.scenario_cluster_key,
                t.signature_text,
                t.resolution_signature_text,
                COALESCE(tc.ContentForLLM, tc.ContentClean) AS thread_content
            FROM dbo.thread_enrichment t
            LEFT JOIN dbo.ThreadsClean tc
                ON tc.QuestionID = CAST(t.thread_id AS NVARCHAR(100))
            WHERE t.solution_usefulness >= 0.2
              AND t.NuggetsMinedUtc IS NULL
              AND t.product = ?
              {batch_filter}
            ORDER BY t.solution_usefulness DESC, t.ingested_at DESC
            """,
            int(limit), product, *batch_args,
        )
    else:
        cur.execute(
            f"""
            SELECT TOP (?)
                t.thread_id,
                t.product,
                t.topic_cluster_key,
                t.scenario_cluster_key,
                t.signature_text,
                t.resolution_signature_text,
                COALESCE(tc.ContentForLLM, tc.ContentClean) AS thread_content
            FROM dbo.thread_enrichment t
            LEFT JOIN dbo.ThreadsClean tc
                ON tc.QuestionID = CAST(t.thread_id AS NVARCHAR(100))
            WHERE t.solution_usefulness >= 0.2
              AND t.NuggetsMinedUtc IS NULL
              AND t.product IS NOT NULL
              AND LTRIM(RTRIM(t.product)) <> ''
              {batch_filter}
            ORDER BY t.solution_usefulness DESC, t.ingested_at DESC
            """,
            int(limit), *batch_args,
        )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def fetch_topic_scenario_catalog(cnx, product: str, max_rows: int = 2000) -> List[Dict[str, Any]]:
    """Fetch issue_cluster nodes for L1 (topic) + L2 (scenario) for this product."""
    cur = cnx.cursor()
    cur.execute(
        """
        SELECT TOP (?)
            cluster_level,
            cluster_id,
            cluster_key,
            parent_cluster_id,
            cluster_signature_text
        FROM dbo.issue_cluster
        WHERE product = ?
          AND cluster_level IN (1, 2)
          AND is_active = 1
        ORDER BY cluster_level ASC, member_count DESC, last_seen_at DESC
        """,
        int(max_rows),
        (product or "").strip(),
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def mark_thread_mined(cnx, thread_id: str) -> None:
    cur = cnx.cursor()
    cur.execute(
        """
        UPDATE dbo.thread_enrichment
        SET NuggetsMinedUtc = COALESCE(NuggetsMinedUtc, SYSUTCDATETIME())
        WHERE thread_id = ?
        """,
        str(thread_id),
    )
    logging.info("Phase4A mark_thread_mined: thread_id=%s rowcount=%d", str(thread_id), cur.rowcount)


# -------------------------
# LLM helpers (Nano)
# -------------------------

def _get_choice_text_any(resp: Any) -> str:
    """Extract text from OpenAI SDK response across possible content shapes."""
    try:
        choice = resp.choices[0]
        msg = getattr(choice, "message", None)
        if msg is not None:
            content = getattr(msg, "content", "")
            if content is None:
                content = ""
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                out: List[str] = []
                for part in content:
                    if isinstance(part, dict):
                        out.append((part.get("text") or part.get("content") or "").strip())
                    else:
                        out.append(str(part).strip())
                return "\n".join([x for x in out if x])
        # fallback
        txt = getattr(choice, "text", None)
        if isinstance(txt, str):
            return txt
    except Exception:
        pass
    return ""


def _extract_resp_meta(resp: Any) -> Dict[str, Any]:
    """Best-effort extraction of response metadata for diagnostics."""
    meta: Dict[str, Any] = {}
    try:
        choice0 = resp.choices[0]
        meta["finish_reason"] = getattr(choice0, "finish_reason", None)
        meta["resp_id"] = getattr(resp, "id", None)
        meta["resp_model"] = getattr(resp, "model", None)
        usage = getattr(resp, "usage", None)
        if usage is not None:
            dumper = getattr(usage, "model_dump", None)
            meta["usage"] = dumper() if callable(dumper) else getattr(usage, "__dict__", None)
    except Exception:
        pass
    return meta


def _safe_json_load(s: Any) -> Dict[str, Any]:
    """Parse a JSON object string safely.

    Always logs parse errors. If PHASE4_DEBUG_JSON=1, returns parse metadata
    to help diagnose why parsing failed.
    """

    if not isinstance(s, str) or not s.strip():
        return {}

    try:
        parsed = json.loads(s)
        return parsed if isinstance(parsed, dict) else {"__non_object_json": True, "__raw": s[:2000]}
    except Exception as e:
        preview = s[:1500]
        logging.warning(
            "JSON parse failed in Phase4: %s; raw_preview=%r; length=%d",
            str(e),
            preview,
            len(s),
        )

        debug = (os.environ.get("PHASE4_DEBUG_JSON") or "").strip().lower() in ("1", "true", "yes")
        if debug:
            return {"__parse_error": str(e), "__raw": s}
        return {}

def extract_nuggets_pass1(
    client,
    deployment: str,
    thread: Dict[str, Any],
    catalog: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Pass 1: propose nuggets + LOCK to an existing cluster_id + propose doc-search queries."""

    system_prompt = (
        "ROLE\n"
        "You are an expert Azure Support Engineer and technical writer.\n\n"
        "TASK\n"
        "From a forum support thread, propose 0..4 candidate Knowledge Nuggets.\n"
        "Each nugget MUST be attached to an existing node from the provided catalog.\n\n"
        "CRITICAL CATALOG RULE\n"
        "- You MUST select the target node by returning its exact integer cluster_id from the catalog.\n"
        "- Do NOT invent IDs, keys, or slugs.\n"
        "- Only L1 (cluster_level=1) Topic and L2 (cluster_level=2) Scenario are allowed.\n"
        "- If none fit, return no nuggets.\n\n"
        "ALLOWED nugget_type\n"
        "- docs_gap\n"
        "- specific_scenario\n"
        "- third_party_interaction\n"
        "- current_limitations\n"
        "- platform_behavior\n"
        "- tooling_hint\n"
        "- workaround\n"
        "- docs_conflict\n"
        "- docs_clarification\n\n"
        "SEARCH QUERIES\n"
        "For each nugget, propose 1-2 specific Azure AI Search queries to find related official docs.\n\n"
        "OUTPUT (STRICT JSON ONLY)\n"
        "{\n"
        "  \"has_any_nuggets\": true,\n"
        "  \"notes\": \"short\",\n"
        "  \"nuggets\": [\n"
        "    {\n"
        "      \"nugget_type\": \"...\",\n"
        "      \"cluster_id\": 12345,\n"
        "      \"title\": \"short title\",\n"
        "      \"draft_content\": \"short draft paragraph\",\n"
        "      \"why_it_matters\": \"short\",\n"
        "      \"evidence_quote\": \"short quote or paraphrase from thread\",\n"
        "      \"search_queries\": [\"q1\",\"q2\"]\n"
        "    }\n"
        "  ]\n"
        "}\n"
    )

    # Keep catalog compact; nano context is limited.
    # Further restrict size to reduce model confusion/hallucinations.
    cat_lines: List[str] = []
    for c in (catalog or [])[:150]:
        cid = c.get("cluster_id")
        lvl = int(c.get("cluster_level") or 0)
        ck = (c.get("cluster_key") or "").strip()
        sig = (c.get("cluster_signature_text") or "").strip()
        if cid is None or not ck or lvl not in (1, 2):
            continue
        cat_lines.append(f"cluster_id={int(cid)} level={lvl} key={ck} sig={sig[:80]}")

    user_payload = {
        "thread": {
            "thread_id": str(thread.get("thread_id") or ""),
            "product": (thread.get("product") or "").strip(),
            "signature_text": (thread.get("signature_text") or "")[:500],
            "resolution_signature_text": (thread.get("resolution_signature_text") or "")[:500],
            "content": (thread.get("thread_content") or "")[:3200],
        },
        "catalog": cat_lines,
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
    ]

    resp = call_aoai_with_retry(
        client,
        model=deployment,
        messages=messages,
        response_format={"type": "json_object"},
        max_completion_tokens=_PASS1_MAX_COMPLETION_TOKENS,
        rate_limiter=get_rate_limiter("mini"),
        caller_tag="phase4a_pass1",
    )

    raw_text = (_get_choice_text_any(resp) or "").strip()
    meta = _extract_resp_meta(resp)

    # Detect reasoning-budget exhaustion: finish_reason=length with empty output
    if meta.get("finish_reason") == "length":
        usage = meta.get("usage") or {}
        reasoning_tok = (usage.get("completion_tokens_details") or {}).get("reasoning_tokens", "?")
        logging.warning(
            "Phase4 Pass1 finish_reason=length (reasoning budget exhausted): "
            "product=%s thread_id=%s reasoning_tokens=%s max_completion_tokens=%d resp_id=%s",
            (thread.get("product") or "").strip(),
            str(thread.get("thread_id") or ""),
            reasoning_tok,
            _PASS1_MAX_COMPLETION_TOKENS,
            meta.get("resp_id"),
        )

    if not raw_text:
        logging.warning(
            "Phase4 Pass1 empty model content: product=%s thread_id=%s resp_id=%s model=%s finish_reason=%s usage=%s",
            (thread.get("product") or "").strip(),
            str(thread.get("thread_id") or ""),
            meta.get("resp_id"),
            meta.get("resp_model"),
            meta.get("finish_reason"),
            meta.get("usage"),
        )

    parsed = _safe_json_load(raw_text)

    nuggets = parsed.get("nuggets") if isinstance(parsed, dict) else None
    if not isinstance(nuggets, list) or len(nuggets) == 0:
        logging.info(
            "Phase4 Pass1 returned no nuggets: product=%s thread_id=%s raw_text_preview=%r length=%d finish_reason=%s",
            (thread.get("product") or "").strip(),
            str(thread.get("thread_id") or ""),
            raw_text[:1500],
            len(raw_text),
            meta.get("finish_reason"),
        )
        return []

    return nuggets


def evaluate_nugget_pass2(
    client,
    deployment: str,
    thread: Dict[str, Any],
    proposed: Dict[str, Any],
    docs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Pass 2: score/decide keep vs discard; FINALIZE fields. (Does NOT change cluster mapping.)"""

    system_prompt = (
        "ROLE\n"
        "You are an expert Azure Support Engineer and technical writer.\n\n"
        "TASK\n"
        "Given a proposed nugget from a thread AND retrieved official docs, decide if the nugget should be stored.\n\n"
        "SCORING\n"
        "Return usefulness_score 0.0..1.0 considering obsolescence risk, doc coverage, usefulness, and likely commonness.\n\n"
        "DECISION\n"
        "- action='discard' if usefulness_score < 0.6 OR nugget is redundant/obsolete.\n\n"
        "IMPORTANT\n"
        "- Do NOT change the nugget's cluster mapping. Do not output cluster_key/cluster_id.\n\n"
        "OUTPUT (STRICT JSON ONLY)\n"
        "{\n"
        "  \"action\": \"keep|discard\",\n"
        "  \"usefulness_score\": 0.0,\n"
        "  \"nugget_type\": \"...\",\n"
        "  \"title\": \"final title\",\n"
        "  \"content\": \"final content\",\n"
        "  \"why_it_matters\": \"final why\",\n"
        "  \"evidence_quote\": \"...\",\n"
        "  \"notes\": \"short reason for keep/discard\"\n"
        "}\n"
    )

    doc_snips: List[Dict[str, Any]] = []
    for d in (docs or [])[:6]:
        if not isinstance(d, dict):
            continue
        doc_snips.append(
            {
                "title": (d.get("title") or "")[:200],
                "url": (d.get("url") or "")[:600],
                "content": (d.get("content") or "")[:900],
            }
        )

    # Only pass cluster_id as read-only context
    user_payload = {
        "thread": {
            "thread_id": str(thread.get("thread_id") or ""),
            "product": (thread.get("product") or "").strip(),
            "signature_text": (thread.get("signature_text") or "")[:500],
            "resolution_signature_text": (thread.get("resolution_signature_text") or "")[:500],
        },
        "proposed": proposed,
        "docs": doc_snips,
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
    ]

    resp = call_aoai_with_retry(
        client,
        model=deployment,
        messages=messages,
        response_format={"type": "json_object"},
        max_completion_tokens=_PASS2_MAX_COMPLETION_TOKENS,
        rate_limiter=get_rate_limiter("mini"),
        caller_tag="phase4a_pass2",
    )

    raw_text = (_get_choice_text_any(resp) or "").strip()
    meta = _extract_resp_meta(resp)

    if meta.get("finish_reason") == "length":
        logging.warning(
            "Phase4 Pass2 finish_reason=length: thread_id=%s resp_id=%s usage=%s",
            str(thread.get("thread_id") or ""),
            meta.get("resp_id"),
            meta.get("usage"),
        )

    return _safe_json_load(raw_text)


# ---------------------------------------------------------------------------
# Per-thread pipeline (runs entirely off the main thread)
# ---------------------------------------------------------------------------

def _process_single_thread(
    client_pass1,
    deployment_pass1: str,
    client_pass2,
    deployment_pass2: str,
    t: Dict[str, Any],
    catalog: List[Dict[str, Any]],
    by_id: Dict[int, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Full two-pass nugget pipeline for a single thread.
    Returns a summary dict with thread_id, status, nuggets kept/discarded.
    """
    thread_id = str(t.get("thread_id") or "")
    product = (t.get("product") or "").strip()

    result: Dict[str, Any] = {
        "thread_id": thread_id,
        "product": product,
        "status": "pending",
        "proposed": 0,
        "kept": 0,
        "discarded": 0,
        "nuggets": [],
    }

    try:
        logging.info("Phase4A thread START: thread_id=%s product=%s", thread_id, product)

        # ---- Pass 1: propose nuggets (Mini) ----
        proposed = extract_nuggets_pass1(client_pass1, deployment_pass1, t, catalog)
        result["proposed"] = len(proposed)
        logging.info("Phase4A Pass1 done: thread_id=%s proposed=%d", thread_id, len(proposed))

        if not proposed:
            result["status"] = "no_nuggets_proposed"
            # Mark thread as mined even if no nuggets found
            with _open_sql() as cnx:
                mark_thread_mined(cnx, thread_id)
                cnx.commit()
            logging.info("Phase4A thread DONE (no nuggets): thread_id=%s", thread_id)
            return result

        # ---- Pass 2: evaluate each nugget (Nano) ----
        kept_nuggets: List[Dict[str, Any]] = []

        for nugget in proposed:
            # Validate cluster_id from Pass 1 exists in catalog
            proposed_cid = nugget.get("cluster_id")
            try:
                proposed_cid_int = int(proposed_cid)
            except (ValueError, TypeError):
                proposed_cid_int = None

            if proposed_cid_int is None or proposed_cid_int not in by_id:
                logging.info(
                    "Phase4 Pass1 proposed invalid cluster_id=%r for thread_id=%s; discarding.",
                    proposed_cid,
                    thread_id,
                )
                result["discarded"] += 1
                continue

            catalog_node = by_id[proposed_cid_int]

            # Retrieve docs using provided queries (best-effort)
            docs: List[Dict[str, Any]] = []
            search_queries = nugget.get("search_queries") or []
            if isinstance(search_queries, list):
                for q in search_queries[:2]:
                    if isinstance(q, str) and q.strip():
                        try:
                            docs.extend(_search_kb_articles(q.strip(), top_k=3, product=product))
                        except Exception:
                            pass
            logging.debug(
                "Phase4A Pass2 input: thread_id=%s cluster_id=%d cluster_key=%s docs_retrieved=%d",
                thread_id,
                proposed_cid_int,
                (catalog_node.get("cluster_key") or "").strip(),
                len(docs),
            )

            evaluated = evaluate_nugget_pass2(
                client_pass2,
                deployment_pass2,
                t,
                nugget,
                docs,
            )

            action = (evaluated.get("action") or "").strip().lower()
            score_val = evaluated.get("usefulness_score")
            logging.info(
                "Phase4A Pass2 result: thread_id=%s cluster_id=%d action=%s score=%s nugget_type=%s",
                thread_id,
                proposed_cid_int,
                action or "(empty)",
                score_val,
                (evaluated.get("nugget_type") or nugget.get("nugget_type") or ""),
            )
            if action != "keep":
                result["discarded"] += 1
                continue

            # Merge required mapping fields (pass2 must not change mapping)
            evaluated["cluster_id"] = proposed_cid_int
            evaluated["cluster_level"] = int(catalog_node.get("cluster_level") or 0)
            evaluated["cluster_key"] = (catalog_node.get("cluster_key") or "").strip()
            evaluated["target_level"] = "topic" if evaluated["cluster_level"] == 1 else "scenario"

            kept_nuggets.append(evaluated)
            result["kept"] += 1

        # ---- Persist kept nuggets + mark thread mined ----
        logging.info(
            "Phase4A SQL persist START: thread_id=%s nuggets_to_insert=%d",
            thread_id,
            len(kept_nuggets),
        )
        with _open_sql() as cnx:
            for nug in kept_nuggets:
                _upsert_nugget_to_db(cnx, thread_id, product, t, nug)
            mark_thread_mined(cnx, thread_id)
            cnx.commit()
        logging.info(
            "Phase4A SQL persist DONE (commit OK): thread_id=%s nuggets_inserted=%d",
            thread_id,
            len(kept_nuggets),
        )

        result["nuggets"] = [
            {
                "title": n.get("title"),
                "type": n.get("nugget_type"),
                "score": n.get("usefulness_score"),
                "cluster_id": n.get("cluster_id"),
            }
            for n in kept_nuggets
        ]
        result["status"] = "ok"
        logging.info(
            "Phase4A thread DONE: thread_id=%s proposed=%d kept=%d discarded=%d",
            thread_id,
            result["proposed"],
            result["kept"],
            result["discarded"],
        )
        return result

    except Exception as e:
        logging.error(
            "Phase4A EXCEPTION thread_id=%s product=%s proposed=%d kept=%d discarded=%d | %s",
            thread_id,
            product,
            result.get("proposed", 0),
            result.get("kept", 0),
            result.get("discarded", 0),
            str(e),
        )
        logging.exception("Phase4A traceback thread_id=%s", thread_id)
        result["status"] = "error"
        result["error"] = str(e)[:500]
        return result


# ---------------------------------------------------------------------------
# SQL: open connection + nugget upsert
# ---------------------------------------------------------------------------

def _open_sql() -> pyodbc.Connection:
    return sql_connect()


def _upsert_nugget_to_db(
    cnx: pyodbc.Connection,
    thread_id: str,
    product: str,
    thread: Dict[str, Any],
    nugget: Dict[str, Any],
) -> None:
    cur = cnx.cursor()

    nugget_type = (nugget.get("nugget_type") or "unknown").strip()[:100]
    title = (nugget.get("title") or "").strip()[:500]
    content = (nugget.get("content") or nugget.get("draft_content") or "").strip()
    why = (nugget.get("why_it_matters") or "").strip()[:2000]
    score = float(nugget.get("usefulness_score") or 0.0)

    target_level = (nugget.get("target_level") or "").strip()[:50]
    topic_key = (thread.get("topic_cluster_key") or "").strip()[:500]
    scenario_key = (thread.get("scenario_cluster_key") or "").strip()[:500]

    # EvidenceJson: store as a JSON string so the nvarchar column stays queryable
    evidence_raw = nugget.get("evidence_quote") or nugget.get("evidence") or ""
    evidence_json = json.dumps({"quote": (evidence_raw or "").strip()[:2000]}, ensure_ascii=False)

    logging.debug(
        "Phase4A _upsert_nugget: thread_id=%s type=%s title=%r target_level=%s score=%s",
        str(thread_id),
        nugget_type,
        title[:80],
        target_level,
        score,
    )

    cur.execute(
        """
        INSERT INTO dbo.KnowledgeNuggets
        (
            ThreadID, Product, NuggetType, Title, ContentText, WhyItMatters,
            EvidenceJson, UsefulnessScore, TargetLevel,
            TopicClusterKey, ScenarioClusterKey,
            ModelVersion, CreatedAtUtc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME())
        """,
        str(thread_id)[:100],
        product[:200],
        nugget_type,
        title,
        content,
        why,
        evidence_json,
        score,
        target_level,
        topic_key,
        scenario_key,
        "gpt-5-mini",
    )
    logging.info(
        "Phase4A _upsert_nugget INSERT OK: thread_id=%s type=%s title=%r score=%s",
        str(thread_id),
        nugget_type,
        title[:80],
        score,
    )


# ---------------------------------------------------------------------------
# Orchestrator (Azure Function entry point)
# ---------------------------------------------------------------------------

def run_phase4a_nugget_mining(req: func.HttpRequest) -> func.HttpResponse:
    try:
        limit = max(1, min(int(req.params.get("limit", "50")), 2000))
        max_workers = max(1, min(int(req.params.get("workers", str(_MAX_WORKERS))), 40))
        product = (req.params.get("product") or "").strip()
        batch_id = (req.params.get("batch_id") or "").strip()
    except Exception:
        limit = 50
        max_workers = _MAX_WORKERS
        product = ""

    logs: List[Dict[str, Any]] = []

    try:
        logging.info("Phase4A START: limit=%d max_workers=%d", limit, max_workers)

        with _open_sql() as cnx:
            threads = fetch_threads_for_nuggets(cnx, limit, product=product, batch_id=batch_id)

        logging.info("Phase4A threads fetched: count=%d", len(threads))

        if not threads:
            logging.info("Phase4A: no threads need nugget mining, exiting.")
            return func.HttpResponse(
                json.dumps({"status": "ok", "processed": 0, "msg": "no threads need nugget mining"}, ensure_ascii=False),
                mimetype="application/json",
            )

        products = list({(t.get("product") or "").strip() for t in threads if (t.get("product") or "").strip()})
        logging.info("Phase4A products found: %s", products)

        catalog_by_product: Dict[str, List[Dict[str, Any]]] = {}
        by_id_by_product: Dict[str, Dict[int, Dict[str, Any]]] = {}

        with _open_sql() as cnx:
            for prod in products:
                cat = fetch_topic_scenario_catalog(cnx, prod)
                catalog_by_product[prod] = cat
                by_id_by_product[prod] = {int(c["cluster_id"]): c for c in cat if c.get("cluster_id") is not None}
                logging.info("Phase4A catalog loaded: product=%s nodes=%d", prod, len(cat))

        client_pass1 = make_mini_client()
        deployment_pass1 = get_mini_deployment()
        logging.info("Phase4A Pass1 model: %s", deployment_pass1)

        client_pass2 = make_mini_client()
        deployment_pass2 = get_mini_deployment()
        logging.info("Phase4A Pass2 model: %s", deployment_pass2)

        logging.info("Phase4A submitting %d threads to thread pool (workers=%d)", len(threads), max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for t in threads:
                prod = (t.get("product") or "").strip()
                futures[executor.submit(
                    _process_single_thread,
                    client_pass1, deployment_pass1,
                    client_pass2, deployment_pass2,
                    t,
                    catalog_by_product.get(prod, []),
                    by_id_by_product.get(prod, {}),
                )] = t
                
            for fut in as_completed(futures):
                try:
                    r = fut.result()
                    logs.append(r)
                    if r.get("status") == "error":
                        logging.error(
                            "Phase4A future complete [ERROR]: thread_id=%s product=%s proposed=%s kept=%s discarded=%s | %s",
                            r.get("thread_id"),
                            r.get("product"),
                            r.get("proposed"),
                            r.get("kept"),
                            r.get("discarded"),
                            r.get("error", "(no error text)"),
                        )
                    else:
                        logging.info(
                            "Phase4A future complete: thread_id=%s status=%s proposed=%s kept=%s discarded=%s",
                            r.get("thread_id"),
                            r.get("status"),
                            r.get("proposed"),
                            r.get("kept"),
                            r.get("discarded"),
                        )
                except Exception as e:
                    t = futures[fut]
                    tid = str(t.get("thread_id") or "")
                    logging.error("Phase4A future raised exception: thread_id=%s error=%s", tid, str(e)[:500])
                    logs.append({"thread_id": tid, "status": "future_error", "error": str(e)[:500]})
        summary: Dict[str, int] = {}
        for lg in logs:
            s = lg.get("status", "unknown")
            summary[s] = summary.get(s, 0) + 1

        logging.info(
            "Phase4A COMPLETE: processed=%d summary=%s",
            len(logs),
            summary,
        )

        return func.HttpResponse(
            json.dumps({"status": "ok", "processed": len(logs), "summary": summary, "details": logs}, ensure_ascii=False, default=str),
            mimetype="application/json",
        )

    except Exception as e:
        logging.exception("Phase 4A Fatal Error")
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}, ensure_ascii=False),
            status_code=500,
            mimetype="application/json",
        )