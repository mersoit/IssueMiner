import os
import json
import logging
import hashlib
import pyodbc
import re
import datetime as dt
from typing import List, Dict, Any, Optional, Tuple
import azure.functions as func
from openai import AzureOpenAI

from ado_devops import create_work_item, update_work_item, upsert_wiki_page
from aoai_helpers import (
    make_gpt52_client,
    get_gpt52_deployment,
    make_mini_client,
    get_mini_deployment,
    make_pro_client,
    get_pro_deployment,
    call_aoai_with_retry,
    call_pro_json_with_retry,
    estimate_tokens,
    get_rate_limiter,
    sql_connect as _aoai_sql_connect,
    safe_float as _safe_float,
    safe_int as _safe_int,
    slug as _slug,
    ensure_single_slashes as _ensure_single_slashes,
)


# ---------------------------------------------------------
# Configuration & Helpers
# ---------------------------------------------------------

def sql_connect() -> pyodbc.Connection:
    cnx = _aoai_sql_connect(autocommit=False)
    return cnx

def make_aoai_client(model_env_var: str = "GPT52") -> AzureOpenAI:
    """Backwards-compatible alias – always returns the GPT-5.2 client."""
    return make_gpt52_client()

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

# Add near other helpers/constants
_ADO_WIKI_PATH_MAX = _env_int("ADO_WIKI_PATH_MAX", 235)
_ADO_WIKI_LEAF_SEG_MAX = _env_int("ADO_WIKI_LEAF_SEG_MAX", 80)

def _short_hash(s: str, n: int = 8) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()[: max(4, int(n))]

def _fit_wiki_page_path(path: str, max_len: int = _ADO_WIKI_PATH_MAX) -> str:
    """
    ADO Wiki requires path length <= 235 chars.
    Strategy: shrink only the last segment, then hard-fallback to cluster+hash.
    """
    path = _ensure_single_slashes(path or "")
    if len(path) <= max_len:
        return path

    parts = [p for p in path.split("/") if p]
    if not parts:
        return "/"

    # last segment is the page name
    last = parts[-1]
    prefix = "/" + "/".join(parts[:-1]) if len(parts) > 1 else ""
    # reserve 1 for slash between prefix and last
    budget = max_len - len(prefix) - 1
    if budget <= 0:
        # prefix itself too long; last-ditch: hash the entire path
        return "/" + _short_hash(path, 16)

    # Attempt 1: trim last segment to budget (and also an explicit cap if set)
    cap = min(budget, max(10, int(_ADO_WIKI_LEAF_SEG_MAX)))
    trimmed = (last[:cap]).rstrip("-")
    candidate = _ensure_single_slashes(f"{prefix}/{trimmed}")
    if len(candidate) <= max_len:
        return candidate

    # Attempt 2: aggressive deterministic fallback
    fallback = f"p-{_short_hash(path, 16)}"
    fallback = fallback[:budget]
    return _ensure_single_slashes(f"{prefix}/{fallback}")

# Max lengths for DB writes.  0 => no truncation (columns are NVARCHAR(MAX) after ALTER).
_P3_EMERG_TITLE_MAX = _env_int("P3_EMERG_TITLE_MAX", 500)
_P3_EMERG_SUMMARY_MAX = _env_int("P3_EMERG_SUMMARY_MAX", 0)
_P3_EMERG_IMPACT_MAX = _env_int("P3_EMERG_IMPACT_MAX", 0)
_P3_EMERG_WORKAROUND_MAX = _env_int("P3_EMERG_WORKAROUND_MAX", 0)
_P3_EMERG_ROOTCAUSE_MAX = _env_int("P3_EMERG_ROOTCAUSE_MAX", 0)
_P3_EMERG_RAWJSON_MAX = _env_int("P3_EMERG_RAWJSON_MAX", 0)

_P3_COMMON_TITLE_MAX = _env_int("P3_COMMON_TITLE_MAX", 500)
_P3_COMMON_CLUSTERKEY_MAX = _env_int("P3_COMMON_CLUSTERKEY_MAX", 500)

def _generate_path_hash(prod: str, cat: str, sub: str, issue: Optional[str]) -> str:
    raw = f"{prod}|{cat}|{sub}|{issue or ''}".lower().strip()
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

def _trunc(s: Any, max_len: int) -> Optional[str]:
    if s is None:
        return None
    try:
        txt = str(s)
    except Exception:
        txt = ""
    txt = txt.strip()
    if not txt:
        return None
    if max_len and max_len > 0:
        return txt[:max_len]
    return txt


# ---------------------------------------------------------
# Azure AI Search (KB articles retrieval)
# ---------------------------------------------------------

def _search_kb_articles(query: str, top_k: int = 5, product: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Azure AI Search query for the rag-apim-swa index schema.
    Retrievable fields: chunk_id, parent_id, chunk, Order, Product, URL, Title
    Note: field names are PascalCase — Title, Product, URL.
    """
    endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
    api_key = os.getenv("AZURE_SEARCH_API_KEY")
    index = os.getenv("AZURE_SEARCH_INDEX")

    if not endpoint or not api_key or not index:
        return []

    try:
        import requests
    except Exception:
        return []

    api_version = os.getenv("AZURE_SEARCH_API_VERSION", "2024-07-01")
    url = f"{endpoint.rstrip('/')}/indexes/{index}/docs/search?api-version={api_version}"

    payload: Dict[str, Any] = {
        "search": query,
        "queryType": "simple",
        "top": int(max(1, min(top_k, 10))),
        "select": "Title,chunk,parent_id,URL,Product",
    }

    product_clean = (product or "").strip()
    if product_clean:
        payload["filter"] = f"Product eq '{product_clean.replace(chr(39), chr(39)+chr(39))}'"

    headers = {
        "Content-Type": "application/json",
        "api-key": api_key,
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=30)

    if not resp.ok:
        body = (resp.text or "")[:5000]
        logging.error(
            "AzureSearch request failed status=%s url=%s query=%s index=%s api_version=%s product=%s body=%s",
            resp.status_code,
            url,
            (query or "")[:200],
            index,
            api_version,
            product_clean,
            body,
        )
        resp.raise_for_status()

    data = resp.json() or {}
    vals = data.get("value") or []
    out: List[Dict[str, Any]] = []

    for v in vals:
        if not isinstance(v, dict):
            continue

        title = v.get("Title") or ""
        doc_url = v.get("URL") or v.get("parent_id") or ""
        chunk = v.get("chunk") or ""

        out.append(
            {
                "title": str(title)[:200],
                "url": str(doc_url)[:600],
                "content": str(chunk)[:1400],
            }
        )

    return out


# ---------------------------------------------------------
# Data Fetching (Phase 2 Integration)
# ---------------------------------------------------------

def fetch_cluster_meta(cnx: pyodbc.Connection, cluster_id: int) -> Dict[str, Any]:
    cursor = cnx.cursor()
    cursor.execute("""
        SELECT cluster_id, cluster_level, product, cluster_key, parent_cluster_id,
               cluster_signature_text, resolution_signature_text, member_count, last_seen_at,
               max_solution_usefulness
        FROM dbo.issue_cluster
        WHERE cluster_id = ? AND is_active = 1
    """, int(cluster_id))
    row = cursor.fetchone()
    if not row:
        return {}
    return {
        "cluster_id": int(row[0]),
        "cluster_level": int(row[1]),
        "product": row[2],
        "cluster_key": row[3] or f"Cluster-{row[0]}",
        "parent_cluster_id": row[4],
        "signature": row[5],
        "fix_pattern": row[6],
        "count": int(row[7] or 0),
        "last_seen_at": row[8],
        "max_solution_usefulness": _safe_float(row[9], 0.0),
    }

def _fetch_cluster_min_meta(cnx: pyodbc.Connection, cluster_id: int) -> Dict[str, Any]:
    """
    Lightweight fetch used for wiki path building from parent chain.
    """
    cur = cnx.cursor()
    cur.execute("""
        SELECT cluster_id, cluster_level, product, cluster_key, parent_cluster_id
        FROM dbo.issue_cluster
        WHERE cluster_id = ? AND is_active = 1
    """, int(cluster_id))
    r = cur.fetchone()
    if not r:
        return {}
    return {
        "cluster_id": int(r[0]),
        "cluster_level": int(r[1]),
        "product": (r[2] or "").strip(),
        "cluster_key": (r[3] or f"Cluster-{r[0]}").strip(),
        "parent_cluster_id": _safe_int(r[4]),
    }

def _build_cluster_wiki_segments(
    cnx: pyodbc.Connection,
    cluster_id: int,
) -> Tuple[str, List[str]]:
    """
    Builds wiki path segments for:
      /<product>/<topic>/<scenario>/<variant>/<leaf>

    Rules:
    - topic == cluster_level 1
    - scenario == cluster_level 2
    - variant == cluster_level 3
    - leaf == cluster_level 4
    - segment names are cluster_key (slugged)
    - product is taken from the leaf cluster meta (string, slugged)
    """
    chain: List[Dict[str, Any]] = []
    seen: set[int] = set()

    cid = int(cluster_id)
    while cid and cid not in seen and len(chain) < 10:
        seen.add(cid)
        m = _fetch_cluster_min_meta(cnx, cid)
        if not m:
            break
        chain.append(m)
        cid = m.get("parent_cluster_id") or 0

    if not chain:
        return ("common", ["unknown-topic", "unknown-scenario", "unknown-variant", f"cluster-{cluster_id}"])

    product = _slug(chain[0].get("product") or "common")

    seg_by_level = {
        1: "unknown-topic",
        2: "unknown-scenario",
        3: "unknown-variant",
        4: "unknown-leaf",
    }

    for node in chain:
        lvl = int(node.get("cluster_level") or 0)
        if lvl in (1, 2, 3, 4):
            seg_by_level[lvl] = _slug(node.get("cluster_key") or f"cluster-{node.get('cluster_id')}")

    return product, [seg_by_level[1], seg_by_level[2], seg_by_level[3], seg_by_level[4]]

def fetch_leaf_context(cnx: pyodbc.Connection, leaf_cluster_id: int, top_n: int = 12) -> Dict[str, Any]:
    meta = fetch_cluster_meta(cnx, leaf_cluster_id)
    if not meta:
        return {}

    cursor = cnx.cursor()
    cursor.execute("""
        SELECT TOP (?)
            t.thread_id,
            t.source_url,
            COALESCE(NULLIF(LTRIM(RTRIM(t.signature_text)), ''), t.product) AS best_title,
            t.signature_text,
            t.resolution_signature_text,
            COALESCE(tc.ContentForLLM, tc.ContentClean) AS thread_content,
            t.solution_usefulness
        FROM dbo.thread_enrichment t
        LEFT JOIN dbo.ThreadsClean tc
            ON tc.QuestionID = CAST(t.thread_id AS NVARCHAR(100))
        WHERE t.ResolutionLeafClusterID = ?
        ORDER BY t.solution_usefulness DESC, t.source_created_at DESC
    """, int(top_n), int(leaf_cluster_id))

    cols = [c[0] for c in cursor.description]
    threads = [dict(zip(cols, r)) for r in cursor.fetchall()]
    return {"meta": meta, "threads": threads}

def fetch_variant_context_no_leaf(cnx: pyodbc.Connection, variant_cluster_id: int, top_n: int = 18) -> Dict[str, Any]:
    meta = fetch_cluster_meta(cnx, variant_cluster_id)
    if not meta:
        return {}

    cursor = cnx.cursor()
    cursor.execute("""
        SELECT TOP (?)
            t.thread_id,
            t.source_url,
            COALESCE(NULLIF(LTRIM(RTRIM(t.signature_text)), ''), t.product) AS best_title,
            t.signature_text,
            t.resolution_signature_text,
            COALESCE(tc.ContentForLLM, tc.ContentClean) AS thread_content,
            t.solution_usefulness
        FROM dbo.thread_enrichment t
        LEFT JOIN dbo.ThreadsClean tc
            ON tc.QuestionID = CAST(t.thread_id AS NVARCHAR(100))
        WHERE t.VariantClusterID = ?
          AND t.ResolutionLeafClusterID IS NULL
        ORDER BY t.solution_usefulness DESC, t.source_created_at DESC
    """, int(top_n), int(variant_cluster_id))

    cols = [c[0] for c in cursor.description]
    threads = [dict(zip(cols, r)) for r in cursor.fetchall()]
    return {"meta": meta, "threads": threads}

def fetch_scenario_context_no_leaf(cnx: pyodbc.Connection, scenario_cluster_id: int, top_n: int = 18) -> Dict[str, Any]:
    meta = fetch_cluster_meta(cnx, scenario_cluster_id)
    if not meta:
        return {}

    cursor = cnx.cursor()
    cursor.execute("""
        SELECT TOP (?)
            t.thread_id,
            t.source_url,
            COALESCE(NULLIF(LTRIM(RTRIM(t.signature_text)), ''), t.product) AS best_title,
            t.signature_text,
            t.resolution_signature_text,
            COALESCE(tc.ContentForLLM, tc.ContentClean) AS thread_content,
            t.solution_usefulness
        FROM dbo.thread_enrichment t
        LEFT JOIN dbo.ThreadsClean tc
            ON tc.QuestionID = CAST(t.thread_id AS NVARCHAR(100))
        WHERE t.ScenarioClusterID = ?
          AND t.ResolutionLeafClusterID IS NULL
        ORDER BY t.solution_usefulness DESC, t.source_created_at DESC
    """, int(top_n), int(scenario_cluster_id))

    cols = [c[0] for c in cursor.description]
    threads = [dict(zip(cols, r)) for r in cursor.fetchall()]
    return {"meta": meta, "threads": threads}

def fetch_cluster_context(cnx, cluster_id: int) -> Dict[str, Any]:
    return fetch_leaf_context(cnx, cluster_id, top_n=12)


# ---------------------------------------------------------
# Area Path Management
# ---------------------------------------------------------

def upsert_area_path(cnx, path_data: Dict[str, str]) -> Optional[int]:
    p = path_data.get("product")
    c = path_data.get("category")
    if not p or not c:
        return None

    s = path_data.get("subcategory", "General")
    i = path_data.get("issue_type")

    path_hash = _generate_path_hash(p, c, s, i)

    cursor = cnx.cursor()
    cursor.execute("""
        MERGE dbo.AreaPaths AS target
        USING (SELECT ? AS Hash) AS source
        ON target.FullPathHash = source.Hash
        WHEN MATCHED THEN
            UPDATE SET LastSeenUtc = SYSUTCDATETIME(), ReferenceCount = ReferenceCount + 1
        WHEN NOT MATCHED THEN
            INSERT (Product, Category, Subcategory, IssueType, FullPathHash, ReferenceCount)
            VALUES (?, ?, ?, ?, ?, 1);
    """, path_hash, p, c, s, i, path_hash)

    cnx.commit()

    cursor.execute("SELECT PathID FROM dbo.AreaPaths WHERE FullPathHash = ?", path_hash)
    row = cursor.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------
# Knowledge Usage Marking (Option 2)
# ---------------------------------------------------------

def mark_threads_used_for_knowledge(cnx: pyodbc.Connection, thread_ids: List[str], kind: str) -> None:
    thread_ids = [str(x) for x in (thread_ids or []) if x]
    if not thread_ids:
        return

    kind = (kind or "").strip().lower()
    col_by_kind = {
        "common_leaf": "CommonLeafUsedUtc",
        "common_variant": "CommonVariantUsedUtc",
        "common_scenario": "CommonScenarioUsedUtc",
        "emergent": "EmergentUsedUtc",
    }
    col = col_by_kind.get(kind)
    if not col:
        raise ValueError(f"Unknown kind: {kind}")

    placeholders = ",".join("?" for _ in thread_ids)
    cur = cnx.cursor()
    cur.execute(
        f"""
        UPDATE dbo.thread_enrichment
        SET {col} = COALESCE({col}, SYSUTCDATETIME())
        WHERE thread_id IN ({placeholders});
        """,
        *thread_ids
    )


# ---------------------------------------------------------
# LLM Logic
# ---------------------------------------------------------

def generate_gpt5_common_leaf_support_playbook(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generates a concise support playbook for a leaf from the perspective of Azure Technical Support.
    Uses threads + retrieved KB articles as evidence, but allows intermediate steps based on domain knowledge.
    Uses GPT-5.2 for high-quality playbook generation.
    """
    client = make_gpt52_client()
    deployment = get_gpt52_deployment()

    meta = context.get("meta") or {}
    threads = context.get("threads") or []
    docs = context.get("docs") or []

    if not meta or not threads:
        return {}

    evidence_threads = ""
    for t in threads:
        evidence_threads += (
            f"--- Thread {t.get('thread_id')} (usefulness={t.get('solution_usefulness')}) ---\n"
            f"URL: {t.get('source_url')}\n"
            f"Title: {t.get('best_title')}\n"
            f"Symptom Sig: {t.get('signature_text')}\n"
            f"Fix Sig: {t.get('resolution_signature_text')}\n"
            f"Content: {(t.get('thread_content') or '')}\n\n"
        )

    evidence_docs = ""
    for d in docs:
        evidence_docs += (
            f"--- Doc ---\n"
            f"Title: {d.get('title')}\n"
            f"URL: {d.get('url')}\n"
            f"Excerpt: {(d.get('content') or '')}\n\n"
        )

    system_prompt = (
        "ROLE\n"
        "You are an Azure Technical Support Engineer (CSS) knowledge base builder. "
        "These engineers can guide the customer AND run Azure-side checks "
        "(resource provider state, control-plane operations, platform logs/telemetry). "
        "Write a concise internal-style support playbook.\n\n"

        "LANGUAGE\n"
        "- Write ALL output in Vietnamese.\n"
        "- Use simple, clear Vietnamese suitable for junior engineers.\n"
        "- Keep English technical terms as-is when natural in Vietnamese tech context "
        "(e.g., 'subscription', 'resource group', 'cluster', 'CNAME', 'TLS', 'ARM', "
        "'control plane', 'data plane', 'node pool', 'SKU', etc.).\n"
        "- Do NOT translate Azure service names, CLI commands, error messages, "
        "or tool names (ASC, AppLens, Kusto, Jarvis).\n"
        "- Example: 'Kiểm tra xem CNAME record đã trỏ đúng về default hostname chưa.'\n\n"

        "SCOPE\n"
        "- Target: the specific L4 leaf issue pattern (cluster_signature_text + resolution_signature_text).\n"
        "- Use provided threads + docs as evidence, but you MAY add reasonable intermediate "
        "troubleshooting steps that are not explicitly written.\n"
        "- Be direct and technical; no customer soft skills; no verbosity.\n\n"

        "REVIEW ANNOTATIONS\n"
        "This playbook will be reviewed by Technical Advisors (TA) before publishing.\n"
        "- Use [need confirmation] when a fact, command, UI path, or table name may be inaccurate.\n"
        "- Use [TA should check] when a step requires TA verification of tool/table existence.\n"
        "- Use [general knowledge] when inferring from Azure domain knowledge not in the provided data.\n"
        "- Use [docs gap] when no official documentation exists for a stated behavior.\n"
        "These annotations MUST remain in the output — they are NOT errors.\n\n"

        "SAFETY / DATA HANDLING\n"
        "- Do NOT include secrets or personal data: no tenant/subscription IDs, tokens, headers, cookies, raw full logs.\n"
        "- If suggesting a tool/script/command, give general how-to and explain expected output "
        "and how to interpret it in the solution section; make it explicit with tips (optional) in notes section.\n"
        "- For Azure-side actions, describe what to check and what outcomes mean; "
        "do NOT claim access to restricted data fields. Remind them to use internal Microsoft wiki for how-to.\n\n"

        "OUTPUT FORMAT (STRICT)\n"
        "Return ONLY valid JSON (no markdown). Shape:\n"
        "{\n"
        "  \"area_path\": {\"product\":\"string\",\"category\":\"string\",\"subcategory\":\"string\",\"issue_type\":\"string?\"},\n"
        "  \"content\": {\n"
        "    \"title\": \"string\",\n"
        "    \"symptoms\": [\"...\"],\n"
        "    \"anti_symptoms\": [\"...\"],\n"
        "    \"checks\": [\"...\"],\n"
        "    \"solution\": [\"...\"],\n"
        "    \"verification\": [\"...\"],\n"
        "    \"flows\": [\"...\"],\n"
        "    \"notes\": [\"...\"],\n"
        "    \"worked_example\": {\n"
        "      \"scenario_summary\": \"string – 2-3 sentences describing a realistic case based on the provided threads\",\n"
        "      \"steps_taken\": [\n"
        "        \"string – each step the engineer would take chronologically, including: "
        "what they checked, what they saw, what they concluded. "
        "Include Azure backend steps (ASC/AppLens/Kusto) that would NOT appear in a forum thread. "
        "Include customer communication points (what info was requested, what was received). "
        "Mark offline/backend-only steps with (Azure-side).\"\n"
        "      ],\n"
        "      \"resolution\": \"string – what ultimately fixed it and how it was confirmed\",\n"
        "      \"time_estimate\": \"string – realistic elapsed time from case open to resolution\",\n"
        "      \"lessons_learned\": [\"string – 1-3 key takeaways from this case\"]\n"
        "    }\n"
        "  }\n"
        "}\n\n"

        "WORKED EXAMPLE RULES\n"
        "- Base the example on the highest-usefulness thread provided, but FILL IN gaps:\n"
        "  * What Azure backend checks the engineer would run (ASC insights, AppLens detectors, Kusto queries)\n"
        "  * What customer data was requested (subscription ID, resource name, timestamps, screenshots)\n"
        "  * What offline investigation happened between customer replies\n"
        "  * What the engineer saw in backend tools and how they interpreted it\n"
        "- The example should read like a case timeline, not a generic procedure.\n"
        "- If the thread is from a forum (Q&A/Stack Overflow), note that real support cases "
        "would include backend investigation steps that are not visible in public threads.\n"
        "- Mark any speculative backend steps with [general knowledge].\n\n"

        "QUALITY BAR (NON-NEGOTIABLE)\n"
        "- Each bullet must be specific, testable, and short. Prefer 4-8 bullets per section "
        "but going for more lengthy content is allowed when necessary.\n"
        "- Avoid generic advice like 'check configuration' without naming WHAT and HOW.\n"
        "- For resolution steps that involve waiting (DNS propagation, cert issuance, "
        "replication, portal state changes): include a realistic time estimate in parentheses, "
        "e.g., '(thường mất 5-30 phút; DNS có thể lên đến 48h tùy TTL)'.\n"
        "- Distinguish clearly:\n"
        "  * symptoms: observable signals that strongly point to THIS leaf (errors, states, timings, UI/API behaviors).\n"
        "  * anti_symptoms: signals that DISCONFIRM this leaf and point to adjacent causes.\n"
        "  * checks: concrete steps to confirm/triage; include BOTH customer-side and Azure-side checks when applicable.\n"
        "  * solution: actionable fix steps with BRANCHING. Use explicit if/then wording ('IF X THEN Y; ELSE Z', use Vietnamese words for 'IF','THEN','Else' and put in bold' ).\n"
        "  * verification: how to prove resolution and prevent false positives.\n\n"

        "FLOWS (OPTIONAL BUT PREFERRED WHEN IT HELPS)\n"
        "- Provide a directional flow using arrows, e.g., 'Client -> Front Door -> SWA -> Auth -> Backend'.\n"
        "- Annotate failure points with (Customer-check) or (Azure-check) and the signal to look for.\n"
        "- If flow is not useful, output an empty array.\n\n"

        "NOTES (MAKE IT PRACTICAL)\n"
        "- Include: (1) key components glossary (2-6 items) explaining relationship and WHY each component matters "
        "for this issue (not just what it is), "
        "and (2) 'behavioral notes' (timers/propagation delays/eventual consistency/platform quirks).\n"
        "- If you mention an undocumented behavior, phrase it as an observation/possibility, not a guaranteed fact.\n\n"

        "SEE ALSO\n"
        "- In the notes array, include 2-4 entries prefixed with 'See also:' that name related leaf/variant keys "
        "an engineer might confuse with this one, with a one-line Vietnamese explanation of how to distinguish them.\n"
    )

    user_prompt = (
        f"LEAF META\n"
        f"- cluster_id: {meta.get('cluster_id')}\n"
        f"- product: {meta.get('product')}\n"
        f"- cluster_key: {meta.get('cluster_key')}\n"
        f"- cluster_signature_text: {meta.get('signature')}\n"
        f"- resolution_signature_text: {meta.get('fix_pattern')}\n"
        f"- member_count: {meta.get('count')}\n"
        f"- max_solution_usefulness: {meta.get('max_solution_usefulness')}\n\n"
        f"THREAD EVIDENCE (top threads):\n{evidence_threads}\n\n"
        f"DOC EVIDENCE (Azure AI Search):\n{evidence_docs}\n"
    )

    try:
        return call_aoai_with_retry(
            client,
            model=deployment,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            max_completion_tokens=int(os.getenv("AOAI_MAX_COMPLETION_TOKENS_GPT52", "16000")),
            temperature=0.2,
            estimated_prompt_tokens=estimate_tokens(system_prompt) + estimate_tokens(user_prompt),
            rate_limiter=get_rate_limiter("gpt52"),
            caller_tag="phase3_leaf_gpt52",
        )
    except Exception as e:
        logging.error(
            "GPT-5.2 common_leaf playbook generation failed for cluster %s: %s",
            meta.get("cluster_id"),
            str(e),
        )
        return {}

def generate_gpt5_content(kind: str, context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Backwards-compatible generator for emergent/common_variant/common_scenario.
    common_leaf is now generated by generate_gpt5_common_leaf_support_playbook.
    """
    client = make_aoai_client("GPT52")
    deployment = get_gpt52_deployment()

    meta = context["meta"]
    threads = context["threads"]
    if not threads:
        return {}

    evidence_text = ""
    for t in threads:
        evidence_text += (
            f"--- Thread {t['thread_id']} (Usefulness: {t['solution_usefulness']}) ---\n"
            f"Title: {t.get('best_title')}\n"
            f"Symptom Sig: {t.get('signature_text')}\n"
            f"Fix Sig: {t.get('resolution_signature_text')}\n"
            f"Content: {(t.get('thread_content') or '')[:1200]}\n\n"
        )

    if kind == "emergent":
        task_desc = (
            "Analyze these recent support threads to generate an EMERGENT ISSUE REPORT.\n"
            "This is likely a new bug, regression, or service outage impacting multiple users."
        )
        json_structure = """
        {
            "area_path": { "product": "string", "category": "string", "subcategory": "string", "issue_type": "string (optional)" },
            "content": {
                "title": "Severe: <Impact Summary>",
                "summary": "Brief executive summary of the outage/bug.",
                "impact_analysis": "Who is affected and how severely?",
                "workaround": "Any immediate steps to mitigate?",
                "root_cause_theory": "Best guess based on logs/errors seen.",
                "confidence_score": 0.0-1.0
            }
        }
        """
    else:
        scope_note = {
            "common_variant": "This is a variant (symptom family). Many threads have NO leaf match so they are all put under this category.",
            "common_scenario": "This is a scenario (context). Provide troubleshooting matrix by constraints and highlight dominant variants.",
        }.get(kind, "")

        task_desc = (
            "Generate a COMMON ISSUE KNOWLEDGE BASE ARTICLE.\n"
            f"{scope_note}\n"
            "Do not claim a single definitive fix unless strongly supported by evidence."
        )
        json_structure = """
        {
            "area_path": { "product": "string", "category": "string", "subcategory": "string", "issue_type": "string (optional)" },
            "content": {
                "title": "How to troubleshoot <Symptom/Error>...",
                "problem_statement": "Clear description of the symptom and context.",
                "resolution_steps": ["step 1", "step 2", "step 3"],
                "validation_checks": ["check 1", "check 2"],
                "diagnostic_logic": {
                    "key_symptoms": ["..."],
                    "anti_symptoms": ["..."],
                    "distinctions": "..."
                }
            }
        }
        """

    system_prompt = (
        "You are a Principal Escalation Engineer.\n"
        f"{task_desc}\n"
        "MANDATORY: Classify 'area_path' strictly (Product/Category/Subcategory).\n"
        "Output strictly valid JSON matching the requested structure.\n"
        "Avoid secrets. Do not include tenant IDs, subscription IDs, tokens, or full logs.\n"
    )

    user_prompt = (
        f"Cluster ID: {meta['cluster_id']}\n"
        f"Cluster Level: {meta.get('cluster_level')}\n"
        f"Cluster Product: {meta['product']}\n"
        f"Cluster Key: {meta['cluster_key']}\n"
        f"Cluster Signature: {meta.get('signature')}\n"
        f"Cluster Resolution Signature: {meta.get('fix_pattern')}\n"
        f"Member Count: {meta.get('count')}\n\n"
        f"EVIDENCE:\n{evidence_text}\n\n"
        f"Generate JSON matching:\n{json_structure}"
    )

    try:
        resp = client.chat.completions.create(
            model=deployment,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            max_completion_tokens=4000,
        )
        return json.loads(resp.choices[0].message.content or "{}")
    except Exception as e:
        logging.error("GPT-5.2 Generation failed for cluster %s: %s", meta.get("cluster_id"), str(e))
        return {}


# ---------------------------------------------------------
# Phase 3 Orchestrators
# ---------------------------------------------------------

def _select_leaf_candidates_for_common(
    cnx: pyodbc.Connection,
    limit: int,
    min_max_usefulness: float = 0.6,
    min_members: int = 2,
    product: str = "",
) -> List[int]:
    """
    Leaf KB generation:
      - (member_count >= min_members AND max_solution_usefulness >= min_max_usefulness)
      - OR max_solution_usefulness >= high_usefulness (high-quality single-thread leaves)
      - only if not already in CommonIssueSolutions
    """
    high_usefulness = float(os.getenv("PHASE3_LEAF_HIGH_USEFULNESS", "0.9"))
    product = (product or "").strip()
    prod_filter = "AND c.product = ?" if product else ""
    cur = cnx.cursor()
    args = [int(limit), int(min_members), float(min_max_usefulness), float(high_usefulness)]
    if product:
        args.append(product)
    cur.execute(f"""
        SELECT TOP (?) c.cluster_id
        FROM dbo.issue_cluster c
        LEFT JOIN dbo.CommonIssueSolutions s ON s.ClusterID = c.cluster_id
        WHERE c.cluster_level = 4
          AND c.is_active = 1
          AND s.ClusterID IS NULL
          AND (
              (c.member_count >= ? AND ISNULL(c.max_solution_usefulness, 0.0) >= ?)
              OR
              (ISNULL(c.max_solution_usefulness, 0.0) >= ?)
          )
          {prod_filter}
        ORDER BY c.max_solution_usefulness DESC, c.member_count DESC, c.last_seen_at DESC
    """, *args)
    return [int(r[0]) for r in cur.fetchall()]

def _select_variant_candidates_for_common(cnx: pyodbc.Connection, limit: int) -> List[int]:
    cur = cnx.cursor()
    cur.execute("""
        SELECT TOP (?)
            c.cluster_id
        FROM dbo.issue_cluster c
        LEFT JOIN dbo.CommonIssueSolutions s ON s.ClusterID = c.cluster_id
        WHERE c.cluster_level = 3
          AND c.is_active = 1
          AND s.ClusterID IS NULL
          AND EXISTS (
                SELECT 1
                FROM dbo.thread_enrichment t
                WHERE t.VariantClusterID = c.cluster_id
                  AND t.ResolutionLeafClusterID IS NULL
            )
        ORDER BY c.last_seen_at DESC, c.member_count DESC
    """, int(limit))
    return [int(r[0]) for r in cur.fetchall()]

def _select_scenario_candidates_for_common(cnx: pyodbc.Connection, limit: int) -> List[int]:
    cur = cnx.cursor()
    cur.execute("""
        SELECT TOP (?)
            c.cluster_id
        FROM dbo.issue_cluster c
        LEFT JOIN dbo.CommonIssueSolutions s ON s.ClusterID = c.cluster_id
        WHERE c.cluster_level = 2
          AND c.is_active = 1
          AND s.ClusterID IS NULL
          AND EXISTS (
                SELECT 1
                FROM dbo.thread_enrichment t
                WHERE t.ScenarioClusterID = c.cluster_id
                  AND t.ResolutionLeafClusterID IS NULL
            )
        ORDER BY c.last_seen_at DESC, c.member_count DESC
    """, int(limit))
    return [int(r[0]) for r in cur.fetchall()]


def run_phase3_emergent_processing(limit: int = 20) -> func.HttpResponse:
    try:
        results = []
        with sql_connect() as cnx:
            cursor = cnx.cursor()
            cursor.execute("""
                SELECT TOP (?) c.cluster_id
                FROM dbo.issue_cluster c
                LEFT JOIN dbo.EmergentIssues e ON e.ClusterID = c.cluster_id
                WHERE c.cluster_level = 4
                  AND c.is_active = 1
                  AND c.member_count >= 3
                  AND e.ClusterID IS NULL
                ORDER BY c.last_seen_at DESC
            """, int(limit))

            ids = [int(r[0]) for r in cursor.fetchall()]
            for cid in ids:
                ctx = fetch_leaf_context(cnx, cid, top_n=12)
                if not ctx or not ctx["threads"]:
                    continue

                output = generate_gpt5_content("emergent", ctx)
                content = output.get("content", {})
                path = output.get("area_path", {})

                if not content or not content.get("title"):
                    continue

                upsert_area_path(cnx, path)

                # Full GPT output preserved; columns are NVARCHAR(MAX) after ALTER.
                title = _trunc(content.get("title"), _P3_EMERG_TITLE_MAX)
                summary = _trunc(content.get("summary"), _P3_EMERG_SUMMARY_MAX)
                impact = _trunc(content.get("impact_analysis"), _P3_EMERG_IMPACT_MAX)
                workaround = _trunc(content.get("workaround"), _P3_EMERG_WORKAROUND_MAX)
                root = _trunc(content.get("root_cause_theory"), _P3_EMERG_ROOTCAUSE_MAX)
                raw_json = _trunc(json.dumps(output, ensure_ascii=False), _P3_EMERG_RAWJSON_MAX)
                primary_key = _trunc(ctx["meta"]["cluster_key"], _P3_COMMON_CLUSTERKEY_MAX)

                cursor.execute("""
                    INSERT INTO dbo.EmergentIssues
                    (ClusterID, PrimaryClusterKey, Title, Summary, Impact, SuggestedWorkaround, RootCauseHypothesis, Confidence, RawAggregationJson, LastUpdatedUtc, FirstSeenUtc, ModelVersion)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME(), ?)
                """,
                cid,
                primary_key,
                title,
                summary,
                impact,
                workaround,
                root,
                float(content.get("confidence_score") or 0.5),
                raw_json,
                "GPT-5.2"
                )

                mark_threads_used_for_knowledge(
                    cnx,
                    [str(t["thread_id"]) for t in ctx["threads"]],
                    "emergent"
                )
                cnx.commit()

                results.append({"id": cid, "title": title})

        return func.HttpResponse(json.dumps(results, ensure_ascii=False), mimetype="application/json")
    except Exception as e:
        logging.exception("Emergent Processing Error")
        return func.HttpResponse(str(e), status_code=500)

def run_phase3_common_processing(
    limit: int = 20,
    min_members: int = 2,
    min_usefulness: float = 0.6,
    product: str = "",
) -> func.HttpResponse:
    """
    Common issues:
      - Leaf: (member_count >= min_members AND max_solution_usefulness >= min_usefulness)
              OR (max_solution_usefulness >= high_usefulness)
              => generate support playbook using top 4 threads + Azure AI Search docs

    Uses GPT-5.2 via ThreadPoolExecutor.
    """
    try:
        results: List[Dict[str, Any]] = []

        min_leaf_max_usefulness = min_usefulness
        leaf_limit = max(1, int(limit))

        # ----------------------------------------------------------
        # 1) Gather all candidate contexts (serial, fast SQL reads)
        # ----------------------------------------------------------
        work_items: List[Dict[str, Any]] = []

        with sql_connect() as cnx:
            leaf_ids = _select_leaf_candidates_for_common(
                cnx, leaf_limit,
                min_max_usefulness=min_leaf_max_usefulness,
                min_members=min_members,
                product=product,
            )

            for cid in leaf_ids:
                ctx = fetch_leaf_context(cnx, cid, top_n=12)
                if not ctx or not ctx.get("threads"):
                    continue

                threads_sorted = sorted(
                    ctx["threads"],
                    key=lambda x: _safe_float(x.get("solution_usefulness"), 0.0),
                    reverse=True,
                )
                ctx_top4 = {"meta": ctx["meta"], "threads": threads_sorted[:4]}

                q = (
                    f"{ctx['meta'].get('product', '')} "
                    f"{ctx['meta'].get('signature', '')} "
                    f"{ctx['meta'].get('fix_pattern', '')}"
                ).strip()
                docs = _search_kb_articles(q, top_k=6, product=ctx["meta"].get("product"))
                ctx_top4["docs"] = docs

                work_items.append({"cid": cid, "ctx": ctx_top4, "docs": docs})

        if not work_items:
            return func.HttpResponse(
                json.dumps([], ensure_ascii=False), mimetype="application/json"
            )

        logging.info(
            "[Phase3] Common leaf processing: %d candidates gathered, starting GPT-5.2 pool",
            len(work_items),
        )

        # ----------------------------------------------------------
        # 2) Parallel GPT-5.2 calls
        # ----------------------------------------------------------
        def _generate_playbook(item: Dict[str, Any]) -> Dict[str, Any]:
            cid = item["cid"]
            ctx_top4 = item["ctx"]
            docs = item["docs"]
            meta = ctx_top4["meta"]

            logging.info(
                "[Phase3] cluster_id=%d (%s) – calling GPT-5.2",
                cid, meta.get("cluster_key", "?"),
            )

            try:
                output = generate_gpt5_common_leaf_support_playbook(ctx_top4)
            except Exception as e:
                logging.error(
                    "[Phase3] cluster_id=%d – GPT-5.2 call failed: %s", cid, str(e)[:300]
                )
                return {"cid": cid, "status": "gpt52_error", "error": str(e)}

            content = output.get("content", {})
            if not content or not content.get("title"):
                logging.warning("[Phase3] cluster_id=%d – empty/no-title output", cid)
                return {"cid": cid, "status": "empty_output"}

            logging.info(
                "[Phase3] cluster_id=%d (%s) – GPT-5.2 returned: %s",
                cid, meta.get("cluster_key", "?"), content.get("title", "?")[:80],
            )

            return {
                "cid": cid,
                "status": "ok",
                "output": output,
                "content": content,
                "ctx": ctx_top4,
                "docs": docs,
            }

        pool_workers = min(len(work_items), int(os.getenv("PHASE3_GPT52_MAX_WORKERS", "8")))
        logging.info("[phase3_common] Starting gpt52 pool: %d items, workers=%d", len(work_items), pool_workers)
        from concurrent.futures import ThreadPoolExecutor, as_completed
        futures_map = {}
        with ThreadPoolExecutor(max_workers=pool_workers) as executor:
            for item in work_items:
                futures_map[executor.submit(_generate_playbook, item)] = item["cid"]
            pro_results = []
            for fut in as_completed(futures_map):
                try:
                    pro_results.append(fut.result())
                except Exception as e:
                    pro_results.append({"cid": futures_map[fut], "status": "worker_error", "error": str(e)})
        logging.info("[phase3_common] Pool complete: %d results", len(pro_results))

        # ----------------------------------------------------------
        # 3) Serial DB writes (safe, single connection)
        # ----------------------------------------------------------
        with sql_connect() as cnx:
            cursor = cnx.cursor()

            for pr in pro_results:
                if not isinstance(pr, dict) or pr.get("status") != "ok":
                    results.append(pr if isinstance(pr, dict) else {"status": "unknown"})
                    continue

                cid = pr["cid"]
                content = pr["content"]
                output = pr["output"]
                ctx_top4 = pr["ctx"]
                docs = pr["docs"]
                path = output.get("area_path", {})

                area_path_id = upsert_area_path(cnx, path)

                problem = ""
                symptoms = content.get("symptoms") or []
                checks = content.get("checks") or []
                if isinstance(symptoms, list) and symptoms:
                    problem += "Symptoms:\n- " + "\n- ".join(symptoms[:10]) + "\n"
                if isinstance(checks, list) and checks:
                    problem += "\nChecks:\n- " + "\n- ".join(checks[:10]) + "\n"

                diagnostic_bundle = {
                    "symptoms": content.get("symptoms") or [],
                    "anti_symptoms": content.get("anti_symptoms") or [],
                    "checks": content.get("checks") or [],
                    "solution": content.get("solution") or [],
                    "verification": content.get("verification") or [],
                    "flows": content.get("flows") or [],
                    "notes": content.get("notes") or [],
                    "source_threads": [
                        {
                            "thread_id": str(t.get("thread_id", "")),
                            "source_url": str(t.get("source_url") or ""),
                        }
                        for t in ctx_top4["threads"]
                        if t.get("thread_id")
                    ],
                    "docs": [
                        {"title": d.get("title"), "url": d.get("url")}
                        for d in (docs or [])
                    ],
                }

                cursor.execute(
                    """
                    INSERT INTO dbo.CommonIssueSolutions
                    (ClusterID, PrimaryClusterKey, Title, ProblemStatement,
                     ResolutionSteps, ValidationChecks, DiagnosticLogicJson,
                     AreaPathID, GeneratedUtc, ModelVersion)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), ?)
                    """,
                    cid,
                    _trunc(ctx_top4["meta"]["cluster_key"], _P3_COMMON_CLUSTERKEY_MAX),
                    _trunc(content.get("title"), _P3_COMMON_TITLE_MAX),
                    problem,
                    json.dumps(content.get("solution") or [], ensure_ascii=False),
                    json.dumps(content.get("verification") or [], ensure_ascii=False),
                    json.dumps(diagnostic_bundle, ensure_ascii=False),
                    area_path_id,
                    "GPT-5.2",
                )

                mark_threads_used_for_knowledge(
                    cnx,
                    [str(t["thread_id"]) for t in ctx_top4["threads"]],
                    "common_leaf",
                )
                cnx.commit()

                results.append({
                    "kind": "leaf",
                    "id": cid,
                    "title": content.get("title"),
                })

        ok_count = sum(1 for r in results if isinstance(r, dict) and r.get("title"))

        return func.HttpResponse(
            json.dumps({"status": "ok", "processed": ok_count, "details": results}, ensure_ascii=False),
            mimetype="application/json",
        )
    except Exception as e:
        logging.exception("Common Processing Error")
        return func.HttpResponse(str(e), status_code=500)


# ---------------------------------------------------------
# ADO Output Logic (Updated for Phase 2 Schema)
# ---------------------------------------------------------
def run_phase3_search_selftest(req: func.HttpRequest) -> func.HttpResponse:
    q = (req.params.get("q") or "").strip()
    if not q:
        q = "static web apps custom domain 404"

    try:
        top_k = int(req.params.get("top_k", "5"))
    except Exception:
        top_k = 5

    cfg = {
        "AZURE_SEARCH_ENDPOINT_set": bool(os.getenv("AZURE_SEARCH_ENDPOINT")),
        "AZURE_SEARCH_API_KEY_set": bool(os.getenv("AZURE_SEARCH_API_KEY")),
        "AZURE_SEARCH_INDEX": os.getenv("AZURE_SEARCH_INDEX") or "",
        "AZURE_SEARCH_API_VERSION": os.getenv("AZURE_SEARCH_API_VERSION", "2024-07-01"),
        "select_fields": "Title,chunk,parent_id,URL,Product",
    }

    try:
        hits = _search_kb_articles(q, top_k=top_k)
        return func.HttpResponse(
            json.dumps({"status": "ok", "query": q, "config": cfg, "hit_count": len(hits), "hits": hits}, ensure_ascii=False),
            mimetype="application/json",
        )
    except Exception as e:
        logging.exception("Azure AI Search selftest failed")
        return func.HttpResponse(
            json.dumps({"status": "error", "query": q, "config": cfg, "error": str(e)}, ensure_ascii=False),
            status_code=500,
            mimetype="application/json",
        )


def push_phase3_to_devops(limit: int = 50) -> func.HttpResponse:
    """Push unpushed CommonIssueSolutions to ADO Wiki.

    Wiki path is built from the issue_cluster parent chain:
      /<root>/<product>/<L1 topic>/<L2 scenario>/<L3 variant>/<L4 leaf>

    This aligns with Phase 4B (variant @ L3), 4C (scenario @ L2),
    4D (topic @ L1).  The leaf playbook page lives directly at the
    L4 cluster_key segment — no extra title child page.
    """
    work_item_type = os.getenv("ADO_WORKITEM_TYPE", "Issue")
    wiki_id = os.environ["ADO_WIKI_ID"]

    wiki_root = (os.getenv("ADO_WIKI_ROOT", "") or "").strip()
    wiki_root = wiki_root.strip("/")
    wiki_root = f"/{wiki_root}" if wiki_root else ""

    logs = []

    def _md_list(items: Any, ordered: bool = False) -> str:
        if not isinstance(items, list) or not items:
            return ""
        lines = []
        for i, x in enumerate(items, start=1):
            s = str(x).strip()
            if not s:
                continue
            lines.append(f"{i}. {s}" if ordered else f"- {s}")
        return ("\n".join(lines) + "\n") if lines else ""

    def _safe_load_json(s: Any) -> Dict[str, Any]:
        if not isinstance(s, str) or not s.strip():
            return {}
        try:
            v = json.loads(s)
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}

    try:
        with sql_connect() as cnx:
            cursor = cnx.cursor()

            # Common -> Wiki (path derives from issue_cluster parent chain)
            cursor.execute("""
                SELECT TOP (?)
                    cis.ClusterID,
                    cis.Title,
                    cis.ResolutionSteps,
                    cis.ValidationChecks,
                    cis.DiagnosticLogicJson
                FROM dbo.CommonIssueSolutions cis
                WHERE cis.AdoPushedUtc IS NULL
            """, limit)

            common_rows = [dict(zip([c[0] for c in cursor.description], r)) for r in cursor.fetchall()]

            for row in common_rows:
                cluster_id = int(row["ClusterID"])
                title = row.get("Title") or ""

                steps = json.loads(row["ResolutionSteps"]) if isinstance(row.get("ResolutionSteps"), str) else []
                validations = json.loads(row["ValidationChecks"]) if isinstance(row.get("ValidationChecks"), str) else []

                diag = _safe_load_json(row.get("DiagnosticLogicJson"))
                diag_symptoms = diag.get("symptoms") or []
                diag_anti = diag.get("anti_symptoms") or []
                diag_checks = diag.get("checks") or []
                diag_flows = diag.get("flows") or []
                diag_notes = diag.get("notes") or []
                diag_docs = diag.get("docs") or []
                diag_source_threads = diag.get("source_threads") or []

                md = f"# {title}\n\n"

                if diag_symptoms:
                    md += "## Symptoms\n" + _md_list(diag_symptoms) + "\n"
                if diag_checks:
                    md += "## Triage / Checks\n" + _md_list(diag_checks) + "\n"

                md += "## Resolution Steps\n"
                md += _md_list(steps, ordered=True) or "_No resolution steps captured._\n"

                md += "\n## Validation\n"
                md += _md_list(validations) or "_No validation checks captured._\n"

                if diag_anti:
                    md += "\n## Anti-symptoms (disconfirmers)\n" + _md_list(diag_anti)
                if diag_flows:
                    md += "\n## Flows\n" + _md_list(diag_flows)
                if diag_notes:
                    md += "\n## Notes\n" + _md_list(diag_notes)

                # --- References ---
                ref_lines: List[str] = []

                source_threads = diag.get("source_threads") or []
                if isinstance(source_threads, list):
                    for st in source_threads:
                        if not isinstance(st, dict):
                            continue
                        tid = str(st.get("thread_id") or "").strip()
                        surl = str(st.get("source_url") or "").strip()
                        if tid and surl:
                            ref_lines.append(f"- Thread {tid} — {surl}")
                        elif surl:
                            ref_lines.append(f"- {surl}")

                if isinstance(diag_docs, list):
                    for d in diag_docs:
                        if not isinstance(d, dict):
                            continue
                        d_title = (d.get("title") or "").strip()
                        d_url = (d.get("url") or "").strip()
                        if d_title and d_url:
                            ref_lines.append(f"- {d_title} ({d_url})")
                        elif d_url:
                            ref_lines.append(f"- {d_url}")

                if ref_lines:
                    md += "\n## References\n"
                    md += "\n".join(ref_lines) + "\n\n"

                # Build wiki path from cluster hierarchy (L1/L2/L3/L4).
                # The leaf playbook page lives directly at the L4 segment,
                # aligning with Phase 4B (L3), 4C (L2), 4D (L1).
                product_seg, cluster_segs = _build_cluster_wiki_segments(cnx, cluster_id)
                raw_page_path = (
                    f"{wiki_root}/{product_seg}/"
                    + "/".join(cluster_segs)
                )
                page_path = _fit_wiki_page_path(raw_page_path)

                page = upsert_wiki_page(wiki_id, page_path, md)
                if page and page.get("url"):
                    cursor.execute(
                        "UPDATE dbo.CommonIssueSolutions SET AdoWikiPath=?, AdoPushedUtc=SYSUTCDATETIME() WHERE ClusterID=?",
                        page_path, cluster_id
                    )
                    cnx.commit()
                    logs.append(f"Common Pushed: {cluster_id} -> Wiki {page_path}")

        return func.HttpResponse(json.dumps({"status": "ok", "logs": logs}, ensure_ascii=False), mimetype="application/json")

    except Exception as e:
        logging.exception("ADO Push Error")
        return func.HttpResponse(str(e), status_code=500)