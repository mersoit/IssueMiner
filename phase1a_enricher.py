import os
import json
import logging
import re
import hashlib
import datetime as dt
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import ceil
from typing import Any, Dict, Optional, List, Tuple

import azure.functions as func
import pyodbc

from aoai_helpers import (
    # Clients & rate limiting
    make_nano_client,
    get_nano_deployment,
    get_rate_limiter,
    call_aoai_with_retry,
    estimate_tokens,
    RateLimiter,
    # SQL
    sql_connect,
    # Utilities
    safe_float as _safe_float,
    safe_str as _safe_str,
    clamp as _clamp,
    norm_space as _norm_space,
    json_dumps_compact as _json_dumps,
    now_utc as _now_utc,
    sha1_hex as _sha1_hex,
    extract_aoai_error as _extract_aoai_error,
    # JSON parsing
    best_effort_parse_json as _best_effort_parse_json,
    get_choice_text as _get_choice_text,
)


# -------------------------
# SQL helpers
# -------------------------

def fetch_candidates(
    cnx: pyodbc.Connection,
    limit: int,
    force: bool,
    date_from: str = "",
    date_to: str = "",
) -> List[Dict[str, Any]]:
    """
    Pull rows from ThreadsClean that need enrichment.
    Checks against dbo.thread_enrichment now to skip already processed.
    Matches ID types (INT vs NVARCHAR).
    Optional date_from / date_to filter on DateCreatedUtc (ISO date strings).
    """
    cur = cnx.cursor()

    date_filter = ""
    date_args: list = []
    if date_from:
        date_filter += " AND c.ProcessedUtc >= ?"
        date_args.append(date_from)
    if date_to:
        date_filter += " AND c.ProcessedUtc <= ?"
        date_args.append(date_to)

    if force:
        cur.execute(f"""
            SELECT TOP ({limit})
                c.QuestionID,
                c.DateCreatedUtc,
                c.SourceUrl,
                c.AskerName,
                c.ContentClean,
                c.ContentForLLM,
                c.CleanHash
            FROM dbo.ThreadsClean c
            WHERE 1=1 {date_filter}
            ORDER BY ISNULL(c.ProcessedUtc, '1900-01-01') DESC, c.QuestionID DESC
        """, *date_args)
    else:
        cur.execute(f"""
            SELECT TOP ({limit})
                c.QuestionID,
                c.DateCreatedUtc,
                c.SourceUrl,
                c.AskerName,
                c.ContentClean,
                c.ContentForLLM,
                c.CleanHash
            FROM dbo.ThreadsClean c
            LEFT JOIN dbo.thread_enrichment e
                ON e.thread_id = CAST(c.QuestionID AS NVARCHAR(50))
            WHERE e.thread_id IS NULL {date_filter}
            ORDER BY ISNULL(c.ProcessedUtc, '1900-01-01') DESC, c.QuestionID DESC
        """, *date_args)

    rows = []
    cols = [d[0] for d in cur.description]
    for r in cur.fetchall():
        rows.append({cols[i]: r[i] for i in range(len(cols))})
    return rows


def upsert_thread_enrichment(
    cnx: pyodbc.Connection,
    src_row: Dict[str, Any],
    payload: Dict[str, Any]
) -> None:
    """
    Upsert dbo.thread_enrichment matching the new schema with snake_case columns.
    Maps internal payload keys (PascalCase) to DB columns.
    """
    cur = cnx.cursor()

    thread_id = str(src_row["QuestionID"])

    source_name = "msqa"
    s_url = src_row.get("SourceUrl") or ""
    s_url_lower = s_url.lower()
    if "learn.microsoft.com" in s_url_lower:
        source_name = "msqa"
    elif "stackoverflow.com" in s_url_lower:
        source_name = "so"
    elif "github.com" in s_url_lower:
        source_name = "github"
    else:
        source_name = "msqa"

    s_created = src_row.get("DateCreatedUtc")
    if isinstance(s_created, dt.datetime) and s_created.tzinfo is not None:
        s_created = s_created.astimezone(dt.timezone.utc).replace(tzinfo=None)

    lang = payload["Language"]
    prod = payload["Product"]
    cls = payload["Classification"]
    res_status = payload["ResolutionStatus"]

    sol_useful = float(payload["SolutionUsefulness"])
    is_emergent = 1 if payload["IsEmergent"] else 0
    emergent_sig = payload.get("EmergentSignal")

    t_key = payload.get("TopicClusterKey") or ""
    s_key = payload.get("ScenarioClusterKey") or ""
    v_key = payload.get("VariantClusterKey") or ""
    r_key = payload.get("ResolutionLeafKey") or ""

    sig = payload.get("SignatureText") or ""
    res_sig = payload.get("ResolutionSignatureText") or ""

    n_json = payload["RawLLMJson"]
    n_model = payload["ModelVersion"]
    n_prompt = "v3_2026-01-26"

    cur.execute("""
        MERGE dbo.thread_enrichment AS t
        USING (SELECT ? AS thread_id) AS s
        ON t.thread_id = s.thread_id
        WHEN MATCHED THEN
            UPDATE SET
                source_url = ?,
                source_created_at = ?,
                ingested_at = SYSUTCDATETIME(),
                language = ?,
                product = ?,
                classification = ?,
                resolution_status = ?,
                solution_usefulness = ?,
                is_emergent = ?,
                emergent_signal = ?,
                topic_cluster_key = ?,
                scenario_cluster_key = ?,
                variant_cluster_key = ?,
                resolution_leaf_key = ?,
                signature_text = ?,
                resolution_signature_text = ?,
                nano_json = ?,
                nano_model = ?,
                nano_prompt_version = ?
        WHEN NOT MATCHED THEN
            INSERT (
                thread_id, source, source_url, source_created_at, ingested_at,
                language, product, classification, resolution_status,
                solution_usefulness, is_emergent, emergent_signal,
                topic_cluster_key, scenario_cluster_key, variant_cluster_key, resolution_leaf_key,
                signature_text, resolution_signature_text,
                nano_json, nano_model, nano_prompt_version
            )
            VALUES (
                ?, ?, ?, ?, SYSUTCDATETIME(),
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?, ?
            );
    """,
    # USING
    thread_id,
    # UPDATE SET
    s_url, s_created, lang, prod, cls, res_status,
    sol_useful, is_emergent, emergent_sig,
    t_key, s_key, v_key, r_key,
    sig, res_sig,
    n_json, n_model, n_prompt,
    # INSERT VALUES
    thread_id, source_name, s_url, s_created,
    lang, prod, cls, res_status,
    sol_useful, is_emergent, emergent_sig,
    t_key, s_key, v_key, r_key,
    sig, res_sig,
    n_json, n_model, n_prompt
    )
    cnx.commit()

# -------------------------
# Azure Foundry (AOAI) – uses centralized helpers
# -------------------------

# Keep module-level aliases so external code that imports make_aoai_client still works
def make_aoai_client():
    """Backwards-compatible alias – returns the shared nano client."""
    return make_nano_client()


def call_nano_llm(thread_id: str, content_for_llm: str, known_products: Optional[List[str]] = None) -> Tuple[Dict[str, Any], str, bool, str]:
    deployment = get_nano_deployment()
    client = make_nano_client()

    # Build the product guidance dynamically from DB-known products
    if known_products:
        product_list_str = ", ".join(known_products)
        product_instruction = (
            f"- product: You MUST reuse an existing product name from this list when the thread matches: [{product_list_str}].\n"
            "  Rules (follow in order):\n"
            "  1. Copy the EXACT string from the list — same spelling, same casing, same spacing. Do NOT paraphrase, abbreviate, or expand it.\n"
            "  2. If two entries look similar (e.g. 'Front Door' and 'Frontdoor'), pick the one that is already in the list; never invent a variant.\n"
            "  3. Only create a new product name if the thread clearly belongs to an Azure service that has NO match in the list.\n"
            "     In that case use the short official service name without the 'Azure' prefix.\n"
            "  4. If it is not an Azure service, use 'Other'.\n"
        )
    else:
        product_instruction = (
            "- product: one of Azure services [System Center Operations Manager, System Center Orchestrator, System Center Service Manager, DevOps, Web Apps, Functions, Static Web Apps, App Service, Redis, Storage Account, Logic Apps, Ai Search, Service Bus, Event Grid, Event Hub, APIM, ARM, Policy, Virtual Network, Monitor, System Center]. "
            "If it belongs to another Azure Service then list that Service without the Azure prefix. If it's not Azure Service note as Other'\n"
        )

    system = (
        "You are an enrichment classifier for technical support forum threads (Microsoft Q&A, GitHub issues, StackOverflow, etc.).\n\n"
        "Task: Read CONTENT and return EXACTLY one JSON object (no markdown, no comments) using ONLY these keys:\n"
        "language, product, classification, resolution_status,\n"
        "solution_usefulness, is_emergent, emergent_signal,\n"
        "topic_cluster_key, scenario_cluster_key, variant_cluster_key, resolution_leaf_key,\n"
        "signature_text, resolution_signature_text\n\n"

        "Allowed values:\n"
        "- language: one of [en, zh, ja, ko, es, fr, de, pt, ru, other].\n"
        + product_instruction +
        "- classification: one of [emergent_issue, common_issue, learn_microsoft, faq, good_trivia, bad_trivia, custom_code, not_usable].\n"
        "- resolution_status: one of [solved, workaround, unsolved, unknown].\n\n"

        "Scoring:\n"
        "- solution_usefulness: 0.0..1.0 (value for support troubleshooting/solving).\n"
        "  0.0-0.2 generic/fluff/likely low-quality answer or only AI generated answer; no concrete diagnostics or insight.\n"
        "  0.3-0.5 useful hints/insights/diagnostics methods/constraints even if the article was not solution oriented.\n"
        "  0.6-0.8 strong diagnostics + plausible fix/workaround with context.\n"
        "  0.9-1.0 confirmed root cause + reproducible fix + clear scope/constraints.\n\n"

        "Emergent detection (read carefully before setting is_emergent):\n"
        "Set is_emergent=true ONLY when the thread shows STRONG evidence of AT LEAST TWO of these signals:\n"
        "  (a) REGRESSION: poster explicitly states it worked before and recently broke "
        "      (look for: 'used to work', 'stopped working', 'broke after', 'recently', 'since last week').\n"
        "  (b) PLATFORM-SIDE ROOT CAUSE: error messages, HTTP 5xx responses, or service behavior "
        "      that cannot be explained by customer misconfiguration "
        "      (look for: 500/502/503 from the service itself, 'service unavailable', unexpected API behavior "
        "      with no config change on customer side).\n"
        "  (c) CROSS-TENANT BREADTH: poster references seeing same issue across multiple environments, "
        "      tenants, or subscriptions, OR the symptoms are generic enough that they could not be "
        "      caused by a single misconfiguration "
        "      (look for: 'all my customers', 'multiple subscriptions', 'not just me', 'others are seeing this').\n"
        "  (d) UNRESOLVED WITH NO KNOWN FIX: issue is unsolved AND no official workaround exists in the thread "
        "      (resolution_status=unsolved AND solution_usefulness < 0.4).\n"
        "  (e) SUDDEN ONSET: issue appeared suddenly with no change on customer side "
        "      (look for: 'no changes made', 'nothing changed', 'happened overnight', 'started today').\n"
        "Do NOT set is_emergent=true for:\n"
        "  - Single-user misconfiguration or setup errors (even if frustrating).\n"
        "  - Known product limitations or documented behaviors.\n"
        "  - Issues caused by customer code bugs.\n"
        "  - Questions about how to use a feature (even if unanswered).\n"
        "  - Performance issues with no service-side signal.\n"
        "emergent_signal: 0.0..1.0. Score based on how many of signals (a)-(e) are present and how strong they are.\n"
        "  0.0-0.3: is_emergent=false; at most 1 weak signal present.\n"
        "  0.4-0.6: borderline; 1 strong or 2 weak signals; set is_emergent=true only if very confident.\n"
        "  0.7-0.9: 2+ strong signals; clear cross-user pattern with no obvious config explanation.\n"
        "  1.0: all signals present; near-certain platform incident.\n"
        "If is_emergent=false, set emergent_signal=null.\n\n"

        "Keying / taxonomy rules (VERY IMPORTANT):\n"
        "Keys must be deterministic: lowercase, kebab-case, hyphen delimited; NO ids/timestamps/usernames/tenant names/subscriptions/paths.\n\n"

        "PRODUCT MUST NOT APPEAR IN KEYS (CRITICAL):\n"
        "The product field is already stored separately. NEVER include the product name, product alias, or\n"
        "product abbreviation as a token in any key (topic, scenario, variant, leaf).\n"
        "Product should be determined by the name of the product that the support team of that product would be best to assist the content of the issue with.\n"
        "For example, if the issue is about SQL server connection issue but it is solely about virtual network issue, the product should be Virtual Network rather than SQL Server.\n"
        "Examples of what NOT to do:\n"
        "  BAD (product=APIM):   topic='api-management-custom-domain'  <- 'api-management' is the product alias\n"
        "  BAD (product=APIM):   topic='apim-networking'               <- 'apim' is the product abbreviation\n"
        "  GOOD (product=APIM):  topic='custom-domain'\n"
        "  GOOD (product=APIM):  topic='networking'\n"
        "  BAD (product=Static Web Apps): topic='static-web-apps-custom-domain'\n"
        "  GOOD (product=Static Web Apps): topic='custom-domain'\n"
        "This rule applies to ALL levels: topic, scenario, variant, leaf.\n\n"

        "Definitions:\n"
        "- Topic: the TECHNICAL AREA or SERVICE COMPONENT within the product where the issue occurs.\n"
        "  It must be short (2-4 tokens), stable, and reusable across many different operations.\n"
        "  Think: 'What part of the product is involved?' NOT 'What product and what part?'\n"
        "  Examples for APIM: 'custom-domain', 'networking', 'authentication', 'developer-portal',\n"
        "  'policy', 'migration', 'backend-integration', 'logging-diagnostics', 'gateway', 'deployment'.\n"
        "  A topic must be broad enough to contain at least 3-5 distinct scenario types.\n"
        "  Do NOT create a topic for a single operation or single error.\n"
        "- Scenario: the SPECIFIC OPERATION or FAILURE MODE within the topic.\n"
        "  Format: <operation-or-context>[-<constraint>]  (NO product prefix, NO topic prefix repeat).\n"
        "  Examples under topic='custom-domain': 'add-domain', 'validation-stuck', 'ssl-binding', 'delete-stuck'.\n"
        "  Examples under topic='authentication': 'oauth2-token-validate', 'managed-identity-backend', 'jwt-401'.\n"
        "- Variant: the DISTINCTIVE SIGNAL or BRANCHING CONDITION within the scenario.\n"
        "  Format: <error-token-or-signal>[-<constraint>]  (short, no prefix repetition).\n\n"

        "Clustering keys (2-5 tokens each, NO product prefix):\n"
        "- topic_cluster_key: <technical-area-or-component>  (2-4 tokens, no product name/alias)\n"
        "- scenario_cluster_key: <operation-or-failure-mode>[-<constraint>]  (must differ from topic key)\n"
        "- variant_cluster_key: <error-signal-or-branch>[-<constraint>]  (must differ from scenario key)\n\n"

        "Leaf key policy (STRICT):\n"
        "- Only create a specific leaf when ALL are true:\n"
        "  (a) resolution_status is solved OR workaround\n"
        "  (b) solution_usefulness >= 0.6\n"
        "  (c) there is a specific root cause AND a concrete fix/config change that is not generic\n"
        "- If any are false, set resolution_leaf_key to:\n"
        "  \"<variant_cluster_key>__unknown-resolution\"\n"
        "- If true, set resolution_leaf_key to:\n"
        "  \"<variant_cluster_key>__rc-<rootcause_tag>__fix-<fix_tag1>-<fix_tag2>\"\n"
        "  rootcause_tag and fix_tag(s) must be short kebab-case tags, not sentences.\n"
        "  Do NOT invent fix tags for generic advice (update, restart, clear cache) unless highly specific.\n\n"

        "Signature texts (for embeddings; <= 350 chars; no secrets; no long logs):\n"
        "- signature_text must be symptom-centric and comparable across threads:\n"
        "  \"<Product>; <component>; <operation>; <symptom/error or error-code>; <constraints>; <signal>;\"\n"
        "- resolution_signature_text must be tag-like and comparable:\n"
        "  \"rc:<rootcause_tag or unknown>; fix:<fix_tag1>|<fix_tag2>|...; scope:<constraint/scope tags>;\"\n\n"

        "General normalization rules:\n"
        "- If error codes exist, include the most stable one in keys/signatures (e.g., 401, 403, 429, timeout, AADSTSxxxx).\n"
        "- Replace versions with major-only when relevant (e.g., python-3, node-18).\n"
        "- If uncertain about a token, omit it rather than hallucinate.\n\n"

        "Output rules:\n"
        "- Return ONLY valid JSON with double quotes. No trailing commas.\n"
        "- Always fill ALL keys (best effort), even if classification is not_usable.\n"
        "- If insufficient/off-topic: classification=\"not_usable\", solution_usefulness=0.0, resolution_status=\"unknown\", product=\"Other\".\n"
    )

    user = (
        f"ThreadID: {thread_id}\n"
        "Fill ALL fields based on CONTENT:\n"
        f"{content_for_llm}"
    )

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]

    try:
        resp = call_aoai_with_retry(
            client,
            model=deployment,
            messages=messages,
            response_format={"type": "json_object"},
            estimated_prompt_tokens=estimate_tokens(system) + estimate_tokens(user),
            rate_limiter=get_rate_limiter("nano"),  # ← explicit nano limiter
            caller_tag="phase1a",
        )
        raw_text = _get_choice_text(resp)
        obj, ok, err = _best_effort_parse_json(raw_text)
        if ok:
            return obj, raw_text, True, ""
        raise RuntimeError(f"invalid_json:{err}")
    except Exception as e:
        raise RuntimeError(_extract_aoai_error(e))


# -------------------------
# Product normalization (DB-driven + fuzzy match)
# -------------------------
# We query the DB for known product names and inject them into the 1A prompt.
# Post-LLM, we fuzzy-match the output against known products.
# When the DB has no products yet (cold start), we seed from a hardcoded
# alias map so the very first batch normalizes correctly.

_PRODUCT_ALIASES: Dict[str, str] = {
    "API Management": "APIM",
    "Api Management": "APIM",
    "Azure API Management": "APIM",
    "Azure APIM": "APIM",
    "api-management": "APIM",
    "Azure DevOps": "DevOps",
    "Azure OpenAI": "OpenAI",
    "Azure Ai Search": "Ai Search",
    "Azure AI Search": "Ai Search",
    "Entra External ID": "Entra External ID",
    "Entra": "Entra External ID",
    "System Center Orchestrator": "System Center",
    "SCOM": "System Center",
    "System Center Operations Manager": "System Center",
    "SCSM": "System Center",
    "System Center Service Manager": "System Center",
    "Vnet": "Virtual Network",
    "VNet": "Virtual Network",
    "vnet": "Virtual Network",
    "Azure Virtual Network": "Virtual Network",
    "Azure VNet": "Virtual Network",
    "IIS": "Internet Information Services",
    "Internet Information Server": "Internet Information Services",
    "Windows IIS": "Internet Information Services",
}

# Products that frequently appear in IIS Q&A threads but are actually separate products.
# If the LLM returns one of these for an IIS-tagged thread, we trust the LLM — the
# thread is about that product, not IIS.
_CROSS_PRODUCT_SIGNALS: Dict[str, List[str]] = {
    # signal tokens (lowercase) -> canonical product
    "application gateway":    ["application gateway", "app gateway", "appgw", "waf policy", "backend probe"],
    "Application Gateway":    ["Application Gateway"],
    "Virtual Network":        ["vnet integration", "nsg", "udr", "peering"],
    "AKS":                    ["aks", "kubernetes", "kubectl", "helm"],
    "ARM":                    ["arm template", "bicep", "resource manager"],
    "Front Door":             ["front door", "afd", "cdn profile"],
    "Load Balancer":          ["load balancer", "nlb", "slb"],
}


def _detect_cross_product(
    raw_llm_product: str,
    signature_text: str,
    known_products: List[str],
) -> Optional[str]:
    """
    If the LLM assigned a product (e.g. IIS) but the thread signature clearly
    signals a different known product, return that product instead.
    Returns None if no strong cross-product signal is detected.
    """
    sig_lower = (signature_text or "").lower()
    raw_lower = raw_llm_product.lower()

    for canonical, signals in _CROSS_PRODUCT_SIGNALS.items():
        # Skip if the LLM already picked this product
        if canonical.lower() == raw_lower:
            continue
        # Skip if this canonical isn't in the known product list
        if not any(canonical.lower() == kp.lower() for kp in known_products):
            continue
        # Check if any signal phrase appears in the thread signature
        if any(sig in sig_lower for sig in signals):
            return canonical
    return None


_SEED_PRODUCTS: List[str] = [
    "APIM", "DevOps", "Web Apps", "Functions", "Static Web Apps",
    "App Service", "Redis", "Storage Account", "Logic Apps",
    "Ai Search", "Service Bus", "Event Grid", "Event Hub",
    "ARM", "Policy", "Virtual Network", "Monitor", "OpenAI",
    "Entra External ID", "System Center", "Internet Information Services",
]


def fetch_known_products(cnx: pyodbc.Connection) -> List[str]:
    """Return canonical product names from dbo.enrichment_products.

    On cold start (empty table) the seed list is inserted automatically so the
    very first batch has a stable reference set to normalise against.
    """
    cur = cnx.cursor()
    cur.execute(
        "SELECT product_name FROM dbo.enrichment_products ORDER BY product_name"
    )
    db_products = [str(row[0]).strip() for row in cur.fetchall() if row[0]]
    if db_products:
        return db_products
    # Cold start — seed the table so the LLM prompt and normaliser are consistent
    # from the very first batch.
    logging.info("[1A] enrichment_products empty — seeding %d products", len(_SEED_PRODUCTS))
    for name in _SEED_PRODUCTS:
        cur.execute("""
            IF NOT EXISTS (SELECT 1 FROM dbo.enrichment_products WHERE product_name = ?)
                INSERT INTO dbo.enrichment_products (product_name, is_seed, thread_count)
                VALUES (?, 1, 0)
        """, name, name)
    cnx.commit()
    return list(_SEED_PRODUCTS)


def fetch_product_aliases(cnx: pyodbc.Connection) -> Dict[str, str]:
    """Return {alias_name: canonical_name} from dbo.product_aliases.

    Falls back to the hardcoded _PRODUCT_ALIASES when the table is empty
    (e.g., aliases not yet seeded via the dashboard).
    """
    cur = cnx.cursor()
    try:
        cur.execute("SELECT alias_name, canonical_name FROM dbo.product_aliases")
        db_aliases = {row[0].strip(): row[1].strip() for row in cur.fetchall()}
    except Exception:
        db_aliases = {}
    if db_aliases:
        return db_aliases
    return dict(_PRODUCT_ALIASES)


def _register_product(cnx: pyodbc.Connection, product_name: str) -> None:
    """Upsert a normalised product name into dbo.enrichment_products.

    Silently skipped for empty or 'Other' values.
    """
    name = (product_name or "").strip()
    if not name or name == "Other":
        return
    try:
        cur = cnx.cursor()
        cur.execute("""
            MERGE dbo.enrichment_products AS t
            USING (SELECT ? AS product_name) AS s
            ON t.product_name = s.product_name
            WHEN MATCHED THEN
                UPDATE SET last_seen_at = SYSUTCDATETIME(),
                           thread_count = thread_count + 1
            WHEN NOT MATCHED THEN
                INSERT (product_name, is_seed) VALUES (?, 0);
        """, name, name)
        cnx.commit()
    except Exception as e:
        logging.warning("[1A] _register_product failed for %r: %s", name, e)


def _normalize_product(
    raw: str,
    known_products: Optional[List[str]] = None,
    db_aliases: Optional[Dict[str, str]] = None,
) -> str:
    """Fuzzy-match raw product name against known DB products.

    Strategy (in order):
      0. Exact alias lookup: DB aliases, then hardcoded _PRODUCT_ALIASES.
      1. Exact case-insensitive match against known products.
      2. Substring containment: if a known product is fully contained in raw
         (e.g., "Azure API Management" contains "APIM" after alias expansion)
         or raw is contained in a known product.
      3. Normalized token overlap (>=60% Jaccard) picks the best known product.
      4. Fall through: return raw as-is (new product, will become canonical).
    """
    raw = (raw or "").strip()
    if not raw or raw == "Other":
        return raw

    # 0a. Check DB alias map first
    if db_aliases:
        alias_hit = db_aliases.get(raw)
        if alias_hit:
            return alias_hit

    # 0b. Fall back to hardcoded alias map
    alias_hit = _PRODUCT_ALIASES.get(raw)
    if alias_hit:
        return alias_hit

    if not known_products:
        return raw

    raw_lower = raw.lower()

    # 1. Exact match (case-insensitive)
    for kp in known_products:
        if kp.lower() == raw_lower:
            return kp

    # Build token sets for fuzzy matching
    def _tokens(s: str) -> set:
        return set(s.lower().replace("-", " ").replace("_", " ").split())

    # Common prefixes/suffixes that are noise for matching
    _NOISE = {"azure", "microsoft"}

    def _sig_tokens(s: str) -> set:
        return _tokens(s) - _NOISE

    raw_sig = _sig_tokens(raw)

    # 2. Containment (either direction) after stripping noise
    for kp in known_products:
        kp_sig = _sig_tokens(kp)
        if not kp_sig or not raw_sig:
            continue
        # raw's meaningful tokens are a subset of known, or vice versa
        if raw_sig <= kp_sig or kp_sig <= raw_sig:
            return kp

    # 3. Jaccard overlap >= 0.6
    best_score = 0.0
    best_match = None
    for kp in known_products:
        kp_sig = _sig_tokens(kp)
        if not kp_sig or not raw_sig:
            continue
        intersection = raw_sig & kp_sig
        union = raw_sig | kp_sig
        score = len(intersection) / len(union) if union else 0.0
        if score > best_score:
            best_score = score
            best_match = kp

    if best_match and best_score >= 0.6:
        return best_match

    # 4. No match — return as-is; this becomes a new canonical product
    return raw


# -------------------------
# Mapping LLM JSON -> DB payload
# -------------------------
def map_llm_to_db_payload(
    thread_id: str,
    llm_json: Dict[str, Any],
    raw_llm_json_text: str,
    known_products: Optional[List[str]] = None,
    db_aliases: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Converts LLM output into the new thread_enrichment row shape.
    """
    lang = _safe_str(llm_json.get("language", "en")).lower()
    valid_langs = {"en", "zh", "ja", "ko", "es", "fr", "de", "pt", "ru", "other"}
    if lang not in valid_langs:
        lang = "other"

    product = _normalize_product(_safe_str(llm_json.get("product", "Other")), known_products, db_aliases)

    # Cross-product bleed guard: if the thread's signature clearly signals a different
    # known product (e.g. App Gateway thread tagged under IIS), override the assignment.
    if known_products:
        sig_text = _safe_str(llm_json.get("signature_text", ""))
        cross = _detect_cross_product(product, sig_text, known_products)
        if cross:
            logging.debug("[1A] cross_product_override thread=%s %s -> %s", thread_id, product, cross)
            product = cross

    if len(product) > 64:
        product = product[:64]

    classification = _safe_str(llm_json.get("classification", "not_usable")).strip().lower()
    valid_class = {
        "emergent_issue", "common_issue", "learn_microsoft", "faq",
        "good_trivia", "bad_trivia", "custom_code", "not_usable"
    }
    if classification not in valid_class:
        classification = "not_usable"

    resolution = _safe_str(llm_json.get("resolution_status", "unknown")).strip().lower()
    if resolution not in {"solved", "workaround", "unsolved", "unknown"}:
        resolution = "unknown"

    sol_useful = _clamp(_safe_float(llm_json.get("solution_usefulness", 0.0), 0.0), 0.0, 1.0)
    
    is_emergent = bool(llm_json.get("is_emergent", False))
    emergent_sig = llm_json.get("emergent_signal", None)
    emergent_sig_f = None
    if emergent_sig is not None:
        emergent_sig_f = _clamp(_safe_float(emergent_sig, 0.0), 0.0, 1.0)

    topic_key = _norm_space(_safe_str(llm_json.get("topic_cluster_key", "")).strip())[:200]
    scenario_key = _norm_space(_safe_str(llm_json.get("scenario_cluster_key", "")).strip())[:250]
    variant_key = _norm_space(_safe_str(llm_json.get("variant_cluster_key", "")).strip())[:300]
    res_leaf_key = _norm_space(_safe_str(llm_json.get("resolution_leaf_key", "")).strip())[:350]

    if not topic_key:
        topic_key = f"{product}:unknown"
    if not scenario_key: scenario_key = topic_key
    if not variant_key: variant_key = scenario_key
    if not res_leaf_key: res_leaf_key = f"{variant_key}__unknown"

    sig_text = _norm_space(_safe_str(llm_json.get("signature_text", "")).strip())
    res_sig_text = _norm_space(_safe_str(llm_json.get("resolution_signature_text", "")).strip())

    model_version = os.getenv("AOAI_MODEL_VERSION", "gpt-5-nano")

    return {
        "ThreadID": thread_id,
        "Language": lang,
        "Product": product,
        "Classification": classification,
        "ResolutionStatus": resolution,
        
        "SolutionUsefulness": sol_useful,
        "IsEmergent": 1 if is_emergent else 0,
        "EmergentSignal": emergent_sig_f if is_emergent else None,

        "TopicClusterKey": topic_key,
        "ScenarioClusterKey": scenario_key,
        "VariantClusterKey": variant_key,
        "ResolutionLeafKey": res_leaf_key,

        "SignatureText": sig_text,
        "ResolutionSignatureText": res_sig_text,

        "ModelVersion": model_version,
        "RawLLMJson": raw_llm_json_text if raw_llm_json_text else _json_dumps(llm_json),
    }


# -------------------------
# Function entry point
# -------------------------
def run_phase1a_enrich(req: func.HttpRequest) -> func.HttpResponse:
    try:
        limit = int(req.params.get("limit", "25"))
        limit = max(1, min(limit, 1000000))

        dryrun = req.params.get("dryrun", "0") == "1"
        force = req.params.get("force", "0") == "1"
        debug = req.params.get("debug", "0") == "1"
        date_from = (req.params.get("date_from") or "").strip()
        date_to   = (req.params.get("date_to")   or "").strip()

        # Env validation
        _ = os.environ["SQL_CONNECTION_STRING"]
        _ = os.environ["AOAI_ENDPOINT"]
        _ = os.environ["AOAI_API_KEY"]
        _ = os.environ["AOAI_DEPLOYMENT"]

        max_input_chars = int(os.getenv("PHASE2_MAX_INPUT_CHARS", "4500"))
        max_workers = int(os.getenv("PHASE2_MAX_WORKERS", "16"))
        progress_every = int(os.getenv("PHASE2_PROGRESS_EVERY", "100"))

        # Use centralized rate limiter (shared across all phases in process)
        limiter = get_rate_limiter()

        results = []
        processed = 0

        with sql_connect() as cnx:
            known_products = fetch_known_products(cnx)
            db_aliases = fetch_product_aliases(cnx)
            logging.info("[1A] known_products from DB: %s", known_products)
            logging.info("[1A] product aliases loaded: %d", len(db_aliases))

            candidates = fetch_candidates(cnx, limit=limit, force=force, date_from=date_from, date_to=date_to)

            jobs = []
            for row in candidates:
                thread_id = str(row["QuestionID"])
                content = row.get("ContentForLLM") or row.get("ContentClean") or ""
                content = _norm_space(_safe_str(content))[:max_input_chars]
                jobs.append((thread_id, content, row))

            aoai_results = []

            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                future_map = {}
                for (thread_id, content, src_row) in jobs:
                    future = ex.submit(call_nano_llm, thread_id, content, known_products)
                    future_map[future] = (thread_id, content, src_row)

                for idx, fut in enumerate(as_completed(future_map)):
                    thread_id, content_for_llm, src_row = future_map[fut]
                    try:
                        llm_json, raw_text, parse_ok, parse_error = fut.result()
                        payload = map_llm_to_db_payload(thread_id, llm_json, raw_text, known_products, db_aliases)
                    except Exception as e:
                        # Fallback payload
                        payload = {
                            "ThreadID": thread_id,
                            "Language": "other",
                            "Product": "Other",
                            "Classification": "not_usable",
                            "ResolutionStatus": "unknown",
                            "SolutionUsefulness": 0.0,
                            "IsEmergent": 0,
                            "EmergentSignal": None,
                            "TopicClusterKey": None,
                            "ScenarioClusterKey": None,
                            "VariantClusterKey": None,
                            "ResolutionLeafKey": None,
                            "SignatureText": None,
                            "ResolutionSignatureText": None,
                            "ModelVersion": os.getenv("AOAI_MODEL_VERSION", "gpt-5-nano"),
                            "RawLLMJson": f"aoai_error: {str(e)}",
                        }
                        parse_ok = False
                        parse_error = "aoai_error"

                    aoai_results.append((payload, parse_ok, parse_error, src_row))

                    if debug and (idx + 1) % progress_every == 0:
                        results.append({"progress": f"{idx + 1}/{len(jobs)} AOAI done"})

            # DB Write
            if not dryrun:
                with sql_connect() as db:
                    for payload, parse_ok, parse_error, src_row in aoai_results:
                        db_error = None
                        try:
                            upsert_thread_enrichment(db, src_row, payload)
                            # Auto-register the normalised product name so it is
                            # available to future batches and to UI dropdowns.
                            _register_product(db, payload["Product"])
                        except Exception as e:
                            db_error = f"upsert_thread_enrichment failed: {e}"

                        processed += 1
                        
                        out = {
                            "thread_id": payload["ThreadID"],
                            "product": payload["Product"],
                            "classification": payload["Classification"],
                            "solution_usefulness": payload["SolutionUsefulness"],
                            "variant_key": payload.get("VariantClusterKey"),
                            "parse_ok": parse_ok,
                        }
                        if debug:
                            out.update({
                                "parse_error": parse_error,
                                **({"db_error": db_error} if db_error else {}),
                            })
                        results.append(out)
            else:
                for payload, parse_ok, parse_error, src_row in aoai_results:
                     results.append({
                        "thread_id": payload["ThreadID"],
                        "dryrun_payload": payload,
                        "parse_ok": parse_ok
                     })

        return func.HttpResponse(
            json.dumps({
                "status": "ok",
                "processed": processed,
                "dryrun": dryrun,
                "force": force,
                "results": results
            }, ensure_ascii=False),
            mimetype="application/json"
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}),
            mimetype="application/json",
            status_code=500
        )