# -*- coding: utf-8 -*-
"""
Phase 4B – Populate Variant Wiki Pages
=======================================
For each L3 (variant) cluster that lacks a wiki page, this phase:
  1. Gathers all child L4 (leaf) clusters + their CommonIssueSolutions playbooks.
  2. Calls GPT-5.2 to produce a concise, decision-tree-style variant guide
     aimed at junior support engineers.
  3. Pushes the Markdown to the ADO Wiki (creating ancestor pages as needed).
  4. Records the wiki path + timestamp back in the DB.

The variant page is intentionally *not* a reading session.  It focuses on:
  - A 1-2 sentence TL;DR
  - An if/then quick-triage tree to route to the right leaf
  - Essential commands (customer-side + Azure backend) with plain-English explanations
  - A short leaf-distinguisher matrix
  - Common pitfalls specific to this variant

Deep architecture (topic-level) and scenario flowcharts are populated in later
phases and are NOT duplicated here.
"""

import os
import json
import logging
import re
import hashlib
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyodbc
import azure.functions as func
from openai import AzureOpenAI

from ado_devops import upsert_wiki_page

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

_MAX_WORKERS = int(os.getenv("PHASE4B_MAX_WORKERS", "4"))
_MAX_COMPLETION_TOKENS = int(os.getenv("PHASE4B_MAX_TOKENS", "3500"))
_BATCH_SIZE = int(os.getenv("PHASE4B_BATCH_SIZE", "50"))


# ---------------------------------------------------------
# Infra helpers (mirrors Phase 3 conventions)
# ---------------------------------------------------------

def _sql_connect() -> pyodbc.Connection:
    return pyodbc.connect(os.environ["SQL_CONNECTION_STRING"], timeout=45)


def _make_gpt52_client() -> AzureOpenAI:
    endpoint = os.environ.get("AOAI_ENDPOINT_GPT52") or os.environ.get("AOAI_ENDPOINT")
    key = os.environ.get("AOAI_API_KEY_GPT52") or os.environ.get("AOAI_API_KEY")
    if not endpoint or not key:
        raise RuntimeError("Missing AOAI endpoint/key for GPT-5.2 (set AOAI_ENDPOINT_GPT52 / AOAI_API_KEY_GPT52)")
    api_version = os.getenv("AOAI_API_VERSION", "2025-01-01-preview")
    return AzureOpenAI(azure_endpoint=endpoint.rstrip("/"), api_key=key, api_version=api_version)


def _get_gpt52_deployment() -> str:
    dep = (os.environ.get("AOAI_DEPLOYMENT_GPT52") or "").strip()
    if not dep:
        raise RuntimeError("Missing AOAI_DEPLOYMENT_GPT52 – refusing fallback.")
    return dep


def _slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s[:80] or "untitled"


def _ensure_single_slashes(path: str) -> str:
    p = (path or "").strip().replace("\\", "/")
    while "//" in p:
        p = p.replace("//", "/")
    if not p.startswith("/"):
        p = "/" + p
    if p != "/" and p.endswith("/"):
        p = p[:-1]
    return p


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def _safe_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(v) if v is not None else default
    except Exception:
        return default


# ---------------------------------------------------------
# Wiki path builder (same logic as Phase 3)
# ---------------------------------------------------------

def _fetch_cluster_min_meta(cnx: pyodbc.Connection, cluster_id: int) -> Dict[str, Any]:
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


def _build_variant_wiki_path(
    cnx: pyodbc.Connection,
    variant_cluster_id: int,
    wiki_root: str,
) -> str:
    """
    Build /<root>/<product>/<topic>/<scenario>/<variant>
    by walking the parent chain from the variant up.
    """
    chain: List[Dict[str, Any]] = []
    seen: set = set()
    cid = int(variant_cluster_id)

    while cid and cid not in seen and len(chain) < 10:
        seen.add(cid)
        m = _fetch_cluster_min_meta(cnx, cid)
        if not m:
            break
        chain.append(m)
        cid = m.get("parent_cluster_id") or 0

    product = _slug((chain[0].get("product") if chain else "") or "common")

    seg_by_level = {
        1: "unknown-topic",
        2: "unknown-scenario",
        3: "unknown-variant",
    }
    for node in chain:
        lvl = int(node.get("cluster_level") or 0)
        if lvl in seg_by_level:
            seg_by_level[lvl] = _slug(node.get("cluster_key") or f"cluster-{node.get('cluster_id')}")

    raw = f"{wiki_root}/{product}/{seg_by_level[1]}/{seg_by_level[2]}/{seg_by_level[3]}"
    return _ensure_single_slashes(raw)


# ---------------------------------------------------------
# SQL: candidate selection + leaf context
# ---------------------------------------------------------

def _select_variant_candidates(
    cnx: pyodbc.Connection,
    limit: int,
    min_members: int = 3,
    min_usefulness: float = 0.4,
) -> List[Dict[str, Any]]:
    """
    L3 variants that:
      - are active
      - WikiPath IS NULL (not yet wiki-populated)
      - max_solution_usefulness >= min_usefulness
      - member_count >= min_members
    Variants whose L4 children have a CommonIssueSolutions playbook are sorted first.
    Safe fallback if WikiPath column doesn't exist yet.
    """
    cur = cnx.cursor()
    try:
        cur.execute("""
            SELECT TOP (?)
                v.cluster_id,
                v.cluster_level,
                v.product,
                v.cluster_key,
                v.parent_cluster_id,
                v.cluster_signature_text,
                v.member_count
            FROM dbo.issue_cluster v
            WHERE v.cluster_level = 3
              AND v.is_active = 1
              AND v.WikiPath IS NULL
              AND ISNULL(v.max_solution_usefulness, 0.0) >= ?
              AND ISNULL(v.member_count, 0) >= ?
            ORDER BY
                CASE WHEN EXISTS (
                    SELECT 1
                    FROM dbo.issue_cluster l
                    JOIN dbo.CommonIssueSolutions cis ON cis.ClusterID = l.cluster_id
                    WHERE l.parent_cluster_id = v.cluster_id
                      AND l.cluster_level = 4
                      AND l.is_active = 1
                ) THEN 0 ELSE 1 END,
                v.member_count DESC, v.last_seen_at DESC
        """, int(limit), float(min_usefulness), int(min_members))
    except pyodbc.Error as e:
        if "Invalid column name" in str(e):
            logging.warning(
                "WikiPath or max_solution_usefulness column missing. "
                "Falling back to selecting active L3 variants with member_count >= %d.",
                min_members,
            )
            fb_args = [int(limit), int(min_members)]
            if product:
                fb_args.append(product)
            fb_prod_filter = "AND v.product = ?" if product else ""
            cur.execute(f"""
                SELECT TOP (?)
                    v.cluster_id,
                    v.cluster_level,
                    v.product,
                    v.cluster_key,
                    v.parent_cluster_id,
                    v.cluster_signature_text,
                    v.member_count
                FROM dbo.issue_cluster v
                WHERE v.cluster_level = 3
                  AND v.is_active = 1
                  AND ISNULL(v.member_count, 0) >= ?
                  {fb_prod_filter}
                ORDER BY v.member_count DESC, v.last_seen_at DESC
            """, *fb_args)
        else:
            raise

    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _fetch_leaves_for_variant(cnx: pyodbc.Connection, variant_cluster_id: int) -> List[Dict[str, Any]]:
    """
    Fetch all L4 leaves under a variant, joined with their CommonIssueSolutions
    playbook if it exists.  This gives GPT-5.2 full context about the quality
    leaves to build a decision tree around.
    """
    cur = cnx.cursor()
    cur.execute("""
        SELECT
            l.cluster_id,
            l.cluster_key,
            l.cluster_signature_text,
            l.resolution_signature_text,
            l.member_count,
            l.max_solution_usefulness,
            l.WikiPath            AS wiki_path,
            cis.Title           AS playbook_title,
            cis.ProblemStatement AS playbook_problem,
            cis.DiagnosticLogicJson AS playbook_diag_json
        FROM dbo.issue_cluster l
        LEFT JOIN dbo.CommonIssueSolutions cis
            ON cis.ClusterID = l.cluster_id
        WHERE l.parent_cluster_id = ?
          AND l.cluster_level = 4
          AND l.is_active = 1
        ORDER BY l.member_count DESC, l.last_seen_at DESC
    """, int(variant_cluster_id))
    cols = [c[0] for c in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    # Parse playbook diagnostics JSON inline
    for r in rows:
        raw = r.pop("playbook_diag_json", None)
        if isinstance(raw, str) and raw.strip():
            try:
                r["playbook_diag"] = json.loads(raw)
            except Exception:
                r["playbook_diag"] = {}
        else:
            r["playbook_diag"] = {}
    return rows


def _fetch_sample_threads_for_variant(cnx: pyodbc.Connection, variant_cluster_id: int, top_n: int = 8) -> List[Dict[str, Any]]:
    """
    A small set of representative threads assigned to this variant (any leaf or
    no-leaf) so the model has real-world signal phrasing to work with.
    """
    cur = cnx.cursor()
    cur.execute("""
        SELECT TOP (?)
            t.thread_id,
            t.signature_text,
            t.resolution_signature_text,
            t.solution_usefulness
        FROM dbo.thread_enrichment t
        WHERE t.VariantClusterID = ?
        ORDER BY t.solution_usefulness DESC, t.source_created_at DESC
    """, int(top_n), int(variant_cluster_id))
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _mark_variant_wiki_populated(
    cnx: pyodbc.Connection,
    variant_cluster_id: int,
    wiki_path: str,
    markdown: str,
    model_name: str,
) -> None:
    """Persist wiki metadata + content into the SAME issue_cluster row."""
    cur = cnx.cursor()
    content_hash = hashlib.sha256((markdown or "").encode("utf-8")).hexdigest()

    try:
        cur.execute("""
            UPDATE dbo.issue_cluster
            SET WikiPath = ?,
                WikiPushedUtc = SYSUTCDATETIME(),
                WikiContentMarkdown = ?,
                WikiContentHash = ?,
                WikiModel = ?
            WHERE cluster_id = ?
        """, wiki_path, markdown, content_hash, model_name, int(variant_cluster_id))
    except pyodbc.Error as e:
        if "Invalid column name" in str(e):
            # Support partial migrations. Try minimum update.
            try:
                cur.execute("""
                    UPDATE dbo.issue_cluster
                    SET WikiPath = ?,
                        WikiPushedUtc = SYSUTCDATETIME()
                    WHERE cluster_id = ?
                """, wiki_path, int(variant_cluster_id))
            except pyodbc.Error:
                logging.warning(
                    "Wiki columns missing; skipping DB mark for cluster_id=%d",
                    int(variant_cluster_id),
                )
        else:
            raise


# ---------------------------------------------------------
# Wiki link resolver
# ---------------------------------------------------------

def _wiki_page_url(wiki_path: str) -> str:
    """
    Convert a stored WikiPath (e.g. /apim/custom-domain/cert-fails) into
    the full ADO Wiki browser URL that works as a Markdown hyperlink.

    ADO Wiki markdown link format:
        [text](https://dev.azure.com/ORG/PROJECT/_wiki/wikis/WIKI_ID?pagePath=/path)

    The pagePath value must be percent-encoded (spaces → %20, etc.)
    so the link resolves correctly regardless of spaces in the path.
    """
    org  = os.getenv("ADO_ORG_URL", "").rstrip("/")
    proj = os.getenv("ADO_PROJECT", "")
    wid  = os.getenv("ADO_WIKI_ID", "")

    if not (org and proj and wid):
        # Env vars not available (e.g. unit test) — return path as-is
        return wiki_path

    import urllib.parse
    proj_enc = urllib.parse.quote(proj, safe="")
    wid_enc  = urllib.parse.quote(wid,  safe="")
    # Encode each path segment individually to preserve the / separators
    encoded_path = "/".join(
        urllib.parse.quote(seg, safe="")
        for seg in wiki_path.split("/")
    )
    return (
        f"{org}/{proj_enc}/_wiki/wikis/{wid_enc}"
        f"?pagePath={encoded_path}"
    )


def _resolve_wiki_links(md: str, cnx: pyodbc.Connection) -> str:
    """
    Convert all [[cluster-key]] wiki-link tokens in generated Markdown
    to proper ADO Wiki hyperlinks or plain text.

    Rules:
    - [[key]] where cluster has WikiPath  -> [key](full_ado_wiki_url)
    - [[key]] where cluster has no WikiPath -> plain `key` (no broken link)
    - [[display|key]] same logic, key for lookup, display for label
    """
    keys = set(re.findall(r'\[\[(?:[^|\]]+\|)?([^\]]+)\]\]', md))
    if not keys:
        return md

    # Batch-lookup WikiPath for all referenced keys
    path_map: Dict[str, Optional[str]] = {}
    cur = cnx.cursor()
    placeholders = ",".join("?" for _ in keys)
    try:
        cur.execute(
            f"SELECT cluster_key, WikiPath FROM dbo.issue_cluster "
            f"WHERE cluster_key IN ({placeholders}) AND is_active = 1",
            *keys,
        )
        for row in cur.fetchall():
            path_map[str(row[0])] = row[1] or None
    except Exception:
        pass

    def _replace(m: re.Match) -> str:
        inner = m.group(1)
        if "|" in inner:
            display, key = inner.split("|", 1)
        else:
            display = inner
            key = inner
        key = key.strip()
        display = display.strip()
        wiki_path = path_map.get(key)
        if wiki_path:
            url = _wiki_page_url(wiki_path)
            return f"[{display}]({url})"
        # Not on wiki yet — render as plain text so no broken link
        return display

    return re.sub(r'\[\[([^\]]+)\]\]', _replace, md)


# ---------------------------------------------------------
# GPT-5.2 prompt + call
# ---------------------------------------------------------

def _build_leaf_context_block(leaves: List[Dict[str, Any]]) -> str:
    """
    Compact text representation of each leaf for the LLM prompt.
    Marks whether the leaf already has a wiki page so the model
    knows which [[key]] links are resolvable.
    """
    parts: List[str] = []
    for i, lf in enumerate(leaves, 1):
        diag = lf.get("playbook_diag") or {}
        symptoms = diag.get("symptoms") or []
        checks = diag.get("checks") or []
        solution_preview = diag.get("solution") or []
        published = "✓ wiki published" if lf.get("wiki_path") else "✗ not on wiki yet"

        block = (
            f"LEAF {i}: {lf.get('cluster_key', '?')} [{published}]\n"
            f"  Signature: {(lf.get('cluster_signature_text') or '')[:200]}\n"
            f"  Fix pattern: {(lf.get('resolution_signature_text') or '')[:200]}\n"
            f"  Member count: {lf.get('member_count', 0)}\n"
        )
        if lf.get("playbook_title"):
            block += f"  Playbook title: {lf['playbook_title']}\n"
        if symptoms:
            block += f"  Symptoms: {'; '.join(str(s)[:120] for s in symptoms[:5])}\n"
        if checks:
            block += f"  Checks: {'; '.join(str(c)[:120] for c in checks[:4])}\n"
        if solution_preview:
            block += f"  Solution hints: {'; '.join(str(s)[:100] for s in solution_preview[:3])}\n"

        parts.append(block)
    return "\n".join(parts)


def _build_thread_context_block(threads: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for t in threads:
        parts.append(
            f"- thread_id={t.get('thread_id')} | "
            f"symptom_sig={( t.get('signature_text') or '')[:180]} | "
            f"fix_sig={( t.get('resolution_signature_text') or '')[:180]} | "
            f"usefulness={_safe_float(t.get('solution_usefulness')):.2f}"
        )
    return "\n".join(parts)


def generate_variant_wiki_content(
    client: AzureOpenAI,
    deployment: str,
    product: str,
    variant_key: str,
    variant_signature: str,
    leaves: List[Dict[str, Any]],
    sample_threads: List[Dict[str, Any]],
) -> str:
    """
    Calls GPT-5.2 and returns ready-to-push Markdown for the variant wiki page.
    """
    leaf_block = _build_leaf_context_block(leaves)
    thread_block = _build_thread_context_block(sample_threads)

    system_prompt = (
        "ROLE\n"
        "You are a senior Azure support mentor writing an internal wiki snippet for junior "
        "(fresher-level) support engineers contracted to Microsoft.\n\n"

        "LANGUAGE\n"
        "- Write ALL output in Vietnamese.\n"
        "- Use simple, clear Vietnamese suitable for junior engineers who may be "
        "new to Azure support.\n"
        "- Keep English technical terms as-is when natural in Vietnamese tech context "
        "(e.g., 'subscription', 'resource group', 'CNAME', 'TLS', 'ARM', "
        "'control plane', 'data plane', 'node pool', 'SKU', 'cluster_key', etc.).\n"
        "- Do NOT translate Azure service names, CLI commands, error messages, "
        "or tool names (ASC, AppLens, Kusto, Jarvis).\n"
        "- Example: 'Kiểm tra xem CNAME record đã trỏ đúng về default hostname chưa.'\n\n"

        "GOAL\n"
        "Create a concise, scannable VARIANT GUIDE page that helps an engineer quickly:\n"
        "  1. Understand what this variant is about (1-2 sentences).\n"
        "  2. Triage which resolution pattern likely applies using a simple if/then tree.\n"
        "  3. Run a few essential commands (customer-side AND Azure-backend) to confirm.\n"
        "  4. Distinguish between different issue-solution patterns.\n\n"

        "REVIEW ANNOTATIONS\n"
        "This page will be reviewed by Technical Advisors (TA) before publishing.\n"
        "- Use [need confirmation] when a fact, command, UI path, or table name may be inaccurate.\n"
        "- Use [TA should check] when a step requires TA verification of tool/table existence.\n"
        "- Use [general knowledge] when inferring from Azure domain knowledge not in provided data.\n"
        "- Use [docs gap] when no official documentation exists for a stated behavior.\n"
        "These annotations MUST remain in the output — they are NOT errors.\n\n"

        "AZURE SUPPORT TOOLING (use this when writing Azure Backend commands)\n"
        "Engineers have access to the following tools, in recommended investigation order:\n\n"
        "1. ASC (Azure Support Center) — case front door.\n"
        "   Finds: resource topology, subscription/resource config, health state, curated\n"
        "   platform diagnostics, Insights, audit/sign-in logs (Entra), diagnostic settings,\n"
        "   deep links to AppLens/Jarvis/Monitor.\n"
        "   Scope: curated + product-sanitized telemetry, case-scoped, read-only.\n"
        "   When referencing ASC: specify which blade/tab/insight to check.\n\n"
        "2. AppLens — guided investigations.\n"
        "   Finds: pre-modeled dashboards + troubleshooting flows (often Kusto-backed),\n"
        "   TSG decision trees, scenario-based investigations.\n"
        "   Scope: guided outputs (not free-form); depends on what the product team exposed.\n"
        "   When referencing AppLens: name the specific detector or investigation flow.\n\n"
        "3. Kusto (internal clusters) — queryable control-plane telemetry.\n"
        "   Finds: HTTP/request logs, tenant events, provisioning flows, capacity by\n"
        "   SKU/region/zone, container logs, RP ARM traffic.\n"
        "   Scope: access-group dependent; generally RP/ARM/control-plane and\n"
        "   support-approved tables.\n"
        "   When referencing Kusto: describe what table/cluster to query and what to\n"
        "   look for.\n\n"
        "4. Jarvis (Geneva Monitoring) — service monitoring.\n"
        "   Finds: read-only service dashboards/logs/metrics.\n"
        "   Scope: depends on RP + Geneva namespace + data sensitivity.\n"
        "   When referencing Jarvis: specify namespace/dashboard and what metric to check.\n\n"
        "ESCALATION RULE — flag '> ⚠️ **Escalate to FTE**' when the investigation needs ANY of:\n"
        "  • Stamp/fabric node execution details (host/node logs, crash dumps, fabric logs)\n"
        "  • Unscrubbed or customer-data-scoped telemetry (ATC/Lockbox boundaries)\n"
        "  • Near-real-time node-level metrics or raw MDM streams\n"
        "  • Geneva/Jarvis Actions (repair/mitigation/sync/cluster exec)\n"
        "  • Internal RP DB / control-plane state store not exposed via DP tools\n"
        "Otherwise, the engineer should work through: ASC → AppLens → Kusto → Jarvis read-only.\n\n"

        "CONSTRAINTS\n"
        "- Output ONLY Markdown. No JSON wrapper.\n"
        "- Keep it SHORT. No long paragraphs. Use bullets, tables, and code blocks.\n"
        "- For EVERY command: state what it does in plain English (Vietnamese), what output to look for, "
        "and what that output means. Engineers are not deeply technical.\n"
        "- For Azure Backend checks: always give a concrete starting action even if the exact "
        "table/detector name is uncertain. Describe WHAT to look for (operation name, error code, "
        "field name) even when adding [need confirmation] on the exact location. "
        "Never write only '[TA should check]' without a described investigation goal.\n"
        "- For resolution steps that involve waiting (DNS propagation, cert issuance, "
        "replication, portal state changes): include a realistic time estimate, "
        "e.g., '(thường mất 5-30 phút; DNS có thể lên đến 48h tùy TTL)'.\n"
        "- Customer-side commands: Azure CLI, PowerShell, browser DevTools, curl, etc.\n"
        "- Azure-backend commands: use the tooling described above. Reference the specific tool "
        "(ASC / AppLens / Kusto / Jarvis) and what to look for in it.\n"
        "- DO NOT cover deep architecture or full scenario flowcharts (those are separate wiki pages), "
        "instead focus on decision tree type guidance specific to the variant.\n"
        "- DO NOT repeat the full leaf playbooks; leaf nodes are generated for engineers to read in full.\n"
        "- Don't rely solely on leaf nodes to write this — evaluate the variant scope yourself.\n"
        "- No secrets, tenant IDs, subscription IDs in examples — use placeholders like <subscription-id>.\n\n"

        "STRUCTURE (use these exact headings)\n"
        "## TL;DR\n"
        "(1-2 câu: variant này là gì, khi nào engineer gặp nó.)\n\n"
        "## Quick Triage\n"
        "(IF/THEN decision tree. Mỗi nhánh phải kết thúc bằng → route to Leaf [[leaf-key]] "
        "(chỉ dùng [[]] cho leaf có nhãn [✓ wiki published]), "
        "→ go to Step N, hoặc → Escalate to FTE (kèm lý do).)\n\n"
        "## Essential Commands\n"
        "### Customer-Side\n"
        "### Azure Backend\n"
        "(Mỗi command: Does / Look for / Means format.)\n\n"
        "## Pattern Comparison\n"
        "(Table so sánh các leaf patterns. Columns: Pattern | Customer nói gì | Engineer thấy gì | Hướng fix.)\n\n"
        "## Common Pitfalls\n"
        "(Bullet list các lỗi hay mắc khi xử lý variant này.)\n\n"
        "## See Also\n"
        "(List 2-4 variant/scenario keys liên quan mà engineer có thể nhầm lẫn. "
        "Format: - [[key]] – cách phân biệt với page này, viết bằng tiếng Việt.)\n"
    )

    user_prompt = (
        f"PRODUCT: {product}\n"
        f"VARIANT KEY: {variant_key}\n"
        f"VARIANT SIGNATURE: {(variant_signature or '')[:300]}\n\n"
        f"CHILD LEAVES ({len(leaves)} total):\n{leaf_block}\n\n"
        f"SAMPLE THREADS ({len(sample_threads)} shown):\n{thread_block}\n\n"
        "Generate the Markdown variant guide now."
    )

    resp = client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        max_completion_tokens=_MAX_COMPLETION_TOKENS,
        temperature=0.2,
    )

    content = (resp.choices[0].message.content or "").strip()

    # Prepend an H1 title so the wiki page has a proper heading
    title_line = f"# {variant_key}\n\n"
    if not content.startswith("# "):
        content = title_line + content

    return content


# ---------------------------------------------------------
# Per-variant pipeline
# ---------------------------------------------------------

def _process_single_variant(
    client: AzureOpenAI,
    deployment: str,
    variant: Dict[str, Any],
    wiki_id: str,
    wiki_root: str,
) -> Dict[str, Any]:
    """
    End-to-end: fetch leaves + threads ? GPT-5.2 ? wiki push ? DB mark.
    Returns a log dict.
    """
    variant_id = int(variant["cluster_id"])
    variant_key = (variant.get("cluster_key") or f"variant-{variant_id}").strip()
    product = (variant.get("product") or "unknown").strip()
    variant_sig = (variant.get("cluster_signature_text") or "").strip()

    result: Dict[str, Any] = {
        "variant_id": variant_id,
        "variant_key": variant_key,
        "product": product,
        "status": "pending",
    }

    try:
        with _sql_connect() as cnx:
            leaves = _fetch_leaves_for_variant(cnx, variant_id)
            sample_threads = _fetch_sample_threads_for_variant(cnx, variant_id, top_n=8)

        if not leaves:
            result["status"] = "skipped_no_leaves"
            return result

        result["leaf_count"] = len(leaves)

        # --- Generate content ---
        md = generate_variant_wiki_content(
            client, deployment,
            product=product,
            variant_key=variant_key,
            variant_signature=variant_sig,
            leaves=leaves,
            sample_threads=sample_threads,
        )

        if not md or len(md.strip()) < 40:
            result["status"] = "skipped_empty_generation"
            return result

        # --- Resolve [[key]] links to real ADO wiki paths ---
        with _sql_connect() as cnx:
            md = _resolve_wiki_links(md, cnx)

        # --- Push to wiki ---
        with _sql_connect() as cnx:
            wiki_path = _build_variant_wiki_path(cnx, variant_id, wiki_root)

        page = upsert_wiki_page(wiki_id, wiki_path, md)

        if not page:
            result["status"] = "wiki_push_failed"
            return result

        # --- Mark as done ---
        with _sql_connect() as cnx:
            _mark_variant_wiki_populated(
                cnx,
                variant_id,
                wiki_path,
                markdown=md,
                model_name=deployment,
            )
            cnx.commit()

        result["status"] = "ok"
        result["wiki_path"] = wiki_path

        # After this variant is done, ensure its parent scenario (L2) is also
        # wiki-populated. The scenario page benefits from knowing its child
        # variant content is already generated.
        parent_id = variant.get("parent_cluster_id")
        if parent_id:
            try:
                with _sql_connect() as cnx2:
                    cur2 = cnx2.cursor()
                    cur2.execute("""
                        SELECT cluster_id, cluster_level, product, cluster_key,
                               parent_cluster_id, cluster_signature_text, member_count
                        FROM dbo.issue_cluster
                        WHERE cluster_id = ? AND cluster_level = 2
                          AND is_active = 1 AND WikiPath IS NULL
                    """, int(parent_id))
                    cols = [c[0] for c in cur2.description]
                    row = cur2.fetchone()
                    if row:
                        parent_node = dict(zip(cols, row))
                        from phase4c_populate_scenarios import (
                            _process_single_scenario,
                        )
                        sc_res = _process_single_scenario(
                            client, deployment, parent_node,
                            wiki_id, wiki_root, model_tier="gpt52",
                        )
                        logging.info(
                            "[4B] auto-populated parent scenario_id=%d: %s",
                            int(parent_id), sc_res.get("status"),
                        )
                        result["parent_scenario"] = {
                            "scenario_id": int(parent_id),
                            "status": sc_res.get("status"),
                        }
            except Exception as pe:
                logging.warning(
                    "[4B] failed to auto-populate parent scenario_id=%s: %s",
                    parent_id, pe,
                )

        return result

    except Exception as e:
        logging.exception("Phase4B error variant_id=%d", variant_id)
        result["status"] = "error"
        result["error"] = str(e)
        return result


# ---------------------------------------------------------
# Orchestrator (Azure Function entry point)
# ---------------------------------------------------------

def run_phase4b_populate_variants(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered Azure Function.

    Query params:
      - limit   (int, default 50)  – max variants to process
      - workers (int, default 4)   – parallel GPT-5.2 calls
    """
    try:
        limit = max(1, min(int(req.params.get("limit", str(_BATCH_SIZE))), 500))
        max_workers = max(1, min(int(req.params.get("workers", str(_MAX_WORKERS))), 12))
        min_members = max(1, int(req.params.get("min_members", "3")))
        min_usefulness = max(0.0, min(1.0, float(req.params.get("min_usefulness", "0.4"))))
        product = (req.params.get("product") or "").strip()
    except Exception:
        limit = _BATCH_SIZE
        max_workers = _MAX_WORKERS
        min_members = 3
        min_usefulness = 0.4
        product = ""

    wiki_id = os.environ["ADO_WIKI_ID"]
    wiki_root = (os.getenv("ADO_WIKI_ROOT", "") or "").strip().strip("/")
    wiki_root = f"/{wiki_root}" if wiki_root else ""

    logs: List[Dict[str, Any]] = []

    try:
        client = _make_gpt52_client()
        deployment = _get_gpt52_deployment()

        # Select candidates
        with _sql_connect() as cnx:
            candidates = _select_variant_candidates(cnx, limit, min_members=min_members, min_usefulness=min_usefulness, product=product)

        if not candidates:
            return func.HttpResponse(
                json.dumps({"status": "ok", "processed": 0, "msg": "no variants need wiki population"}, ensure_ascii=False),
                mimetype="application/json",
            )

        logging.info("[4B] Starting variant wiki population: %d candidates, %d workers", len(candidates), max_workers)

        # Track parent scenario IDs already auto-populated to avoid redundant work
        # when multiple variants share the same L2 parent.
        _populated_parent_ids: set = set()

        # Process in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _process_single_variant,
                    client, deployment, v, wiki_id, wiki_root,
                ): v
                for v in candidates
            }

            for fut in as_completed(futures):
                try:
                    res = fut.result()
                    # Track parent scenario IDs that were auto-populated
                    ps = res.get("parent_scenario")
                    if ps and ps.get("status") == "ok":
                        _populated_parent_ids.add(ps.get("scenario_id"))
                except Exception as e:
                    v = futures[fut]
                    res = {
                        "variant_id": v.get("cluster_id"),
                        "variant_key": v.get("cluster_key"),
                        "status": "future_error",
                        "error": str(e),
                    }
                logs.append(res)

        # Summary
        summary: Dict[str, int] = {}
        for lg in logs:
            s = lg.get("status", "unknown")
            summary[s] = summary.get(s, 0) + 1

        return func.HttpResponse(
            json.dumps({
                "status": "ok",
                "processed": len(logs),
                "summary": summary,
                "details": logs,
            }, ensure_ascii=False, default=str),
            mimetype="application/json",
        )

    except Exception as e:
        logging.exception("Phase 4B Fatal Error")
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}, ensure_ascii=False),
            status_code=500,
            mimetype="application/json",
        )