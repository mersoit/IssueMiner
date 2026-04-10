# -*- coding: utf-8 -*-
"""
Phase 4C - Populate Scenario Wiki Pages
=======================================
For each L2 (scenario) cluster not yet wiki-populated, this phase:
  1. Gathers child L3 (variant) clusters + their leaf playbook summaries.
  2. Gathers KnowledgeNuggets associated with this scenario.
  3. Gathers representative threads for real-world signal phrasing.
  4. Calls GPT-5.2 or GPT-5 Pro to produce a text-based troubleshooting
     flowchart and scenario guide aimed at junior support engineers.
  5. Pushes Markdown to ADO Wiki (creating ancestor pages as needed).
  6. Records wiki path + timestamp back in the DB.

The scenario page focuses on:
  - Overview & Components involved
  - Text-based Troubleshooting Flow (decision tree)
  - Preliminary Checks (customer-side CLI/PS + Azure backend tooling)
  - Variants Breakdown (brief, routing-oriented)
  - Knowledge Nuggets & Known Gaps (docs_gap, limitations, workarounds, etc.)

Style: technical writing. Concise, no decor, simple phrasing.

Model selection (query param ?model=):
  "pro"   – GPT-5 Pro via Responses API  (default)
  "gpt52" – GPT-5.2 via chat.completions
"""

import os
import json
import logging
import re
import hashlib
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyodbc
import azure.functions as func
from openai import AzureOpenAI

from ado_devops import upsert_wiki_page
from phase3_phase3_functions import _search_kb_articles
from phase4b_populate_variants import _resolve_wiki_links
from aoai_helpers import (
    make_pro_client,
    get_pro_deployment,
    call_pro_with_retry,
    _extract_responses_text,
    get_rate_limiter,
    make_gpt52_client,
    get_gpt52_deployment,
    call_aoai_with_retry,
    estimate_tokens,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

_MAX_WORKERS = int(os.getenv("PHASE4C_MAX_WORKERS", "4"))
_MAX_COMPLETION_TOKENS = int(os.getenv("PHASE4C_MAX_TOKENS", "16000"))
_BATCH_SIZE = int(os.getenv("PHASE4C_BATCH_SIZE", "50"))

# Default model tier: "gpt52" or "mini"
_DEFAULT_MODEL = os.getenv("PHASE4C_MODEL", "gpt52").strip().lower()


# ---------------------------------------------------------
# Infra helpers (mirrors Phase 4B conventions)
# ---------------------------------------------------------

def _sql_connect() -> pyodbc.Connection:
    return pyodbc.connect(os.environ["SQL_CONNECTION_STRING"], timeout=45)


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
# Wiki path builder
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


def _build_scenario_wiki_path(
    cnx: pyodbc.Connection,
    scenario_cluster_id: int,
    wiki_root: str,
) -> str:
    """
    Build /<root>/<product>/<topic>/<scenario>
    by walking the parent chain from the scenario up.
    """
    chain: List[Dict[str, Any]] = []
    seen: set = set()
    cid = int(scenario_cluster_id)

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
    }
    for node in chain:
        lvl = int(node.get("cluster_level") or 0)
        if lvl in seg_by_level:
            seg_by_level[lvl] = _slug(
                node.get("cluster_key") or f"cluster-{node.get('cluster_id')}"
            )

    raw = f"{wiki_root}/{product}/{seg_by_level[1]}/{seg_by_level[2]}"
    return _ensure_single_slashes(raw)


# ---------------------------------------------------------
# SQL: candidate selection + context gathering
# ---------------------------------------------------------

def _select_scenario_candidates(
    cnx: pyodbc.Connection,
    limit: int,
    min_members: int = 6,
    min_usefulness: float = 0.4,
    child_min_members: int = 3,
    product: str = "",
) -> List[Dict[str, Any]]:
    product = (product or "").strip()
    prod_filter = "AND s.product = ?" if product else ""
    cur = cnx.cursor()
    try:
        # arg order must match SQL placeholder order:
        # TOP(?), member_count>=?, [product=?], max_solution_usefulness>=?, member_count>=?
        args: list = [int(limit), int(min_members)]
        if product:
            args.append(product)
        args += [float(min_usefulness), int(child_min_members)]
        cur.execute(f"""
            SELECT TOP (?)
                s.cluster_id,
                s.cluster_level,
                s.product,
                s.cluster_key,
                s.parent_cluster_id,
                s.cluster_signature_text,
                s.member_count
            FROM dbo.issue_cluster s
            WHERE s.cluster_level = 2
              AND s.is_active = 1
              AND s.WikiPath IS NULL
              AND ISNULL(s.member_count, 0) >= ?
              {prod_filter}
              AND EXISTS (
                  SELECT 1
                  FROM dbo.issue_cluster v
                  WHERE v.parent_cluster_id = s.cluster_id
                    AND v.cluster_level = 3
                    AND v.is_active = 1
                    AND ISNULL(v.max_solution_usefulness, 0.0) >= ?
                    AND ISNULL(v.member_count, 0) >= ?
              )
            ORDER BY s.member_count DESC, s.last_seen_at DESC
        """, *args)
    except pyodbc.Error as e:
        if "Invalid column name" in str(e):
            logging.warning(
                "WikiPath or max_solution_usefulness column missing. "
                "Falling back to selecting active L2 scenarios with member_count >= %d.",
                min_members,
            )
            fb_args = [int(limit), int(min_members)]
            if product:
                fb_args.append(product)
            fb_prod_filter = "AND s.product = ?" if product else ""
            cur.execute(f"""
                SELECT TOP (?)
                    s.cluster_id,
                    s.cluster_level,
                    s.product,
                    s.cluster_key,
                    s.parent_cluster_id,
                    s.cluster_signature_text,
                    s.member_count
                FROM dbo.issue_cluster s
                WHERE s.cluster_level = 2
                  AND s.is_active = 1
                  AND ISNULL(s.member_count, 0) >= ?
                  {fb_prod_filter}
                ORDER BY s.member_count DESC, s.last_seen_at DESC
            """, *fb_args)
        else:
            raise

    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _fetch_variants_for_scenario(
    cnx: pyodbc.Connection,
    scenario_cluster_id: int,
) -> List[Dict[str, Any]]:
    """
    Fetch L3 variant children of this scenario, each joined with a summary
    of their best leaf playbook info for GPT context.
    """
    cur = cnx.cursor()
    cur.execute("""
        SELECT
            v.cluster_id,
            v.cluster_key,
            v.cluster_signature_text,
            v.member_count,
            v.WikiPath            AS wiki_path
        FROM dbo.issue_cluster v
        WHERE v.parent_cluster_id = ?
          AND v.cluster_level = 3
          AND v.is_active = 1
        ORDER BY v.member_count DESC, v.last_seen_at DESC
    """, int(scenario_cluster_id))
    cols = [c[0] for c in cur.description]
    variants = [dict(zip(cols, r)) for r in cur.fetchall()]

    for v in variants:
        vid = int(v["cluster_id"])
        cur.execute("""
            SELECT TOP 5
                l.cluster_key,
                l.cluster_signature_text,
                l.member_count,
                l.WikiPath            AS wiki_path,
                cis.Title           AS playbook_title,
                cis.DiagnosticLogicJson AS playbook_diag_json
            FROM dbo.issue_cluster l
            LEFT JOIN dbo.CommonIssueSolutions cis
                ON cis.ClusterID = l.cluster_id
            WHERE l.parent_cluster_id = ?
              AND l.cluster_level = 4
              AND l.is_active = 1
            ORDER BY l.member_count DESC
        """, vid)
        leaf_cols = [c[0] for c in cur.description]
        leaves_raw = [dict(zip(leaf_cols, r)) for r in cur.fetchall()]

        leaf_summaries: List[Dict[str, Any]] = []
        for lf in leaves_raw:
            diag_raw = lf.pop("playbook_diag_json", None)
            diag = {}
            if isinstance(diag_raw, str) and diag_raw.strip():
                try:
                    diag = json.loads(diag_raw)
                except Exception:
                    pass
            leaf_summaries.append({
                "cluster_key": lf.get("cluster_key"),
                "signature": (lf.get("cluster_signature_text") or "")[:200],
                "member_count": lf.get("member_count", 0),
                "wiki_path": lf.get("wiki_path"),
                "playbook_title": lf.get("playbook_title"),
                "symptoms": (diag.get("symptoms") or [])[:4],
                "checks": (diag.get("checks") or [])[:3],
            })
        v["leaf_summaries"] = leaf_summaries

    return variants


def _fetch_nuggets_for_scenario(
    cnx: pyodbc.Connection,
    scenario_cluster_key: str,
    product: str,
) -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    try:
        cur.execute("""
            SELECT
                NuggetType,
                Title,
                ContentText,
                WhyItMatters,
                UsefulnessScore
            FROM dbo.KnowledgeNuggets
            WHERE ScenarioClusterKey = ?
              AND Product = ?
            ORDER BY UsefulnessScore DESC
        """, scenario_cluster_key, product)
    except pyodbc.Error as e:
        if "Invalid column name" in str(e):
            try:
                cur.execute("""
                    SELECT
                        NuggetType,
                        Title,
                        ContentText,
                        WhyItMatters
                    FROM dbo.KnowledgeNuggets
                    WHERE ScenarioClusterKey = ?
                      AND Product = ?
                """, scenario_cluster_key, product)
            except pyodbc.Error:
                return []
        else:
            raise

    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _fetch_sample_threads_for_scenario(
    cnx: pyodbc.Connection,
    scenario_cluster_id: int,
    top_n: int = 10,
) -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    cur.execute("""
        SELECT TOP (?)
            t.thread_id,
            t.signature_text,
            t.resolution_signature_text,
            t.solution_usefulness
        FROM dbo.thread_enrichment t
        WHERE t.ScenarioClusterID = ?
        ORDER BY t.solution_usefulness DESC, t.source_created_at DESC
    """, int(top_n), int(scenario_cluster_id))
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _mark_scenario_wiki_populated(
    cnx: pyodbc.Connection,
    scenario_cluster_id: int,
    wiki_path: str,
    markdown: str,
    model_name: str,
) -> None:
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
        """, wiki_path, markdown, content_hash, model_name, int(scenario_cluster_id))
    except pyodbc.Error as e:
        if "Invalid column name" in str(e):
            try:
                cur.execute("""
                    UPDATE dbo.issue_cluster
                    SET WikiPath = ?,
                        WikiPushedUtc = SYSUTCDATETIME()
                    WHERE cluster_id = ?
                """, wiki_path, int(scenario_cluster_id))
            except pyodbc.Error:
                logging.warning(
                    "Wiki columns missing; skipping DB mark for cluster_id=%d",
                    int(scenario_cluster_id),
                )
        else:
            raise


# ---------------------------------------------------------
# Prompt context builders
# ---------------------------------------------------------

def _build_variant_context_block(variants: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for i, v in enumerate(variants, 1):
        block = (
            f"VARIANT {i}: {v.get('cluster_key', '?')}\n"
            f"  Signature: {(v.get('cluster_signature_text') or '')[:250]}\n"
            f"  Case count: {v.get('member_count', 0)}\n"
        )
        leaves = v.get("leaf_summaries") or []
        if leaves:
            block += f"  Known leaves ({len(leaves)}):\n"
            for j, lf in enumerate(leaves, 1):
                block += f"    L{j}: {lf.get('cluster_key', '?')}"
                if lf.get("playbook_title"):
                    block += f" – {lf['playbook_title']}"
                block += f" (cases={lf.get('member_count', 0)})\n"
                sig = lf.get("signature") or ""
                if sig:
                    block += f"      Sig: {sig[:180]}\n"
                symptoms = lf.get("symptoms") or []
                if symptoms:
                    block += f"      Symptoms: {'; '.join(str(s)[:100] for s in symptoms)}\n"
                checks = lf.get("checks") or []
                if checks:
                    block += f"      Checks: {'; '.join(str(c)[:100] for c in checks)}\n"
        parts.append(block)
    return "\n".join(parts)


def _build_nugget_context_block(nuggets: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for i, n in enumerate(nuggets, 1):
        ntype = n.get("NuggetType", "unknown")
        title = n.get("Title", "?")
        content = (n.get("ContentText") or "")[:300]
        why = (n.get("WhyItMatters") or "")[:200]
        score = _safe_float(n.get("UsefulnessScore"))
        parts.append(
            f"NUGGET {i} [type={ntype}] (score={score:.2f}): {title}\n"
            f"  Content: {content}\n"
            f"  Why it matters: {why}\n"
        )
    return "\n".join(parts)


def _build_thread_context_block(threads: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for t in threads:
        parts.append(
            f"- thread_id={t.get('thread_id')} | "
            f"symptom={( t.get('signature_text') or '')[:180]} | "
            f"fix={( t.get('resolution_signature_text') or '')[:180]} | "
            f"usefulness={_safe_float(t.get('solution_usefulness')):.2f}"
        )
    return "\n".join(parts)


def _build_doc_context_block(docs: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for i, d in enumerate(docs or [], 1):
        if not isinstance(d, dict):
            continue
        parts.append(
            f"DOC {i}: {(d.get('title') or '')[:200]}\n"
            f"URL: {(d.get('url') or '')[:600]}\n"
            f"Content: {(d.get('content') or '')[:900]}\n"
        )
    return "\n".join(parts)


# ---------------------------------------------------------
# Prompt builder (shared between both model tiers)
# ---------------------------------------------------------

def _build_prompts(
    product: str,
    scenario_key: str,
    scenario_signature: str,
    variants: List[Dict[str, Any]],
    nuggets: List[Dict[str, Any]],
    sample_threads: List[Dict[str, Any]],
    docs: List[Dict[str, Any]],
) -> tuple[str, str]:
    """Returns (system_prompt, user_prompt) for either model tier."""

    variant_block = _build_variant_context_block(variants)
    nugget_block = _build_nugget_context_block(nuggets)
    thread_block = _build_thread_context_block(sample_threads)
    doc_block = _build_doc_context_block(docs)

    system_prompt = (
        "ROLE\n"
        "You are a senior Azure support mentor and technical writer.\n\n"

        "LANGUAGE\n"
        "- Write ALL output in Vietnamese.\n"
        "- Use simple, clear Vietnamese suitable for junior engineers who may be "
        "new to Azure support.\n"
        "- Keep English technical terms as-is when natural in Vietnamese tech context "
        "(e.g., 'subscription', 'resource group', 'CNAME', 'TLS', 'ARM', "
        "'control plane', 'data plane', 'node pool', 'SKU', etc.).\n"
        "- Do NOT translate Azure service names, CLI commands, error messages, "
        "or tool names (ASC, AppLens, Kusto, Jarvis).\n"
        "- Example: 'Kiểm tra xem customer đã tạo đúng CNAME record trỏ về default hostname chưa.'\n\n"

        "GOAL\n"
        "Create a SCENARIO GUIDE wiki page for junior support engineers.\n"
        "A 'Scenario' (L2) groups multiple 'Variants' (L3). This page helps engineers:\n"
        "  1. Understand the scenario scope and components involved.\n"
        "  2. Collect the right information at case intake.\n"
        "  3. Follow a text-based troubleshooting flow to isolate the issue.\n"
        "  4. Run preliminary checks (customer-side + Azure backend) to pinpoint root cause.\n"
        "  5. Route to the correct variant for detailed resolution.\n"
        "  6. Be aware of known gaps, limitations, and platform behaviors.\n\n"

        "REVIEW ANNOTATIONS\n"
        "This page will be reviewed by Technical Advisors (TA) before publishing.\n"
        "- Use [need confirmation] when a fact, command, UI path, or table name may be inaccurate.\n"
        "- Use [TA should check] when a step requires TA verification of tool/table existence.\n"
        "- Use [general knowledge] when inferring from Azure domain knowledge not in provided data.\n"
        "- Use [docs gap] when no official documentation exists for a stated behavior.\n"
        "These annotations MUST remain in the output — they are NOT errors.\n\n"

        "AZURE SUPPORT TOOLING (use this when writing Preliminary Checks / Azure Backend)\n"
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
        "ESCALATION RULE — include an '> ⚠️ **Escalate to FTE**' callout when a step\n"
        "requires ANY of:\n"
        "  • Stamp/fabric node execution details (host/node logs, crash dumps, fabric logs)\n"
        "  • Unscrubbed or customer-data-scoped telemetry (ATC/Lockbox boundaries)\n"
        "  • Near-real-time node-level metrics or raw MDM streams\n"
        "  • Geneva/Jarvis Actions (repair/mitigation/sync/cluster exec)\n"
        "  • Internal RP DB / control-plane state store not exposed via DP tools\n"
        "Otherwise, the engineer should work through: ASC → AppLens → Kusto → Jarvis read-only.\n\n"

        "CONSTRAINTS\n"
        "- Output ONLY Markdown. No JSON wrapper.\n"
        "- Technical writing: concise, no decor, simple phrasing.\n"
        "- For the Troubleshooting Flow: DO NOT draw ASCII trees or text art. "
        "Use a NUMBERED STEP LIST. Each step must include: Check, Command (if any), "
        "Look for, Interpretation, and Next (IF/THEN/ELSE) routing to another step or a Variant.\n"
        "- Every step in the Troubleshooting Flow MUST end with a clear NEXT directive: "
        "either → go to Step N, → route to Variant [[variant-key]], or → Escalate to FTE (kèm lý do). "
        "Never end a step with 'collect evidence' as the final instruction without specifying where to send it.\n"
        "- For EVERY command/check: state what it does, what output to look for, "
        "what that output means. Engineers are fresher-level.\n"
        "- For Azure Backend checks: always give a concrete starting action even if the exact "
        "table/detector name is uncertain. Describe WHAT to look for (operation name, error code, "
        "field name) even when adding [need confirmation] on the exact location.\n"
        "- For resolution steps that involve waiting (DNS propagation, cert issuance, "
        "replication, portal state changes): include a realistic time estimate, "
        "e.g., '(thường mất 5-30 phút; DNS có thể lên đến 48h tùy TTL)'.\n"
        "- Customer-side commands: Azure CLI, PowerShell, curl, browser DevTools, etc.\n"
        "- Azure-backend commands: reference the specific tool (ASC / AppLens / Kusto / Jarvis) "
        "and what to look for in it. Describe query purpose and where to run it.\n"
        "- For Knowledge Nuggets: explicitly call out the nugget type "
        "(docs_gap, current_limitations, platform_behavior, workaround, "
        "tooling_hint, docs_conflict, docs_clarification, third_party_interaction, "
        "specific_scenario). Mark docs_gap items clearly so they can be escalated.\n"
        "- No secrets, tenant IDs, subscription IDs — use placeholders like <subscription-id>.\n"
        "- Do NOT duplicate full leaf playbooks; summarize enough to route.\n\n"

        "STRUCTURE (use these exact headings)\n"
        "## Overview & Components\n"
        "(2-4 câu mô tả scenario scope. Sau đó bullet list các component chính liên quan. "
        "Với MỖI component: giải thích ngắn gọn nó LÀ GÌ, nó LÀM GÌ trong flow này, "
        "và TẠI SAO nó quan trọng cho troubleshooting — không chỉ là glossary. "
        "Ví dụ: '**CNAME record** – Là DNS record trỏ subdomain (vd: www) về hostname khác. "
        "Trong flow này, CNAME phải trỏ về SWA default hostname để traffic route đúng. "
        "Nếu CNAME sai hoặc thiếu, validation sẽ fail và custom domain không thể bind.')\n\n"
        "## First Response Checklist\n"
        "(Danh sách 5-10 thông tin cần thu thập từ customer TRƯỚC KHI bắt đầu investigate. "
        "Đây là engineer technical support có quyền truy cập một số Azure backend tools, "
        "nên bao gồm cả thông tin cần thiết cho backend investigation. "
        "Format: - [ ] <thông tin cần thu thập>\n"
        "Ưu tiên các ID/thông tin hữu ích cho Azure backend:\n"
        "  - Subscription ID (luôn cần)\n"
        "  - Resource name / Resource Group\n"
        "  - Thời điểm xảy ra issue (UTC)\n"
        "  - Error message chính xác (screenshot nếu có)\n"
        "  - Correlation ID / Activity ID (nếu có trong error)\n"
        "  - Region / Location của resource\n"
        "  - Các ID khác tùy service (vd: Cluster name, Domain name, Endpoint URL...)\n"
        "Chỉ list những thông tin THỰC SỰ cần cho scenario này — không generic.)\n\n"
        "## Troubleshooting Flow\n"
        "(NUMBERED step list. Mỗi step: Check → Command → Look for → Interpretation → Next.)\n\n"
        "## Preliminary Checks\n"
        "### Customer-Side\n"
        "### Azure Backend\n"
        "(Mỗi check: tool name, blade/tab, what to look for, what it means.)\n\n"
        "## Variants Breakdown\n"
        "(Với mỗi variant: tên, 1-2 câu mô tả, khi nào route đến, case volume hint. "
        "Format variant key as [[variant-key]] chỉ khi variant có nhãn [✓ wiki published] trong context. "
        "Nếu variant chưa được publish thì viết tên plain không có [[ ]].)\n\n"
        "## Knowledge Nuggets & Known Gaps\n"
        "(Group by type. Prefix: 🔍 DOCS GAP, ⚠️ LIMITATION, 💡 WORKAROUND, 🔧 TOOLING HINT. "
        "Nếu không có nuggets: 'Chưa có nuggets cho scenario này.')\n\n"
        "## Official Documentation\n"
        "(List 3-8 relevant official doc links. Nếu không có: 'Chưa có docs cho scenario này.')\n\n"
        "## See Also\n"
        "(List 2-4 scenario/variant keys liên quan mà engineer có thể nhầm lẫn. "
        "Format: - [[key]] – cách phân biệt với page này.)\n"
    )

    user_prompt = (
        f"PRODUCT: {product}\n"
        f"SCENARIO KEY: {scenario_key}\n"
        f"SCENARIO SIGNATURE: {(scenario_signature or '')[:400]}\n\n"
        f"CHILD VARIANTS ({len(variants)} total):\n{variant_block}\n\n"
        f"KNOWLEDGE NUGGETS ({len(nuggets)} total):\n"
        f"{nugget_block if nuggets else '(none mined yet)'}\n\n"
        f"SAMPLE THREADS ({len(sample_threads)} shown):\n"
        f"{thread_block if sample_threads else '(none available)'}\n\n"
        f"OFFICIAL DOCS (Azure AI Search) ({len(docs)} shown):\n"
        f"{doc_block if docs else '(none retrieved)'}\n\n"
        "Generate the Markdown scenario guide now."
    )

    return system_prompt, user_prompt


# ---------------------------------------------------------
# Generation: model-tier-aware dispatch
# ---------------------------------------------------------

def generate_scenario_wiki_content(
    client: AzureOpenAI,
    deployment: str,
    product: str,
    scenario_key: str,
    scenario_signature: str,
    variants: List[Dict[str, Any]],
    nuggets: List[Dict[str, Any]],
    sample_threads: List[Dict[str, Any]],
    docs: List[Dict[str, Any]],
    model_tier: str = "pro",
) -> str:
    """
    Generate Markdown for the scenario wiki page.

    model_tier:
      "pro"   – GPT-5 Pro via Responses API  (higher output quality, longer outputs)
      "gpt52" – GPT-5.2 via chat.completions (faster, cheaper, still excellent)
    """
    system_prompt, user_prompt = _build_prompts(
        product, scenario_key, scenario_signature,
        variants, nuggets, sample_threads, docs,
    )

    if model_tier == "gpt52":
        resp = call_aoai_with_retry(
            client,
            model=deployment,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_completion_tokens=_MAX_COMPLETION_TOKENS,
            estimated_prompt_tokens=estimate_tokens(system_prompt) + estimate_tokens(user_prompt),
            rate_limiter=get_rate_limiter("gpt52"),
            caller_tag="phase4c_scenario_gpt52",
        )
        content = (resp.choices[0].message.content or "").strip()
    else:
        # Default: GPT-5 Pro via Responses API
        resp = call_pro_with_retry(
            client,
            model=deployment,
            instructions=system_prompt,
            input_text=user_prompt,
            max_output_tokens=_MAX_COMPLETION_TOKENS,
            rate_limiter=get_rate_limiter("pro"),
            caller_tag="phase4c_scenario_pro",
        )
        content = (_extract_responses_text(resp) or "").strip()

    title_line = f"# {scenario_key}\n\n"
    if not content.startswith("# "):
        content = title_line + content

    return content


# ---------------------------------------------------------
# Per-scenario pipeline
# ---------------------------------------------------------

def _process_single_scenario(
    client: AzureOpenAI,
    deployment: str,
    scenario: Dict[str, Any],
    wiki_id: str,
    wiki_root: str,
    model_tier: str = "pro",
) -> Dict[str, Any]:
    """
    End-to-end: fetch context → GPT → wiki push → DB mark.
    Returns a log dict.
    """
    scenario_id = int(scenario["cluster_id"])
    scenario_key = (scenario.get("cluster_key") or f"scenario-{scenario_id}").strip()
    product = (scenario.get("product") or "unknown").strip()
    scenario_sig = (scenario.get("cluster_signature_text") or "").strip()

    result: Dict[str, Any] = {
        "scenario_id": scenario_id,
        "scenario_key": scenario_key,
        "product": product,
        "model_tier": model_tier,
        "status": "pending",
    }

    try:
        logging.info("[4C] scenario_id=%d (%s) – gathering context", scenario_id, scenario_key)
        with _sql_connect() as cnx:
            variants = _fetch_variants_for_scenario(cnx, scenario_id)
            nuggets = _fetch_nuggets_for_scenario(cnx, scenario_key, product)
            sample_threads = _fetch_sample_threads_for_scenario(cnx, scenario_id, top_n=10)

        docs_query = f"{product} {scenario_key} {(scenario_sig or '')[:120]}".strip()
        docs = _search_kb_articles(docs_query, top_k=6)

        result["variant_count"] = len(variants)
        result["nugget_count"] = len(nuggets)
        result["thread_count"] = len(sample_threads)
        result["doc_count"] = len(docs)

        if not variants and not sample_threads:
            result["status"] = "skipped_no_data"
            logging.info("[4C] scenario_id=%d – skipped (no data)", scenario_id)
            return result

        logging.info(
            "[4C] scenario_id=%d (%s) – calling %s (variants=%d, threads=%d, docs=%d)",
            scenario_id, scenario_key, model_tier.upper(),
            len(variants), len(sample_threads), len(docs),
        )
        md = generate_scenario_wiki_content(
            client,
            deployment,
            product=product,
            scenario_key=scenario_key,
            scenario_signature=scenario_sig,
            variants=variants,
            nuggets=nuggets,
            sample_threads=sample_threads,
            docs=docs,
            model_tier=model_tier,
        )

        if not md or len(md.strip()) < 40:
            result["status"] = "skipped_empty_generation"
            logging.warning("[4C] scenario_id=%d – empty generation, skipping", scenario_id)
            return result

        logging.info("[4C] scenario_id=%d – %s returned %d chars, pushing to wiki",
                     scenario_id, model_tier.upper(), len(md))

        with _sql_connect() as cnx:
            wiki_path = _build_scenario_wiki_path(cnx, scenario_id, wiki_root)
            # Resolve [[key]] links to real ADO wiki paths (or plain text if unpublished)
            md = _resolve_wiki_links(md, cnx)

        page = upsert_wiki_page(wiki_id, wiki_path, md)

        if not page:
            with _sql_connect() as cnx:
                _mark_scenario_wiki_populated(
                    cnx, scenario_id, "push_failed",
                    markdown="", model_name=model_tier,
                )
                cnx.commit()
            result["status"] = "wiki_push_failed"
            result["wiki_path"] = wiki_path
            logging.error("[4C] scenario_id=%d – wiki push failed for path %s", scenario_id, wiki_path)
            return result

        with _sql_connect() as cnx:
            _mark_scenario_wiki_populated(
                cnx, scenario_id, wiki_path,
                markdown=md, model_name=deployment,
            )
            cnx.commit()

        result["status"] = "ok"
        result["wiki_path"] = wiki_path
        logging.info("[4C] scenario_id=%d (%s) – OK → %s", scenario_id, scenario_key, wiki_path)

        # After this scenario is done, ensure its parent topic (L1) is also
        # wiki-populated. The topic page benefits from knowing its child
        # scenario content is already generated.
        parent_id = scenario.get("parent_cluster_id")
        if parent_id:
            try:
                with _sql_connect() as cnx2:
                    cur2 = cnx2.cursor()
                    cur2.execute("""
                        SELECT cluster_id, cluster_level, product, cluster_key,
                               cluster_signature_text, member_count
                        FROM dbo.issue_cluster
                        WHERE cluster_id = ? AND cluster_level = 1
                          AND is_active = 1 AND WikiPath IS NULL
                    """, int(parent_id))
                    cols = [c[0] for c in cur2.description]
                    row = cur2.fetchone()
                    if row:
                        parent_node = dict(zip(cols, row))
                        from phase4d_populate_topics import (
                            _make_gpt52_client as _4d_client,
                            _get_gpt52_deployment as _4d_deployment,
                            _process_single_topic,
                        )
                        topic_client = _4d_client()
                        topic_dep = _4d_deployment()
                        tp_res = _process_single_topic(
                            topic_client, topic_dep, parent_node,
                            wiki_id, wiki_root,
                        )
                        logging.info(
                            "[4C] auto-populated parent topic_id=%d: %s",
                            int(parent_id), tp_res.get("status"),
                        )
                        result["parent_topic"] = {
                            "topic_id": int(parent_id),
                            "status": tp_res.get("status"),
                        }
            except Exception as pe:
                logging.warning(
                    "[4C] failed to auto-populate parent topic_id=%s: %s",
                    parent_id, pe,
                )

        return result

    except Exception as e:
        logging.exception("Phase4C error scenario_id=%d", scenario_id)
        result["status"] = "error"
        result["error"] = str(e)
        return result


# ---------------------------------------------------------
# Orchestrator (Azure Function entry point)
# ---------------------------------------------------------

def run_phase4c_populate_scenarios(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered Azure Function.

    Query params:
      - limit           (int, default 50)   – max scenarios to process
      - workers         (int, default 20)   – max parallel calls
      - initial_workers (int, default 8)    – parallel calls in the first ramp window
      - ramp_step       (int, default 4)    – workers added per ramp interval
      - ramp_interval   (int, default 60)   – seconds between ramp-ups
      - model           (str, default "pro") – "pro" or "gpt52"
    """
    from aoai_helpers import ProWorkerPool

    try:
        limit = max(1, min(int(req.params.get("limit", str(_BATCH_SIZE))), 500))
        max_workers = max(1, min(int(req.params.get("workers", "20")), 24))
        initial_workers = max(1, min(int(req.params.get("initial_workers", "8")), max_workers))
        ramp_step = max(1, min(int(req.params.get("ramp_step", "4")), 8))
        ramp_interval = max(10, min(int(req.params.get("ramp_interval", "60")), 300))
        model_tier = req.params.get("model", _DEFAULT_MODEL).strip().lower()
        if model_tier not in ("pro", "gpt52"):
            model_tier = "pro"
        min_members = max(1, int(req.params.get("min_members", "6")))
        min_usefulness = max(0.0, min(1.0, float(req.params.get("min_usefulness", "0.4"))))
        child_min_members = max(1, int(req.params.get("child_min_members", "3")))
        product = (req.params.get("product") or "").strip()
    except Exception:
        limit = _BATCH_SIZE
        max_workers = 20
        initial_workers = 8
        ramp_step = 4
        ramp_interval = 60
        model_tier = _DEFAULT_MODEL
        min_members = 6
        min_usefulness = 0.4
        child_min_members = 3
        product = ""

    wiki_id = os.environ["ADO_WIKI_ID"]
    wiki_root = (os.getenv("ADO_WIKI_ROOT", "") or "").strip().strip("/")
    wiki_root = f"/{wiki_root}" if wiki_root else ""

    try:
        # Instantiate the appropriate client + deployment based on model_tier
        if model_tier == "mini":
            from aoai_helpers import make_mini_client, get_mini_deployment
            client = make_mini_client()
            deployment = get_mini_deployment()
        else:
            # default: gpt52
            client = make_gpt52_client()
            deployment = get_gpt52_deployment()
            model_tier = "gpt52"

        logging.info("[4C] model_tier=%s deployment=%s", model_tier, deployment)

        with _sql_connect() as cnx:
            candidates = _select_scenario_candidates(
                cnx, limit,
                min_members=min_members,
                min_usefulness=min_usefulness,
                child_min_members=child_min_members,
                product=product,
            )

        if not candidates:
            return func.HttpResponse(
                json.dumps(
                    {
                        "status": "ok",
                        "processed": 0,
                        "model_tier": model_tier,
                        "msg": "no scenarios need wiki population",
                    },
                    ensure_ascii=False,
                ),
                mimetype="application/json",
            )

        logging.info(
            "[4C] Starting scenario wiki population: %d candidates, model=%s, "
            "initial=%d, max=%d workers, ramp_step=%d, ramp_interval=%d s",
            len(candidates), model_tier, initial_workers, max_workers, ramp_step, ramp_interval,
        )

        def _process(scenario: Dict[str, Any]) -> Dict[str, Any]:
            return _process_single_scenario(
                client, deployment, scenario, wiki_id, wiki_root,
                model_tier=model_tier,
            )

        from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed
        pool_workers = min(len(candidates), max_workers)
        logging.info("[4C] Starting pool: %d items, workers=%d", len(candidates), pool_workers)
        with ThreadPoolExecutor(max_workers=pool_workers) as executor:
            futures_map = {executor.submit(_process, c): c for c in candidates}
            logs = []
            for fut in _as_completed(futures_map):
                try:
                    logs.append(fut.result())
                except Exception as e:
                    c = futures_map[fut]
                    logs.append({"scenario_id": c.get("cluster_id"), "status": "worker_error", "error": str(e)})
        logging.info("[4C] Pool complete: %d results", len(logs))

        summary: Dict[str, int] = {}
        for lg in logs:
            s = (lg.get("status") if isinstance(lg, dict) else "unknown") or "unknown"
            summary[s] = summary.get(s, 0) + 1

        ok_count = summary.get("ok", 0)

        return func.HttpResponse(
            json.dumps(
                {
                    "status": "ok",
                    "processed": ok_count,
                    "attempted": len(logs),
                    "model_tier": model_tier,
                    "summary": summary,
                    "details": logs,
                },
                ensure_ascii=False,
                default=str,
            ),
            mimetype="application/json",
        )

    except Exception as e:
        logging.exception("Phase 4C Fatal Error")
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}, ensure_ascii=False),
            status_code=500,
            mimetype="application/json",
        )