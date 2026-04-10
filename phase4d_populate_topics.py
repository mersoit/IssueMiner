# -*- coding: utf-8 -*-
"""
Phase 4D - Populate Topic Wiki Pages
====================================
For each L1 (topic) cluster not yet wiki-populated, this phase:
  1. Gathers child L2 (scenario) clusters + their variant/leaf summaries.
  2. Gathers KnowledgeNuggets attached to this topic.
  3. Retrieves related official documentation via Azure AI Search.
  4. Calls GPT-5.2 to produce an educational overview page that explains
     how the service area works, key terms, component flow, and where
     issues typically occur.
  5. Pushes Markdown to ADO Wiki (creating ancestor pages as needed).
  6. Records wiki path + timestamp back in the DB.

The topic page is education + orientation for support engineers who may
be new to this area. It focuses on:
  - What the topic covers and why customers hit issues here
  - Key terms / definitions (plain English)
  - End-to-end flow: control plane + data plane paths
  - Common failure points mapped to scenarios
  - Knowledge Nuggets: docs_gap, limitations, platform behavior, etc.
  - Official Documentation: links to relevant official docs

Style: technical writing. Concise, no decor, simple English.
NOT a troubleshooting playbook (that is the Scenario page).
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

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
_MAX_WORKERS = int(os.getenv("PHASE4D_MAX_WORKERS", "4"))
_MAX_COMPLETION_TOKENS = int(os.getenv("PHASE4D_MAX_TOKENS", "4500"))
_BATCH_SIZE = int(os.getenv("PHASE4D_BATCH_SIZE", "30"))


# ---------------------------------------------------------
# Infra helpers (same pattern as Phase 4B/4C)
# ---------------------------------------------------------

def _sql_connect() -> pyodbc.Connection:
    return pyodbc.connect(os.environ["SQL_CONNECTION_STRING"], timeout=45)


def _make_gpt52_client() -> AzureOpenAI:
    endpoint = os.environ.get("AOAI_ENDPOINT_GPT52") or os.environ.get("AOAI_ENDPOINT")
    key = os.environ.get("AOAI_API_KEY_GPT52") or os.environ.get("AOAI_API_KEY")
    if not endpoint or not key:
        raise RuntimeError(
            "Missing AOAI endpoint/key for GPT-5.2 "
            "(set AOAI_ENDPOINT_GPT52 / AOAI_API_KEY_GPT52)"
        )
    api_version = os.getenv("AOAI_API_VERSION", "2025-01-01-preview")
    return AzureOpenAI(
        azure_endpoint=endpoint.rstrip("/"), api_key=key, api_version=api_version
    )


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
# Wiki path builder
# ---------------------------------------------------------

def _build_topic_wiki_path(
    product: str,
    topic_key: str,
    wiki_root: str,
) -> str:
    """
    Build /<root>/<product>/<topic>
    Topic is L1 – top of the hierarchy, no parent walk needed.
    """
    raw = f"{wiki_root}/{_slug(product)}/{_slug(topic_key)}"
    return _ensure_single_slashes(raw)


# ---------------------------------------------------------
# SQL: candidate selection + context gathering
# ---------------------------------------------------------

def _select_topic_candidates(
    cnx: pyodbc.Connection,
    limit: int,
    min_members: int = 1,
    require_child_wiki: bool = True,
    product: str = "",
) -> List[Dict[str, Any]]:
    product = (product or "").strip()
    prod_filter = "AND t.product = ?" if product else ""
    cur = cnx.cursor()
    try:
        if require_child_wiki:
            args = [int(limit), int(min_members)]
            if product:
                args.append(product)
            cur.execute(
                f"""
                SELECT TOP (?)
                    t.cluster_id,
                    t.cluster_level,
                    t.product,
                    t.cluster_key,
                    t.cluster_signature_text,
                    t.member_count
                FROM dbo.issue_cluster t
                WHERE t.cluster_level = 1
                  AND t.is_active = 1
                  AND t.WikiPath IS NULL
                  AND ISNULL(t.member_count, 0) >= ?
                  {prod_filter}
                  AND EXISTS (
                      SELECT 1
                      FROM dbo.issue_cluster s
                      WHERE s.parent_cluster_id = t.cluster_id
                        AND s.cluster_level = 2
                        AND s.is_active = 1
                        AND s.WikiPath IS NOT NULL
                  )
                ORDER BY t.member_count DESC, t.last_seen_at DESC
                """,
                *args,
            )
        else:
            args = [int(limit), int(min_members)]
            if product:
                args.append(product)
            cur.execute(
                f"""
                SELECT TOP (?)
                    t.cluster_id,
                    t.cluster_level,
                    t.product,
                    t.cluster_key,
                    t.cluster_signature_text,
                    t.member_count
                FROM dbo.issue_cluster t
                WHERE t.cluster_level = 1
                  AND t.is_active = 1
                  AND t.WikiPath IS NULL
                  AND ISNULL(t.member_count, 0) >= ?
                  {prod_filter}
                  AND EXISTS (
                      SELECT 1
                      FROM dbo.issue_cluster s
                      WHERE s.parent_cluster_id = t.cluster_id
                        AND s.cluster_level = 2
                        AND s.is_active = 1
                  )
                ORDER BY t.member_count DESC, t.last_seen_at DESC
                """,
                *args,
            )
    except pyodbc.Error as e:
        if "Invalid column name" in str(e):
            logging.warning(
                "WikiPath column missing on issue_cluster. "
                "Falling back to selecting all active L1 topics."
            )
            fb_args = [int(limit), int(min_members)]
            if product:
                fb_args.append(product)
            cur.execute(
                f"""
                SELECT TOP (?)
                    t.cluster_id,
                    t.cluster_level,
                    t.product,
                    t.cluster_key,
                    t.cluster_signature_text,
                    t.member_count
                FROM dbo.issue_cluster t
                WHERE t.cluster_level = 1
                  AND t.is_active = 1
                  AND ISNULL(t.member_count, 0) >= ?
                  {prod_filter}
                  AND EXISTS (
                      SELECT 1
                      FROM dbo.issue_cluster s
                      WHERE s.parent_cluster_id = t.cluster_id
                        AND s.cluster_level = 2
                        AND s.is_active = 1
                  )
                ORDER BY t.member_count DESC, t.last_seen_at DESC
                """,
                *fb_args,
            )
        else:
            raise

    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _fetch_scenarios_for_topic(
    cnx: pyodbc.Connection,
    topic_cluster_id: int,
) -> List[Dict[str, Any]]:
    """
    Fetch L2 scenario children of this topic.
    For each scenario, also fetch its top L3 variant summaries so GPT-5.2
    understands the breadth of issues under this topic.
    """
    cur = cnx.cursor()
    cur.execute(
        """
        SELECT
            s.cluster_id,
            s.cluster_key,
            s.cluster_signature_text,
            s.member_count,
            s.WikiPath            AS wiki_path
        FROM dbo.issue_cluster s
        WHERE s.parent_cluster_id = ?
          AND s.cluster_level = 2
          AND s.is_active = 1
        ORDER BY s.member_count DESC, s.last_seen_at DESC
        """,
        int(topic_cluster_id),
    )
    cols = [c[0] for c in cur.description]
    scenarios = [dict(zip(cols, r)) for r in cur.fetchall()]

    # For each scenario, fetch its top variants (compact)
    for sc in scenarios:
        sid = int(sc["cluster_id"])
        cur.execute(
            """
            SELECT TOP 6
                v.cluster_key,
                v.cluster_signature_text,
                v.member_count,
                v.WikiPath        AS wiki_path
            FROM dbo.issue_cluster v
            WHERE v.parent_cluster_id = ?
              AND v.cluster_level = 3
              AND v.is_active = 1
            ORDER BY v.member_count DESC
            """,
            sid,
        )
        vcols = [c[0] for c in cur.description]
        sc["variants"] = [dict(zip(vcols, r)) for r in cur.fetchall()]

    return scenarios


def _fetch_nuggets_for_topic(
    cnx: pyodbc.Connection,
    topic_cluster_key: str,
    product: str,
) -> List[Dict[str, Any]]:
    """
    Fetch KnowledgeNuggets where TopicClusterKey matches.
    This includes both topic-level nuggets (TargetLevel='topic') AND
    scenario-level nuggets whose TopicClusterKey also matches (gives
    GPT broader awareness of gaps across the whole topic).
    Handles UsefulnessScore column being optional.
    """
    cur = cnx.cursor()
    try:
        cur.execute(
            """
            SELECT
                NuggetType,
                Title,
                ContentText,
                WhyItMatters,
                UsefulnessScore,
                TargetLevel,
                ScenarioClusterKey
            FROM dbo.KnowledgeNuggets
            WHERE TopicClusterKey = ?
              AND Product = ?
            ORDER BY UsefulnessScore DESC
            """,
            topic_cluster_key,
            product,
        )
    except pyodbc.Error as e:
        if "Invalid column name" in str(e):
            try:
                cur.execute(
                    """
                    SELECT
                        NuggetType,
                        Title,
                        ContentText,
                        WhyItMatters
                    FROM dbo.KnowledgeNuggets
                    WHERE TopicClusterKey = ?
                      AND Product = ?
                    """,
                    topic_cluster_key,
                    product,
                )
            except pyodbc.Error:
                return []
        else:
            raise

    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _fetch_sample_threads_for_topic(
    cnx: pyodbc.Connection,
    topic_cluster_id: int,
    top_n: int = 8,
) -> List[Dict[str, Any]]:
    """
    Small set of high-usefulness threads under any scenario in this topic.
    Gives GPT real-world phrasing context for the topic area.
    """
    cur = cnx.cursor()
    cur.execute(
        """
        SELECT TOP (?)
            t.thread_id,
            t.signature_text,
            t.resolution_signature_text,
            t.solution_usefulness,
            t.scenario_cluster_key
        FROM dbo.thread_enrichment t
        INNER JOIN dbo.issue_cluster sc
            ON sc.cluster_id = t.ScenarioClusterID
           AND sc.parent_cluster_id = ?
           AND sc.cluster_level = 2
           AND sc.is_active = 1
        ORDER BY t.solution_usefulness DESC, t.source_created_at DESC
        """,
        int(top_n),
        int(topic_cluster_id),
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _mark_topic_wiki_populated(
    cnx: pyodbc.Connection,
    topic_cluster_id: int,
    wiki_path: str,
    markdown: str,
    model_name: str,
) -> None:
    """Persist wiki metadata + content into the SAME issue_cluster row."""
    cur = cnx.cursor()
    content_hash = hashlib.sha256((markdown or "").encode("utf-8")).hexdigest()

    try:
        cur.execute(
            """
            UPDATE dbo.issue_cluster
            SET WikiPath = ?,
                WikiPushedUtc = SYSUTCDATETIME(),
                WikiContentMarkdown = ?,
                WikiContentHash = ?,
                WikiModel = ?
            WHERE cluster_id = ?
            """,
            wiki_path,
            markdown,
            content_hash,
            model_name,
            int(topic_cluster_id),
        )
    except pyodbc.Error as e:
        if "Invalid column name" in str(e):
            try:
                cur.execute(
                    """
                    UPDATE dbo.issue_cluster
                    SET WikiPath = ?,
                        WikiPushedUtc = SYSUTCDATETIME()
                    WHERE cluster_id = ?
                    """,
                    wiki_path,
                    int(topic_cluster_id),
                )
            except pyodbc.Error:
                logging.warning(
                    "Wiki columns missing; skipping DB mark for cluster_id=%d",
                    int(topic_cluster_id),
                )
        else:
            raise


# ---------------------------------------------------------
# GPT-5.2 prompt context builders
# ---------------------------------------------------------

def _build_scenario_context_block(scenarios: List[Dict[str, Any]]) -> str:
    """Compact text for each scenario + its variant summaries."""
    parts: List[str] = []
    for i, sc in enumerate(scenarios, 1):
        published = "✓ wiki published" if sc.get("wiki_path") else "✗ not on wiki yet"
        block = (
            f"SCENARIO {i}: {sc.get('cluster_key', '?')} [{published}]\n"
            f"  Signature: {(sc.get('cluster_signature_text') or '')[:300]}\n"
            f"  Case count: {sc.get('member_count', 0)}\n"
        )
        variants = sc.get("variants") or []
        if variants:
            block += f"  Variants ({len(variants)}):\n"
            for j, v in enumerate(variants, 1):
                vkey = v.get("cluster_key", "?")
                vsig = (v.get("cluster_signature_text") or "")[:150]
                vcount = v.get("member_count", 0)
                vpub = "✓" if v.get("wiki_path") else "✗"
                block += f"    V{j}: {vkey} [{vpub}] (cases={vcount})\n"
                if vsig:
                    block += f"        {vsig}\n"
        parts.append(block)
    return "\n".join(parts)


def _build_nugget_context_block(nuggets: List[Dict[str, Any]]) -> str:
    """Compact text for each nugget, preserving type for GPT to organize."""
    parts: List[str] = []
    for i, n in enumerate(nuggets, 1):
        ntype = n.get("NuggetType", "unknown")
        title = n.get("Title", "?")
        content = (n.get("ContentText") or "")[:300]
        why = (n.get("WhyItMatters") or "")[:200]
        score = _safe_float(n.get("UsefulnessScore"))
        level = n.get("TargetLevel", "?")
        scenario = n.get("ScenarioClusterKey") or ""

        line = f"NUGGET {i} [type={ntype}, level={level}]"
        if scenario:
            line += f" (scenario={scenario})"
        line += f" (score={score:.2f}): {title}\n"
        line += f"  Content: {content}\n"
        if why:
            line += f"  Why it matters: {why}\n"
        parts.append(line)
    return "\n".join(parts)


def _build_thread_context_block(threads: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for t in threads:
        scenario = t.get("scenario_cluster_key") or ""
        parts.append(
            f"- thread_id={t.get('thread_id')} | "
            f"scenario={scenario} | "
            f"symptom={( t.get('signature_text') or '')[:160]} | "
            f"fix={( t.get('resolution_signature_text') or '')[:160]} | "
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
# GPT-5.2 prompt + call
# ---------------------------------------------------------

def generate_topic_wiki_content(
    client: AzureOpenAI,
    deployment: str,
    product: str,
    topic_key: str,
    topic_signature: str,
    scenarios: List[Dict[str, Any]],
    nuggets: List[Dict[str, Any]],
    sample_threads: List[Dict[str, Any]],
    docs: List[Dict[str, Any]],
) -> str:
    """
    Calls GPT-5.2 to produce Markdown for the topic wiki page.
    """
    scenario_block = _build_scenario_context_block(scenarios)
    nugget_block = _build_nugget_context_block(nuggets)
    thread_block = _build_thread_context_block(sample_threads)
    doc_block = _build_doc_context_block(docs)

    system_prompt = (
        "ROLE\n"
        "You are a senior Azure support mentor and technical writer.\n\n"

        "LANGUAGE\n"
        "- Write ALL output in Vietnamese.\n"
        "- Use simple, clear Vietnamese suitable for junior engineers who may be "
        "completely new to this Azure service area.\n"
        "- Keep English technical terms as-is when natural in Vietnamese tech context "
        "(e.g., 'subscription', 'resource group', 'CNAME', 'TLS', 'ARM', "
        "'control plane', 'data plane', 'node pool', 'SKU', etc.).\n"
        "- Do NOT translate Azure service names, CLI commands, error messages, "
        "or tool names (ASC, AppLens, Kusto, Jarvis).\n"
        "- Example: 'Custom domain validation yêu cầu DNS record phải publicly resolvable.'\n\n"

        "GOAL\n"
        "Create a TOPIC OVERVIEW wiki page for junior support engineers who may\n"
        "be new to this area. A 'Topic' (L1) groups multiple 'Scenarios' (L2).\n"
        "This page educates the reader on HOW the service area works, not on\n"
        "how to troubleshoot specific issues (that is the Scenario pages).\n\n"
        "The reader may have little prior knowledge. Use simple language.\n"
        "Be brief and direct. No marketing language. No filler.\n\n"

        "REVIEW ANNOTATIONS\n"
        "This page will be reviewed by Technical Advisors (TA) before publishing.\n"
        "- Use [need confirmation] when a fact, command, UI path, or table name may be inaccurate.\n"
        "- Use [TA should check] when a step requires TA verification.\n"
        "- Use [general knowledge] when inferring from Azure domain knowledge not in provided data.\n"
        "- Use [docs gap] when no official documentation exists for a stated behavior.\n"
        "These annotations MUST remain in the output — they are NOT errors.\n\n"

        "CONSTRAINTS\n"
        "- Output ONLY Markdown. No JSON wrapper.\n"
        "- Technical writing: concise, scannable, plain language.\n"
        "- For the end-to-end flow section: Draw ASCII art diagrams to show flow.\n"
        "  Followed up by a NUMBERED STEP LIST (more visually consistent in wiki).\n"
        "  Required format example:\n"
        "    1. **Client request** – mô tả ngắn.\n"
        "       - Giải thích đơn giản chuyện gì xảy ra ở bước này.\n"
        "       - Failure point: <cái gì hỏng> (Scenario: [[scenario-key]]).\n"
        "       - Signal: <dấu hiệu nhận biết>.\n"
        "    2. **Routing / gateway** – mô tả ngắn.\n"
        "       - Failure point: ...\n"
        "  Keep each step short. Add 1-2 failure points max per step.\n"
        "- Clearly separate CONTROL PLANE (create/update/delete operations via\n"
        "  ARM, portal, CLI) from DATA PLANE (runtime request path) when both\n"
        "  exist for this topic.\n"
        "- Components & Key Terms section: với MỖI term/component, giải thích:\n"
        "  (a) Nó LÀ GÌ (1 câu)\n"
        "  (b) Nó LÀM GÌ trong context của topic này (1 câu)\n"
        "  (c) TẠI SAO engineer cần biết — khi nào nó gây ra vấn đề (1 câu)\n"
        "  Format: **Term** – (a). (b). (c).\n"
        "  Only include terms an engineer actually needs to handle support cases.\n"
        "- Scenario mapping: for each child scenario, 1-2 sentences on what it\n"
        "  covers and when a case falls into it. Format scenario key as [[scenario-key]] "
        "  ONLY when that scenario has label [✓ wiki published] in the context. "
        "  If not yet published, write the key as plain text without [[ ]].\n"
        "- For Knowledge Nuggets: group by type. Mark docs_gap items as\n"
        "  '🔍 DOCS GAP' so they stand out. Mark current_limitations as\n"
        "  '⚠️ LIMITATION'. Include workaround and tooling_hint items inline.\n"
        "- Prefer accuracy over completeness. Use the provided OFFICIAL DOCS\n"
        "  excerpts to correct/anchor facts.\n"
        "- No secrets, tenant IDs, subscription IDs — use placeholders.\n"
        "- You may extrapolate from general Azure knowledge to fill in the\n"
        "  architecture/flow sections, but clearly mark with [general knowledge].\n\n"

        "STRUCTURE (use these exact headings)\n"
        "## Cách Sử Dụng Wiki Này\n"
        "(1 đoạn ngắn giải thích cấu trúc wiki 4 cấp:\n"
        "- **Topic page** (trang này): giới thiệu tổng quan service area, kiến thức nền tảng.\n"
        "- **Scenario page**: hướng dẫn troubleshooting cho từng nhóm vấn đề, có decision tree.\n"
        "- **Variant page**: hướng dẫn triage chi tiết, commands cụ thể, so sánh patterns.\n"
        "- **Leaf page**: playbook đầy đủ cho từng issue pattern cụ thể, có worked example.\n"
        "Gợi ý: nếu đã biết triệu chứng → đi thẳng tới Scenario Map bên dưới.)\n\n"

        "## Topic Này Bao Gồm Gì\n"
        "(2-4 câu: đây là area gì, tại sao customer mở case ở đây,\n"
        "Azure services/features nào liên quan.)\n\n"

        "## Components & Key Terms\n"
        "(Bullet list. Với mỗi term:\n"
        "**Term** – (a) Là gì. (b) Làm gì trong context này. (c) Tại sao quan trọng / khi nào gây issue.\n"
        "Chỉ include terms thực sự cần cho support work.)\n\n"

        "## Cách Hoạt Động (End-to-End Flow)\n"
        "(ASCII art diagram + NUMBERED steps.\n"
        "Split into Control Plane and Data Plane subsections if applicable.\n"
        "Annotate common failure points with Scenario references [[scenario-key]].)\n\n"

        "## Điểm Hay Xảy Ra Lỗi\n"
        "(Bullet list of the most common failure points, each mapped to the\n"
        "relevant scenario [[scenario-key]]. Brief: cái gì hỏng và dấu hiệu nhận biết.)\n\n"

        "## Scenario Map\n"
        "(Table format. Columns: Scenario key (as [[scenario-key]]) | Mô tả | Volume hint.\n"
        "Với mỗi scenario: 1-2 câu mô tả, khi nào case thuộc scenario này.)\n\n"

        "## Known Gaps & Nuggets\n"
        "(Group nuggets by type. Prefix: 🔍 DOCS GAP, ⚠️ LIMITATION, 💡 WORKAROUND, 🔧 TOOLING HINT.\n"
        "Nếu không có nuggets: 'Chưa có nuggets cho topic này.')\n\n"

        "## Official Documentation\n"
        "(List 3-8 relevant official doc links. Use the provided doc titles/URLs.\n"
        "Nếu không có docs: 'Chưa retrieve được official docs.')\n\n"

        "## See Also\n"
        "(List 2-4 topic keys liên quan (nếu có) mà engineer có thể nhầm lẫn. "
        "Format: - [[key]] – cách phân biệt.)\n\n"

        "## Lab Assignment\n"
        "(Thiết kế 1 bài lab thực hành cho engineer tự làm để hiểu topic này.\n"
        "NGUYÊN TẮC:\n"
        "- Lab phải MINIMAL: set up nhanh (dưới 15-20 phút), dùng Azure free tier / trial nếu có thể.\n"
        "- Lab phải cover được ít nhất 2-3 scenario chính trong topic bằng cách reproduce "
        "các action mà customer thường làm dẫn đến issue.\n"
        "- Format:\n"
        "  ### Mục Tiêu\n"
        "  (2-3 câu: engineer sẽ học được gì sau lab.)\n"
        "  ### Chuẩn Bị\n"
        "  (Bullet list: resources cần tạo, tools cần cài.)\n"
        "  ### Các Bước Thực Hành\n"
        "  (Numbered steps. Mỗi step: command/action + expected result + what to observe.\n"
        "  Include cả 'happy path' VÀ 'break it' steps — engineer phải thấy cả lúc hoạt động đúng "
        "  và lúc hỏng để hiểu failure signals.\n"
        "  Gợi ý reproduce: tạo sai DNS record, thử bind domain đã tồn tại, xóa resource rồi thử lại, etc.)\n"
        "  ### Câu Hỏi Tự Kiểm Tra\n"
        "  (3-5 câu hỏi engineer nên tự trả lời được sau khi hoàn thành lab.\n"
        "  Câu hỏi phải map về các scenario trong topic — giúp connect kiến thức lab với case thực tế.)\n"
        "  ### Dọn Dẹp\n"
        "  (Commands để delete resources sau khi lab xong — tránh phát sinh chi phí.)\n"
        "- Lab steps PHẢI dùng Azure CLI hoặc Portal — ưu tiên CLI cho reproducibility.\n"
        "- Nếu lab cần domain name, gợi ý dùng free subdomain services hoặc Azure DNS zone.\n"
        "- Nếu không chắc step nào hoạt động chính xác, đánh dấu [need confirmation].)\n"
    )

    user_prompt = (
        f"PRODUCT: {product}\n"
        f"TOPIC KEY: {topic_key}\n"
        f"TOPIC SIGNATURE: {(topic_signature or '')[:500]}\n\n"
        f"CHILD SCENARIOS ({len(scenarios)} total):\n{scenario_block}\n\n"
        f"KNOWLEDGE NUGGETS ({len(nuggets)} total):\n"
        f"{nugget_block if nuggets else '(none mined yet)'}\n\n"
        f"SAMPLE THREADS ({len(sample_threads)} shown):\n"
        f"{thread_block if sample_threads else '(none available)'}\n\n"
        f"OFFICIAL DOCS (Azure AI Search) ({len(docs)} shown):\n"
        f"{doc_block if docs else '(none retrieved)'}\n\n"
        "Generate the Markdown topic overview now."
    )

    resp = client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        max_completion_tokens=_MAX_COMPLETION_TOKENS,
        temperature=0.25,
    )

    content = (resp.choices[0].message.content or "").strip()

    # Prepend H1 title if model didn't
    title_line = f"# {topic_key}\n\n"
    if not content.startswith("# "):
        content = title_line + content

    return content


# ---------------------------------------------------------
# Per-topic pipeline
# ---------------------------------------------------------

def _process_single_topic(
    client: AzureOpenAI,
    deployment: str,
    topic: Dict[str, Any],
    wiki_id: str,
    wiki_root: str,
) -> Dict[str, Any]:
    """
    End-to-end: fetch context ? GPT-5.2 ? wiki push ? DB mark.
    Returns a log dict.
    """
    topic_id = int(topic["cluster_id"])
    topic_key = (topic.get("cluster_key") or f"topic-{topic_id}").strip()
    product = (topic.get("product") or "unknown").strip()
    topic_sig = (topic.get("cluster_signature_text") or "").strip()

    result: Dict[str, Any] = {
        "topic_id": topic_id,
        "topic_key": topic_key,
        "product": product,
        "status": "pending",
    }

    try:
        # --- Gather context ---
        with _sql_connect() as cnx:
            scenarios = _fetch_scenarios_for_topic(cnx, topic_id)
            nuggets = _fetch_nuggets_for_topic(cnx, topic_key, product)
            sample_threads = _fetch_sample_threads_for_topic(
                cnx, topic_id, top_n=8
            )

        # --- Retrieve official docs (Azure AI Search; same helper used by Phase 4A) ---
        # Keep query stable and compact; prefer product + topic key.
        docs_query = f"{product} {topic_key}".strip()
        docs = _search_kb_articles(docs_query, top_k=6)

        result["scenario_count"] = len(scenarios)
        result["nugget_count"] = len(nuggets)
        result["thread_count"] = len(sample_threads)
        result["doc_count"] = len(docs)

        if not scenarios and not sample_threads:
            result["status"] = "skipped_no_data"
            return result

        # --- Generate content ---
        md = generate_topic_wiki_content(
            client,
            deployment,
            product=product,
            topic_key=topic_key,
            topic_signature=topic_sig,
            scenarios=scenarios,
            nuggets=nuggets,
            sample_threads=sample_threads,
            docs=docs,
        )

        if not md or len(md.strip()) < 40:
            result["status"] = "skipped_empty_generation"
            return result

        # --- Build wiki path (simple: /<root>/<product>/<topic>) ---
        wiki_path = _build_topic_wiki_path(product, topic_key, wiki_root)

        # --- Resolve [[key]] links to real ADO wiki paths ---
        with _sql_connect() as cnx:
            md = _resolve_wiki_links(md, cnx)

        # --- Push to wiki ---
        page = upsert_wiki_page(wiki_id, wiki_path, md)

        if not page:
            # Write a sentinel so this candidate is not re-selected on the next call.
            # Without this the loop retries the same topics forever.
            with _sql_connect() as cnx:
                _mark_topic_wiki_populated(
                    cnx, topic_id, "push_failed",
                    markdown="", model_name=deployment,
                )
                cnx.commit()
            result["status"] = "wiki_push_failed"
            result["wiki_path"] = wiki_path
            return result

        # --- Mark as done in DB ---
        with _sql_connect() as cnx:
            _mark_topic_wiki_populated(
                cnx,
                topic_id,
                wiki_path,
                markdown=md,
                model_name=deployment,
            )
            cnx.commit()

        result["status"] = "ok"
        result["wiki_path"] = wiki_path
        return result

    except Exception as e:
        logging.exception("Phase4D error topic_id=%d", topic_id)
        result["status"] = "error"
        result["error"] = str(e)
        return result


# ---------------------------------------------------------
# Orchestrator (Azure Function entry point)
# ---------------------------------------------------------

def run_phase4d_populate_topics(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered Azure Function.

    Query params:
      - limit   (int, default 30)  – max topics to process
      - workers (int, default 4)   – parallel GPT-5.2 calls
    """
    try:
        limit = max(1, min(int(req.params.get("limit", str(_BATCH_SIZE))), 200))
        max_workers = max(
            1, min(int(req.params.get("workers", str(_MAX_WORKERS))), 12)
        )
        min_members = max(1, int(req.params.get("min_members", "1")))
        require_child_wiki = req.params.get("require_child_wiki", "1") != "0"
        product = (req.params.get("product") or "").strip()
    except Exception:
        limit = _BATCH_SIZE
        max_workers = _MAX_WORKERS
        min_members = 1
        require_child_wiki = True
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
            candidates = _select_topic_candidates(
                cnx, limit,
                min_members=min_members,
                require_child_wiki=require_child_wiki,
                product=product,
            )

        if not candidates:
            return func.HttpResponse(
                json.dumps(
                    {
                        "status": "ok",
                        "processed": 0,
                        "msg": "no topics need wiki population",
                    },
                    ensure_ascii=False,
                ),
                mimetype="application/json",
            )

        logging.info(
            "[4D] Starting topic wiki population: %d candidates, %d workers",
            len(candidates),
            max_workers,
        )

        # Process in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _process_single_topic,
                    client,
                    deployment,
                    t,
                    wiki_id,
                    wiki_root,
                ): t
                for t in candidates
            }

            for fut in as_completed(futures):
                try:
                    res = fut.result()
                except Exception as e:
                    t = futures[fut]
                    res = {
                        "topic_id": t.get("cluster_id"),
                        "topic_key": t.get("cluster_key"),
                        "status": "future_error",
                        "error": str(e),
                    }
                logs.append(res)

        # Summary
        summary: Dict[str, int] = {}
        for lg in logs:
            s = lg.get("status", "unknown")
            summary[s] = summary.get(s, 0) + 1

        ok_count = summary.get("ok", 0)

        return func.HttpResponse(
            json.dumps(
                {
                    "status": "ok",
                    "processed": ok_count,
                    "attempted": len(logs),
                    "summary": summary,
                    "details": logs,
                },
                ensure_ascii=False,
                default=str,
            ),
            mimetype="application/json",
        )

    except Exception as e:
        logging.exception("Phase 4D Fatal Error")
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}, ensure_ascii=False),
            status_code=500,
            mimetype="application/json",
        )

