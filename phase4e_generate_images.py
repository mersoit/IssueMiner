# -*- coding: utf-8 -*-
"""
phase4e_generate_images.py — Generate Azure UI screenshots / topology diagrams
and inline them into already-populated wiki markdown (L1/L2/L3 clusters).

Pipeline contract
─────────────────
Phase 4B/4C/4D embed special image request blocks in their generated
`WikiContentMarkdown`:

    <!-- AZURE_IMAGE_REQUEST
    kind: diagram | portal_screenshot
    caption: <short caption>
    prompt: <detailed prompt suitable for gpt-image-2>
    -->

Phase 4E:
  1. Scans dbo.issue_cluster for rows whose WikiContentMarkdown contains
     unresolved request blocks.
  2. Registers each block in dbo.cluster_images (status='pending').
  3. Generates the image via gpt-image-2 (rate-limited to ~2 per minute).
  4. Uploads the PNG bytes to blob storage (container defined below).
  5. Replaces the request block with a proper Markdown image reference,
     writes back WikiContentMarkdown, and updates cluster_images row.

Idempotent: a given (cluster_id, request_hash) is only generated once.
"""
from __future__ import annotations

import os
import re
import json
import time
import uuid
import base64
import hashlib
import logging
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import pyodbc

from ado_devops import upload_wiki_attachment, upsert_wiki_page

try:
    import azure.functions as func
except ImportError:  # local CLI use
    func = None  # type: ignore


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_IMAGES_PER_MINUTE = float(os.getenv("PHASE4E_IMAGES_PER_MINUTE", "2"))
_MIN_INTERVAL_SECS = max(1.0, 60.0 / max(_IMAGES_PER_MINUTE, 0.1)) + 1.0  # +1s buffer
_DEFAULT_SIZE = os.getenv("PHASE4E_IMAGE_SIZE", "1024x1024")
_BLOB_CONTAINER = os.getenv("PHASE4E_BLOB_CONTAINER", "wiki-images")
_MAX_BATCH = int(os.getenv("PHASE4E_MAX_BATCH", "20"))
_MAX_PROMPT_CHARS = int(os.getenv("PHASE4E_MAX_PROMPT_CHARS", "3500"))


# ---------------------------------------------------------------------------
# Placeholder format used by 4B/4C/4D system prompts
# ---------------------------------------------------------------------------
# Keep it loose so minor LLM variation still parses.

_IMAGE_REQUEST_RE = re.compile(
    r"<!--\s*AZURE_IMAGE_REQUEST\b(?P<body>.*?)-->",
    re.DOTALL | re.IGNORECASE,
)

_KEY_LINE_RE = re.compile(
    r"^\s*(?P<key>[A-Za-z_]+)\s*:\s*(?P<val>.*?)\s*$",
    re.MULTILINE,
)


def _parse_request_body(body: str) -> Dict[str, str]:
    """Parse the inside of an AZURE_IMAGE_REQUEST block.

    Keys: kind, caption, prompt (prompt may span multiple lines — we detect
    it as everything after the 'prompt:' line).
    """
    out = {"kind": "", "caption": "", "prompt": ""}

    # Extract prompt (multi-line tolerant): everything after first 'prompt:' line.
    prompt_match = re.search(r"(?im)^\s*prompt\s*:\s*(?P<rest>.*)$", body)
    if prompt_match:
        prompt_start = prompt_match.start("rest")
        out["prompt"] = body[prompt_start:].strip()

    # Extract other keys from the portion BEFORE the prompt: line if present.
    head = body[: prompt_match.start()] if prompt_match else body
    for m in _KEY_LINE_RE.finditer(head):
        key = m.group("key").strip().lower()
        val = m.group("val").strip()
        if key in ("kind", "caption") and val:
            out[key] = val

    # Normalise
    kind = (out["kind"] or "").strip().lower()
    if kind not in ("diagram", "portal_screenshot"):
        kind = "diagram"
    out["kind"] = kind
    out["caption"] = (out["caption"] or "")[:500]
    out["prompt"] = (out["prompt"] or "")[:_MAX_PROMPT_CHARS]
    return out


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

_TABLE_DDL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.objects WHERE type='U' AND name='cluster_images'
)
BEGIN
    CREATE TABLE dbo.cluster_images (
        image_id         INT IDENTITY(1,1) PRIMARY KEY,
        cluster_id       INT           NOT NULL,
        cluster_level    INT           NOT NULL,
        product          NVARCHAR(128) NULL,
        kind             NVARCHAR(32)  NOT NULL,
        caption          NVARCHAR(500) NULL,
        prompt           NVARCHAR(MAX) NOT NULL,
        request_hash     NVARCHAR(64)  NOT NULL,
        blob_container   NVARCHAR(128) NULL,
        blob_path        NVARCHAR(900) NULL,
        image_url        NVARCHAR(900) NULL,
        status           NVARCHAR(32)  NOT NULL DEFAULT 'pending',
        error_message    NVARCHAR(2000) NULL,
        model            NVARCHAR(64)  NULL,
        size             NVARCHAR(32)  NULL,
        created_at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        generated_at     DATETIME2     NULL,
        CONSTRAINT UQ_cluster_images UNIQUE (cluster_id, request_hash)
    );
END
"""


def ensure_table(cnx: pyodbc.Connection) -> None:
    cur = cnx.cursor()
    cur.execute(_TABLE_DDL)
    cnx.commit()


def _sql_connect() -> pyodbc.Connection:
    from sql_helpers import sql_connect as _shared
    return _shared()


# ---------------------------------------------------------------------------
# Scanning: find pending requests in issue_cluster.WikiContentMarkdown
# ---------------------------------------------------------------------------

def _fetch_candidate_clusters(
    cnx: pyodbc.Connection,
    product: str = "",
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """Return issue_cluster rows with wiki content that still has request blocks."""
    cur = cnx.cursor()
    args: List[Any] = [int(limit)]
    prod_filter = ""
    if product:
        prod_filter = " AND ic.product = ?"
        args.append(product)
    cur.execute(
        f"""
        SELECT TOP (?)
            ic.cluster_id,
            ic.cluster_level,
            ic.product,
            ic.WikiContentMarkdown
        FROM dbo.issue_cluster ic
        WHERE ic.cluster_level IN (1,2,3)
          AND ic.WikiContentMarkdown IS NOT NULL
          AND ic.WikiContentMarkdown LIKE '%AZURE_IMAGE_REQUEST%'
          {prod_filter}
        ORDER BY ic.cluster_level, ic.cluster_id
        """,
        *args,
    )
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in rows]


def _register_requests_for_cluster(
    cnx: pyodbc.Connection,
    row: Dict[str, Any],
) -> int:
    """Extract AZURE_IMAGE_REQUEST blocks and upsert cluster_images rows.

    Returns number of new pending rows created.
    """
    md: str = row.get("WikiContentMarkdown") or ""
    if not md or "AZURE_IMAGE_REQUEST" not in md:
        return 0

    added = 0
    cur = cnx.cursor()
    for m in _IMAGE_REQUEST_RE.finditer(md):
        body = m.group("body") or ""
        parsed = _parse_request_body(body)
        if not parsed.get("prompt"):
            continue
        req_hash = hashlib.sha256(
            f"{parsed['kind']}|{parsed['caption']}|{parsed['prompt']}".encode("utf-8")
        ).hexdigest()

        cur.execute("""
            IF NOT EXISTS (
                SELECT 1 FROM dbo.cluster_images
                WHERE cluster_id = ? AND request_hash = ?
            )
            INSERT INTO dbo.cluster_images
                (cluster_id, cluster_level, product, kind, caption, prompt,
                 request_hash, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending');
        """,
            int(row["cluster_id"]), req_hash,
            int(row["cluster_id"]), int(row["cluster_level"] or 0),
            (row.get("product") or None),
            parsed["kind"], parsed["caption"], parsed["prompt"], req_hash,
        )
        if cur.rowcount:
            added += 1
    cnx.commit()
    return added


def _fetch_pending_images(cnx: pyodbc.Connection, limit: int) -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    cur.execute(
        """
        SELECT TOP (?)
            image_id, cluster_id, cluster_level, product, kind, caption,
            prompt, request_hash
        FROM dbo.cluster_images
        WHERE status = 'pending'
        ORDER BY image_id
        """,
        int(limit),
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _mark_image_generated(
    cnx: pyodbc.Connection,
    image_id: int,
    blob_container: str,
    blob_path: str,
    image_url: str,
    model: str,
    size: str,
) -> None:
    cur = cnx.cursor()
    cur.execute(
        """
        UPDATE dbo.cluster_images
        SET status = 'generated',
            blob_container = ?,
            blob_path = ?,
            image_url = ?,
            model = ?,
            size = ?,
            generated_at = SYSUTCDATETIME(),
            error_message = NULL
        WHERE image_id = ?
        """,
        blob_container, blob_path, image_url, model, size, int(image_id),
    )
    cnx.commit()


def _mark_image_failed(cnx: pyodbc.Connection, image_id: int, err: str) -> None:
    cur = cnx.cursor()
    cur.execute(
        """
        UPDATE dbo.cluster_images
        SET status = 'failed',
            error_message = ?
        WHERE image_id = ?
        """,
        (err or "")[:1900], int(image_id),
    )
    cnx.commit()


def _replace_placeholder_in_cluster(
    cnx: pyodbc.Connection,
    cluster_id: int,
    request_hash: str,
    image_url: str,
    caption: str,
) -> bool:
    """Replace the first matching placeholder block inside this cluster's
    WikiContentMarkdown with a rendered image reference. Returns True on change.
    """
    cur = cnx.cursor()
    cur.execute(
        "SELECT WikiContentMarkdown FROM dbo.issue_cluster WHERE cluster_id = ?",
        int(cluster_id),
    )
    row = cur.fetchone()
    if not row or not row[0]:
        return False
    md: str = row[0]

    caption_clean = (caption or "").strip()
    alt_text = caption_clean or "image"
    image_md = f"![{alt_text}]({image_url})"
    if caption_clean:
        image_md = f"{image_md}\n*{caption_clean}*"

    replaced = False

    def _sub(m: re.Match) -> str:
        nonlocal replaced
        if replaced:
            return m.group(0)
        body = m.group("body") or ""
        parsed = _parse_request_body(body)
        h = hashlib.sha256(
            f"{parsed['kind']}|{parsed['caption']}|{parsed['prompt']}".encode("utf-8")
        ).hexdigest()
        if h == request_hash:
            replaced = True
            return image_md
        return m.group(0)

    new_md = _IMAGE_REQUEST_RE.sub(_sub, md)
    if not replaced:
        return False

    new_hash = hashlib.sha256((new_md or "").encode("utf-8")).hexdigest()
    cur.execute(
        """
        UPDATE dbo.issue_cluster
        SET WikiContentMarkdown = ?,
            WikiContentHash = ?
        WHERE cluster_id = ?
        """,
        new_md, new_hash, int(cluster_id),
    )
    cnx.commit()
    return True


def _republish_cluster_page(cnx: pyodbc.Connection, cluster_id: int) -> bool:
    """Push the spliced markdown back to ADO.

    4B/4C/4D push at generation time and only ever revisit nodes whose WikiPath is
    NULL, so without this the live page keeps showing the raw AZURE_IMAGE_REQUEST
    comment even though the DB copy has the image.
    """
    wiki_id = os.getenv("ADO_WIKI_ID", "")
    if not wiki_id:
        logging.warning("[4E] ADO_WIKI_ID not set; cannot republish cluster_id=%d", cluster_id)
        return False

    cur = cnx.cursor()
    cur.execute(
        """
        SELECT COALESCE(WikiPath, VariantWikiPath, ScenarioWikiPath, TopicWikiPath),
               WikiContentMarkdown
        FROM dbo.issue_cluster WHERE cluster_id = ?
        """,
        int(cluster_id),
    )
    row = cur.fetchone()
    if not row or not row[0] or not row[1]:
        return False

    page = upsert_wiki_page(wiki_id, row[0], row[1])
    if not page:
        logging.warning("[4E] republish failed cluster_id=%d path=%s", cluster_id, row[0])
        return False

    cur.execute(
        "UPDATE dbo.issue_cluster SET WikiPushedUtc = SYSUTCDATETIME() WHERE cluster_id = ?",
        int(cluster_id),
    )
    cnx.commit()
    return True


# ---------------------------------------------------------------------------
# Blob storage
# ---------------------------------------------------------------------------

def _get_blob_service():
    conn_str = os.environ.get("AzureWebJobsStorage") or os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise RuntimeError("AzureWebJobsStorage not set — cannot upload images.")
    from azure.storage.blob import BlobServiceClient, ContentSettings  # noqa: F401
    return BlobServiceClient.from_connection_string(conn_str)


def _upload_png(png_bytes: bytes, blob_path: str) -> Tuple[str, str, str]:
    """Upload PNG bytes. Returns (container, blob_path, public_url)."""
    from azure.storage.blob import ContentSettings

    svc = _get_blob_service()
    container = svc.get_container_client(_BLOB_CONTAINER)
    try:
        container.get_container_properties()
    except Exception:
        container.create_container()

    blob = container.get_blob_client(blob_path)
    blob.upload_blob(
        png_bytes,
        overwrite=True,
        content_settings=ContentSettings(content_type="image/png"),
    )
    account = svc.account_name
    url = f"https://{account}.blob.core.windows.net/{_BLOB_CONTAINER}/{blob_path}"
    return _BLOB_CONTAINER, blob_path, url


# ---------------------------------------------------------------------------
# Image generation
# ---------------------------------------------------------------------------

def _system_style_prefix(kind: str) -> str:
    if kind == "portal_screenshot":
        return (
            "Generate a clean, realistic Azure Portal UI screenshot. "
            "Use the standard Microsoft Azure Portal visual language: left vertical nav, "
            "top breadcrumb, blade layout, accurate blade titles, tabs, labels, "
            "and field positions. Highlight the specified control(s) with a yellow "
            "rounded rectangle outline (2-3 px). No photorealistic people, no branding "
            "beyond Azure's own. No fake error dialogs unless explicitly asked. "
            "High legibility, crisp typography. Do not invent UI elements not requested.\n\n"
            "Requested content:\n"
        )
    return (
        "Generate a clear architecture/topology diagram for Azure. Use a clean, "
        "flat-design style (no 3D, no photorealism). Use official-looking Azure icon "
        "outlines for services, labelled clearly. Use numbered arrows (1, 2, 3…) "
        "to indicate the flow. Include only the components requested. White or "
        "very light background. High legibility typography.\n\n"
        "Requested content:\n"
    )


def _generate_image_bytes(prompt: str, kind: str) -> Tuple[bytes, str, str]:
    """Call gpt-image-2 and return (png_bytes, model_name, size).

    Raises on failure. Expects b64_json response.
    """
    from aoai_helpers import make_image_client, get_image_deployment

    client = make_image_client()
    deployment = get_image_deployment()

    composed = _system_style_prefix(kind) + (prompt or "").strip()
    composed = composed[:_MAX_PROMPT_CHARS]

    resp = client.images.generate(
        model=deployment,
        prompt=composed,
        n=1,
        size=_DEFAULT_SIZE,
    )

    data_list = getattr(resp, "data", None) or []
    if not data_list:
        raise RuntimeError("Image API returned no data")

    item = data_list[0]
    b64 = getattr(item, "b64_json", None)
    if not b64:
        url = getattr(item, "url", None)
        if not url:
            raise RuntimeError("Image API returned neither b64_json nor url")
        import requests as _rq
        rr = _rq.get(url, timeout=60)
        rr.raise_for_status()
        return rr.content, str(deployment), _DEFAULT_SIZE

    return base64.b64decode(b64), str(deployment), _DEFAULT_SIZE


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def _process_one(
    cnx: pyodbc.Connection,
    item: Dict[str, Any],
    last_call_time_ref: List[float],
) -> Dict[str, Any]:
    image_id = int(item["image_id"])
    cluster_id = int(item["cluster_id"])
    req_hash = item["request_hash"]
    kind = item.get("kind") or "diagram"
    caption = item.get("caption") or ""
    prompt = item.get("prompt") or ""

    # Rate-limit: ensure minimum interval since the last call in THIS process.
    now = time.time()
    wait = (last_call_time_ref[0] + _MIN_INTERVAL_SECS) - now
    if wait > 0:
        logging.info("[4E] Sleeping %.1fs to respect image rate limit", wait)
        time.sleep(wait)

    try:
        png, model, size = _generate_image_bytes(prompt, kind)
    except Exception as e:
        last_call_time_ref[0] = time.time()
        logging.error("[4E] image_id=%d generation failed: %s", image_id, str(e)[:400])
        _mark_image_failed(cnx, image_id, str(e))
        return {"image_id": image_id, "cluster_id": cluster_id, "status": "failed", "error": str(e)[:300]}

    last_call_time_ref[0] = time.time()

    # Upload.
    today = dt.datetime.utcnow().strftime("%Y/%m/%d")
    unique = uuid.uuid4().hex[:10]
    blob_path = f"{today}/cluster_{cluster_id}_{image_id}_{unique}.png"
    try:
        container, path, url = _upload_png(png, blob_path)
    except Exception as e:
        logging.error("[4E] image_id=%d upload failed: %s", image_id, str(e)[:400])
        _mark_image_failed(cnx, image_id, f"upload_failed: {e}")
        return {"image_id": image_id, "cluster_id": cluster_id, "status": "failed", "error": str(e)[:300]}

    _mark_image_generated(cnx, image_id, container, path, url, model, size)

    # Splice into wiki markdown.
    try:
        replaced = _replace_placeholder_in_cluster(cnx, cluster_id, req_hash, url, caption)
    except Exception as e:
        logging.warning("[4E] image_id=%d replace failed: %s", image_id, str(e)[:300])
        replaced = False

    republished = False
    if replaced:
        try:
            republished = _republish_cluster_page(cnx, cluster_id)
        except Exception as e:
            logging.warning("[4E] image_id=%d republish failed: %s", image_id, str(e)[:300])

    return {
        "image_id": image_id,
        "cluster_id": cluster_id,
        "status": "generated",
        "url": url,
        "replaced_placeholder": bool(replaced),
        "republished": bool(republished),
    }


def run_phase4e_generate_images(req) -> Any:
    """HTTP entry point.

    Query params:
      product=<name>   optional: restrict scanning to this product
      limit=<int>      max pending images to process in this call (default 20)
      scan_only=1      only scan + register, do not generate
    """
    try:
        product = (req.params.get("product") or "").strip() if req else ""
        limit = int(req.params.get("limit", str(_MAX_BATCH))) if req else _MAX_BATCH
        scan_only = (req.params.get("scan_only", "0") == "1") if req else False
        limit = max(1, min(int(limit), 50))

        cnx = _sql_connect()
        try:
            ensure_table(cnx)

            # 1. scan clusters with requests, register any new ones
            candidates = _fetch_candidate_clusters(cnx, product=product, limit=500)
            registered = 0
            for row in candidates:
                try:
                    registered += _register_requests_for_cluster(cnx, row)
                except Exception as e:
                    logging.warning("[4E] register failed cluster=%s err=%s",
                                    row.get("cluster_id"), str(e)[:300])

            if scan_only:
                resp_body = {
                    "status": "ok",
                    "processed": 0,
                    "registered": registered,
                    "candidates_scanned": len(candidates),
                    "message": "scan_only",
                }
                if func is None:
                    return resp_body
                return func.HttpResponse(json.dumps(resp_body), mimetype="application/json")

            # 2. process pending images (respecting rate limit)
            pending = _fetch_pending_images(cnx, limit=limit)
            results: List[Dict[str, Any]] = []
            last_call_time_ref = [0.0]
            for item in pending:
                try:
                    r = _process_one(cnx, item, last_call_time_ref)
                except Exception as e:
                    logging.exception("[4E] unexpected error on image_id=%s", item.get("image_id"))
                    try:
                        _mark_image_failed(cnx, int(item["image_id"]), f"unexpected: {e}")
                    except Exception:
                        pass
                    r = {"image_id": item.get("image_id"), "status": "failed", "error": str(e)[:300]}
                results.append(r)

            summary: Dict[str, int] = {}
            for r in results:
                s = r.get("status", "unknown")
                summary[s] = summary.get(s, 0) + 1

            # still-pending after this call?
            cur = cnx.cursor()
            cur.execute("SELECT COUNT(*) FROM dbo.cluster_images WHERE status = 'pending'")
            still_pending = int(cur.fetchone()[0] or 0)

            resp_body = {
                "status": "ok",
                "processed": len(results),
                "registered": registered,
                "candidates_scanned": len(candidates),
                "still_pending": still_pending,
                "summary": summary,
                "details": results,
            }
            if func is None:
                return resp_body
            return func.HttpResponse(
                json.dumps(resp_body, ensure_ascii=False, default=str),
                mimetype="application/json",
            )
        finally:
            try:
                cnx.close()
            except Exception:
                pass
    except Exception as e:
        logging.exception("Phase 4E fatal error")
        if func is None:
            return {"status": "error", "error": str(e)}
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}),
            status_code=500,
            mimetype="application/json",
        )


# ---------------------------------------------------------------------------
# Assessment-driven illustration (4E-B)
#
# The placeholder flow above only fires if 4B/4C/4D volunteered an
# AZURE_IMAGE_REQUEST block, and in practice they almost never do. This path
# instead READS an already-published page and judges whether a diagram would
# genuinely help, then splices one in. Insertion is strictly additive: the
# original markdown is never rewritten, only added to.
# ---------------------------------------------------------------------------

_ASSESS_MAX_CHARS = int(os.getenv("PHASE4E_ASSESS_MAX_CHARS", "7000"))

_ASSESS_SYSTEM = (
    "You decide whether a technical wiki page would be MATERIALLY improved by ONE image.\n"
    "The bar is deliberately very high. Default to NO.\n\n"
    "CALIBRATION: across a normal set of support pages, FEWER THAN 1 IN 5 deserve an image. "
    "If you find yourself constructing a justification, the answer is NO. A page being long, "
    "detailed, or technical is NOT a reason. 'It would help visualise the flow' is NOT a "
    "reason — that is true of almost any page and is exactly the trap to avoid.\n\n"
    "Answer YES only if ALL of these hold:\n"
    "  1. The page's core subject is an ABSTRACT STRUCTURE — components spanning at least "
    "TWO different ownership or trust boundaries (e.g. tenant vs service, client vs cloud, "
    "control plane vs data plane) — not a single linear sequence.\n"
    "  2. Which component owns which failure is genuinely hard to keep straight from the "
    "prose alone.\n"
    "  3. The image would carry information NOT already conveyed by the page's headings, "
    "numbered steps, or any existing text/ASCII diagram.\n\n"
    "Answer NO — this covers the large majority of pages — when:\n"
    "  - the page already contains an ASCII/text flow or step list conveying the same thing;\n"
    "  - it is a linear checklist, CLI sequence, config values, or symptom→cause→fix;\n"
    "  - it concerns a single setting, toggle, permission, licence state, or UI location;\n"
    "  - the 'flow' is simply: user acts → service responds → error appears;\n"
    "  - a diagram would mostly re-draw the page's own section headings;\n"
    "  - the subject is concrete, familiar, or self-evident to a support engineer.\n\n"
    "Diagram types allowed:\n"
    "  - 'diagram': boundary / ownership / topology maps. Preferred.\n"
    "  - 'portal_screenshot': ONLY when several interacting fields in one blade must be "
    "seen together. Almost never correct for conceptual pages.\n\n"
    "Before deciding, state to yourself what the image would actually show. If that is a "
    "restatement of the page structure, answer NO.\n\n"
    "If YES, write a self-contained image prompt naming EVERY component/box, EVERY arrow "
    "with its label, and the layout. It must be derived strictly from the page content — "
    "invent nothing. Target <= 1200 characters.\n"
    "Also pick 'anchor_heading': the EXACT text of an existing '## ' heading on the page "
    "that the image should be placed directly beneath. Copy it verbatim, without the '##'. "
    "If none fits, use an empty string.\n"
    "Write 'caption' in Vietnamese to match the page language.\n\n"
    'Return STRICT JSON only: {"warranted": true|false, "reason": "<=200 chars", '
    '"would_show": "<=160 chars, what the image would depict", '
    '"kind": "diagram|portal_screenshot", "caption": "...", "prompt": "...", '
    '"anchor_heading": "..."}'
)

# L1 topic pages are architecture overviews and legitimately earn diagrams more often;
# deeper levels are concrete troubleshooting and should almost always be refused.
_LEVEL_GUIDANCE = {
    1: "This is a TOPIC overview page. A diagram is plausible here, but still only if the "
       "page maps a genuine multi-system boundary rather than restating its own headings.",
    2: "This is a SCENARIO page describing one concrete failure mode. The bar is HIGHER than "
       "for topic pages. Expect NO unless it genuinely spans several trust/service boundaries.",
    3: "This is a VARIANT page — a single diagnostic branch. Almost always answer NO.",
}


def _assess_page_for_diagram(product: str, level: int, key: str, markdown: str) -> Dict[str, Any]:
    """Ask the quality model whether this page warrants one image."""
    from aoai_helpers import (
        call_aoai_with_retry, estimate_tokens, get_gpt52_deployment,
        get_rate_limiter, make_gpt52_client,
    )

    level_name = {1: "Topic (L1)", 2: "Scenario (L2)", 3: "Variant (L3)"}.get(level, f"L{level}")
    body = (markdown or "")[:_ASSESS_MAX_CHARS]
    user = (
        f"PRODUCT: {product}\nLEVEL: {level_name}\nPAGE KEY: {key}\n"
        f"{_LEVEL_GUIDANCE.get(level, '')}\n\n"
        f"--- PAGE MARKDOWN ---\n{body}\n--- END ---"
    )
    resp = call_aoai_with_retry(
        make_gpt52_client(),
        model=get_gpt52_deployment(),
        messages=[
            {"role": "system", "content": _ASSESS_SYSTEM},
            {"role": "user", "content": user},
        ],
        response_format={"type": "json_object"},
        max_completion_tokens=int(os.getenv("PHASE4E_ASSESS_MAX_OUTPUT", "4000")),
        estimated_prompt_tokens=estimate_tokens(_ASSESS_SYSTEM) + estimate_tokens(user),
        rate_limiter=get_rate_limiter("gpt52"),
        caller_tag="phase4e_assess",
    )
    raw = (resp.choices[0].message.content or "").strip()
    try:
        data = json.loads(raw)
    except Exception:
        return {"warranted": False, "reason": "unparseable assessment"}

    if not bool(data.get("warranted")):
        return {"warranted": False, "reason": str(data.get("reason") or "")[:200]}

    kind = str(data.get("kind") or "diagram").strip()
    if kind not in ("diagram", "portal_screenshot"):
        kind = "diagram"
    prompt = str(data.get("prompt") or "").strip()
    if len(prompt) < 40:
        return {"warranted": False, "reason": "prompt too thin"}

    return {
        "warranted": True,
        "reason": str(data.get("reason") or "")[:200],
        "kind": kind,
        "caption": str(data.get("caption") or "").strip()[:400],
        "prompt": prompt,
        "anchor_heading": str(data.get("anchor_heading") or "").strip(),
    }


def _insert_image_block(md: str, block: str, anchor_heading: str) -> str:
    """Insert `block` into `md` without altering a single existing character.

    Placed after the body of the anchor heading's section when one matches, else
    after the H1 intro, else appended.
    """
    lines = md.splitlines(keepends=True)

    anchor_idx = -1
    if anchor_heading:
        want = anchor_heading.strip().lstrip("#").strip().lower()
        for i, ln in enumerate(lines):
            if ln.lstrip().startswith("##"):
                if ln.lstrip().lstrip("#").strip().lower() == want:
                    anchor_idx = i
                    break

    if anchor_idx >= 0:
        # place at the end of that section, just before the next heading
        j = anchor_idx + 1
        while j < len(lines) and not lines[j].lstrip().startswith("#"):
            j += 1
        insert_at = j
    else:
        # after the H1 title block, before the first '##'
        insert_at = 0
        for i, ln in enumerate(lines):
            if ln.lstrip().startswith("## "):
                insert_at = i
                break
        else:
            insert_at = len(lines)

    return "".join(lines[:insert_at]) + block + "".join(lines[insert_at:])


def _fetch_illustration_candidates(
    cnx: pyodbc.Connection, product: str, levels: List[int], limit: int,
) -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    lv = ",".join(str(int(x)) for x in levels) or "1"
    args: List[Any] = [int(limit)]
    prod = ""
    if product:
        prod = " AND ic.product = ?"
        args.append(product)
    cur.execute(
        f"""
        SELECT TOP (?)
            ic.cluster_id, ic.cluster_level, ic.product, ic.cluster_key,
            ic.member_count, ic.WikiContentMarkdown
        FROM dbo.issue_cluster ic
        WHERE ic.is_active = 1
          AND ic.cluster_level IN ({lv})
          AND ic.WikiContentMarkdown IS NOT NULL
          AND LEN(ic.WikiContentMarkdown) > 400
          AND COALESCE(ic.WikiPath, ic.VariantWikiPath, ic.ScenarioWikiPath, ic.TopicWikiPath) IS NOT NULL
          AND CHARINDEX('![', ic.WikiContentMarkdown) = 0
          AND NOT EXISTS (
                SELECT 1 FROM dbo.cluster_images ci
                WHERE ci.cluster_id = ic.cluster_id AND ci.status IN ('generated','pending')
          )
          {prod}
        ORDER BY ic.cluster_level, ic.member_count DESC, ic.cluster_id
        """,
        *args,
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _illustrate_one(
    cnx: pyodbc.Connection, row: Dict[str, Any], last_call_time_ref: List[float],
) -> Dict[str, Any]:
    cid = int(row["cluster_id"])
    level = int(row["cluster_level"])
    product = str(row.get("product") or "")
    key = str(row.get("cluster_key") or "")
    md = row.get("WikiContentMarkdown") or ""

    verdict = _assess_page_for_diagram(product, level, key, md)
    if not verdict.get("warranted"):
        return {"cluster_id": cid, "level": level, "key": key,
                "status": "not_warranted", "reason": verdict.get("reason", "")}

    kind = verdict["kind"]
    caption = verdict.get("caption") or ""
    prompt = verdict["prompt"]
    req_hash = hashlib.sha256(f"{kind}|{caption}|{prompt}".encode("utf-8")).hexdigest()

    cur = cnx.cursor()
    cur.execute(
        """
        INSERT INTO dbo.cluster_images
            (cluster_id, cluster_level, product, kind, caption, prompt, request_hash, status)
        VALUES (?,?,?,?,?,?,?, 'pending')
        """,
        cid, level, product, kind, caption[:500], prompt, req_hash,
    )
    cnx.commit()
    cur.execute(
        "SELECT image_id FROM dbo.cluster_images WHERE cluster_id=? AND request_hash=?",
        cid, req_hash,
    )
    image_id = int(cur.fetchone()[0])

    # gpt-image-2 is capacity-2; keep the documented spacing between calls.
    wait = _MIN_INTERVAL_SECS - (time.time() - last_call_time_ref[0])
    if wait > 0:
        time.sleep(wait)
    last_call_time_ref[0] = time.time()

    try:
        png, model, size = _generate_image_bytes(prompt, kind)
    except Exception as e:
        _mark_image_failed(cnx, image_id, f"generate_failed: {e}")
        return {"cluster_id": cid, "level": level, "key": key,
                "status": "generate_failed", "error": str(e)[:300]}

    try:
        # The storage account rejects shared-key auth, so keep the image inside the
        # wiki as an attachment — it also avoids needing a public container.
        wiki_id = os.environ["ADO_WIKI_ID"]
        att_name = f"{(product or 'general').replace(' ', '-').lower()}-l{level}-{cid}-{req_hash[:10]}.png"
        url = upload_wiki_attachment(wiki_id, att_name, png)
        if not url:
            raise RuntimeError("wiki attachment upload returned no path")
        container, path = "wiki-attachments", att_name
    except Exception as e:
        _mark_image_failed(cnx, image_id, f"upload_failed: {e}")
        return {"cluster_id": cid, "level": level, "key": key,
                "status": "upload_failed", "error": str(e)[:300]}

    _mark_image_generated(cnx, image_id, container, path, url, model, size)

    alt = caption or key
    block = f"\n![{alt}]({url})\n"
    if caption:
        block += f"*{caption}*\n"
    new_md = _insert_image_block(md, block, verdict.get("anchor_heading", ""))

    # Hard guarantee: nothing but the block was added.
    if new_md.replace(block, "", 1) != md:
        _mark_image_failed(cnx, image_id, "insertion_would_alter_content")
        return {"cluster_id": cid, "level": level, "key": key, "status": "insert_unsafe"}

    cur.execute(
        "UPDATE dbo.issue_cluster SET WikiContentMarkdown=?, WikiContentHash=? WHERE cluster_id=?",
        new_md, hashlib.sha256(new_md.encode("utf-8")).hexdigest(), cid,
    )
    cnx.commit()

    pushed = _republish_cluster_page(cnx, cid)
    return {
        "cluster_id": cid, "level": level, "key": key, "status": "illustrated",
        "kind": kind, "caption": caption, "url": url, "pushed": bool(pushed),
        "reason": verdict.get("reason", ""),
        "chars_before": len(md), "chars_after": len(new_md),
    }


def run_phase4e_assess_and_illustrate(req) -> Any:
    """Read published pages, judge whether a diagram helps, generate + push."""
    try:
        p = getattr(req, "params", {}) or {}
        product = (p.get("product") or "").strip()
        levels = [int(x) for x in (p.get("levels") or "1,2").split(",") if x.strip()]
        limit = int(p.get("limit", "40"))
        assess_only = p.get("assess_only", "0") == "1"
        max_images = int(p.get("max_images", "999"))

        cnx = _sql_connect()
        try:
            ensure_table(cnx)
            rows = _fetch_illustration_candidates(cnx, product, levels, limit)
            results: List[Dict[str, Any]] = []
            last_call = [0.0]
            made = 0

            for row in rows:
                if assess_only:
                    v = _assess_page_for_diagram(
                        str(row.get("product") or ""), int(row["cluster_level"]),
                        str(row.get("cluster_key") or ""), row.get("WikiContentMarkdown") or "",
                    )
                    results.append({
                        "cluster_id": int(row["cluster_id"]),
                        "level": int(row["cluster_level"]),
                        "key": row.get("cluster_key"),
                        "warranted": bool(v.get("warranted")),
                        "reason": v.get("reason", ""),
                        "kind": v.get("kind", ""),
                    })
                    continue

                if made >= max_images:
                    break
                r = _illustrate_one(cnx, row, last_call)
                if r.get("status") == "illustrated":
                    made += 1
                results.append(r)

            summary: Dict[str, int] = {}
            for r in results:
                k = r.get("status") or ("warranted" if r.get("warranted") else "not_warranted")
                summary[k] = summary.get(k, 0) + 1

            body = {
                "status": "ok",
                "candidates": len(rows),
                "assess_only": assess_only,
                "images_created": made,
                "summary": summary,
                "details": results,
            }
            if func is None:
                return body
            return func.HttpResponse(
                json.dumps(body, ensure_ascii=False, default=str), mimetype="application/json"
            )
        finally:
            try:
                cnx.close()
            except Exception:
                pass
    except Exception as e:
        logging.exception("Phase 4E assess/illustrate fatal error")
        if func is None:
            return {"status": "error", "error": str(e)}
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}),
            status_code=500, mimetype="application/json",
        )
