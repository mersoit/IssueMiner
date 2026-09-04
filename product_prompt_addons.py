# -*- coding: utf-8 -*-
"""
product_prompt_addons.py — Per-product prompt customization layer.

Operators attach freeform guidance to a canonical enriched product name
(e.g. "Virtual Network"). GPT-5.4-nano converts that guidance into
concise system-prompt ADDITIONS, one per supported phase. Each phase reads
its addon at runtime and appends it to its generic system prompt ONLY when
the processed thread's product matches.

Supported phases:
  phase1b  – catalog proposal / categorization (L1/L2/L3/L4 naming)
  phase4a  – nugget mining (pass 1)
  phase4b  – variant wiki generation (L3)
  phase4c  – scenario wiki generation (L2)
  phase4d  – topic wiki generation (L1)
"""
from __future__ import annotations

import os
import json
import time
import logging
import threading
from typing import Any, Dict, List, Optional

import pyodbc


PHASE_KEYS = ("phase1b", "phase4a", "phase4b", "phase4c", "phase4d")

PHASE_PURPOSE: Dict[str, str] = {
    "phase1b": (
        "Builds/maintains the 4-level common-issue catalog "
        "(L1 topic, L2 scenario, L3 variant, L4 leaf). "
        "Addons should steer how categories are named, grouped and prioritized."
    ),
    "phase4a": (
        "Mines nuggets (docs gaps, limitations, workarounds, tooling hints) "
        "from individual threads. Addons should steer what kinds of nuggets matter most."
    ),
    "phase4b": (
        "Generates the per-variant (L3) wiki page for junior support engineers. "
        "Addons should steer coverage (components, pitfalls, comparisons)."
    ),
    "phase4c": (
        "Generates the per-scenario (L2) wiki page. Addons should steer "
        "preliminary checks, routing logic and platform behaviors."
    ),
    "phase4d": (
        "Generates the per-topic (L1) wiki overview page. Addons should steer "
        "how the topic is framed, which components to explain, and how they fit "
        "within the larger service."
    ),
}


# ---------------------------------------------------------------------------
# DB / DDL
# ---------------------------------------------------------------------------

_TABLE_DDL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.objects WHERE type = 'U' AND name = 'product_prompt_addons'
)
BEGIN
    CREATE TABLE dbo.product_prompt_addons (
        product_name     NVARCHAR(128) NOT NULL PRIMARY KEY,
        raw_description  NVARCHAR(MAX) NULL,
        phase1b_addon    NVARCHAR(MAX) NULL,
        phase4a_addon    NVARCHAR(MAX) NULL,
        phase4b_addon    NVARCHAR(MAX) NULL,
        phase4c_addon    NVARCHAR(MAX) NULL,
        phase4d_addon    NVARCHAR(MAX) NULL,
        generated_at     DATETIME2     NULL,
        generated_model  NVARCHAR(64)  NULL,
        updated_at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
"""


def ensure_table(cnx: pyodbc.Connection) -> None:
    cur = cnx.cursor()
    cur.execute(_TABLE_DDL)
    cnx.commit()


def _get_connection_string() -> str:
    cs = os.environ.get("SQL_CONNECTION_STRING")
    if cs:
        return cs
    try:
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "local.settings.json")
        with open(p, "r", encoding="utf-8-sig") as f:
            cs = json.load(f).get("Values", {}).get("SQL_CONNECTION_STRING")
            if cs:
                return cs
    except Exception:
        pass
    raise RuntimeError("SQL_CONNECTION_STRING not found")


def _open_own_cnx() -> pyodbc.Connection:
    from sql_helpers import sql_connect as _shared
    return _shared()


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def get_customization(cnx: pyodbc.Connection, product: str) -> Optional[Dict[str, Any]]:
    product = (product or "").strip()
    if not product:
        return None
    ensure_table(cnx)
    cur = cnx.cursor()
    cur.execute("""
        SELECT product_name, raw_description,
               phase1b_addon, phase4a_addon, phase4b_addon,
               phase4c_addon, phase4d_addon,
               generated_at, generated_model, updated_at
        FROM dbo.product_prompt_addons
        WHERE product_name = ?
    """, product)
    row = cur.fetchone()
    if not row:
        return None
    cols = [c[0] for c in cur.description]
    return dict(zip(cols, row))


def list_customizations(cnx: pyodbc.Connection) -> List[Dict[str, Any]]:
    ensure_table(cnx)
    cur = cnx.cursor()
    cur.execute("""
        SELECT
            product_name,
            updated_at,
            generated_at,
            generated_model,
            CASE WHEN raw_description IS NULL OR raw_description = '' THEN 0 ELSE 1 END AS has_raw,
            CASE WHEN phase1b_addon    IS NULL OR phase1b_addon    = '' THEN 0 ELSE 1 END AS has_p1b,
            CASE WHEN phase4a_addon    IS NULL OR phase4a_addon    = '' THEN 0 ELSE 1 END AS has_p4a,
            CASE WHEN phase4b_addon    IS NULL OR phase4b_addon    = '' THEN 0 ELSE 1 END AS has_p4b,
            CASE WHEN phase4c_addon    IS NULL OR phase4c_addon    = '' THEN 0 ELSE 1 END AS has_p4c,
            CASE WHEN phase4d_addon    IS NULL OR phase4d_addon    = '' THEN 0 ELSE 1 END AS has_p4d
        FROM dbo.product_prompt_addons
        ORDER BY product_name
    """)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def save_customization(
    cnx: pyodbc.Connection,
    product: str,
    raw_description: Optional[str] = None,
    addons: Optional[Dict[str, str]] = None,
    generated_model: Optional[str] = None,
    generated: bool = False,
) -> None:
    """Upsert raw_description and/or addon columns for the given product."""
    product = (product or "").strip()
    if not product:
        raise ValueError("product is required")
    ensure_table(cnx)
    addons = addons or {}
    _invalidate_cache(product)

    cur = cnx.cursor()
    cur.execute("""
        IF NOT EXISTS (SELECT 1 FROM dbo.product_prompt_addons WHERE product_name = ?)
            INSERT INTO dbo.product_prompt_addons (product_name) VALUES (?);
    """, product, product)

    sets: List[str] = ["updated_at = SYSUTCDATETIME()"]
    args: List[Any] = []

    if raw_description is not None:
        sets.append("raw_description = ?")
        args.append(raw_description)

    for key in PHASE_KEYS:
        col = f"{key}_addon"
        if key in addons:
            sets.append(f"{col} = ?")
            val = addons.get(key)
            args.append(val if (val is not None and str(val).strip() != "") else None)

    if generated:
        sets.append("generated_at = SYSUTCDATETIME()")
        if generated_model is not None:
            sets.append("generated_model = ?")
            args.append(generated_model)

    args.append(product)
    cur.execute(
        f"UPDATE dbo.product_prompt_addons SET {', '.join(sets)} WHERE product_name = ?",
        *args,
    )
    cnx.commit()


def delete_customization(cnx: pyodbc.Connection, product: str) -> None:
    product = (product or "").strip()
    if not product:
        return
    ensure_table(cnx)
    cur = cnx.cursor()
    cur.execute("DELETE FROM dbo.product_prompt_addons WHERE product_name = ?", product)
    cnx.commit()
    _invalidate_cache(product)


# ---------------------------------------------------------------------------
# GPT-5.4-nano generator
# ---------------------------------------------------------------------------

_GENERATOR_SYSTEM = (
    "You are a prompt-engineering assistant for an Azure Q&A mining pipeline.\n"
    "You convert a user's plain-English, product-specific guidance into a set of "
    "short, focused system-prompt ADDITIONS — one per pipeline phase.\n"
    "\n"
    "Each phase already has a generic system prompt. Your job is to produce an "
    "'addon' paragraph that will be APPENDED to that phase's prompt ONLY when "
    "processing the specified product.\n"
    "\n"
    "PHASES (with their purpose):\n"
    "- phase1b: Builds/maintains the 4-level common-issue catalog "
    "(L1 topic, L2 scenario, L3 variant, L4 leaf). Addons should shape how "
    "categories are named/grouped/prioritized.\n"
    "- phase4a: Mines 'nuggets' (docs gaps, limitations, workarounds, tooling hints) "
    "from threads. Addons should steer what kinds of nuggets matter most.\n"
    "- phase4b: Generates the per-variant (L3) wiki page for junior support engineers. "
    "Addons should steer coverage (components, pitfalls, comparisons).\n"
    "- phase4c: Generates the per-scenario (L2) wiki page. Addons should steer "
    "preliminary checks, routing logic and platform behaviors.\n"
    "- phase4d: Generates the per-topic (L1) wiki overview page. Addons should steer "
    "how the topic is framed, which components to explain, and how they fit within "
    "the larger service.\n"
    "\n"
    "RULES:\n"
    "- Each addon MUST be concise: 2-6 sentences, <= 800 characters.\n"
    "- Do NOT restate generic phase instructions.\n"
    "- Do NOT contradict safe defaults (kebab-case keys, no product name in keys, "
    "Markdown-only output for wiki pages, etc.).\n"
    "- If a phase is not relevant to the user's description, return an empty string.\n"
    "- Output STRICT JSON ONLY, no markdown fences.\n"
    "\n"
    "OUTPUT SCHEMA:\n"
    "{\n"
    "  \"phase1b\": \"...\",\n"
    "  \"phase4a\": \"...\",\n"
    "  \"phase4b\": \"...\",\n"
    "  \"phase4c\": \"...\",\n"
    "  \"phase4d\": \"...\"\n"
    "}\n"
)


def generate_addons(product: str, raw_description: str) -> Dict[str, Any]:
    """Call GPT-5.4-nano and return {'addons': {...}, 'model': str}."""
    from aoai_helpers import (
        make_nano_client,
        get_nano_deployment,
        call_aoai_with_retry,
        get_rate_limiter,
        get_choice_text,
    )

    product = (product or "").strip()
    raw_description = (raw_description or "").strip()
    if not product:
        raise ValueError("product is required")
    if not raw_description:
        raise ValueError("raw_description is required")

    raw_description = raw_description[:8000]

    client = make_nano_client()
    deployment = get_nano_deployment()

    user_payload = {"product": product, "description": raw_description}

    resp = call_aoai_with_retry(
        client,
        model=deployment,
        messages=[
            {"role": "system", "content": _GENERATOR_SYSTEM},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
        response_format={"type": "json_object"},
        max_completion_tokens=4000,
        rate_limiter=get_rate_limiter("nano"),
        caller_tag="product_prompt_addons_generate",
    )
    raw = (get_choice_text(resp) or "").strip()
    if not raw:
        raise RuntimeError("Empty response from prompt generator")

    try:
        parsed = json.loads(raw)
    except Exception as e:
        raise RuntimeError(f"Could not parse generator response as JSON: {e}") from e
    if not isinstance(parsed, dict):
        raise RuntimeError("Generator returned non-object JSON")

    out: Dict[str, str] = {}
    for key in PHASE_KEYS:
        v = parsed.get(key)
        out[key] = v.strip() if isinstance(v, str) else ""

    return {"addons": out, "model": str(deployment)}


# ---------------------------------------------------------------------------
# Cached phase-addon reader (used from inside each phase)
# ---------------------------------------------------------------------------

_CACHE_TTL = float(os.environ.get("PRODUCT_PROMPT_ADDON_TTL", "60"))
_CACHE: Dict[tuple, tuple] = {}  # (product, phase_key) -> (expires_at, text)
_CACHE_LOCK = threading.Lock()


def _invalidate_cache(product: Optional[str] = None) -> None:
    with _CACHE_LOCK:
        if product is None:
            _CACHE.clear()
            return
        stale = [k for k in list(_CACHE.keys()) if k[0] == product]
        for k in stale:
            _CACHE.pop(k, None)


def _fetch_addon(cnx: pyodbc.Connection, product: str, phase_key: str) -> str:
    col = f"{phase_key}_addon"
    cur = cnx.cursor()
    cur.execute(
        f"SELECT {col} FROM dbo.product_prompt_addons WHERE product_name = ?",
        product,
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return ""
    return str(row[0]).strip()


def get_phase_addon(
    product: str,
    phase_key: str,
    cnx: Optional[pyodbc.Connection] = None,
) -> str:
    """Return cached addon text for (product, phase_key), or empty string."""
    product = (product or "").strip()
    phase_key = (phase_key or "").strip()
    if not product or phase_key not in PHASE_KEYS:
        return ""

    key = (product, phase_key)
    now = time.time()
    with _CACHE_LOCK:
        hit = _CACHE.get(key)
        if hit and hit[0] > now:
            return hit[1]

    own = False
    text = ""
    try:
        if cnx is None:
            cnx = _open_own_cnx()
            own = True
        try:
            ensure_table(cnx)
            text = _fetch_addon(cnx, product, phase_key)
        finally:
            if own:
                try:
                    cnx.close()
                except Exception:
                    pass
    except Exception as e:
        logging.warning(
            "product_prompt_addons: lookup failed product=%s phase=%s err=%s",
            product, phase_key, str(e)[:300],
        )
        text = ""

    with _CACHE_LOCK:
        _CACHE[key] = (now + _CACHE_TTL, text)
    return text


def append_product_addon(
    system_prompt: str,
    product: Optional[str],
    phase_key: str,
    cnx: Optional[pyodbc.Connection] = None,
) -> str:
    """Append the addon for this product+phase to the system prompt, if any."""
    addon = get_phase_addon(product or "", phase_key, cnx=cnx)
    if not addon:
        return system_prompt
    return (
        f"{system_prompt}\n\n"
        "PRODUCT-SPECIFIC GUIDANCE (ADDITIONAL)\n"
        f"Product: {product}\n"
        f"{addon}\n"
    )
