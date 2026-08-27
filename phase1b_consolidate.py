"""Phase 1B-C — catalog consolidation.

Phase 1B proposes catalog nodes batch by batch. Because each batch only sees the
catalog as it existed at that moment, semantically identical topics get created
under slightly different keys (`copilot-access` / `copilot-access-blocked` /
`copilot-enablement-and-access`). Left alone that produces one near-duplicate wiki
page per variant, most with no members.

This phase merges duplicate nodes at a given level: survivors keep their key,
losers are deactivated with `merged_into_cluster_id` set, and their children are
re-parented onto the survivor (or merged into the survivor's same-key child, so
the (level, parent, key) uniqueness constraint is never violated).

Route params:
  product        required, e.g. "M365 Copilot"
  level          catalog level to consolidate (default 1)
  dryrun         "1" to report the plan without writing
  max_groups     cap the number of merge groups applied
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import azure.functions as func
import pyodbc

from aoai_helpers import (
    call_aoai_with_retry,
    estimate_tokens,
    get_gpt52_deployment,
    get_rate_limiter,
    make_gpt52_client,
)
from sql_helpers import sql_connect

_MAX_SIG = 200


def fetch_level_nodes(cnx: pyodbc.Connection, product: str, level: int) -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    cur.execute(
        """
        SELECT c.cluster_id, c.cluster_key, c.cluster_signature_text, c.member_count,
               (SELECT COUNT(*) FROM dbo.issue_cluster k
                 WHERE k.parent_cluster_id = c.cluster_id AND k.is_active = 1) AS child_count
        FROM dbo.issue_cluster c
        WHERE c.product = ? AND c.cluster_level = ? AND c.is_active = 1
        ORDER BY c.cluster_key
        """,
        product, level,
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _build_prompt(product: str, level: int, nodes: List[Dict[str, Any]]) -> Tuple[str, str]:
    level_name = {1: "Topic", 2: "Scenario", 3: "Variant", 4: "Leaf"}.get(level, f"L{level}")
    listing = "\n".join(
        f'- {n["cluster_key"]} (members={n["member_count"] or 0}, children={n["child_count"]}) :: '
        f'{(n["cluster_signature_text"] or "")[:_MAX_SIG]}'
        for n in nodes
    )
    instructions = (
        f"You are consolidating the L{level} ({level_name}) layer of a support knowledge-base "
        f"catalog for the product '{product}'.\n\n"
        "The catalog was built incrementally, so it contains MANY near-duplicate entries that "
        "describe the same underlying subsystem or support domain under different keys.\n\n"
        "TASK: group entries that belong together, and pick ONE canonical key per group.\n\n"
        "RULES:\n"
        "1. Merge only entries that genuinely describe the SAME durable subsystem/domain. "
        "Do NOT merge distinct subsystems just because they share a word.\n"
        "2. Prefer a canonical key that is short, broad, and already present in the list. "
        "Prefer the entry with the most children/members.\n"
        "3. The canonical key MUST be one of the listed keys (do not invent new keys).\n"
        "4. A group must contain at least 2 keys (the canonical plus >=1 to merge).\n"
        "5. Leave genuinely unique entries out of the output entirely.\n"
        "6. Aim for a final catalog of roughly 15-40 broad L1 topics. Be decisive: it is better "
        "to merge two related narrow topics than to leave both as separate wiki pages.\n\n"
        'Return STRICT JSON only: {"groups":[{"canonical":"key","merge":["key",...],'
        '"reason":"short why"}]}'
    )
    return instructions, f"Catalog entries for '{product}' at L{level}:\n{listing}"


def propose_merges(product: str, level: int, nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    instructions, payload = _build_prompt(product, level, nodes)
    messages = [
        {"role": "system", "content": instructions},
        {"role": "user", "content": payload},
    ]
    resp = call_aoai_with_retry(
        make_gpt52_client(),
        model=get_gpt52_deployment(),
        messages=messages,
        response_format={"type": "json_object"},
        max_completion_tokens=int(os.getenv("P1BC_MAX_OUTPUT", "16000")),
        estimated_prompt_tokens=estimate_tokens(instructions) + estimate_tokens(payload),
        rate_limiter=get_rate_limiter("gpt52"),
        caller_tag="phase1b_consolidate",
    )
    raw = resp.choices[0].message.content or ""
    try:
        data = json.loads(raw)
    except Exception:
        logging.error("[1B-C] could not parse merge proposal: %s", (raw or "")[:400])
        return []

    valid_keys = {n["cluster_key"] for n in nodes}
    groups: List[Dict[str, Any]] = []
    for g in data.get("groups") or []:
        canonical = str(g.get("canonical") or "").strip()
        merge = [str(m).strip() for m in (g.get("merge") or [])]
        merge = [m for m in merge if m in valid_keys and m != canonical]
        if canonical in valid_keys and merge:
            groups.append({"canonical": canonical, "merge": merge,
                           "reason": str(g.get("reason") or "")[:400]})
    return groups


def _reparent_or_merge_children(cur: pyodbc.Cursor, loser_id: int, winner_id: int, reason: str) -> int:
    """Move loser's children onto winner. If winner already has a child with the same key,
    merge into that child instead of re-parenting (keeps (level,parent,key) unique)."""
    cur.execute(
        """SELECT cluster_id, cluster_key, cluster_level FROM dbo.issue_cluster
           WHERE parent_cluster_id = ? AND is_active = 1""", loser_id)
    children = cur.fetchall()
    moved = 0
    for child_id, child_key, child_level in children:
        cur.execute(
            """SELECT TOP 1 cluster_id FROM dbo.issue_cluster
               WHERE parent_cluster_id = ? AND cluster_key = ? AND cluster_level = ? AND is_active = 1""",
            winner_id, child_key, child_level)
        twin = cur.fetchone()
        if twin:
            moved += _reparent_or_merge_children(cur, child_id, twin[0], reason)
            cur.execute(
                """UPDATE dbo.issue_cluster
                   SET is_active = 0, merged_into_cluster_id = ?, merge_reason = ?,
                       last_maintained_at = SYSUTCDATETIME()
                   WHERE cluster_id = ?""", twin[0], reason, child_id)
        else:
            cur.execute(
                """UPDATE dbo.issue_cluster
                   SET parent_cluster_id = ?, last_maintained_at = SYSUTCDATETIME()
                   WHERE cluster_id = ?""", winner_id, child_id)
            moved += 1
    return moved


def apply_merges(cnx: pyodbc.Connection, product: str, level: int,
                 nodes: List[Dict[str, Any]], groups: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_key = {n["cluster_key"]: n for n in nodes}
    cur = cnx.cursor()
    merged = reparented = 0

    for g in groups:
        winner = by_key.get(g["canonical"])
        if not winner:
            continue
        wid = int(winner["cluster_id"])
        for key in g["merge"]:
            loser = by_key.get(key)
            if not loser or int(loser["cluster_id"]) == wid:
                continue
            lid = int(loser["cluster_id"])
            reason = f"consolidated into '{g['canonical']}': {g['reason']}"[:400]
            reparented += _reparent_or_merge_children(cur, lid, wid, reason)
            cur.execute(
                """UPDATE dbo.issue_cluster
                   SET is_active = 0, merged_into_cluster_id = ?, merge_reason = ?,
                       last_maintained_at = SYSUTCDATETIME()
                   WHERE cluster_id = ?""", wid, reason, lid)
            merged += 1

    # Point enrichment rows at survivors so later phases never resolve a dead node.
    col = {1: "TopicClusterID", 2: "ScenarioClusterID",
           3: "VariantClusterID", 4: "ResolutionLeafClusterID"}.get(level)
    rebound = 0
    if col:
        cur.execute(
            f"""UPDATE te SET te.{col} = ic.merged_into_cluster_id
                FROM dbo.thread_enrichment te
                JOIN dbo.issue_cluster ic ON ic.cluster_id = te.{col}
                WHERE te.product = ? AND ic.is_active = 0 AND ic.merged_into_cluster_id IS NOT NULL""",
            product)
        rebound = cur.rowcount
    cnx.commit()
    return {"merged_nodes": merged, "children_reparented": reparented, "enrichment_rebound": rebound}


def run_phase1b_consolidate(req: func.HttpRequest) -> func.HttpResponse:
    try:
        product = (req.params.get("product") or "").strip()
        if not product:
            return func.HttpResponse(json.dumps({"status": "error", "error": "product is required"}),
                                     status_code=400, mimetype="application/json")
        level = int(req.params.get("level", "1"))
        dryrun = req.params.get("dryrun", "0") == "1"
        max_groups = int(req.params.get("max_groups", "0"))

        with sql_connect() as cnx:
            nodes = fetch_level_nodes(cnx, product, level)
            if len(nodes) < 2:
                return func.HttpResponse(json.dumps({"status": "ok", "note": "nothing to consolidate",
                                                     "nodes": len(nodes)}),
                                         mimetype="application/json")

            groups = propose_merges(product, level, nodes)
            if max_groups > 0:
                groups = groups[:max_groups]

            to_remove = sum(len(g["merge"]) for g in groups)
            result: Dict[str, Any] = {
                "status": "ok",
                "product": product,
                "level": level,
                "dryrun": dryrun,
                "nodes_before": len(nodes),
                "groups": len(groups),
                "nodes_to_merge": to_remove,
                "projected_nodes_after": len(nodes) - to_remove,
                "plan": [{"canonical": g["canonical"], "merge": g["merge"], "reason": g["reason"]}
                         for g in groups],
            }
            if not dryrun:
                result.update(apply_merges(cnx, product, level, nodes, groups))
                result["nodes_after"] = len(fetch_level_nodes(cnx, product, level))
            return func.HttpResponse(json.dumps(result, default=str), mimetype="application/json")

    except Exception as e:
        logging.exception("[1B-C] failed")
        return func.HttpResponse(json.dumps({"status": "error", "error": str(e)}),
                                 status_code=500, mimetype="application/json")
