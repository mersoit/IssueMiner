"""Pipeline health checks — run between phases to catch structural damage early.

Most IssueMiner failures are silent: a phase reports status=ok while leaving threads
half-assigned, nuggets pointing at merged-away clusters, or wiki nodes with no members.
These checks assert the invariants each phase is supposed to preserve.

Route params:
  product   optional, scope every check to one product
  strict    "1" -> return HTTP 500 when any ERROR-severity check fails
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

import azure.functions as func
import pyodbc

from sql_helpers import sql_connect

ERROR = "ERROR"
WARN = "WARN"
INFO = "INFO"


def _scope(product: Optional[str], alias: str) -> tuple[str, list]:
    return (f" AND {alias}.product = ?", [product]) if product else ("", [])


def _one(cur: pyodbc.Cursor, sql: str, args: list) -> int:
    cur.execute(sql, *args)
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def run_checks(cnx: pyodbc.Connection, product: Optional[str]) -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    out: List[Dict[str, Any]] = []

    def add(name: str, severity: str, count: int, detail: str, fix: str = "") -> None:
        out.append({"check": name, "severity": severity if count else INFO,
                    "count": count, "detail": detail, "fix": fix if count else ""})

    ic, ic_args = _scope(product, "c")
    te, te_args = _scope(product, "te")

    # ---- catalog structure ----
    add("catalog.dead_parent", ERROR,
        _one(cur, f"""SELECT COUNT(*) FROM dbo.issue_cluster c
                      JOIN dbo.issue_cluster p ON p.cluster_id = c.parent_cluster_id
                      WHERE c.is_active = 1 AND p.is_active = 0 {ic}""", ic_args),
        "active nodes whose parent was merged away",
        "re-run phase1b_consolidate (it re-parents) or repoint parent_cluster_id to the survivor")

    add("catalog.missing_parent", ERROR,
        _one(cur, f"""SELECT COUNT(*) FROM dbo.issue_cluster c
                      WHERE c.is_active = 1 AND c.cluster_level > 1
                        AND c.parent_cluster_id IS NULL {ic}""", ic_args),
        "non-L1 nodes with no parent (unreachable in the tree)",
        "delete or re-parent; they can never receive wiki content")

    add("catalog.duplicate_key", ERROR,
        _one(cur, f"""SELECT COUNT(*) FROM (
                        SELECT c.cluster_level, ISNULL(c.parent_cluster_id,-1) p, c.cluster_key
                        FROM dbo.issue_cluster c WHERE c.is_active = 1 {ic}
                        GROUP BY c.cluster_level, ISNULL(c.parent_cluster_id,-1), c.cluster_key
                        HAVING COUNT(*) > 1) x""", ic_args),
        "duplicate (level, parent, key) among active nodes",
        "merge duplicates; violates the intended uniqueness contract")

    add("catalog.inactive_no_target", WARN,
        _one(cur, f"""SELECT COUNT(*) FROM dbo.issue_cluster c
                      WHERE c.is_active = 0 AND c.merged_into_cluster_id IS NULL {ic}""", ic_args),
        "deactivated nodes with no merge target (history lost)")

    add("catalog.no_signature", WARN,
        _one(cur, f"""SELECT COUNT(*) FROM dbo.issue_cluster c
                      WHERE c.is_active = 1
                        AND (c.cluster_signature_text IS NULL
                             OR LTRIM(RTRIM(c.cluster_signature_text)) = '') {ic}""", ic_args),
        "active nodes with no signature text (LLM has nothing to ground on)")

    # ---- assignment integrity ----
    add("assign.points_at_dead_node", ERROR,
        _one(cur, f"""SELECT COUNT(*) FROM dbo.thread_enrichment te
                      JOIN dbo.issue_cluster c ON c.cluster_id IN
                           (te.TopicClusterID, te.ScenarioClusterID,
                            te.VariantClusterID, te.ResolutionLeafClusterID)
                      WHERE c.is_active = 0 {te}""", te_args),
        "threads bound to a merged-away cluster",
        "run phase1b_consolidate, which rebinds these to survivors")

    add("assign.completed_no_leaf", ERROR,
        _one(cur, f"""SELECT COUNT(*) FROM dbo.thread_enrichment te
                      WHERE te.AssignmentCompletedUtc IS NOT NULL
                        AND te.ResolutionLeafClusterID IS NULL {te}""", te_args),
        "threads marked assignment-complete but with no leaf",
        "these silently vanish from every downstream phase; re-run 2E with force=1 for them")

    add("assign.partial_chain", WARN,
        _one(cur, f"""SELECT COUNT(*) FROM dbo.thread_enrichment te
                      WHERE te.TopicClusterID IS NOT NULL
                        AND te.ResolutionLeafClusterID IS NULL
                        AND te.AssignmentCompletedUtc IS NULL {te}""", te_args),
        "threads stalled part-way down the topic->leaf chain")

    add("assign.stale_claim", WARN,
        _one(cur, f"""SELECT COUNT(*) FROM dbo.thread_enrichment te
                      WHERE te.AssignmentStartedUtc IS NOT NULL
                        AND te.AssignmentCompletedUtc IS NULL
                        AND te.AssignmentStartedUtc < DATEADD(hour, -2, SYSUTCDATETIME()) {te}""", te_args),
        "assignment claims held >2h (crashed worker)",
        "2E clears these via stale_claim_minutes; safe to re-run")

    # ---- eligibility funnel (explains 'nothing happened' runs) ----
    eligible = _one(cur, f"""SELECT COUNT(*) FROM dbo.thread_enrichment te
                             WHERE ISNULL(te.solution_usefulness,0) >= 0.4
                               AND (te.classification IS NULL OR te.classification NOT IN
                                    ('emergent_issue','not_usable','learn_microsoft'))
                               AND te.product <> 'Other' {te}""", te_args)
    assigned = _one(cur, f"""SELECT COUNT(*) FROM dbo.thread_enrichment te
                             WHERE te.ResolutionLeafClusterID IS NOT NULL {te}""", te_args)
    add("funnel.eligible_unassigned", WARN, max(0, eligible - assigned),
        f"eligible={eligible} assigned={assigned}; remainder still owed a leaf")

    # ---- nuggets ----
    add("nugget.orphan_scenario", ERROR,
        _one(cur, f"""SELECT COUNT(*) FROM dbo.KnowledgeNuggets n
                      WHERE NOT EXISTS (
                          SELECT 1 FROM dbo.issue_cluster c
                          WHERE c.is_active = 1 AND c.cluster_level = 2
                            AND c.cluster_key = n.ScenarioClusterKey)
                        {' AND n.Product = ?' if product else ''}""",
             [product] if product else []),
        "nuggets whose scenario key matches no active L2 node",
        "these never surface in a wiki page; re-mine after fixing the catalog")

    add("nugget.orphan_topic", WARN,
        _one(cur, f"""SELECT COUNT(*) FROM dbo.KnowledgeNuggets n
                      WHERE NOT EXISTS (
                          SELECT 1 FROM dbo.issue_cluster c
                          WHERE c.is_active = 1 AND c.cluster_level = 1
                            AND c.cluster_key = n.TopicClusterKey)
                        {' AND n.Product = ?' if product else ''}""",
             [product] if product else []),
        "nuggets whose topic key matches no active L1 node")

    # ---- wiki readiness ----
    add("wiki.node_without_members", WARN,
        _one(cur, f"""SELECT COUNT(*) FROM dbo.issue_cluster c
                      WHERE c.is_active = 1 AND c.cluster_level = 1
                        AND NOT EXISTS (SELECT 1 FROM dbo.thread_enrichment t
                                        WHERE t.TopicClusterID = c.cluster_id) {ic}""", ic_args),
        "L1 topics with no threads (would render as empty wiki pages)",
        "consolidate or skip them at publish time")

    return out


def run_pipeline_healthcheck(req: func.HttpRequest) -> func.HttpResponse:
    try:
        product = (req.params.get("product") or "").strip() or None
        strict = req.params.get("strict", "0") == "1"

        with sql_connect() as cnx:
            checks = run_checks(cnx, product)

        errors = [c for c in checks if c["severity"] == ERROR]
        warns = [c for c in checks if c["severity"] == WARN]
        body = {
            "status": "fail" if errors else ("warn" if warns else "ok"),
            "product": product or "(all)",
            "errors": len(errors),
            "warnings": len(warns),
            "checks": checks,
        }
        code = 500 if (strict and errors) else 200
        return func.HttpResponse(json.dumps(body, indent=2, default=str),
                                 status_code=code, mimetype="application/json")
    except Exception as e:
        logging.exception("[healthcheck] failed")
        return func.HttpResponse(json.dumps({"status": "error", "error": str(e)}),
                                 status_code=500, mimetype="application/json")
