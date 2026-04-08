# -*- coding: utf-8 -*-
import os
import json
import logging
from typing import Dict, Any

import azure.functions as func
import pyodbc

from aoai_helpers import sql_connect as _aoai_sql_connect


# ---------------------------------------------------------
# DB
# ---------------------------------------------------------

def sql_connect() -> pyodbc.Connection:
    return _aoai_sql_connect(autocommit=False)


# ---------------------------------------------------------
# DDL + recompute
# ---------------------------------------------------------

def _ensure_max_usefulness_column(cnx: pyodbc.Connection) -> bool:
    """
    Adds dbo.issue_cluster.max_solution_usefulness DECIMAL(4,3) if it doesn't exist.
    Returns True if created, False if already existed.
    """
    cur = cnx.cursor()

    cur.execute(
        """
        SELECT 1
        FROM sys.columns c
        JOIN sys.objects o ON o.object_id = c.object_id
        WHERE o.type = 'U'
          AND o.name = 'issue_cluster'
          AND c.name = 'max_solution_usefulness';
        """
    )
    exists = cur.fetchone() is not None
    if exists:
        return False

    logging.warning("phase2g adding column dbo.issue_cluster.max_solution_usefulness DECIMAL(4,3)")
    cur.execute(
        """
        ALTER TABLE dbo.issue_cluster
        ADD max_solution_usefulness DECIMAL(4,3) NULL;
        """
    )
    return True


def recompute_leaf_counts_and_usefulness(cnx: pyodbc.Connection) -> None:
    """
    Recompute member_count and max_solution_usefulness on all issue_cluster levels.

    Counting strategy (per level):
      Count threads that match EITHER:
        (a) the FK column (e.g. TopicClusterID = cluster_id)  -- set by Phase 2E
        (b) the enrichment key column matches the cluster_key for the same product
            (e.g. topic_cluster_key = cluster_key AND product = product)  -- set by Phase 1A
      Threads are DISTINCT-counted so a thread matching both paths is counted once.

    max_solution_usefulness:
      MAX(solution_usefulness) from directly matched threads,
      then rolled up to parents as MAX(child.max_solution_usefulness).
    """
    cur = cnx.cursor()

    # 1) L4 leaf counts + max usefulness
    cur.execute(
        """
        ;WITH leaf_threads AS (
            SELECT te.thread_id, ic.cluster_id AS cid, te.solution_usefulness
            FROM dbo.thread_enrichment te
            JOIN dbo.issue_cluster ic
              ON ic.cluster_level = 4 AND ic.is_active = 1
             AND (
                   te.ResolutionLeafClusterID = ic.cluster_id
                OR (te.product = ic.product
                    AND te.resolution_leaf_key IS NOT NULL
                    AND LTRIM(RTRIM(te.resolution_leaf_key)) <> ''
                    AND te.resolution_leaf_key = ic.cluster_key)
             )
            WHERE te.product IS NOT NULL AND LTRIM(RTRIM(te.product)) <> ''
        ),
        leaf_stats AS (
            SELECT
                cid,
                COUNT(DISTINCT thread_id) AS thread_count,
                MAX(CAST(ISNULL(solution_usefulness, 0.0) AS DECIMAL(4,3))) AS max_usefulness
            FROM leaf_threads
            GROUP BY cid
        )
        UPDATE ic
        SET
            ic.member_count = ISNULL(ls.thread_count, 0),
            ic.max_solution_usefulness = ls.max_usefulness
        FROM dbo.issue_cluster ic
        LEFT JOIN leaf_stats ls ON ls.cid = ic.cluster_id
        WHERE ic.is_active = 1 AND ic.cluster_level = 4;
        """
    )

    # 2) L3 variant counts
    cur.execute(
        """
        ;WITH variant_threads AS (
            SELECT te.thread_id, ic.cluster_id AS cid, te.solution_usefulness
            FROM dbo.thread_enrichment te
            JOIN dbo.issue_cluster ic
              ON ic.cluster_level = 3 AND ic.is_active = 1
             AND (
                   te.VariantClusterID = ic.cluster_id
                OR (te.product = ic.product
                    AND te.variant_cluster_key IS NOT NULL
                    AND LTRIM(RTRIM(te.variant_cluster_key)) <> ''
                    AND te.variant_cluster_key = ic.cluster_key)
             )
            WHERE te.product IS NOT NULL AND LTRIM(RTRIM(te.product)) <> ''
        ),
        variant_stats AS (
            SELECT
                cid,
                COUNT(DISTINCT thread_id) AS thread_count,
                MAX(CAST(ISNULL(solution_usefulness, 0.0) AS DECIMAL(4,3))) AS direct_max
            FROM variant_threads
            GROUP BY cid
        ),
        child_max AS (
            SELECT
                l4.parent_cluster_id AS cid,
                MAX(l4.max_solution_usefulness) AS child_usefulness
            FROM dbo.issue_cluster l4
            WHERE l4.is_active = 1 AND l4.cluster_level = 4
              AND l4.max_solution_usefulness IS NOT NULL
            GROUP BY l4.parent_cluster_id
        )
        UPDATE ic
        SET
            ic.member_count = ISNULL(vs.thread_count, 0),
            ic.max_solution_usefulness = (
                SELECT MAX(v) FROM (VALUES (vs.direct_max), (cm.child_usefulness)) AS t(v)
            )
        FROM dbo.issue_cluster ic
        LEFT JOIN variant_stats vs ON vs.cid = ic.cluster_id
        LEFT JOIN child_max cm ON cm.cid = ic.cluster_id
        WHERE ic.is_active = 1 AND ic.cluster_level = 3;
        """
    )

    # 3) L2 scenario counts
    cur.execute(
        """
        ;WITH scenario_threads AS (
            SELECT te.thread_id, ic.cluster_id AS cid, te.solution_usefulness
            FROM dbo.thread_enrichment te
            JOIN dbo.issue_cluster ic
              ON ic.cluster_level = 2 AND ic.is_active = 1
             AND (
                   te.ScenarioClusterID = ic.cluster_id
                OR (te.product = ic.product
                    AND te.scenario_cluster_key IS NOT NULL
                    AND LTRIM(RTRIM(te.scenario_cluster_key)) <> ''
                    AND te.scenario_cluster_key = ic.cluster_key)
             )
            WHERE te.product IS NOT NULL AND LTRIM(RTRIM(te.product)) <> ''
        ),
        scenario_stats AS (
            SELECT
                cid,
                COUNT(DISTINCT thread_id) AS thread_count,
                MAX(CAST(ISNULL(solution_usefulness, 0.0) AS DECIMAL(4,3))) AS direct_max
            FROM scenario_threads
            GROUP BY cid
        ),
        child_max AS (
            SELECT
                l3.parent_cluster_id AS cid,
                MAX(l3.max_solution_usefulness) AS child_usefulness
            FROM dbo.issue_cluster l3
            WHERE l3.is_active = 1 AND l3.cluster_level = 3
              AND l3.max_solution_usefulness IS NOT NULL
            GROUP BY l3.parent_cluster_id
        )
        UPDATE ic
        SET
            ic.member_count = ISNULL(ss.thread_count, 0),
            ic.max_solution_usefulness = (
                SELECT MAX(v) FROM (VALUES (ss.direct_max), (cm.child_usefulness)) AS t(v)
            )
        FROM dbo.issue_cluster ic
        LEFT JOIN scenario_stats ss ON ss.cid = ic.cluster_id
        LEFT JOIN child_max cm ON cm.cid = ic.cluster_id
        WHERE ic.is_active = 1 AND ic.cluster_level = 2;
        """
    )

    # 4) L1 topic counts
    cur.execute(
        """
        ;WITH topic_threads AS (
            SELECT te.thread_id, ic.cluster_id AS cid, te.solution_usefulness
            FROM dbo.thread_enrichment te
            JOIN dbo.issue_cluster ic
              ON ic.cluster_level = 1 AND ic.is_active = 1
             AND (
                   te.TopicClusterID = ic.cluster_id
                OR (te.product = ic.product
                    AND te.topic_cluster_key IS NOT NULL
                    AND LTRIM(RTRIM(te.topic_cluster_key)) <> ''
                    AND te.topic_cluster_key = ic.cluster_key)
             )
            WHERE te.product IS NOT NULL AND LTRIM(RTRIM(te.product)) <> ''
        ),
        topic_stats AS (
            SELECT
                cid,
                COUNT(DISTINCT thread_id) AS thread_count,
                MAX(CAST(ISNULL(solution_usefulness, 0.0) AS DECIMAL(4,3))) AS direct_max
            FROM topic_threads
            GROUP BY cid
        ),
        child_max AS (
            SELECT
                l2.parent_cluster_id AS cid,
                MAX(l2.max_solution_usefulness) AS child_usefulness
            FROM dbo.issue_cluster l2
            WHERE l2.is_active = 1 AND l2.cluster_level = 2
              AND l2.max_solution_usefulness IS NOT NULL
            GROUP BY l2.parent_cluster_id
        )
        UPDATE ic
        SET
            ic.member_count = ISNULL(ts.thread_count, 0),
            ic.max_solution_usefulness = (
                SELECT MAX(v) FROM (VALUES (ts.direct_max), (cm.child_usefulness)) AS t(v)
            )
        FROM dbo.issue_cluster ic
        LEFT JOIN topic_stats ts ON ts.cid = ic.cluster_id
        LEFT JOIN child_max cm ON cm.cid = ic.cluster_id
        WHERE ic.is_active = 1 AND ic.cluster_level = 1;
        """
    )


# ---------------------------------------------------------
# Function entry point
# ---------------------------------------------------------

def run_phase2g_count_leaves(req: func.HttpRequest) -> func.HttpResponse:
    """
    Phase 2G:
      - Ensures issue_cluster.max_solution_usefulness exists (DECIMAL(4,3))
      - Recomputes:
          * L4: member_count from ResolutionLeafClusterID OR resolution_leaf_key match
          * L3: member_count from VariantClusterID OR variant_cluster_key match
          * L2: member_count from ScenarioClusterID OR scenario_cluster_key match
          * L1: member_count from TopicClusterID OR topic_cluster_key match
          * All levels: max_solution_usefulness = MAX(direct threads, child max)
    """
    try:
        with sql_connect() as cnx:
            created = _ensure_max_usefulness_column(cnx)
            recompute_leaf_counts_and_usefulness(cnx)
            cnx.commit()

        return func.HttpResponse(
            json.dumps(
                {
                    "status": "ok",
                    "column_created": bool(created),
                    "column_name": "dbo.issue_cluster.max_solution_usefulness",
                    "column_type": "DECIMAL(4,3)",
                },
                ensure_ascii=False,
            ),
            mimetype="application/json",
        )

    except Exception as e:
        logging.exception("Phase 2G Fatal Error")
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}, ensure_ascii=False),
            status_code=500,
            mimetype="application/json",
        )