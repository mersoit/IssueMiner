# -*- coding: utf-8 -*-
"""
dashboard_db.py  –  Database queries for the IssueMiner dashboard
"""
import os
import json
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import pyodbc


def _get_connection_string() -> str:
    """Read SQL_CONNECTION_STRING from env or local.settings.json."""
    cs = os.environ.get("SQL_CONNECTION_STRING")
    if cs:
        return cs
    try:
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "local.settings.json")
        with open(p, "r", encoding="utf-8-sig") as f:
            settings = json.load(f)
        cs = settings.get("Values", {}).get("SQL_CONNECTION_STRING")
        if cs:
            return cs
    except Exception:
        pass
    raise RuntimeError("SQL_CONNECTION_STRING not found in env or local.settings.json")


def sql_connect() -> pyodbc.Connection:
    return pyodbc.connect(_get_connection_string(), timeout=30)


# ─────────────────────────────────────────────────────────
# DDL: ensure products table exists
# ─────────────────────────────────────────────────────────

_PRODUCTS_DDL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.objects
    WHERE type = 'U' AND name = 'products'
)
BEGIN
    CREATE TABLE dbo.products (
        product_id      INT IDENTITY(1,1) PRIMARY KEY,
        product_name    NVARCHAR(128) NOT NULL UNIQUE,
        source          NVARCHAR(64)  NOT NULL DEFAULT 'msqa',
        daily_crawl     BIT           NOT NULL DEFAULT 0,
        emergent_detect BIT           NOT NULL DEFAULT 0,
        created_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        last_crawled_at DATETIME2     NULL,
        last_crawl_count INT          NOT NULL DEFAULT 0
    );
END
"""

_PRODUCTS_ADD_CRAWL_COUNT = """
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.products') AND name = 'last_crawl_count'
)
BEGIN
    ALTER TABLE dbo.products ADD last_crawl_count INT NOT NULL DEFAULT 0;
END
"""

_PRODUCT_URLS_DDL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.objects
    WHERE type = 'U' AND name = 'product_tag_urls'
)
BEGIN
    CREATE TABLE dbo.product_tag_urls (
        url_id      INT IDENTITY(1,1) PRIMARY KEY,
        product_id  INT           NOT NULL REFERENCES dbo.products(product_id),
        tag_url     NVARCHAR(900) NOT NULL,
        created_at  DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
"""

_KNOWLEDGE_SOURCES_DDL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.objects
    WHERE type = 'U' AND name = 'knowledge_sources'
)
BEGIN
    CREATE TABLE dbo.knowledge_sources (
        source_id       INT IDENTITY(1,1) PRIMARY KEY,
        source_type     NVARCHAR(64)  NOT NULL DEFAULT 'ms_learn',
        display_name    NVARCHAR(256) NOT NULL,
        product         NVARCHAR(128) NOT NULL,
        created_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        last_crawled_at DATETIME2     NULL,
        last_doc_count  INT           NOT NULL DEFAULT 0
    );
END
"""

_KNOWLEDGE_SOURCE_URLS_DDL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.objects
    WHERE type = 'U' AND name = 'knowledge_source_urls'
)
BEGIN
    CREATE TABLE dbo.knowledge_source_urls (
        url_id      INT IDENTITY(1,1) PRIMARY KEY,
        source_id   INT           NOT NULL REFERENCES dbo.knowledge_sources(source_id),
        doc_url     NVARCHAR(900) NOT NULL,
        created_at  DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
"""

_PRODUCT_ALIASES_DDL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.objects
    WHERE type = 'U' AND name = 'product_aliases'
)
BEGIN
    CREATE TABLE dbo.product_aliases (
        alias_id        INT IDENTITY(1,1) PRIMARY KEY,
        alias_name      NVARCHAR(128) NOT NULL,
        canonical_name  NVARCHAR(128) NOT NULL,
        created_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_product_aliases_alias UNIQUE (alias_name)
    );
END
"""

_ENRICHMENT_PRODUCTS_DDL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.objects
    WHERE type = 'U' AND name = 'enrichment_products'
)
BEGIN
    CREATE TABLE dbo.enrichment_products (
        product_id      INT IDENTITY(1,1) PRIMARY KEY,
        product_name    NVARCHAR(128) NOT NULL,
        first_seen_at   DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        last_seen_at    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        thread_count    INT           NOT NULL DEFAULT 1,
        is_seed         BIT           NOT NULL DEFAULT 0,
        CONSTRAINT UQ_enrichment_products_name UNIQUE (product_name)
    );
END
"""

_BATCH_ID_DDL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.thread_enrichment') AND name = 'batch_id'
)
BEGIN
    ALTER TABLE dbo.thread_enrichment ADD batch_id NVARCHAR(64) NULL;
END
"""

_BATCH_ID_INDEX_DDL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('dbo.thread_enrichment')
      AND name = 'IX_thread_enrichment_batch_id'
)
BEGIN
    CREATE INDEX IX_thread_enrichment_batch_id
        ON dbo.thread_enrichment (batch_id)
        WHERE batch_id IS NOT NULL;
END
"""


def ensure_tables(cnx: pyodbc.Connection) -> None:
    cur = cnx.cursor()
    cur.execute(_PRODUCTS_DDL)
    cur.execute(_PRODUCTS_ADD_CRAWL_COUNT)
    cur.execute(_PRODUCT_URLS_DDL)
    cur.execute(_KNOWLEDGE_SOURCES_DDL)
    cur.execute(_KNOWLEDGE_SOURCE_URLS_DDL)
    cur.execute(_PRODUCT_ALIASES_DDL)
    cur.execute(_ENRICHMENT_PRODUCTS_DDL)
    cnx.commit()
    # ALTER TABLE must be committed before the index DDL references the new column
    cur.execute(_BATCH_ID_DDL)
    cnx.commit()
    cur.execute(_BATCH_ID_INDEX_DDL)
    cnx.commit()


# ─────────────────────────────────────────────────────────
# Batch management
# ─────────────────────────────────────────────────────────

def create_batch(
    cnx: pyodbc.Connection,
    batch_id: str,
    product: str,
    limit: int,
    force: bool = False,
) -> int:
    """Stamp batch_id onto up to `limit` eligible thread_enrichment rows for `product`.

    Eligible = CatalogCheckedUtc IS NULL (unprocessed by 1B onward).
    If force=True, also stamps rows that already have a different batch_id
    (allows re-scoping) but never touches rows already stamped with this batch_id.
    Returns the number of rows stamped.
    """
    batch_id = (batch_id or "").strip()
    product  = (product  or "").strip()
    if not batch_id or not product:
        raise ValueError("batch_id and product are required")

    cur = cnx.cursor()
    if force:
        cur.execute("""
            UPDATE TOP (?) dbo.thread_enrichment
            SET batch_id = ?
            WHERE product = ?
              AND (batch_id IS NULL OR batch_id <> ?)
              AND product IS NOT NULL AND LTRIM(RTRIM(product)) <> ''
        """, int(limit), batch_id, product, batch_id)
    else:
        cur.execute("""
            UPDATE TOP (?) dbo.thread_enrichment
            SET batch_id = ?
            WHERE product = ?
              AND batch_id IS NULL
              AND CatalogCheckedUtc IS NULL
              AND product IS NOT NULL AND LTRIM(RTRIM(product)) <> ''
        """, int(limit), batch_id, product)
    stamped = cur.rowcount
    cnx.commit()
    return int(stamped)


def list_batches(cnx: pyodbc.Connection) -> List[Dict[str, Any]]:
    """Summary of all batch_ids currently stamped in thread_enrichment."""
    cur = cnx.cursor()
    cur.execute("""
        SELECT
            batch_id,
            COUNT(*)                                                        AS total_threads,
            COUNT(DISTINCT product)                                         AS products,
            SUM(CASE WHEN CatalogCheckedUtc  IS NULL THEN 1 ELSE 0 END)    AS pending_1b,
            SUM(CASE WHEN AssignmentCompletedUtc IS NULL THEN 1 ELSE 0 END) AS pending_2e,
            SUM(CASE WHEN NuggetsMinedUtc    IS NULL THEN 1 ELSE 0 END)    AS pending_4a,
            MIN(ingested_at)                                                AS oldest,
            MAX(ingested_at)                                                AS newest
        FROM dbo.thread_enrichment
        WHERE batch_id IS NOT NULL
        GROUP BY batch_id
        ORDER BY MAX(ingested_at) DESC
    """)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# ─────────────────────────────────────────────────────────
# Product Aliases CRUD
# ─────────────────────────────────────────────────────────

def list_product_aliases(cnx: pyodbc.Connection) -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    cur.execute("""
        SELECT alias_id, alias_name, canonical_name, created_at
        FROM dbo.product_aliases
        ORDER BY canonical_name, alias_name
    """)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def upsert_product_alias(
    cnx: pyodbc.Connection,
    alias_name: str,
    canonical_name: str,
) -> None:
    """Insert or update an alias → canonical mapping."""
    cur = cnx.cursor()
    cur.execute("""
        MERGE dbo.product_aliases AS t
        USING (SELECT ? AS alias_name) AS s
        ON t.alias_name = s.alias_name
        WHEN MATCHED THEN
            UPDATE SET canonical_name = ?
        WHEN NOT MATCHED THEN
            INSERT (alias_name, canonical_name) VALUES (?, ?);
    """, alias_name.strip(), canonical_name.strip(),
        alias_name.strip(), canonical_name.strip())
    cnx.commit()


def delete_product_alias(cnx: pyodbc.Connection, alias_id: int) -> None:
    cur = cnx.cursor()
    cur.execute("DELETE FROM dbo.product_aliases WHERE alias_id = ?", int(alias_id))
    cnx.commit()


def get_alias_map(cnx: pyodbc.Connection) -> Dict[str, str]:
    """Return {alias_name: canonical_name} for all rows in product_aliases."""
    cur = cnx.cursor()
    cur.execute("SELECT alias_name, canonical_name FROM dbo.product_aliases")
    return {row[0].strip(): row[1].strip() for row in cur.fetchall()}


def seed_default_aliases(cnx: pyodbc.Connection) -> int:
    """Insert the built-in alias map if those rows don't already exist. Returns count inserted."""
    defaults: List[Tuple[str, str]] = [
        ("API Management",                  "APIM"),
        ("Api Management",                  "APIM"),
        ("Azure API Management",            "APIM"),
        ("Azure APIM",                      "APIM"),
        ("api-management",                  "APIM"),
        ("Azure DevOps",                    "DevOps"),
        ("Azure OpenAI",                    "OpenAI"),
        ("Azure Ai Search",                 "Ai Search"),
        ("Azure AI Search",                 "Ai Search"),
        ("Entra",                           "Entra External ID"),
        ("System Center Orchestrator",      "System Center"),
        ("SCOM",                            "System Center"),
        ("System Center Operations Manager","System Center"),
        ("SCSM",                            "System Center"),
        ("System Center Service Manager",   "System Center"),
        ("Vnet",                            "Virtual Network"),
        ("VNet",                            "Virtual Network"),
        ("vnet",                            "Virtual Network"),
        ("Azure Virtual Network",           "Virtual Network"),
        ("Azure VNet",                      "Virtual Network"),
    ]
    cur = cnx.cursor()
    count = 0
    for alias, canonical in defaults:
        cur.execute("""
            IF NOT EXISTS (SELECT 1 FROM dbo.product_aliases WHERE alias_name = ?)
                INSERT INTO dbo.product_aliases (alias_name, canonical_name) VALUES (?, ?)
        """, alias, alias, canonical)
        count += cur.rowcount
    cnx.commit()
    return count


def backfill_product_aliases(cnx: pyodbc.Connection) -> int:
    """
    Rewrites thread_enrichment.product for rows whose current product value
    matches a known alias. Returns the number of rows updated.
    """
    alias_map = get_alias_map(cnx)
    if not alias_map:
        return 0
    cur = cnx.cursor()
    total = 0
    for alias, canonical in alias_map.items():
        cur.execute("""
            UPDATE dbo.thread_enrichment
            SET product = ?
            WHERE product = ?
        """, canonical, alias)
        total += cur.rowcount
    cnx.commit()
    return total


# ─────────────────────────────────────────────────────────
# Enrichment Products  (pipeline-owned canonical product registry)
# ─────────────────────────────────────────────────────────

def list_enrichment_products(cnx: pyodbc.Connection) -> List[Dict[str, Any]]:
    """All rows from dbo.enrichment_products, ordered by thread_count desc."""
    cur = cnx.cursor()
    cur.execute("""
        SELECT product_id, product_name, first_seen_at, last_seen_at,
               thread_count, is_seed
        FROM dbo.enrichment_products
        ORDER BY thread_count DESC, product_name
    """)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_enrichment_product_names(cnx: pyodbc.Connection) -> List[str]:
    """Just the product_name list, used by phase1a and UI dropdowns."""
    cur = cnx.cursor()
    cur.execute("""
        SELECT product_name
        FROM dbo.enrichment_products
        ORDER BY product_name
    """)
    return [row[0] for row in cur.fetchall()]


def register_enrichment_product(cnx: pyodbc.Connection, product_name: str, is_seed: bool = False) -> None:
    """Upsert a product name into dbo.enrichment_products.

    On insert: sets first_seen_at, last_seen_at, thread_count=1.
    On update: bumps last_seen_at and thread_count (unless is_seed=True, which
    only inserts and never increments the counter).
    """
    name = (product_name or "").strip()
    if not name or name == "Other":
        return
    cur = cnx.cursor()
    if is_seed:
        cur.execute("""
            IF NOT EXISTS (SELECT 1 FROM dbo.enrichment_products WHERE product_name = ?)
                INSERT INTO dbo.enrichment_products (product_name, is_seed, thread_count)
                VALUES (?, 1, 0)
        """, name, name)
    else:
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


def seed_enrichment_products(cnx: pyodbc.Connection, names: List[str]) -> int:
    """Bulk-insert seed product names (is_seed=1, thread_count=0). Returns count inserted."""
    count = 0
    for name in names:
        name = (name or "").strip()
        if not name:
            continue
        cur = cnx.cursor()
        cur.execute("""
            IF NOT EXISTS (SELECT 1 FROM dbo.enrichment_products WHERE product_name = ?)
                INSERT INTO dbo.enrichment_products (product_name, is_seed, thread_count)
                VALUES (?, 1, 0)
        """, name, name)
        count += cur.rowcount
    cnx.commit()
    return count


def delete_enrichment_product(cnx: pyodbc.Connection, product_id: int) -> None:
    cur = cnx.cursor()
    cur.execute("DELETE FROM dbo.enrichment_products WHERE product_id = ?", int(product_id))
    cnx.commit()


def rename_enrichment_product(
    cnx: pyodbc.Connection,
    old_name: str,
    new_name: str,
) -> int:
    """Rename a product in dbo.enrichment_products and rewrite all matching
    thread_enrichment rows. Also registers old_name as an alias so future
    LLM output normalises to the new name automatically.
    Returns the number of thread_enrichment rows updated.
    """
    old_name = (old_name or "").strip()
    new_name = (new_name or "").strip()
    if not old_name or not new_name or old_name == new_name:
        raise ValueError("old_name and new_name must be different non-empty strings")

    cur = cnx.cursor()

    # 1. Rename in enrichment_products
    cur.execute("""
        UPDATE dbo.enrichment_products
        SET product_name = ?
        WHERE product_name = ?
    """, new_name, old_name)

    # 2. Rewrite thread_enrichment rows
    cur.execute("""
        UPDATE dbo.thread_enrichment
        SET product = ?
        WHERE product = ?
    """, new_name, old_name)
    rows_updated = cur.rowcount

    # 3. Register old_name as alias so future LLM output is auto-corrected
    cur.execute("""
        MERGE dbo.product_aliases AS t
        USING (SELECT ? AS alias_name) AS s
        ON t.alias_name = s.alias_name
        WHEN MATCHED THEN
            UPDATE SET canonical_name = ?
        WHEN NOT MATCHED THEN
            INSERT (alias_name, canonical_name) VALUES (?, ?);
    """, old_name, new_name, old_name, new_name)

    cnx.commit()
    return rows_updated


def merge_enrichment_products(
    cnx: pyodbc.Connection,
    primary_name: str,
    absorbed_name: str,
) -> int:
    """Merge absorbed_name into primary_name.

    - Rewrites thread_enrichment.product for all rows that have absorbed_name.
    - Adds absorbed_name as an alias pointing to primary_name in product_aliases
      so future LLM output is immediately normalised.
    - Adds absorbed_name's thread_count into primary_name's counter.
    - Deletes the absorbed_name row from enrichment_products.
    Returns the number of thread_enrichment rows updated.
    """
    primary_name = (primary_name or "").strip()
    absorbed_name = (absorbed_name or "").strip()
    if not primary_name or not absorbed_name or primary_name == absorbed_name:
        raise ValueError("primary and absorbed must be different non-empty names")

    cur = cnx.cursor()

    # 1. Rewrite thread_enrichment rows
    cur.execute("""
        UPDATE dbo.thread_enrichment
        SET product = ?
        WHERE product = ?
    """, primary_name, absorbed_name)
    rows_updated = cur.rowcount

    # 2. Merge thread_count into primary, update last_seen_at
    cur.execute("""
        UPDATE p
        SET p.thread_count  = p.thread_count + ISNULL(a.thread_count, 0),
            p.last_seen_at  = CASE WHEN a.last_seen_at > p.last_seen_at
                                   THEN a.last_seen_at ELSE p.last_seen_at END
        FROM dbo.enrichment_products p
        LEFT JOIN dbo.enrichment_products a ON a.product_name = ?
        WHERE p.product_name = ?
    """, absorbed_name, primary_name)

    # 3. Register absorbed_name as an alias so it is normalised on future runs
    cur.execute("""
        MERGE dbo.product_aliases AS t
        USING (SELECT ? AS alias_name) AS s
        ON t.alias_name = s.alias_name
        WHEN MATCHED THEN
            UPDATE SET canonical_name = ?
        WHEN NOT MATCHED THEN
            INSERT (alias_name, canonical_name) VALUES (?, ?);
    """, absorbed_name, primary_name, absorbed_name, primary_name)

    # 4. Remove the absorbed product
    cur.execute("""
        DELETE FROM dbo.enrichment_products WHERE product_name = ?
    """, absorbed_name)

    cnx.commit()
    return rows_updated


# ─────────────────────────────────────────────────────────
# Batch force-reset (reprocess a specific batch_id only)
# ─────────────────────────────────────────────────────────

def force_reset_batch(cnx: pyodbc.Connection, batch_id: str) -> Dict[str, int]:
    """
    Reset all per-thread processing timestamps for a specific batch_id so the
    pipeline phases will reprocess only those threads.

    Scopes STRICTLY to batch_id — no other threads are touched.

    Resets:
      - 1A: EnrichedUtc (re-enrich)
      - 1B: CatalogCheckedUtc (re-catalog)
      - 2E: AssignmentStartedUtc, AssignmentCompletedUtc (re-assign leaf)
      - 4A: NuggetsMinedUtc (re-mine nuggets) + deletes existing nugget rows

    Does NOT touch Phase 3 / 4B / 4C / 4D — those are cluster-level, not
    batch-scoped, and resetting them could corrupt data for other products.
    """
    if not batch_id or not batch_id.strip():
        raise ValueError("batch_id is required for force reset")
    batch_id = batch_id.strip()

    cur = cnx.cursor()

    # Delete nuggets first (subquery references thread_enrichment which we delete next)
    cur.execute("""
        DELETE FROM dbo.KnowledgeNuggets
        WHERE ThreadID IN (
            SELECT thread_id FROM dbo.thread_enrichment WHERE batch_id = ?
        )
    """, batch_id)
    deleted_nuggets = int(cur.rowcount or 0)

    # 1A — re-enrich: delete the enrichment row entirely so 1A's MERGE re-inserts it fresh.
    # This also implicitly resets 1B, 2E and 4A since all their timestamps live on the same row.
    cur.execute("""
        DELETE FROM dbo.thread_enrichment
        WHERE batch_id = ?
    """, batch_id)
    reset_1a = int(cur.rowcount or 0)
    reset_1b      = reset_1a  # same row
    reset_2e      = reset_1a
    reset_4a_flag = reset_1a

    cnx.commit()

    return {
        "batch_id":        batch_id,
        "reset_1a":        reset_1a,
        "reset_1b":        reset_1b,
        "reset_2e":        reset_2e,
        "reset_4a_flag":   reset_4a_flag,
        "deleted_nuggets": deleted_nuggets,
    }




def force_reset_2e_for_product(cnx: pyodbc.Connection, product: str) -> int:
    """
    Reset 2E assignment state for all catalog-checked threads of a given product
    so 2E will re-attempt leaf assignment on the next run.
    Scoped strictly to this product — no other products touched.
    Returns number of rows reset.
    """
    product = (product or "").strip()
    if not product:
        raise ValueError("product is required")
    cur = cnx.cursor()
    cur.execute("""
        UPDATE dbo.thread_enrichment
        SET AssignmentStartedUtc    = NULL,
            AssignmentCompletedUtc  = NULL,
            TopicClusterID          = NULL,
            ScenarioClusterID       = NULL,
            VariantClusterID        = NULL,
            ResolutionLeafClusterID = NULL
        WHERE product = ?
          AND CatalogCheckedUtc IS NOT NULL
    """, product)
    n = int(cur.rowcount or 0)
    cnx.commit()
    return n


def force_reset_wiki_for_batch(cnx, batch_id: str,
    """
    Clear wiki/playbook content on catalog clusters touched EXCLUSIVELY by
    this batch+product. If any thread from another batch maps to the same
    cluster, that cluster is left untouched.

    Clears:
      Phase 3  - CommonIssueSolutions rows for L4 leaves
      Phase 4B - WikiPath/WikiContent on L3 variants
      Phase 4C - ScenarioWikiPath on L2 scenarios
      Phase 4D - TopicWikiPath on L1 topics
    """
    if not batch_id or not batch_id.strip():
        raise ValueError("batch_id is required")
    if not product or not product.strip():
        raise ValueError("product is required")
    batch_id = batch_id.strip()
    product  = product.strip()

    cur = cnx.cursor()

    # L4 leaves — Phase 3 playbooks
    cur.execute("""
        DELETE cis
        FROM dbo.CommonIssueSolutions cis
        WHERE cis.ClusterID IN (
            SELECT te.ResolutionLeafClusterID
            FROM dbo.thread_enrichment te
            WHERE te.batch_id = ?
              AND te.product  = ?
              AND te.ResolutionLeafClusterID IS NOT NULL
            GROUP BY te.ResolutionLeafClusterID
            HAVING COUNT(*) = (
                SELECT COUNT(*) FROM dbo.thread_enrichment te2
                WHERE te2.ResolutionLeafClusterID = te.ResolutionLeafClusterID
                  AND te2.product = ?
            )
        )
    """, batch_id, product, product)
    deleted_playbooks = int(cur.rowcount or 0)

    # L3 variants — Phase 4B wiki
    cur.execute("""
        UPDATE ic
        SET ic.WikiPath             = NULL,
            ic.WikiPushedUtc        = NULL,
            ic.WikiContentMarkdown  = NULL,
            ic.WikiModel            = NULL,
            ic.WikiContentHash      = NULL,
            ic.VariantWikiPath      = NULL,
            ic.VariantWikiPushedUtc = NULL
        FROM dbo.issue_cluster ic
        WHERE ic.cluster_level = 3 AND ic.product = ?
          AND ic.cluster_id IN (
              SELECT te.VariantClusterID
              FROM dbo.thread_enrichment te
              WHERE te.batch_id = ? AND te.product = ?
                AND te.VariantClusterID IS NOT NULL
              GROUP BY te.VariantClusterID
              HAVING COUNT(*) = (
                  SELECT COUNT(*) FROM dbo.thread_enrichment te2
                  WHERE te2.VariantClusterID = te.VariantClusterID
                    AND te2.product = ?
              )
          )
    """, product, batch_id, product, product)
    reset_4b = int(cur.rowcount or 0)

    # L2 scenarios — Phase 4C wiki
    cur.execute("""
        UPDATE ic
        SET ic.ScenarioWikiPath      = NULL,
            ic.ScenarioWikiPushedUtc = NULL
        FROM dbo.issue_cluster ic
        WHERE ic.cluster_level = 2 AND ic.product = ?
          AND ic.cluster_id IN (
              SELECT te.ScenarioClusterID
              FROM dbo.thread_enrichment te
              WHERE te.batch_id = ? AND te.product = ?
                AND te.ScenarioClusterID IS NOT NULL
              GROUP BY te.ScenarioClusterID
              HAVING COUNT(*) = (
                  SELECT COUNT(*) FROM dbo.thread_enrichment te2
                  WHERE te2.ScenarioClusterID = te.ScenarioClusterID
                    AND te2.product = ?
              )
          )
    """, product, batch_id, product, product)
    reset_4c = int(cur.rowcount or 0)

    # L1 topics — Phase 4D wiki
    cur.execute("""
        UPDATE ic
        SET ic.TopicWikiPath      = NULL,
            ic.TopicWikiPushedUtc = NULL
        FROM dbo.issue_cluster ic
        WHERE ic.cluster_level = 1 AND ic.product = ?
          AND ic.cluster_id IN (
              SELECT te.TopicClusterID
              FROM dbo.thread_enrichment te
              WHERE te.batch_id = ? AND te.product = ?
                AND te.TopicClusterID IS NOT NULL
              GROUP BY te.TopicClusterID
              HAVING COUNT(*) = (
                  SELECT COUNT(*) FROM dbo.thread_enrichment te2
                  WHERE te2.TopicClusterID = te.TopicClusterID
                    AND te2.product = ?
              )
          )
    """, product, batch_id, product, product)
    reset_4d = int(cur.rowcount or 0)

    cnx.commit()

    return {
        "batch_id":           batch_id,
        "product":            product,
        "deleted_playbooks":  deleted_playbooks,
        "reset_4b_variants":  reset_4b,
        "reset_4c_scenarios": reset_4c,
        "reset_4d_topics":    reset_4d,
    }

def list_batch_ids(cnx: pyodbc.Connection, limit: int = 50) -> List[str]:
    """Return distinct batch_ids that have threads, newest first."""
    cur = cnx.cursor()
    cur.execute("""
        SELECT TOP (?) batch_id
        FROM dbo.thread_enrichment
        WHERE batch_id IS NOT NULL AND LTRIM(RTRIM(batch_id)) <> ''
        GROUP BY batch_id
        ORDER BY MAX(ingested_at) DESC
    """, int(limit))
    return [str(row[0]) for row in cur.fetchall()]


# ─────────────────────────────────────────────────────────
# Products CRUD  (dashboard crawl targets — human-managed)
# ─────────────────────────────────────────────────────────

def list_products(cnx: pyodbc.Connection) -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    cur.execute("""
        SELECT product_id, product_name, source, daily_crawl,
               emergent_detect, created_at, last_crawled_at, last_crawl_count
        FROM dbo.products
        ORDER BY product_name
    """)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_product_urls(cnx: pyodbc.Connection, product_id: int) -> List[str]:
    cur = cnx.cursor()
    cur.execute(
        "SELECT tag_url FROM dbo.product_tag_urls WHERE product_id = ? ORDER BY url_id",
        int(product_id),
    )
    return [row[0] for row in cur.fetchall()]


def insert_product(
    cnx: pyodbc.Connection,
    name: str,
    source: str,
    tag_urls: List[str],
    daily_crawl: bool,
    emergent_detect: bool,
) -> int:
    cur = cnx.cursor()
    cur.execute(
        """
        INSERT INTO dbo.products (product_name, source, daily_crawl, emergent_detect)
        OUTPUT INSERTED.product_id
        VALUES (?, ?, ?, ?)
        """,
        name.strip(), source.strip(), 1 if daily_crawl else 0, 1 if emergent_detect else 0,
    )
    pid = cur.fetchone()[0]
    for url in tag_urls:
        url = (url or "").strip()
        if url:
            cur.execute(
                "INSERT INTO dbo.product_tag_urls (product_id, tag_url) VALUES (?, ?)",
                int(pid), url,
            )
    cnx.commit()
    return int(pid)


def update_product_crawl_time(
    cnx: pyodbc.Connection, product_id: int, crawl_count: int = 0,
) -> None:
    cur = cnx.cursor()
    cur.execute(
        "UPDATE dbo.products SET last_crawled_at = SYSUTCDATETIME(), last_crawl_count = ? WHERE product_id = ?",
        int(crawl_count), int(product_id),
    )
    cnx.commit()


def delete_product(cnx: pyodbc.Connection, product_id: int) -> None:
    cur = cnx.cursor()
    cur.execute("DELETE FROM dbo.product_tag_urls WHERE product_id = ?", int(product_id))
    cur.execute("DELETE FROM dbo.products WHERE product_id = ?", int(product_id))
    cnx.commit()


# ─────────────────────────────────────────────────────────
# Dashboard stats queries
# ─────────────────────────────────────────────────────────

def get_product_thread_counts(cnx: pyodbc.Connection) -> List[Dict[str, Any]]:
    """Thread counts per product from thread_enrichment."""
    cur = cnx.cursor()
    cur.execute("""
        SELECT
            ISNULL(product, 'Unknown') AS product,
            COUNT(*)                   AS thread_count,
            SUM(CASE WHEN is_emergent = 1 THEN 1 ELSE 0 END) AS emergent_count,
            MAX(source_created_at)     AS newest_thread,
            AVG(CAST(ISNULL(solution_usefulness, 0) AS FLOAT)) AS avg_usefulness
        FROM dbo.thread_enrichment
        GROUP BY product
        ORDER BY COUNT(*) DESC
    """)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_catalog_stats(cnx: pyodbc.Connection) -> List[Dict[str, Any]]:
    """Cluster counts per product per level from issue_cluster."""
    cur = cnx.cursor()
    cur.execute("""
        SELECT
            product,
            cluster_level,
            COUNT(*)             AS node_count,
            SUM(member_count)    AS total_members,
            MAX(ISNULL(max_solution_usefulness, 0)) AS best_usefulness
        FROM dbo.issue_cluster
        WHERE is_active = 1
        GROUP BY product, cluster_level
        ORDER BY product, cluster_level
    """)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_emergent_summary(cnx: pyodbc.Connection) -> List[Dict[str, Any]]:
    """Recent emergent issues."""
    cur = cnx.cursor()
    cur.execute("""
        SELECT TOP 20
            e.ClusterID,
            e.PrimaryClusterKey,
            e.Title,
            e.Confidence,
            e.FirstSeenUtc,
            e.LastUpdatedUtc
        FROM dbo.EmergentIssues e
        ORDER BY e.LastUpdatedUtc DESC
    """)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_recent_nuggets(cnx: pyodbc.Connection, limit: int = 30) -> List[Dict[str, Any]]:
    """Recent nuggets from phase4a."""
    cur = cnx.cursor()
    cur.execute(f"""
        SELECT TOP ({int(limit)})
            te.thread_id,
            te.product,
            te.topic_cluster_key,
            te.scenario_cluster_key,
            te.variant_cluster_key,
            te.NuggetsMinedUtc,
            te.signature_text
        FROM dbo.thread_enrichment te
        WHERE te.NuggetsMinedUtc IS NOT NULL
        ORDER BY te.NuggetsMinedUtc DESC
    """)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_pipeline_states(cnx: pyodbc.Connection) -> List[Dict[str, Any]]:
    """Pipeline watermarks."""
    cur = cnx.cursor()
    cur.execute("""
        SELECT PipelineName, LastRunUtc, Status, LastDateCreatedUtc
        FROM dbo.PipelineState
        ORDER BY LastRunUtc DESC
    """)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_wiki_push_summary(cnx: pyodbc.Connection) -> Dict[str, Any]:
    """Counts of CommonIssueSolutions with/without wiki."""
    cur = cnx.cursor()
    cur.execute("""
        SELECT
            COUNT(*)                                              AS total_solutions,
            SUM(CASE WHEN AdoWikiPath IS NOT NULL THEN 1 ELSE 0 END) AS wiki_pushed,
            SUM(CASE WHEN AdoWikiPath IS NULL THEN 1 ELSE 0 END)     AS wiki_pending
        FROM dbo.CommonIssueSolutions
    """)
    row = cur.fetchone()
    return {"total_solutions": row[0] or 0, "wiki_pushed": row[1] or 0, "wiki_pending": row[2] or 0}


def get_overall_counts(cnx: pyodbc.Connection) -> Dict[str, Any]:
    """Single-row summary."""
    cur = cnx.cursor()
    counts: Dict[str, Any] = {}

    cur.execute("SELECT COUNT(*) FROM dbo.ThreadsClean")
    counts["threads_clean"] = cur.fetchone()[0] or 0

    cur.execute("SELECT COUNT(*) FROM dbo.thread_enrichment")
    counts["threads_enriched"] = cur.fetchone()[0] or 0

    cur.execute("SELECT COUNT(*) FROM dbo.issue_cluster WHERE is_active = 1")
    counts["active_clusters"] = cur.fetchone()[0] or 0

    cur.execute("SELECT COUNT(*) FROM dbo.thread_enrichment WHERE AssignmentCompletedUtc IS NOT NULL")
    counts["threads_assigned"] = cur.fetchone()[0] or 0

    cur.execute("SELECT COUNT(*) FROM dbo.thread_enrichment WHERE NuggetsMinedUtc IS NOT NULL")
    counts["threads_nuggets_mined"] = cur.fetchone()[0] or 0

    cur.execute("SELECT COUNT(*) FROM dbo.thread_enrichment WHERE is_emergent = 1")
    counts["emergent_threads"] = cur.fetchone()[0] or 0

    return counts


# ─────────────────────────────────────────────────────────
# Knowledge Sources CRUD
# ─────────────────────────────────────────────────────────

def get_distinct_enriched_products(cnx: pyodbc.Connection) -> List[str]:
    """Distinct product names that exist in thread_enrichment, sorted."""
    cur = cnx.cursor()
    cur.execute("""
        SELECT DISTINCT LTRIM(RTRIM(product))
        FROM dbo.thread_enrichment
        WHERE product IS NOT NULL AND LTRIM(RTRIM(product)) <> ''
        ORDER BY 1
    """)
    return [row[0] for row in cur.fetchall()]


def list_product_names(cnx: pyodbc.Connection) -> List[str]:
    """Canonical product names from dbo.enrichment_products, sorted. Use this for UI dropdowns."""
    return fetch_enrichment_product_names(cnx)


def list_knowledge_sources(cnx: pyodbc.Connection) -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    cur.execute("""
        SELECT source_id, source_type, display_name, product,
               created_at, last_crawled_at, last_doc_count
        FROM dbo.knowledge_sources
        ORDER BY product, display_name
    """)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_knowledge_source_urls(cnx: pyodbc.Connection, source_id: int) -> List[str]:
    cur = cnx.cursor()
    cur.execute(
        "SELECT doc_url FROM dbo.knowledge_source_urls WHERE source_id = ? ORDER BY url_id",
        int(source_id),
    )
    return [row[0] for row in cur.fetchall()]


def insert_knowledge_source(
    cnx: pyodbc.Connection,
    source_type: str,
    display_name: str,
    product: str,
    doc_urls: List[str],
) -> int:
    cur = cnx.cursor()
    cur.execute(
        """
        INSERT INTO dbo.knowledge_sources (source_type, display_name, product)
        OUTPUT INSERTED.source_id
        VALUES (?, ?, ?)
        """,
        source_type.strip(), display_name.strip(), product.strip(),
    )
    sid = cur.fetchone()[0]
    for url in doc_urls:
        url = (url or "").strip()
        if url:
            cur.execute(
                "INSERT INTO dbo.knowledge_source_urls (source_id, doc_url) VALUES (?, ?)",
                int(sid), url,
            )
    cnx.commit()
    return int(sid)


def update_knowledge_source_crawl(
    cnx: pyodbc.Connection, source_id: int, doc_count: int
) -> None:
    cur = cnx.cursor()
    cur.execute(
        """
        UPDATE dbo.knowledge_sources
        SET last_crawled_at = SYSUTCDATETIME(), last_doc_count = ?
        WHERE source_id = ?
        """,
        int(doc_count), int(source_id),
    )
    cnx.commit()


def delete_knowledge_source(cnx: pyodbc.Connection, source_id: int) -> None:
    cur = cnx.cursor()
    cur.execute("DELETE FROM dbo.knowledge_source_urls WHERE source_id = ?", int(source_id))
    cur.execute("DELETE FROM dbo.knowledge_sources WHERE source_id = ?", int(source_id))
    cnx.commit()
