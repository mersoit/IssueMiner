# -*- coding: utf-8 -*-
"""
Emergent Phase 3: Assign emergent-flagged threads to taxonomy branches.

Reads from:
  - dbo.thread_enrichment  (emergent-flagged threads)
  - dbo.emergent_taxonomy  (Product + TopicKey from Phase 1C)

Writes:
  - dbo.thread_enrichment.EmergentTaxonomyID
  - dbo.thread_enrichment.EmergentAssignCompletedUtc

PARALLEL — taxonomy is stable by this point.
Uses Nano for lightweight matching: thread signature -> best taxonomy topic.
"""

import os
import json
import logging
import threading
import pyodbc
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import azure.functions as func
from openai import AzureOpenAI

from aoai_helpers import (
    make_nano_client,
    get_nano_deployment,
    call_aoai_with_retry,
    estimate_tokens,
    sql_connect as _aoai_sql_connect,
)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

_MAX_CONCURRENCY = int(os.getenv("EMRG_ASSIGN_MAX_CONCURRENCY", "4"))
_aoai_sema = threading.BoundedSemaphore(_MAX_CONCURRENCY)
_DB_BATCH = int(os.getenv("EMRG_ASSIGN_DB_BATCH", "500"))


def sql_connect() -> pyodbc.Connection:
    return _aoai_sql_connect(autocommit=False)


# ---------------------------------------------------------
# SQL: Taxonomy + Thread claiming
# ---------------------------------------------------------

def fetch_all_taxonomy(cnx: pyodbc.Connection) -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    cur.execute(
        """
        SELECT TaxonomyID, Product, TopicKey, TopicSignature
        FROM dbo.emergent_taxonomy
        WHERE IsActive = 1
        ORDER BY Product, TopicKey
        """
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def claim_threads_for_emergent_assign(
    cnx: pyodbc.Connection,
    batch_size: int,
    product: str,
    include_completed: bool,
) -> List[Dict[str, Any]]:
    cur = cnx.cursor()
    cur.execute(
        """
        ;WITH cte AS (
            SELECT TOP (?)
                thread_id
            FROM dbo.thread_enrichment WITH (ROWLOCK, READPAST, UPDLOCK)
            WHERE product = ?
              AND (classification = 'emergent_issue' OR is_emergent = 1)
              AND ISNULL(emergent_signal, 0.0) >= 0.5
              AND signature_text IS NOT NULL AND LTRIM(RTRIM(signature_text)) <> ''
              AND (? = 1 OR EmergentAssignCompletedUtc IS NULL)
              AND EmergentAssignStartedUtc IS NULL
            ORDER BY ingested_at DESC, thread_id DESC
        )
        UPDATE te
        SET EmergentAssignStartedUtc = SYSUTCDATETIME()
        OUTPUT INSERTED.thread_id,
               INSERTED.product,
               INSERTED.signature_text,
               INSERTED.resolution_signature_text,
               INSERTED.emergent_signal,
               INSERTED.source_created_at,
               INSERTED.topic_cluster_key
        FROM dbo.thread_enrichment te
        JOIN cte ON cte.thread_id = te.thread_id;
        """,
        int(batch_size),
        str(product),
        1 if include_completed else 0,
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def bulk_write_taxonomy_assignments(
    cnx: pyodbc.Connection,
    assignments: List[Tuple[Optional[int], str]],
) -> None:
    if not assignments:
        return
    cur = cnx.cursor()
    cur.fast_executemany = True
    for i0 in range(0, len(assignments), _DB_BATCH):
        chunk = assignments[i0:i0 + _DB_BATCH]
        cur.executemany(
            """
            UPDATE dbo.thread_enrichment
            SET EmergentTaxonomyID = ?,
                EmergentAssignCompletedUtc = SYSUTCDATETIME()
            WHERE thread_id = ?;
            """,
            chunk,
        )


def release_assign_claims(cnx: pyodbc.Connection, thread_ids: List[str]) -> None:
    if not thread_ids:
        return
    cur = cnx.cursor()
    placeholders = ",".join("?" for _ in thread_ids)
    cur.execute(
        f"""
        UPDATE dbo.thread_enrichment
        SET EmergentAssignStartedUtc = NULL
        WHERE thread_id IN ({placeholders})
          AND EmergentAssignCompletedUtc IS NULL
        """,
        *[str(t) for t in thread_ids],
    )


# ---------------------------------------------------------
# Nano: Assign thread to taxonomy topic
# ---------------------------------------------------------

def _call_nano_assign(
    client: AzureOpenAI,
    deployment: str,
    product: str,
    threads: List[Dict[str, Any]],
    taxonomy: List[Dict[str, Any]],
) -> Dict[str, Any]:
    system_prompt = (
        "You assign emergent-flagged support threads to taxonomy topics.\n\n"
        "You are given:\n"
        "  - A list of threads with their signatures.\n"
        "  - A list of taxonomy topics for this product.\n\n"
        "Task: For each thread, pick the single best matching topic_key.\n"
        "If NO topic is a reasonable match, use \"__unassigned\".\n\n"
        "Output JSON only:\n"
        "{\n"
        "  \"assignments\": [\n"
        "    {\"thread_id\": \"...\", \"topic_key\": \"...\", \"confidence\": 0.0-1.0}\n"
        "  ]\n"
        "}\n\n"
        "Rules:\n"
        "  - Exactly one assignment per input thread_id.\n"
        "  - topic_key must be one of the provided taxonomy topic keys, or \"__unassigned\".\n"
        "  - Prefer the most specific match over broad ones.\n"
        "Return JSON only.\n"
    )

    slim_threads = [
        {
            "thread_id": str(t.get("thread_id") or ""),
            "signature_text": (t.get("signature_text") or "")[:600],
            "resolution_signature_text": (t.get("resolution_signature_text") or "")[:400],
            "topic_hint": t.get("topic_cluster_key") or "",
        }
        for t in threads
    ]

    slim_taxonomy = [
        {
            "topic_key": t.get("TopicKey") or "",
            "signature": (t.get("TopicSignature") or "")[:200],
        }
        for t in taxonomy
    ]

    user_content = json.dumps({
        "product": product,
        "threads": slim_threads,
        "taxonomy_topics": slim_taxonomy,
    }, ensure_ascii=False)

    with _aoai_sema:
        resp = call_aoai_with_retry(
            client,
            model=deployment,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            response_format={"type": "json_object"},
            estimated_prompt_tokens=estimate_tokens(system_prompt) + estimate_tokens(user_content),
            caller_tag="emergent_assign",
        )
    return json.loads(resp.choices[0].message.content or "{}")


# ---------------------------------------------------------
# Worker
# ---------------------------------------------------------

def _process_assign_batch(
    product: str,
    threads: List[Dict[str, Any]],
    taxonomy: List[Dict[str, Any]],
    topic_key_to_id: Dict[str, int],
) -> List[Dict[str, Any]]:
    client = make_nano_client()
    deployment = get_nano_deployment()

    out = _call_nano_assign(client, deployment, product, threads, taxonomy)
    raw_assignments = out.get("assignments") or []

    results: List[Dict[str, Any]] = []
    db_pairs: List[Tuple[Optional[int], str]] = []
    expected = {str(t.get("thread_id") or "") for t in threads}

    for a in raw_assignments:
        if not isinstance(a, dict):
            continue
        tid = str(a.get("thread_id") or "").strip()
        topic_key = str(a.get("topic_key") or "").strip()
        conf = float(a.get("confidence") or 0.0)

        if tid not in expected:
            continue

        tax_id = topic_key_to_id.get(topic_key)
        db_pairs.append((tax_id, tid))
        results.append({
            "thread_id": tid, "topic_key": topic_key,
            "taxonomy_id": tax_id, "confidence": conf,
        })

    assigned_tids = {r["thread_id"] for r in results}
    for tid in expected - assigned_tids:
        db_pairs.append((None, tid))
        results.append({"thread_id": tid, "topic_key": "__missing", "taxonomy_id": None, "confidence": 0.0})

    with sql_connect() as cnx:
        try:
            bulk_write_taxonomy_assignments(cnx, db_pairs)
            cnx.commit()
        except Exception as e:
            cnx.rollback()
            release_assign_claims(cnx, [str(t.get("thread_id")) for t in threads])
            cnx.commit()
            return [{"status": "db_error", "error": str(e)}]

    return results


# ---------------------------------------------------------
# HTTP Entry Point
# ---------------------------------------------------------

def run_emergent_assign(req: func.HttpRequest) -> func.HttpResponse:
    """
    Assign emergent threads to taxonomy branches. Parallel across products.

    Query params:
      batch_size  (default 15, max 25)
      max_batches (default 50, max 500)
      max_workers (default 4, max 12)
      force       (default 0)
    """
    try:
        batch_size = max(1, min(int(req.params.get("batch_size", "15")), 25))
        max_batches = max(1, min(int(req.params.get("max_batches", "50")), 500))
        max_workers = max(1, min(int(req.params.get("max_workers", "4")), 12))
        force = req.params.get("force", "0") == "1"

        all_results: List[Dict[str, Any]] = []
        total_assigned = 0

        # Clear stale claims from crashed/timed-out previous runs
        stale_minutes = max(10, min(int(req.params.get("stale_minutes", "60")), 1440))
        with sql_connect() as cnx:
            cur_stale = cnx.cursor()
            cur_stale.execute("""
                UPDATE dbo.thread_enrichment
                SET EmergentAssignStartedUtc = NULL
                WHERE EmergentAssignStartedUtc IS NOT NULL
                  AND EmergentAssignCompletedUtc IS NULL
                  AND EmergentAssignStartedUtc < DATEADD(MINUTE, -?, SYSUTCDATETIME());
            """, stale_minutes)
            stale_cleared = int(cur_stale.rowcount or 0)
            cnx.commit()
            if stale_cleared:
                logging.info("[emergent_assign] cleared %d stale claims older than %d min", stale_cleared, stale_minutes)

        with sql_connect() as cnx:
            all_taxonomy = fetch_all_taxonomy(cnx)

        if not all_taxonomy:
            return func.HttpResponse(
                json.dumps({"status": "ok", "assigned": 0, "note": "no taxonomy nodes exist"}),
                mimetype="application/json",
            )

        tax_by_product: Dict[str, List[Dict[str, Any]]] = {}
        for t in all_taxonomy:
            prod = str(t.get("Product") or "").strip()
            if prod:
                tax_by_product.setdefault(prod, []).append(t)

        # Pre-compute topic_key -> TaxonomyID mapping per product
        product_meta: Dict[str, Dict[str, int]] = {}
        for product, taxonomy in tax_by_product.items():
            product_meta[product] = {
                str(t.get("TopicKey") or ""): int(t["TaxonomyID"])
                for t in taxonomy
                if t.get("TaxonomyID") is not None
            }

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = []
            # Track per-product batch counts and drained products
            batches_by_product: Dict[str, int] = {p: 0 for p in tax_by_product}
            drained_products: set = set()

            # Round-robin claim across products to avoid serial bottleneck
            while len(drained_products) < len(tax_by_product):
                claimed_any = False
                for product in list(tax_by_product.keys()):
                    if product in drained_products:
                        continue
                    if batches_by_product[product] >= max_batches:
                        drained_products.add(product)
                        continue

                    with sql_connect() as cnx:
                        threads = claim_threads_for_emergent_assign(
                            cnx, batch_size, product, include_completed=force,
                        )
                        cnx.commit()

                    if not threads:
                        drained_products.add(product)
                        continue

                    claimed_any = True
                    batches_by_product[product] += 1
                    futures.append(
                        ex.submit(
                            _process_assign_batch, product, threads,
                            tax_by_product[product], product_meta[product],
                        )
                    )

                if not claimed_any:
                    break

            for f in as_completed(futures):
                try:
                    batch_results = f.result()
                    all_results.extend(batch_results)
                    total_assigned += sum(1 for r in batch_results if r.get("taxonomy_id") is not None)
                except Exception as e:
                    all_results.append({"status": "worker_error", "error": str(e)})

        return func.HttpResponse(
            json.dumps({
                "status": "ok",
                "total_assigned": total_assigned,
                "total_processed": len(all_results),
                "products": sorted(tax_by_product.keys()),
                "details": all_results[:100],
            }, default=str),
            mimetype="application/json",
        )

    except Exception as e:
        logging.exception("emergent_assign failed")
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}),
            status_code=500,
            mimetype="application/json",
        )
