# -*- coding: utf-8 -*-
"""
dashboard.py  –  IssueMiner Dashboard (Streamlit)

Run:
    streamlit run dashboard.py
"""
import os
import sys
import json
import re
import time
import threading
import datetime as dt
from typing import Any, Dict, Optional

import streamlit as st
import pandas as pd

# ── Bootstrap: load local.settings.json into env so DB works ──
_SETTINGS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "local.settings.json")
if os.path.isfile(_SETTINGS_FILE):
    try:
        with open(_SETTINGS_FILE, "r", encoding="utf-8-sig") as _f:
            _vals = json.load(_f).get("Values", {})
            for _k, _v in _vals.items():
                if _k not in os.environ:
                    os.environ[_k] = str(_v)
    except Exception:
        pass

from dashboard_db import (
    sql_connect,
    ensure_tables,
    list_products,
    get_product_urls,
    insert_product,
    update_product_crawl_time,
    delete_product,
    get_product_thread_counts,
    get_catalog_stats,
    get_emergent_summary,
    get_recent_nuggets,
    get_pipeline_states,
    get_wiki_push_summary,
    get_overall_counts,
    get_distinct_enriched_products,
    list_knowledge_sources,
    get_knowledge_source_urls,
    insert_knowledge_source,
    update_knowledge_source_crawl,
    delete_knowledge_source,
    list_product_names,
    list_product_aliases,
    upsert_product_alias,
    delete_product_alias,
    seed_default_aliases,
    backfill_product_aliases,
    list_enrichment_products,
    fetch_enrichment_product_names,
    seed_enrichment_products,
    delete_enrichment_product,
    rename_enrichment_product,
    merge_enrichment_products,
    create_batch,
    list_batches,
)

# Seed list mirrored from phase1a_enricher._SEED_PRODUCTS — used for UI dropdowns
# and the manual seed button before any enrichment has run.
_SEED_PRODUCTS_HINT = [
    "APIM", "DevOps", "Web Apps", "Functions", "Static Web Apps",
    "App Service", "Redis", "Storage Account", "Logic Apps",
    "Ai Search", "Service Bus", "Event Grid", "Event Hub",
    "ARM", "Policy", "Virtual Network", "Monitor", "OpenAI",
    "Entra External ID", "System Center",
    "Application Gateway", "Front Door", "ExpressRoute",
    "Internet Information Services",
]

# ─────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────
st.set_page_config(
    page_title="IssueMiner Dashboard",
    page_icon="⛏️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────
# Background task runner
# ─────────────────────────────────────────────────────────
# st.session_state is NOT accessible from background threads.
# We use a plain module-level dict (_BG_TASKS) as the shared
# state store. The main thread reads from it on every rerun.
#
# Button-spam prevention: st.session_state["_bg_started_<key>"] is
# set to True immediately when a task is launched, so the button is
# disabled on the very next render before _BG_TASKS is even checked.

_BG_TASKS: Dict[str, Any] = {}
_BG_LOCK = threading.Lock()


def _bg_task_start(task_key: str, fn, *args, **kwargs) -> None:
    """Launch fn(*args, **kwargs) in a daemon thread."""
    # Mark started in session_state immediately so the button is
    # disabled on the next render even before the thread writes to _BG_TASKS
    st.session_state[f"_bg_started_{task_key}"] = True

    with _BG_LOCK:
        _BG_TASKS[task_key] = {
            "status":     "running",
            "log":        [],
            "result":     None,
            "started_at": dt.datetime.utcnow().isoformat(),
        }

    def _run():
        try:
            result = fn(*args, **kwargs)
            with _BG_LOCK:
                _BG_TASKS[task_key]["result"] = result
                _BG_TASKS[task_key]["status"] = "done"
        except Exception as e:
            with _BG_LOCK:
                _BG_TASKS[task_key]["status"] = "error"
                _BG_TASKS[task_key]["result"] = str(e)

    threading.Thread(target=_run, daemon=True).start()


def _bg_log(task_key: str, msg: str) -> None:
    """Append a log line — safe to call from any thread."""
    with _BG_LOCK:
        if task_key in _BG_TASKS:
            _BG_TASKS[task_key]["log"].append(msg)


def _bg_task_ui(task_key: str, title: str) -> bool:
    """
    Render status widget for a background task.
    Returns True while the task is still running.
    Auto-refreshes the page every 3 seconds via a meta-refresh tag
    (no sleep — does not block the Streamlit thread).
    """
    # Sync: if task finished, clear the session_state started flag
    with _BG_LOCK:
        state = dict(_BG_TASKS.get(task_key) or {})

    if not state:
        # Task not in _BG_TASKS — could be cleared or never started
        st.session_state.pop(f"_bg_started_{task_key}", None)
        return False

    status = state.get("status", "unknown")
    log    = list(state.get("log") or [])

    if status == "running":
        st.info(f"⏳ **{title}** is running…")
        if log:
            with st.expander("Live log", expanded=True):
                st.text("\n".join(log[-40:]))
        # Auto-refresh every 3 seconds without blocking the thread
        st.markdown(
            '<meta http-equiv="refresh" content="3">',
            unsafe_allow_html=True,
        )
        return True

    # Task finished — clear started flag so button re-enables
    st.session_state.pop(f"_bg_started_{task_key}", None)

    if status == "done":
        st.success(f"✅ **{title}** complete.")
        result = state.get("result")
        if result:
            with st.expander("Result", expanded=False):
                st.json(result) if isinstance(result, (dict, list)) else st.text(str(result))
        if log:
            with st.expander("Log", expanded=False):
                st.text("\n".join(log))
        if st.button(f"Clear {title} status", key=f"{task_key}_clear"):
            with _BG_LOCK:
                _BG_TASKS.pop(task_key, None)
            st.rerun()
        return False

    if status == "error":
        st.error(f"❌ **{title}** failed: {state.get('result', '')}")
        if log:
            with st.expander("Log", expanded=False):
                st.text("\n".join(log))
        if st.button(f"Clear {title} status", key=f"{task_key}_clear"):
            with _BG_LOCK:
                _BG_TASKS.pop(task_key, None)
            st.rerun()
        return False

    return False


def _is_bg_running(task_key: str) -> bool:
    """Check _BG_TASKS first, fall back to session_state started flag."""
    with _BG_LOCK:
        if _BG_TASKS.get(task_key, {}).get("status") == "running":
            return True
    # Fallback: thread started but hasn't written to _BG_TASKS yet
    return bool(st.session_state.get(f"_bg_started_{task_key}"))


# ─────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────
@st.cache_resource(ttl=300)
def _get_cnx():
    cnx = sql_connect()
    ensure_tables(cnx)
    return cnx


def get_cnx():
    try:
        cnx = _get_cnx()
        cnx.cursor().execute("SELECT 1")  # test liveness
        return cnx
    except Exception:
        st.cache_resource.clear()
        return _get_cnx()


# ─────────────────────────────────────────────────────────
# Sidebar navigation
# ─────────────────────────────────────────────────────────
page = st.sidebar.radio(
    "Navigate",
    ["📊 Overview", "📦 Products", "📚 Knowledge Sources", "🚨 Emergent", "💎 Nuggets", "⚙️ Pipelines"],
    index=0,
)


# ═════════════════════════════════════════════════════════
# PAGE: Overview
# ═════════════════════════════════════════════════════════
if page == "📊 Overview":
    st.title("⛏️ IssueMiner Dashboard")

    cnx = get_cnx()

    # ── Top-level KPIs ──
    try:
        ov = get_overall_counts(cnx)
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Threads Cleaned", f"{ov['threads_clean']:,}")
        c2.metric("Threads Enriched", f"{ov['threads_enriched']:,}")
        c3.metric("Assigned (2E)", f"{ov['threads_assigned']:,}")
        c4.metric("Active Clusters", f"{ov['active_clusters']:,}")
        c5.metric("Nuggets Mined", f"{ov['threads_nuggets_mined']:,}")
        c6.metric("Emergent Threads", f"{ov['emergent_threads']:,}")
    except Exception as e:
        st.error(f"Could not load overall counts: {e}")

    st.divider()

    # ── Threads per product ──
    st.subheader("Threads by Product")
    try:
        tc = get_product_thread_counts(cnx)
        if tc:
            df = pd.DataFrame(tc)
            st.dataframe(
                df.style.format({
                    "avg_usefulness": "{:.2f}",
                }),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No enrichment data yet.")
    except Exception as e:
        st.warning(f"Could not load thread counts: {e}")

    st.divider()

    # ── Catalog (issue_cluster) stats ──
    st.subheader("Catalog Nodes by Product & Level")
    try:
        cs = get_catalog_stats(cnx)
        if cs:
            df_cat = pd.DataFrame(cs)
            level_names = {1: "L1 Topic", 2: "L2 Scenario", 3: "L3 Variant", 4: "L4 Leaf"}
            df_cat["level_name"] = df_cat["cluster_level"].map(level_names)

            # Pivot: rows = product, columns = level_name
            pivot = df_cat.pivot_table(
                index="product",
                columns="level_name",
                values="node_count",
                aggfunc="sum",
                fill_value=0,
            )
            for col in ["L1 Topic", "L2 Scenario", "L3 Variant", "L4 Leaf"]:
                if col not in pivot.columns:
                    pivot[col] = 0
            pivot = pivot[["L1 Topic", "L2 Scenario", "L3 Variant", "L4 Leaf"]]
            pivot["Total"] = pivot.sum(axis=1)
            st.dataframe(pivot, use_container_width=True)

            # Member totals
            pivot_members = df_cat.pivot_table(
                index="product",
                columns="level_name",
                values="total_members",
                aggfunc="sum",
                fill_value=0,
            )
            for col in ["L1 Topic", "L2 Scenario", "L3 Variant", "L4 Leaf"]:
                if col not in pivot_members.columns:
                    pivot_members[col] = 0
            pivot_members = pivot_members[["L1 Topic", "L2 Scenario", "L3 Variant", "L4 Leaf"]]
            with st.expander("Member counts by level"):
                st.dataframe(pivot_members, use_container_width=True)
        else:
            st.info("No catalog data yet.")
    except Exception as e:
        st.warning(f"Could not load catalog stats: {e}")

    st.divider()

    # ── Wiki push summary ──
    st.subheader("Wiki / Knowledge Base")
    try:
        ws = get_wiki_push_summary(cnx)
        wc1, wc2, wc3 = st.columns(3)
        wc1.metric("Total Solutions", ws["total_solutions"])
        wc2.metric("Pushed to Wiki", ws["wiki_pushed"])
        wc3.metric("Pending Push", ws["wiki_pending"])
    except Exception as e:
        st.info(f"Wiki stats not available: {e}")


# ═════════════════════════════════════════════════════════
# PAGE: Products
# ═════════════════════════════════════════════════════════
elif page == "📦 Products":
    st.title("📦 Products")

    cnx = get_cnx()

    # ── Background task status widgets ──
    _bg_task_ui("product_crawl", "Thread Crawl")
    _bg_task_ui("pipeline_run",  "Pipeline Run")

    # ── Add New Product (inline) ──
    if "show_new_product_form" not in st.session_state:
        st.session_state.show_new_product_form = False

    if st.button("➕ Add New Product", use_container_width=False):
        st.session_state.show_new_product_form = not st.session_state.show_new_product_form

    # ── Enrich Threads (global) ──
    if "show_enrich_form" not in st.session_state:
        st.session_state.show_enrich_form = False

    if st.button("⚡ Enrich Threads", use_container_width=False):
        st.session_state.show_enrich_form = not st.session_state.show_enrich_form

    if st.session_state.show_enrich_form:
        with st.container(border=True):
            st.subheader("⚡ Enrich Threads (Phase 1A)")
            st.caption(
                "Runs Phase 1A on unenriched rows in ThreadsClean. "
                "Already-enriched threads are always skipped automatically. "
                "Large limits are processed in repeated batches to avoid timeouts."
            )
            en1, en2, en3 = st.columns(3)
            with en1:
                enrich_limit = st.number_input(
                    "Total limit", min_value=1, max_value=100000, value=500, step=100,
                    key="global_elimit",
                    help="Total number of unenriched threads to process across all batches.",
                )
            with en2:
                enrich_batch = st.number_input(
                    "Batch size", min_value=25, max_value=500, value=100, step=25,
                    key="global_ebatch",
                    help="Threads per API call. Keep ≤200 to avoid timeouts.",
                )
            with en3:
                enrich_from = st.date_input("From date (optional)", value=None, key="global_efrom")

            enrich_to = st.date_input("To date (optional)", value=None, key="global_eto")

            if st.button("▶️ Run Enrich", key="global_enrich_run", use_container_width=True):
                import requests as _req
                total_limit = int(enrich_limit)
                batch_size  = int(enrich_batch)
                remaining   = total_limit
                grand_total = 0
                batch_num   = 0

                prog   = st.progress(0, text="Starting enrichment…")
                status = st.empty()

                while remaining > 0:
                    this_batch = min(batch_size, remaining)
                    batch_num += 1
                    params = f"limit={this_batch}"
                    if enrich_from:
                        params += f"&date_from={enrich_from.isoformat()}"
                    if enrich_to:
                        params += f"&date_to={enrich_to.isoformat()}T23:59:59"
                    url = f"http://localhost:7071/api/phase1a_enrich_batch?{params}"
                    status.info(f"Batch {batch_num}: enriching up to {this_batch} threads… (total so far: {grand_total})")
                    try:
                        r = _req.post(url, timeout=300)
                        if r.status_code != 200:
                            st.error(f"HTTP {r.status_code}: {r.text[:300]}")
                            break
                        body = r.json() if isinstance(r.json(), dict) else {}
                        processed = int(body.get("processed", 0))
                        grand_total += processed
                        remaining  -= processed
                        prog.progress(
                            min(grand_total / total_limit, 1.0),
                            text=f"Enriched {grand_total}/{total_limit} threads…",
                        )
                        # If the API returned fewer than requested, nothing left to do
                        if processed < this_batch:
                            break
                    except Exception as ex:
                        st.error(f"Enrich failed on batch {batch_num}: {ex}")
                        break

                prog.progress(1.0, text="Done.")
                st.success(f"✅ Enriched **{grand_total}** threads in {batch_num} batch(es).")
                st.session_state.show_enrich_form = False
                st.rerun()

    if st.session_state.show_new_product_form:
        with st.container(border=True):
            st.subheader("➕ Register New Product")
            with st.form("new_product_form"):
                prod_name = st.text_input(
                    "Product Name",
                    placeholder="e.g. APIM, Static Web Apps, OpenAI",
                    help="This name will be used by Phase 1A enrichment to classify threads.",
                )

                source = st.selectbox(
                    "Knowledge Source",
                    ["Microsoft Q&A", "StackOverflow", "Quora"],
                    index=0,
                )
                source_map = {"Microsoft Q&A": "msqa", "StackOverflow": "stackoverflow", "Quora": "quora"}

                st.markdown("**Tag URLs** (one per line)")
                tag_urls_raw = st.text_area(
                    "Enter tag/topic page URLs",
                    placeholder="https://learn.microsoft.com/en-us/answers/tags/436/azure-app-service\nhttps://learn.microsoft.com/en-us/answers/tags/100/azure-api-management",
                    height=120,
                )

                col1, col2 = st.columns(2)
                with col1:
                    daily_crawl = st.checkbox("Enable daily crawl", value=True)
                with col2:
                    emergent_detect = st.checkbox("Enable emergent detection", value=True)

                st.divider()

                sub_col1, sub_col2 = st.columns(2)
                with sub_col1:
                    submitted = st.form_submit_button("💾 Save Product", use_container_width=True)
                with sub_col2:
                    save_and_crawl = st.form_submit_button("💾 Save & 🕷️ Crawl Now", use_container_width=True)

            if submitted or save_and_crawl:
                if not prod_name or not prod_name.strip():
                    st.error("Product name is required.")
                else:
                    tag_urls = [u.strip() for u in tag_urls_raw.split("\n") if u.strip()]
                    if not tag_urls:
                        st.warning("No tag URLs provided — you can add them later.")

                    try:
                        pid = insert_product(
                            cnx,
                            name=prod_name.strip(),
                            source=source_map.get(source, "msqa"),
                            tag_urls=tag_urls,
                            daily_crawl=daily_crawl,
                            emergent_detect=emergent_detect,
                        )
                        st.success(f"✅ Product **{prod_name}** saved (ID: {pid})")

                        if save_and_crawl:
                            if tag_urls:
                                def _run_crawl(urls, prod, p_id):
                                    from phase0_crawl_threads import crawl_product as _cp, _sql_connect
                                    task_key = "product_crawl"
                                    # Open a fresh connection inside the thread — pyodbc is not thread-safe
                                    bg_cnx = _sql_connect()
                                    try:
                                        def _cb(phase, done, total, msg):
                                            _bg_log(task_key, msg)
                                        summary = _cp(tag_urls=urls, incremental=True,
                                                      cnx=bg_cnx, progress_callback=_cb)
                                        update_product_crawl_time(bg_cnx, p_id,
                                                                  crawl_count=summary.get("ingested", 0))
                                        return summary
                                    finally:
                                        bg_cnx.close()

                                _bg_task_start("product_crawl", _run_crawl,
                                               tag_urls, prod_name.strip(), pid)
                                st.session_state.show_new_product_form = False
                                st.rerun()
                            else:
                                st.warning("No tag URLs to crawl — add URLs first.")
                                update_product_crawl_time(cnx, pid, crawl_count=0)

                        st.session_state.show_new_product_form = False
                        st.rerun()

                    except Exception as ex:
                        if "UNIQUE" in str(ex).upper() or "DUPLICATE" in str(ex).upper():
                            st.error(f"A product named **{prod_name}** already exists.")
                        else:
                            st.error(f"Save failed: {ex}")

    st.divider()

    try:
        products = list_products(cnx)
    except Exception as e:
        st.error(f"Could not load products: {e}")
        products = []

    if not products:
        st.info("No products registered yet. Click **➕ Add New Product** above.")
    else:
        for p in products:
            pid = p["product_id"]
            name = p["product_name"]
            source = p["source"]
            daily = "✅" if p["daily_crawl"] else "❌"
            emergent = "✅" if p["emergent_detect"] else "❌"
            crawled = p.get("last_crawled_at")
            crawled_str = crawled.strftime("%Y-%m-%d %H:%M UTC") if crawled else "Never"
            crawl_count = p.get("last_crawl_count", 0) or 0

            with st.expander(f"**{name}**  |  Source: {source}  |  Crawled: {crawl_count:,}  |  Daily: {daily}  |  Emergent: {emergent}  |  Last crawled: {crawled_str}"):
                # Crawl + enrichment metrics
                mc1, mc2, mc3, mc4 = st.columns(4)
                mc1.metric("Crawled Threads", f"{crawl_count:,}")

                try:
                    tc = get_product_thread_counts(cnx)
                    match = [t for t in tc if t["product"] == name]
                    if match:
                        m = match[0]
                        mc2.metric("Enriched", m["thread_count"])
                        mc3.metric("Emergent", m["emergent_count"])
                        mc4.metric("Avg Usefulness", f"{m['avg_usefulness']:.2f}")
                    else:
                        mc2.metric("Enriched", 0)
                        mc3.metric("Emergent", 0)
                        mc4.metric("Avg Usefulness", "—")
                except Exception:
                    mc2.metric("Enriched", "—")
                    mc3.metric("Emergent", "—")
                    mc4.metric("Avg Usefulness", "—")

                # Show tag URLs
                try:
                    urls = get_product_urls(cnx, pid)
                    if urls:
                        st.markdown("**Tag URLs:**")
                        for u in urls:
                            st.markdown(f"- [{u}]({u})")
                    else:
                        st.caption("No tag URLs configured.")
                except Exception:
                    st.caption("Could not load URLs.")

                # Catalog nodes for this product
                try:
                    cs = get_catalog_stats(cnx)
                    prod_cs = [c for c in cs if c["product"] == name]
                    if prod_cs:
                        level_names = {1: "Topics", 2: "Scenarios", 3: "Variants", 4: "Leaves"}
                        parts = []
                        for c in prod_cs:
                            lbl = level_names.get(c["cluster_level"], f"L{c['cluster_level']}")
                            parts.append(f"{lbl}: **{c['node_count']}** ({c['total_members']} members)")
                        st.markdown(" · ".join(parts))
                except Exception:
                    pass

                # Status
                if crawled:
                    if p["daily_crawl"]:
                        st.success("Status: Crawled & daily crawl enabled")
                    else:
                        st.info("Status: Crawled (no daily crawl)")
                else:
                    st.warning("Status: Not crawled yet")

                # Actions
                col_a, col_b = st.columns(2)
                with col_a:
                    if st.button(f"🗑️ Delete", key=f"del_{pid}"):
                        try:
                            delete_product(cnx, pid)
                            st.success(f"Deleted '{name}'")
                            st.rerun()
                        except Exception as ex:
                            st.error(f"Delete failed: {ex}")
                with col_b:
                    if st.button(f"🔄 Mark Crawled", key=f"crawl_{pid}"):
                        try:
                            update_product_crawl_time(cnx, pid, crawl_count=0)
                            st.success(f"Updated crawl time for '{name}'")
                            st.rerun()
                        except Exception as ex:
                            st.error(f"Update failed: {ex}")

    # ── Enrichment Products ──────────────────────────────────────────────────
    st.divider()
    st.subheader("🗂️ Enrichment Products")
    st.caption(
        "Pipeline-owned canonical product registry. "
        "Phase 1A reads this table for the LLM prompt and writes every normalised product name back here automatically. "
        "Seed once to bootstrap; after that it grows on its own."
    )

    ep_col1, ep_col2 = st.columns([3, 1])
    with ep_col1:
        if st.button("🌱 Seed Enrichment Products", help="Insert the built-in seed list into dbo.enrichment_products if not already present."):
            try:
                n = seed_enrichment_products(get_cnx(), _SEED_PRODUCTS_HINT)
                st.success(f"Seeded {n} product(s).")
                st.rerun()
            except Exception as ex:
                st.error(f"Seed failed: {ex}")
    with ep_col2:
        if st.button("🔧 Backfill from thread_enrichment", key="ep_backfill",
                     help="Register any product names already in thread_enrichment that are missing from this table."):
            try:
                cnx_ep = get_cnx()
                cur_ep = cnx_ep.cursor()
                cur_ep.execute("""
                    SELECT DISTINCT product FROM dbo.thread_enrichment
                    WHERE product IS NOT NULL AND LTRIM(RTRIM(product)) <> ''
                      AND product <> 'Other'
                      AND product NOT IN (SELECT product_name FROM dbo.enrichment_products)
                """)
                missing = [row[0] for row in cur_ep.fetchall()]
                for m in missing:
                    cur_ep.execute("""
                        IF NOT EXISTS (SELECT 1 FROM dbo.enrichment_products WHERE product_name = ?)
                            INSERT INTO dbo.enrichment_products (product_name, is_seed, thread_count)
                            VALUES (?, 0, 0)
                    """, m, m)
                cnx_ep.commit()
                st.success(f"Registered {len(missing)} missing product(s).")
                st.rerun()
            except Exception as ex:
                st.error(f"Backfill failed: {ex}")

    try:
        ep_rows = list_enrichment_products(get_cnx())
        if ep_rows:
            df_ep = pd.DataFrame(ep_rows)[["product_id", "product_name", "thread_count", "is_seed", "first_seen_at", "last_seen_at"]]
            df_ep.columns = ["ID", "Product Name", "Threads", "Seed", "First Seen", "Last Seen"]
            st.dataframe(df_ep, use_container_width=True, hide_index=True)

            del_ep_id = st.number_input("Delete enrichment product by ID", min_value=1, step=1, value=None, key="del_ep_id")
            if st.button("🗑️ Delete Enrichment Product", key="del_ep_btn"):
                if del_ep_id:
                    try:
                        delete_enrichment_product(get_cnx(), int(del_ep_id))
                        st.success(f"Deleted enrichment product ID {del_ep_id}.")
                        st.rerun()
                    except Exception as ex:
                        st.error(f"Delete failed: {ex}")
                else:
                    st.warning("Enter a product ID to delete.")

            # ── Rename ───────────────────────────────────────────
            with st.expander("✏️ Rename Product"):
                st.caption(
                    "Renames the product in `enrichment_products` and rewrites all matching "
                    "`thread_enrichment` rows. The old name is auto-aliased so future LLM output normalises correctly."
                )
                ep_names_ren = [r["product_name"] for r in ep_rows]
                ren_old = st.selectbox("Product to rename", ep_names_ren, key="ren_old")
                ren_new = st.text_input("New name", key="ren_new", placeholder="e.g. Front Door")
                if st.button("✏️ Rename", key="ren_btn", type="primary"):
                    if not ren_new.strip():
                        st.error("New name is required.")
                    elif ren_new.strip() == ren_old:
                        st.warning("New name is the same as the current name.")
                    else:
                        try:
                            n = rename_enrichment_product(get_cnx(), ren_old, ren_new.strip())
                            st.success(f"✅ Renamed **{ren_old}** → **{ren_new.strip()}**. {n} thread_enrichment row(s) updated.")
                            st.rerun()
                        except Exception as ex:
                            st.error(f"Rename failed: {ex}")

            # ── Merge ────────────────────────────────────────────
            with st.expander("🔀 Merge Products"):
                st.caption(
                    "Merges the **absorbed** product into the **primary**. "
                    "All `thread_enrichment` rows are rewritten to the primary name, "
                    "the absorbed name is auto-aliased so future LLM output normalises correctly, "
                    "and the absorbed row is removed from this table."
                )
                ep_names = [r["product_name"] for r in ep_rows]
                mg_col1, mg_col2 = st.columns(2)
                with mg_col1:
                    merge_primary = st.selectbox("Primary (survives)", ep_names, key="merge_primary")
                with mg_col2:
                    absorbed_options = [n for n in ep_names if n != merge_primary]
                    merge_absorbed = st.selectbox("Absorbed (will be deleted)", absorbed_options, key="merge_absorbed") if absorbed_options else None

                if merge_absorbed and st.button("🔀 Run Merge", key="merge_run_btn", type="primary"):
                    try:
                        n = merge_enrichment_products(get_cnx(), merge_primary, merge_absorbed)
                        st.success(f"✅ Merged **{merge_absorbed}** → **{merge_primary}**. {n} thread_enrichment row(s) updated.")
                        st.rerun()
                    except Exception as ex:
                        st.error(f"Merge failed: {ex}")
        else:
            st.info("No enrichment products yet. Click **🌱 Seed Enrichment Products** to populate.")
    except Exception as e:
        st.warning(f"Could not load enrichment products: {e}")

    # ── Product Aliases ──────────────────────────────────────────────────────
    st.divider()
    st.subheader("🏷️ Product Aliases")
    st.caption(
        "Map alternate spellings / abbreviations to a canonical product name. "
        "Phase 1A uses this table to normalize LLM output before writing to the DB."
    )

    alias_col1, alias_col2 = st.columns([3, 1])
    with alias_col1:
        if st.button("🌱 Seed Default Aliases", help="Insert built-in aliases (Vnet→Virtual Network, SCOM→System Center, etc.) if not already present."):
            try:
                n = seed_default_aliases(get_cnx())
                st.success(f"Seeded {n} default alias(es).")
                st.rerun()
            except Exception as ex:
                st.error(f"Seed failed: {ex}")
    with alias_col2:
        if st.button("🔧 Backfill thread_enrichment", help="Rewrite existing thread_enrichment rows whose product matches a known alias."):
            try:
                n = backfill_product_aliases(get_cnx())
                st.success(f"Updated {n} thread_enrichment row(s).")
                st.rerun()
            except Exception as ex:
                st.error(f"Backfill failed: {ex}")

    # Add new alias form
    with st.expander("➕ Add / Update Alias"):
        with st.form("add_alias_form"):
            new_alias = st.text_input("Alias (as the LLM might return it)", placeholder="e.g. Vnet, Azure APIM")
            try:
                enrichment_names = fetch_enrichment_product_names(get_cnx())
            except Exception:
                enrichment_names = []
            canonical_options = sorted(set(enrichment_names + _SEED_PRODUCTS_HINT))
            canonical_input = st.selectbox("Canonical Product Name", canonical_options) if canonical_options else st.text_input("Canonical Product Name")
            alias_submitted = st.form_submit_button("💾 Save Alias")
        if alias_submitted:
            if not new_alias.strip() or not str(canonical_input).strip():
                st.error("Both alias and canonical name are required.")
            else:
                try:
                    upsert_product_alias(get_cnx(), new_alias.strip(), str(canonical_input).strip())
                    st.success(f"Saved: '{new_alias.strip()}' → '{canonical_input}'")
                    st.rerun()
                except Exception as ex:
                    st.error(f"Save failed: {ex}")

    # Show existing aliases
    try:
        aliases = list_product_aliases(get_cnx())
        if aliases:
            df_aliases = pd.DataFrame(aliases)[["alias_name", "canonical_name", "created_at"]]
            df_aliases.columns = ["Alias", "Canonical Product", "Created"]
            st.dataframe(df_aliases, use_container_width=True, hide_index=True)

            del_alias_id = st.number_input("Delete alias by ID", min_value=1, step=1, value=None, key="del_alias_id")
            if st.button("🗑️ Delete Alias", key="del_alias_btn"):
                if del_alias_id:
                    try:
                        delete_product_alias(get_cnx(), int(del_alias_id))
                        st.success(f"Deleted alias ID {del_alias_id}.")
                        st.rerun()
                    except Exception as ex:
                        st.error(f"Delete failed: {ex}")
                else:
                    st.warning("Enter an alias ID to delete.")
        else:
            st.info("No aliases defined yet. Click **🌱 Seed Default Aliases** to populate the built-in set.")
    except Exception as e:
        st.warning(f"Could not load aliases: {e}")


# ═════════════════════════════════════════════════════════
# PAGE: Knowledge Sources
# ═════════════════════════════════════════════════════════
elif page == "📚 Knowledge Sources":
    st.title("📚 Knowledge Sources")
    st.caption(
        "Crawl documentation sites, upload the CSV to blob storage (msdocs/ container), "
        "then trigger the Azure AI Search indexer so new content is immediately searchable."
    )

    cnx = get_cnx()

    # ── Shared crawl + upload + index helper ─────────────
    def _do_crawl_and_publish(
        source_type: str,
        product: str,
        display_name: str,
        doc_urls: list,
        source_id: int,
    ) -> None:
        """Crawl all URLs, build CSV, upload to blob, run indexer, show results."""
        import io, csv as _csv
        from get_ms_learn import (
            crawl_ms_learn_docs, _fetch, _extract_title_and_body,
            upload_csv_to_blob, run_search_indexer,
        )
        import requests as _req

        all_rows = []
        prog = st.progress(0, text="Starting crawl…")

        for ui, url in enumerate(doc_urls):
            prog.progress(ui / max(len(doc_urls), 1), text=f"Crawling {url[:70]}…")
            try:
                if source_type == "ms_learn":
                    rows = crawl_ms_learn_docs(root_url=url, product=product, delay=0.2)
                else:
                    sess = _req.Session()
                    html = _fetch(url, sess)
                    rows = []
                    if html:
                        ext = _extract_title_and_body(html)
                        rows = [{
                            "Order": 0, "Product": product,
                            "URL": url, "Title": ext["title"], "Body": ext["body"],
                        }]
                for r in rows:
                    r["Order"] = len(all_rows) + 1
                    all_rows.append(r)
            except Exception as ex:
                st.warning(f"Crawl failed for {url}: {ex}")

        prog.progress(1.0, text="Crawl done.")

        if not all_rows:
            st.warning("No pages were extracted — nothing to upload.")
            return

        # Build CSV bytes
        buf = io.StringIO()
        writer = _csv.DictWriter(
            buf, fieldnames=["Order", "Product", "URL", "Title", "Body"],
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(all_rows)
        csv_bytes = buf.getvalue().encode("utf-8-sig")

        # Upload to blob
        safe_name = re.sub(r"[^a-z0-9_-]", "_", display_name.lower())[:60]
        blob_name = f"{safe_name}.csv"
        try:
            blob_url = upload_csv_to_blob(csv_bytes, blob_name)
            st.success(f"☁️ Uploaded **{len(all_rows)} pages** → `msdocs/{blob_name}`")
        except Exception as ex:
            st.error(f"Blob upload failed: {ex}")
            # Still offer a download so the crawl isn't wasted
            st.download_button(
                "⬇️ Download CSV (upload failed — save manually)",
                data=csv_bytes,
                file_name=f"{safe_name}.csv",
                mime="text/csv",
                use_container_width=True,
            )
            return

        # Run indexer
        try:
            result = run_search_indexer("rag-apim-swa-indexer")
            if result["status"] == "queued":
                st.success("🔍 Indexer **rag-apim-swa-indexer** queued — content will be searchable shortly.")
            else:
                st.warning(f"Indexer response: HTTP {result['http_status']} — {result['message']}")
        except Exception as ex:
            st.warning(f"Indexer trigger failed: {ex}")

        # Update DB + download button
        update_knowledge_source_crawl(cnx, source_id, len(all_rows))

        st.download_button(
            label=f"⬇️ Download CSV ({len(all_rows)} pages)",
            data=csv_bytes,
            file_name=f"{safe_name}.csv",
            mime="text/csv",
            key=f"ks_dl_final_{source_id}",
            use_container_width=True,
        )

    # ── Add New Knowledge Source ──────────────────────────
    if "show_ks_form" not in st.session_state:
        st.session_state.show_ks_form = False

    if st.button("➕ Add Knowledge Source", use_container_width=False):
        st.session_state.show_ks_form = not st.session_state.show_ks_form

    if st.session_state.show_ks_form:
        with st.container(border=True):
            st.subheader("➕ New Knowledge Source")

            ks_type = st.selectbox(
                "Source Type",
                ["MS Learn", "Custom"],
                key="ks_type",
                help="MS Learn uses toc.json to discover all pages automatically. "
                     "Custom crawls each URL you provide as a single page.",
            )

            ks_name = st.text_input(
                "Display Name",
                placeholder="e.g. APIM Official Docs",
                key="ks_name",
            )

            try:
                product_options = list_product_names(cnx)
            except Exception:
                product_options = []

            if product_options:
                ks_product = st.selectbox(
                    "Product",
                    product_options,
                    key="ks_product",
                    help="Canonical names from the Products table.",
                )
            else:
                ks_product = st.text_input(
                    "Product (no products registered yet — type manually)",
                    key="ks_product_manual",
                )

            url_hint = (
                "One root URL per line. The full TOC will be discovered automatically."
                if ks_type == "MS Learn" else
                "One URL per line — each will be fetched as a single page."
            )
            st.caption(url_hint)
            ks_urls_raw = st.text_area(
                "URLs",
                placeholder="https://learn.microsoft.com/en-us/azure/api-management/",
                height=110,
                key="ks_urls",
            )

            ks_col1, ks_col2 = st.columns(2)
            with ks_col1:
                ks_save = st.button("💾 Save", key="ks_save", use_container_width=True)
            with ks_col2:
                ks_save_crawl = st.button(
                    "💾 Save & 🕷️ Crawl + Upload + Index",
                    key="ks_save_crawl",
                    use_container_width=True,
                )

            if ks_save or ks_save_crawl:
                final_product = (ks_product or "").strip()
                final_name    = (ks_name or "").strip()
                doc_urls      = [u.strip() for u in ks_urls_raw.splitlines() if u.strip()]
                src_type_key  = "ms_learn" if ks_type == "MS Learn" else "custom"

                if not final_name:
                    st.error("Display name is required.")
                elif not final_product:
                    st.error("Product is required.")
                elif not doc_urls:
                    st.error("At least one URL is required.")
                else:
                    try:
                        sid = insert_knowledge_source(
                            cnx,
                            source_type=src_type_key,
                            display_name=final_name,
                            product=final_product,
                            doc_urls=doc_urls,
                        )
                        st.success(f"✅ Saved **{final_name}** (ID: {sid})")

                        if ks_save_crawl:
                            _do_crawl_and_publish(
                                source_type=src_type_key,
                                product=final_product,
                                display_name=final_name,
                                doc_urls=doc_urls,
                                source_id=sid,
                            )

                        st.session_state.show_ks_form = False
                        if not ks_save_crawl:
                            st.rerun()

                    except Exception as ex:
                        st.error(f"Failed: {ex}")

    st.divider()

    # ── Existing Knowledge Sources ────────────────────────
    try:
        sources = list_knowledge_sources(cnx)
    except Exception as e:
        st.error(f"Could not load knowledge sources: {e}")
        sources = []

    if not sources:
        st.info("No knowledge sources yet. Click **➕ Add Knowledge Source** above.")
    else:
        for ks in sources:
            sid         = ks["source_id"]
            stype       = ks["source_type"]
            sname       = ks["display_name"]
            sproduct    = ks["product"]
            scrawled    = ks.get("last_crawled_at")
            sdoc_count  = ks.get("last_doc_count") or 0
            stype_label = "MS Learn" if stype == "ms_learn" else "Custom"
            crawled_str = scrawled.strftime("%Y-%m-%d %H:%M UTC") if scrawled else "Never"

            with st.expander(
                f"**{sname}**  |  {stype_label}  |  Product: {sproduct}  "
                f"|  Docs: {sdoc_count:,}  |  Last crawled: {crawled_str}"
            ):
                try:
                    ks_urls = get_knowledge_source_urls(cnx, sid)
                    if ks_urls:
                        st.markdown("**URLs:**")
                        for u in ks_urls:
                            st.markdown(f"- [{u}]({u})")
                    else:
                        st.caption("No URLs configured.")
                except Exception:
                    st.caption("Could not load URLs.")
                    ks_urls = []

                ks_act1, ks_act2 = st.columns(2)

                with ks_act1:
                    if st.button(
                        "🕷️ Crawl + Upload + Index",
                        key=f"ks_crawl_{sid}",
                        use_container_width=True,
                    ):
                        _do_crawl_and_publish(
                            source_type=stype,
                            product=sproduct,
                            display_name=sname,
                            doc_urls=ks_urls,
                            source_id=sid,
                        )

                with ks_act2:
                    if st.button("🗑️ Delete", key=f"ks_del_{sid}", use_container_width=True):
                        try:
                            delete_knowledge_source(cnx, sid)
                            st.success(f"Deleted '{sname}'")
                            st.rerun()
                        except Exception as ex:
                            st.error(f"Delete failed: {ex}")

# ═════════════════════════════════════════════════════════
# PAGE: Emergent
# ═════════════════════════════════════════════════════════
elif page == "🚨 Emergent":
    st.title("🚨 Emergent Issues")

    cnx = get_cnx()

    try:
        emergents = get_emergent_summary(cnx)
        if emergents:
            df = pd.DataFrame(emergents)
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No emergent issues detected yet.")
    except Exception as e:
        st.warning(f"EmergentIssues table not available: {e}")

    st.divider()

    st.subheader("Emergent Threads (from thread_enrichment)")
    try:
        cur = cnx.cursor()
        cur.execute("""
            SELECT TOP 30
                thread_id, product, signature_text, emergent_signal,
                emergent_signal_text, EmergentTaxonomyID,
                EmergentCompletedUtc
            FROM dbo.thread_enrichment
            WHERE is_emergent = 1
            ORDER BY ISNULL(EmergentCompletedUtc, source_created_at) DESC
        """)
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        if rows:
            df_em = pd.DataFrame(rows)
            st.dataframe(df_em, use_container_width=True, hide_index=True)
        else:
            st.info("No emergent threads found.")
    except Exception as e:
        st.warning(f"Could not load emergent threads: {e}")


# ═════════════════════════════════════════════════════════
# PAGE: Nuggets
# ═════════════════════════════════════════════════════════
elif page == "💎 Nuggets":
    st.title("💎 Recently Mined Nuggets")

    cnx = get_cnx()

    limit = st.slider("Show last N nuggets", 10, 100, 30)

    try:
        nuggets = get_recent_nuggets(cnx, limit=limit)
        if nuggets:
            df = pd.DataFrame(nuggets)
            st.dataframe(df, use_container_width=True, hide_index=True)

            # Nuggets per product chart
            if "product" in df.columns:
                st.subheader("Nuggets by Product")
                counts = df["product"].value_counts()
                st.bar_chart(counts)

            # Nuggets per topic
            if "topic_cluster_key" in df.columns:
                st.subheader("Nuggets by Topic")
                with st.expander("Expand"):
                    tc = df["topic_cluster_key"].value_counts().head(20)
                    st.bar_chart(tc)
        else:
            st.info("No nuggets mined yet.")
    except Exception as e:
        st.warning(f"Could not load nuggets: {e}")


# ═════════════════════════════════════════════════════════
# PAGE: Pipelines
# ═════════════════════════════════════════════════════════
elif page == "⚙️ Pipelines":
    st.title("⚙️ Pipeline Status")

    cnx = get_cnx()

    # ── Background task status ──
    _bg_task_ui("pipeline_run", "Pipeline Run")

    # Pipeline watermarks
    st.subheader("Pipeline Watermarks")
    try:
        states = get_pipeline_states(cnx)
        if states:
            df = pd.DataFrame(states)
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No pipeline state entries found.")
    except Exception as e:
        st.warning(f"Could not load pipeline states: {e}")

    st.divider()

    # Phase progress
    st.subheader("Phase Progress")
    try:
        cur = cnx.cursor()

        phases = {}

        cur.execute("SELECT COUNT(*) FROM dbo.thread_enrichment WHERE CatalogCheckedUtc IS NOT NULL")
        phases["Phase 1B (Catalog Checked)"] = cur.fetchone()[0] or 0

        cur.execute("SELECT COUNT(*) FROM dbo.thread_enrichment WHERE AssignmentCompletedUtc IS NOT NULL")
        phases["Phase 2E (Assigned)"] = cur.fetchone()[0] or 0

        cur.execute("SELECT COUNT(*) FROM dbo.thread_enrichment WHERE EmergentCompletedUtc IS NOT NULL")
        phases["Phase 2F (Emergent Detected)"] = cur.fetchone()[0] or 0

        cur.execute("SELECT COUNT(*) FROM dbo.thread_enrichment WHERE NuggetsMinedUtc IS NOT NULL")
        phases["Phase 4A (Nuggets Mined)"] = cur.fetchone()[0] or 0

        cur.execute("SELECT COUNT(*) FROM dbo.thread_enrichment WHERE CommonLeafUsedUtc IS NOT NULL")
        phases["Phase 3 (Common Leaf Used)"] = cur.fetchone()[0] or 0

        cur.execute("SELECT COUNT(*) FROM dbo.thread_enrichment WHERE EmergentUsedUtc IS NOT NULL")
        phases["Phase 3 (Emergent Used)"] = cur.fetchone()[0] or 0

        total = 0
        try:
            cur.execute("SELECT COUNT(*) FROM dbo.thread_enrichment")
            total = cur.fetchone()[0] or 0
        except Exception:
            pass

        if phases:
            df_phases = pd.DataFrame([
                {"Phase": k, "Threads Processed": v, "% of Total": f"{v/total*100:.1f}%" if total > 0 else "N/A"}
                for k, v in phases.items()
            ])
            st.dataframe(df_phases, use_container_width=True, hide_index=True)

    except Exception as e:
        st.warning(f"Could not load phase progress: {e}")

    st.divider()

    # Daily crawl products
    st.subheader("Daily Crawl Schedule")
    try:
        products = list_products(cnx)
        daily = [p for p in products if p.get("daily_crawl")]
        if daily:
            for p in daily:
                crawled = p.get("last_crawled_at")
                crawled_str = crawled.strftime("%Y-%m-%d %H:%M") if crawled else "Never"
                st.markdown(f"- **{p['product_name']}** ({p['source']}) — last crawled: {crawled_str}")
        else:
            st.info("No products have daily crawl enabled.")
    except Exception as e:
        st.info(f"Products table not available yet: {e}")

    st.divider()

    # ── Batch Management ─────────────────────────────────────
    st.subheader("📦 Batch Management")
    st.caption(
        "A batch scopes a set of enriched threads so phases 1B → 4A only process those rows. "
        "Create a batch below, then use the batch ID in the Demo Run section."
    )

    with st.container(border=True):
        try:
            ep_options = fetch_enrichment_product_names(cnx)
        except Exception:
            ep_options = []

        b_col1, b_col2, b_col3 = st.columns([2, 2, 1])
        with b_col1:
            batch_product = st.selectbox("Product", ep_options, key="batch_product") if ep_options else st.text_input("Product", key="batch_product_txt")
        with b_col2:
            import datetime as _dt
            default_bid = f"batch_{_dt.datetime.utcnow().strftime('%Y%m%d_%H%M')}"
            batch_id_input = st.text_input("Batch ID", value=default_bid, key="batch_id_input")
        with b_col3:
            batch_limit = st.number_input("Threads", min_value=1, max_value=10000, value=100, step=10, key="batch_limit")

        b_force = st.checkbox("Force (re-stamp already-batched rows)", key="batch_force", value=False)
        if st.button("📦 Create Batch", key="batch_create_btn", type="primary"):
            bp = str(batch_product or "").strip()
            bi = batch_id_input.strip()
            if not bp or not bi:
                st.error("Product and Batch ID are required.")
            else:
                try:
                    n = create_batch(get_cnx(), batch_id=bi, product=bp, limit=int(batch_limit), force=b_force)
                    st.success(f"✅ Stamped **{n}** threads with batch_id=`{bi}` for product **{bp}**.")
                    st.rerun()
                except Exception as ex:
                    st.error(f"Failed: {ex}")

    try:
        batches = list_batches(cnx)
        if batches:
            df_batches = pd.DataFrame(batches)
            df_batches.columns = ["Batch ID", "Threads", "Products", "Pending 1B", "Pending 2E", "Pending 4A", "Oldest", "Newest"]
            st.dataframe(df_batches, use_container_width=True, hide_index=True)
        else:
            st.info("No batches created yet.")
    except Exception as e:
        st.warning(f"Could not load batches: {e}")

    st.divider()

    # ── Demo Run: per-product pipeline ──────────────────────
    st.subheader("🧪 Demo Run — Per-Product Pipeline")
    st.caption(
        "Set a **Batch ID** to scope each phase to that batch's threads only. "
        "Leave blank to process all eligible threads (full pipeline behaviour)."
    )

    try:
        all_products = list_products(cnx)
        product_names = sorted([p["product_name"] for p in all_products]) if all_products else []
    except Exception:
        product_names = []

    if not product_names:
        st.info("No products registered. Add a product first.")
    else:
        with st.container(border=True):
            dc1, dc2, dc3 = st.columns([2, 1, 2])
            with dc1:
                demo_product = st.selectbox("Product", product_names, key="demo_product")
            with dc2:
                demo_limit = st.number_input(
                    "Thread limit per phase",
                    min_value=1, max_value=500, value=20, step=5,
                    key="demo_limit",
                )
            with dc3:
                demo_batch_id = st.text_input(
                    "Batch ID (optional)",
                    placeholder="e.g. batch_20250601_1400",
                    key="demo_batch_id",
                    help="Only threads stamped with this batch_id will be processed by 1B, 1C, 2E, 2F and 4A.",
                )

            demo_mode = st.checkbox(
                "🧪 Demo mode — lower thresholds for Phase 3 & 4 (use when running on a small thread set)",
                value=True,
                key="demo_mode",
                help=(
                    "Phase 3: min_members=1, min_usefulness=0.3\n"
                    "Phase 4B: min_members=1, min_usefulness=0.1\n"
                    "Phase 4C: min_members=1, child_min_members=1\n"
                    "Phase 4D: min_members=1, require_child_wiki=0"
                ),
            )

            _FUNC_BASE = "http://localhost:7071/api"
            _b = f"&batch_id={demo_batch_id.strip()}" if demo_batch_id.strip() else ""

            # Demo-mode threshold overrides
            _p3_thresh  = "min_members=1&min_usefulness=0.3"          if demo_mode else "min_members=2&min_usefulness=0.6"
            _p4b_thresh = "min_members=1&min_usefulness=0.1"          if demo_mode else "min_members=3&min_usefulness=0.4"
            _p4c_thresh = "min_members=1&child_min_members=1"         if demo_mode else "min_members=6&child_min_members=3"
            _p4d_thresh = "min_members=1&require_child_wiki=0"        if demo_mode else "min_members=1&require_child_wiki=1"

            # Tuple: (display_name, url, loop_until_empty, per_call_timeout_secs)
            _batch = min(int(demo_limit), 20)   # safe per-call limit for LLM-heavy phases
            demo_phases = [
                # ── Catalog building ──────────────────────────────────────────
                ("Phase 1B – Cluster",
                 f"{_FUNC_BASE}/phase1b_cluster?product={demo_product}&batch_size=10&max_batches=20{_b}",
                 True,  240),
                ("Phase 1C – Emergent cluster",
                 f"{_FUNC_BASE}/phase1c_emergent_cluster?product={demo_product}&batch_size=10&limit=50{_b}",
                 False, 120),
                # ── Leaf assignment (needs 1B catalog complete first) ─────────
                ("Phase 2E – Assign leaf",
                 f"{_FUNC_BASE}/phase2e_assign_leaf?batch_size=10&max_batches=20&product={demo_product}{_b}",
                 True,  240),
                ("Phase 2F – Emergent detect",
                 f"{_FUNC_BASE}/phase2f_detect_emergent?batch_size=10&max_batches=20{_b}",
                 False, 120),
                # ── Count leaves BEFORE wiki/playbook phases ──────────────────
                ("Phase 2G – Count leaves",
                 f"{_FUNC_BASE}/phase2g_count_leaves?product={demo_product}",
                 False,  60),
                # ── Nugget mining (runs on raw threads, before playbooks) ──────
                ("Phase 4A – Nuggets",
                 f"{_FUNC_BASE}/phase4a_nugget_mining?limit={_batch}&product={demo_product}{_b}",
                 True,  120),
                # ── Playbook generation (needs leaf counts + nuggets) ─────────
                ("Phase 3 – Common playbooks",
                 f"{_FUNC_BASE}/phase3_common?limit={_batch}&product={demo_product}&{_p3_thresh}",
                 True,  180),
                ("Phase 3 – Push to wiki",
                 f"{_FUNC_BASE}/phase3_push?limit={demo_limit}",
                 False, 120),
                # ── Wiki population (needs playbooks + leaf counts) ───────────
                # Re-run 2G so member_count reflects any new assignments
                ("Phase 2G – Count leaves (refresh)",
                 f"{_FUNC_BASE}/phase2g_count_leaves?product={demo_product}",
                 False,  60),
                ("Phase 4B – Variants wiki",
                 f"{_FUNC_BASE}/phase4b_populate_variants?limit={_batch}&product={demo_product}&{_p4b_thresh}",
                 True,  180),
                ("Phase 4C – Scenarios wiki",
                 f"{_FUNC_BASE}/phase4c_populate_scenarios?limit={_batch}&product={demo_product}&{_p4c_thresh}",
                 True,  180),
                ("Phase 4D – Topics wiki",
                 f"{_FUNC_BASE}/phase4d_populate_topics?limit={_batch}&product={demo_product}&{_p4d_thresh}",
                 True,  180),
            ]
            # Tuple: (name, url, loop_until_empty, per_call_timeout_secs)

            if _bg_task_ui("pipeline_run", f"Pipeline – {demo_product}"):
                pass  # meta-refresh handles polling
            elif not _is_bg_running("pipeline_run"):
                if st.button("▶️ Run All Phases", key="demo_run",
                             use_container_width=True,
                             disabled=_is_bg_running("pipeline_run")):
                    import requests as _req

                    def _run_pipeline(phases, product, limit):
                        import requests as _rq
                        for phase_name, url, loop, timeout in phases:
                            grand, call_n, ok = 0, 0, True
                            while True:
                                call_n += 1
                                try:
                                    r = _rq.post(url, timeout=timeout)
                                    body = r.json() if "json" in r.headers.get("content-type", "") else {}
                                    if r.status_code != 200:
                                        _bg_log("pipeline_run", f"⚠️ {phase_name}: HTTP {r.status_code}")
                                        ok = False; break
                                    processed = int(body.get("processed", body.get("ingested", 0)) or 0)
                                    grand += processed
                                    _bg_log("pipeline_run", f"  {phase_name} call {call_n}: +{processed} (total {grand})")
                                    if not loop or processed == 0:
                                        break
                                except Exception as ex:
                                    _bg_log("pipeline_run", f"❌ {phase_name}: {ex}")
                                    ok = False; break
                            suffix = f" ({call_n} calls)" if call_n > 1 else ""
                            _bg_log("pipeline_run", f"{'✅' if ok else '❌'} {phase_name}: {'OK' if ok else 'FAILED'} processed={grand}{suffix}")
                        return {"product": product, "limit": limit, "phases": len(phases)}

                    _bg_task_start("pipeline_run", _run_pipeline, demo_phases, demo_product, demo_limit)
                    st.rerun()
