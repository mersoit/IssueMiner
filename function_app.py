import os, json, datetime as dt
import azure.functions as func
import pyodbc
from openai import AzureOpenAI

# Phase 0
from phase0_crawl_threads import run_phase0_crawl

# Phase 1
from batch_cleaner import run_batch_clean
from phase1a_enricher import run_phase1a_enrich
from run_phase1b_cluster import run_phase1b_cluster
from phase1c_emergent_cluster import run_phase1c_emergent_cluster

# Phase 2
from phase2e_assign_leaf import run_phase2e_assign_leaf
from phase2g_count_leaves import run_phase2g_count_leaves

# Phase 3
from phase3_phase3_functions import (
    run_phase3_emergent_processing,
    run_phase3_common_processing,
    push_phase3_to_devops,
    run_phase3_search_selftest,
)

# Phase 4
from phase4_nugget_mining import run_phase4a_nugget_mining
from phase4b_populate_variants import run_phase4b_populate_variants
from phase4c_populate_scenarios import run_phase4c_populate_scenarios
from phase4d_populate_topics import run_phase4d_populate_topics

app = func.FunctionApp()

@app.route(route="phase0_crawl", methods=["GET", "POST"])
def phase0_crawl_route(req: func.HttpRequest) -> func.HttpResponse:
    """
    Phase 0: Crawl Microsoft Q&A tag pages into ThreadsClean.
    Examples:
      - /api/phase0_crawl?url=https://learn.microsoft.com/en-us/answers/tags/29/azure-api-management
      - /api/phase0_crawl?url=<tag_url>&max_pages=10&export=1
      - /api/phase0_crawl?url=<url1>,<url2>&incremental=1
    """
    return run_phase0_crawl(req)

@app.route(route="batch_clean", methods=["GET", "POST"])
def batch_clean_route(req: func.HttpRequest) -> func.HttpResponse:
    return run_batch_clean(req)

@app.route(route="phase1a_enrich_batch", methods=["GET", "POST"])
def phase1a_enrich_batch_route(req: func.HttpRequest) -> func.HttpResponse:
    return run_phase1a_enrich(req)

@app.route(route="phase1b_cluster", methods=["GET", "POST"])
def phase1b_cluster_route(req: func.HttpRequest) -> func.HttpResponse:
    return run_phase1b_cluster(req)

@app.route(route="phase1c_emergent_cluster", methods=["GET", "POST"])
def phase1c_emergent_cluster_route(req: func.HttpRequest) -> func.HttpResponse:
    """
    Optional: ?batch_id=<id> to restrict to a scoped batch.
    """
    return run_phase1c_emergent_cluster(req)

@app.route(route="phase2e_assign_leaf", methods=["GET", "POST"])
def phase2e_assign_leaf_route(req: func.HttpRequest) -> func.HttpResponse:
    """
    Examples:
      - /api/phase2e_assign_leaf?limit=50
      - /api/phase2e_assign_leaf?limit=50&force=1
      - /api/phase2e_assign_leaf?batch_id=demo_1
    """
    return run_phase2e_assign_leaf(req)

from phase2f_detect_emergent import run_phase2f_detect_emergent

@app.route(route="phase2f_detect_emergent", methods=["GET", "POST"])
def phase2f_detect_emergent_route(req: func.HttpRequest) -> func.HttpResponse:
    """
    Optional: ?batch_id=<id> to restrict to a scoped batch.
    """
    return run_phase2f_detect_emergent(req)

from phase_emergent_assign import run_emergent_assign
from phase_emergent_monitor import run_emergent_monitor

@app.route(route="emergent_assign", methods=["GET", "POST"])
def emergent_assign_route(req: func.HttpRequest) -> func.HttpResponse:
    """
    Emergent Phase 3: Assign emergent threads to taxonomy branches (parallel).
    Examples:
      - /api/emergent_assign
      - /api/emergent_assign?batch_size=15&max_workers=4&force=1
    """
    return run_emergent_assign(req)

@app.route(route="emergent_monitor", methods=["GET", "POST"])
def emergent_monitor_route(req: func.HttpRequest) -> func.HttpResponse:
    return run_emergent_monitor(req)

from phase3b_emergent_executor import run_phase3b_emergent

@app.route(route="phase3b_emergent", methods=["GET", "POST"])
def phase3b_emergent_route(req: func.HttpRequest) -> func.HttpResponse:
    """
    Emergent Phase 4: Per-branch incident analysis (sequential within branch).
    Examples:
      - /api/phase3b_emergent
      - /api/phase3b_emergent?product=APIM&batch_size=12
      - /api/phase3b_emergent?product=APIM&topic_key=custom-domain
    """
    return run_phase3b_emergent(req)

@app.route(route="phase2g_count_leaves", methods=["GET", "POST"])
def phase2g_count_leaves_route(req: func.HttpRequest) -> func.HttpResponse:
    return run_phase2g_count_leaves(req)

@app.route(route="phase3_emergent", methods=["GET", "POST"])
def phase3_emergent_route(req: func.HttpRequest) -> func.HttpResponse:
    limit = int(req.params.get("limit", "20"))
    return run_phase3_emergent_processing(limit)

@app.route(route="phase3_common", methods=["GET", "POST"])
def phase3_common_route(req: func.HttpRequest) -> func.HttpResponse:
    """
    Examples:
      - /api/phase3_common
      - /api/phase3_common?limit=20&min_members=2&min_usefulness=0.6
      - /api/phase3_common?limit=50&min_members=1&min_usefulness=0.3
    """
    limit = int(req.params.get("limit", "20"))
    min_members = int(req.params.get("min_members", "2"))
    min_usefulness = float(req.params.get("min_usefulness", "0.6"))
    product = (req.params.get("product") or "").strip()
    return run_phase3_common_processing(limit, min_members=min_members, min_usefulness=min_usefulness, product=product)

@app.route(route="phase3_push", methods=["GET", "POST"])
def phase3_push_route(req: func.HttpRequest) -> func.HttpResponse:
    limit = int(req.params.get("limit", "50"))
    return push_phase3_to_devops(limit)

@app.route(route="phase3_search_selftest", methods=["GET", "POST"])
def phase3_search_selftest_route(req: func.HttpRequest) -> func.HttpResponse:
    return run_phase3_search_selftest(req)

@app.route(route="phase4a_nugget_mining", methods=["GET", "POST"])
def phase4a_nugget_mining_route(req: func.HttpRequest) -> func.HttpResponse:
    """
    Examples:
      - /api/phase4a_nugget_mining
      - /api/phase4a_nugget_mining?limit=10
      - /api/phase4a_nugget_mining?batch_id=demo_1
    """
    return run_phase4a_nugget_mining(req)

@app.route(route="phase4b_populate_variants", methods=["GET", "POST"])
def phase4b_populate_variants_route(req: func.HttpRequest) -> func.HttpResponse:
    """
    Populate variant wiki pages with decision-tree triage content.
    Examples:
      - /api/phase4b_populate_variants
      - /api/phase4b_populate_variants?limit=20&workers=4
      - /api/phase4b_populate_variants?min_members=1&min_usefulness=0.3
    """
    return run_phase4b_populate_variants(req)

@app.route(route="phase4c_populate_scenarios", methods=["GET", "POST"])
def phase4c_populate_scenarios_route(req: func.HttpRequest) -> func.HttpResponse:
    """
    Populate scenario wiki pages with troubleshooting flowchart + nuggets.
    Examples:
      - /api/phase4c_populate_scenarios
      - /api/phase4c_populate_scenarios?limit=20&workers=4
      - /api/phase4c_populate_scenarios?min_members=1&min_usefulness=0.3&child_min_members=1
    """
    return run_phase4c_populate_scenarios(req)

@app.route(route="phase4d_populate_topics", methods=["GET", "POST"])
def phase4d_populate_topics_route(req: func.HttpRequest) -> func.HttpResponse:
    """
    Populate topic wiki pages with service architecture, key terms, and scenario map.
    Examples:
      - /api/phase4d_populate_topics
      - /api/phase4d_populate_topics?limit=15&workers=4
      - /api/phase4d_populate_topics?min_members=1&require_child_wiki=0
    """
    return run_phase4d_populate_topics(req)