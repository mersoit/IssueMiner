"""
Emergent Incident Monitor
=========================
Daily timer-triggered function that:
  1. Checks each active EmergentIncident for new confirming threads.
  2. Updates LastThreadSeenUtc and ThreadCount.
  3. Flags incidents as 'possibly_resolved' if no new threads have been
     seen for EMERGENT_STALE_DAYS (default: 14 days).
  4. Flags as 'resolved' if stale for EMERGENT_RESOLVED_DAYS (default: 30).
  5. Updates ADO Wiki work item status if AdoWorkItemId is set.
"""

import os
import json
import logging
import datetime as dt
from typing import Any, Dict, List, Optional

import pyodbc
import azure.functions as func

from aoai_helpers import sql_connect as _aoai_sql_connect

_STALE_DAYS     = int(os.getenv("EMERGENT_STALE_DAYS", "14"))
_RESOLVED_DAYS  = int(os.getenv("EMERGENT_RESOLVED_DAYS", "30"))


def sql_connect() -> pyodbc.Connection:
    return _aoai_sql_connect(autocommit=False)


def _refresh_incident_thread_stats(cnx: pyodbc.Connection) -> int:
    """
    Update ThreadCount and LastThreadSeenUtc on all active incidents
    from the EmergentIncidentThreads join. Returns rows updated.
    """
    cur = cnx.cursor()
    cur.execute("""
        UPDATE ei
        SET
            ei.ThreadCount      = stats.cnt,
            ei.LastThreadSeenUtc = stats.last_ts,
            ei.FirstThreadSeenUtc = CASE
                WHEN stats.first_ts < ei.FirstThreadSeenUtc
                THEN stats.first_ts ELSE ei.FirstThreadSeenUtc END,
            ei.LastUpdatedUtc   = SYSUTCDATETIME()
        FROM dbo.EmergentIncidents ei
        JOIN (
            SELECT
                eit.IncidentID,
                COUNT(*)        AS cnt,
                MIN(COALESCE(eit.ThreadCreatedAt, eit.AddedUtc)) AS first_ts,
                MAX(COALESCE(eit.ThreadCreatedAt, eit.AddedUtc)) AS last_ts
            FROM dbo.EmergentIncidentThreads eit
            GROUP BY eit.IncidentID
        ) stats ON stats.IncidentID = ei.IncidentID
        WHERE ei.Status IN ('active', 'possibly_resolved');
    """)
    return int(cur.rowcount or 0)


def _flag_stale_incidents(cnx: pyodbc.Connection, stale_days: int, resolved_days: int) -> Dict[str, int]:
    """
    Transition incident status based on time since last thread seen.
    Returns counts of each transition.
    """
    cur = cnx.cursor()
    now = dt.datetime.utcnow()
    stale_cutoff    = now - dt.timedelta(days=stale_days)
    resolved_cutoff = now - dt.timedelta(days=resolved_days)

    # active -> possibly_resolved
    cur.execute("""
        UPDATE dbo.EmergentIncidents
        SET Status        = 'possibly_resolved',
            LastUpdatedUtc = SYSUTCDATETIME()
        WHERE Status = 'active'
          AND LastThreadSeenUtc < ?
          AND LastThreadSeenUtc >= ?
    """, stale_cutoff, resolved_cutoff)
    to_possibly = int(cur.rowcount or 0)

    # active OR possibly_resolved -> resolved
    cur.execute("""
        UPDATE dbo.EmergentIncidents
        SET Status        = 'resolved',
            ResolvedUtc   = COALESCE(ResolvedUtc, SYSUTCDATETIME()),
            LastUpdatedUtc = SYSUTCDATETIME()
        WHERE Status IN ('active', 'possibly_resolved')
          AND LastThreadSeenUtc < ?
    """, resolved_cutoff)
    to_resolved = int(cur.rowcount or 0)

    # Reactivate: if a resolved incident gets new threads within STALE_DAYS, reopen it
    cur.execute("""
        UPDATE dbo.EmergentIncidents
        SET Status        = 'active',
            ResolvedUtc   = NULL,
            LastUpdatedUtc = SYSUTCDATETIME()
        WHERE Status = 'resolved'
          AND LastThreadSeenUtc >= ?
    """, stale_cutoff)
    reopened = int(cur.rowcount or 0)

    return {
        "to_possibly_resolved": to_possibly,
        "to_resolved": to_resolved,
        "reopened": reopened,
    }


def _fetch_incidents_needing_wiki_update(cnx: pyodbc.Connection) -> List[Dict[str, Any]]:
    """Fetch incidents whose Status changed since last wiki push."""
    cur = cnx.cursor()
    cur.execute("""
        SELECT IncidentID, IncidentKey, Product, Title, Status,
               ThreadCount, LastThreadSeenUtc, FirstThreadSeenUtc,
               WikiPath, AdoWorkItemId, Confidence
        FROM dbo.EmergentIncidents
        WHERE Status IN ('possibly_resolved', 'resolved', 'active')
          AND WikiPath IS NOT NULL
          AND LastUpdatedUtc > DATEADD(HOUR, -25, SYSUTCDATETIME())
        ORDER BY LastUpdatedUtc DESC
    """)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def run_emergent_monitor(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP or timer-triggered daily monitor.
    Call manually or bind to a TimerTrigger in function_app.py.
    """
    try:
        stale_days    = int(req.params.get("stale_days",    str(_STALE_DAYS)))
        resolved_days = int(req.params.get("resolved_days", str(_RESOLVED_DAYS)))

        with sql_connect() as cnx:
            refreshed = _refresh_incident_thread_stats(cnx)
            transitions = _flag_stale_incidents(cnx, stale_days, resolved_days)
            wiki_pending = _fetch_incidents_needing_wiki_update(cnx)
            cnx.commit()

        # TODO: for each incident in wiki_pending with AdoWorkItemId set,
        # call ado_devops.update_work_item to add a comment or change state.
        # Placeholder for now — wire up when ADO work item creation is ready.
        wiki_updates = []
        for inc in wiki_pending:
            wiki_updates.append({
                "incident_key": inc.get("IncidentKey"),
                "status":       inc.get("Status"),
                "thread_count": inc.get("ThreadCount"),
                "ado_item":     inc.get("AdoWorkItemId"),
                "action":       "pending_ado_update",
            })

        return func.HttpResponse(
            json.dumps({
                "status":           "ok",
                "stats_refreshed":  refreshed,
                "transitions":      transitions,
                "wiki_pending":     len(wiki_updates),
                "wiki_updates":     wiki_updates,
                "stale_days":       stale_days,
                "resolved_days":    resolved_days,
            }, ensure_ascii=False, default=str),
            mimetype="application/json",
        )

    except Exception as e:
        logging.exception("emergent_monitor failed")
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}, ensure_ascii=False),
            status_code=500,
            mimetype="application/json",
        )