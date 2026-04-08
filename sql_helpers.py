import os
import json
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import pyodbc


def sql_connect() -> pyodbc.Connection:
    cs = os.environ["SQL_CONNECTION_STRING"]
    return pyodbc.connect(cs)


def get_watermark(cnx: pyodbc.Connection, pipeline_name: str) -> Tuple[dt.datetime, int]:
    cur = cnx.cursor()
    cur.execute(
        "SELECT LastDateCreatedUtc, LastQuestionID FROM dbo.PipelineState WHERE PipelineName=?",
        pipeline_name
    )
    row = cur.fetchone()
    if not row:
        return dt.datetime(1900, 1, 1, tzinfo=dt.timezone.utc), 0

    last_dt = row[0] or dt.datetime(1900, 1, 1, tzinfo=dt.timezone.utc)
    last_qid = row[1] or 0

    if isinstance(last_dt, dt.datetime) and last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=dt.timezone.utc)

    return last_dt, int(last_qid)


def update_watermark(
    cnx: pyodbc.Connection,
    pipeline_name: str,
    last_dt: dt.datetime,
    last_qid: int,
    status: str
) -> None:
    if isinstance(last_dt, dt.datetime) and last_dt.tzinfo is not None:
        # store as naive UTC for SQL if your column is datetime, not datetimeoffset
        last_dt = last_dt.astimezone(dt.timezone.utc).replace(tzinfo=None)

    cur = cnx.cursor()
    cur.execute("""
        UPDATE dbo.PipelineState
        SET LastDateCreatedUtc=?, LastQuestionID=?, LastRunUtc=SYSUTCDATETIME(), Status=?
        WHERE PipelineName=?
    """, last_dt, int(last_qid), status, pipeline_name)
    cnx.commit()


def fetch_threads_to_enrich(
    cnx: pyodbc.Connection,
    max_rows: int,
    force_full: bool,
    last_dt: dt.datetime,
    last_qid: int
) -> List[Dict[str, Any]]:
    """
    Pull threads from ThreadsClean that are not enriched yet (or all if force_full).
    Adjust column names if yours differ.
    """
    # Convert last_dt to naive UTC for comparison if needed
    if isinstance(last_dt, dt.datetime) and last_dt.tzinfo is not None:
        last_dt_sql = last_dt.astimezone(dt.timezone.utc).replace(tzinfo=None)
    else:
        last_dt_sql = last_dt

    cur = cnx.cursor()

    if force_full:
        cur.execute(f"""
            SELECT TOP ({int(max_rows)})
                QuestionID,
                DateCreatedUtc,
                AskerName,
                SourceUrl,
                ContentClean,
                ContentForLLM
            FROM dbo.ThreadsClean
            ORDER BY QuestionID ASC
        """)
    else:
        # Requires ThreadsEnriched.ProcessedUtc OR store marker on ThreadsClean.
        # This implementation uses ThreadsEnriched existence.
        cur.execute(f"""
            SELECT TOP ({int(max_rows)})
                tc.QuestionID,
                tc.DateCreatedUtc,
                tc.AskerName,
                tc.SourceUrl,
                tc.ContentClean,
                tc.ContentForLLM
            FROM dbo.ThreadsClean tc
            LEFT JOIN dbo.ThreadsEnriched te
                ON te.QuestionID = tc.QuestionID
            WHERE te.QuestionID IS NULL
              AND (
                ISNULL(tc.DateCreatedUtc, '1900-01-01') > ?
                OR tc.QuestionID > ?
              )
            ORDER BY tc.QuestionID ASC
        """, last_dt_sql, int(last_qid))

    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    out: List[Dict[str, Any]] = []

    for r in rows:
        d = dict(zip(cols, r))
        # normalize dt
        if isinstance(d.get("DateCreatedUtc"), dt.datetime) and d["DateCreatedUtc"].tzinfo is None:
            d["DateCreatedUtc"] = d["DateCreatedUtc"].replace(tzinfo=dt.timezone.utc)
        out.append(d)

    return out


def upsert_thread_enriched(
    cnx: pyodbc.Connection,
    question_id: int,
    date_created_utc: Optional[dt.datetime],
    source_url: Optional[str],
    enriched_json: Dict[str, Any],
    usefulness_score: float,
    classification: Optional[str],
    product: Optional[str],
    best_title: Optional[str],
    keywords: List[str],
    topic_sig: str,
    common_sig: str,
    emergent_sig: str
) -> None:
    """
    Writes enriched info for each thread.
    """
    # store JSON string, avoid huge output
    enriched_compact = json.dumps(enriched_json, ensure_ascii=False)

    # datetime handling (store as naive UTC if column is datetime)
    dtu = None
    if isinstance(date_created_utc, dt.datetime):
        if date_created_utc.tzinfo is not None:
            dtu = date_created_utc.astimezone(dt.timezone.utc).replace(tzinfo=None)
        else:
            dtu = date_created_utc

    cur = cnx.cursor()
    cur.execute("""
    MERGE dbo.ThreadsEnriched AS t
    USING (SELECT ? AS QuestionID) AS s
    ON t.QuestionID = s.QuestionID
    WHEN MATCHED THEN
      UPDATE SET
        DateCreatedUtc=?,
        SourceUrl=?,
        Product=?,
        Classification=?,
        UsefulnessScore=?,
        BestTitle=?,
        KeywordsJson=?,
        TopicSigHash=?,
        CommonSigHash=?,
        EmergentSigHash=?,
        EnrichedJson=?,
        ProcessedUtc=SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
      INSERT (
        QuestionID, DateCreatedUtc, SourceUrl,
        Product, Classification, UsefulnessScore,
        BestTitle, KeywordsJson,
        TopicSigHash, CommonSigHash, EmergentSigHash,
        EnrichedJson, ProcessedUtc
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
    """,
    int(question_id),
    dtu, source_url, product, classification, float(usefulness_score), best_title,
    json.dumps(keywords, ensure_ascii=False),
    topic_sig, common_sig, emergent_sig, enriched_compact,
    int(question_id), dtu, source_url, product, classification, float(usefulness_score), best_title,
    json.dumps(keywords, ensure_ascii=False),
    topic_sig, common_sig, emergent_sig, enriched_compact
    )
    cnx.commit()


def find_cluster_by_signature(
    cnx: pyodbc.Connection,
    cluster_type: str,
    signature_hash: str,
    window_days: int
) -> Optional[int]:
    """
    Returns ClusterID if found and within window (for emergent).
    """
    cur = cnx.cursor()
    cur.execute("""
        SELECT TOP 1 ClusterID
        FROM dbo.Clusters
        WHERE ClusterType=? AND SignatureHash=?
          AND (window_days = 0 OR CreatedUtc >= DATEADD(day, -?, SYSUTCDATETIME()))
        ORDER BY CreatedUtc DESC
    """, cluster_type, signature_hash, int(window_days))
    row = cur.fetchone()
    return int(row[0]) if row else None


def upsert_cluster(
    cnx: pyodbc.Connection,
    cluster_type: str,
    signature_hash: str,
    signature_text: str,
    product: Optional[str],
    window_days: int
) -> int:
    """
    If cluster exists (by signature), return it; else create.
    For emergent, window_days matters. For topic/common, set huge window_days.
    """
    existing = find_cluster_by_signature(cnx, cluster_type, signature_hash, window_days)
    if existing:
        # touch updated time
        cur = cnx.cursor()
        cur.execute("""
            UPDATE dbo.Clusters
            SET LastSeenUtc=SYSUTCDATETIME()
            WHERE ClusterID=?
        """, existing)
        cnx.commit()
        return existing

    cur = cnx.cursor()
    cur.execute("""
        INSERT INTO dbo.Clusters (ClusterType, SignatureHash, SignatureText, Product, CreatedUtc, LastSeenUtc)
        OUTPUT INSERTED.ClusterID
        VALUES (?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME())
    """, cluster_type, signature_hash, signature_text, product)
    row = cur.fetchone()
    cnx.commit()
    return int(row[0])


def upsert_cluster_member(
    cnx: pyodbc.Connection,
    cluster_id: int,
    question_id: int
) -> None:
    """
    Adds member if not exists.
    """
    cur = cnx.cursor()
    cur.execute("""
        MERGE dbo.ClusterMembers AS t
        USING (SELECT ? AS ClusterID, ? AS QuestionID) AS s
        ON t.ClusterID=s.ClusterID AND t.QuestionID=s.QuestionID
        WHEN NOT MATCHED THEN
          INSERT (ClusterID, QuestionID, AddedUtc) VALUES (?, ?, SYSUTCDATETIME());
    """, int(cluster_id), int(question_id), int(cluster_id), int(question_id))
    cnx.commit()
