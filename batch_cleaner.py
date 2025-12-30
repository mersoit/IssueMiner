import os
import json
import re
import hashlib
import datetime as dt
from datetime import timezone
from typing import Optional, Tuple

import traceback
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import pandas as pd
import pyodbc


# ============================================================
# Phase 1: Deterministic cleaning (NON-GREEDY, LINE-SAFE)
# ============================================================

UTC = timezone.utc


def _norm_newlines(s: str) -> str:
    return (s or "").replace("\ufeff", "").replace("\r\n", "\n").replace("\r", "\n")


def normalize_ws(text: str) -> str:
    """
    Preserve line breaks (important for code blocks), but:
      - trim trailing spaces per line
      - collapse long runs of spaces/tabs
      - collapse 3+ blank lines to 2
    """
    t = _norm_newlines(text)
    t = "\n".join([ln.rstrip() for ln in t.split("\n")])
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


# Lines that are basically UI-only and safe to remove when they appear alone.
# (We intentionally avoid removing short words like "Follow" when embedded in a sentence.)
NOISE_LINE_EXACT = {
    "Add",
    "Add comment",
    "Comment",
    "Discard draft",
    "Insert image",
    "Cancel",
    "Add file",
    "Post your answer",
    "Your answer",
    "Sign in to comment",
    "Sign in to answer",
    "Sign in to follow",
    "Follow question",
    "I have the same question",
    "No comments",
    "Report a concern",
    "Loading...",
    "Do not use comments to answer questions.Loading...",
    "Use answers to provide solutions to the user's question.Loading...",
    "Was this answer helpful?",
    "Yes",
    "No",
    "Most helpful",
    "Newest",
    "Oldest",
}

# Regex patterns for UI-only lines (still non-greedy: line-based).
NOISE_LINE_REGEX = [
    # Share / social buttons
    r"^\s*Add to Collections\s*$",
    r"^\s*Add to plan\s*$",
    r"^\s*Share via\s*$",
    r"^\s*(Facebook|x\.com|LinkedIn|Email)\s*$",

    # Reputation / badges / votes / counts
    r"^\s*\d[\d,]*\s*Reputation points\s*$",
    r"^\s*•\s*Moderator\s*$",
    r"^\s*\d+\s*votes\s*$",
    r"^\s*\d+\s*\{count\}\s*votes\s*$",
    r"^\s*\d+\s*comments?\s*$",
    r"^\s*Hide comments for this question\s*$",
    r"^\s*Show comments for this answer\s*$",
    r"^\s*Show comments for this question\s*$",
    r"^\s*No comments\s*$",

    # Editor toolbar garbage that often gets concatenated into one line
    r"^\s*Heading\s*2Heading\s*3Heading\s*4.*View Markdown\s*$",
    r"^\s*ColumnRemove columnInsert column beforeInsert column afterRowRemove rowInsert row afterInsert row beforeView Markdown\s*$",

    # Upload widget lines
    r"^\s*Browse,\s*drag\s*&\s*drop,\s*or\s*paste\s*an\s*image.*$",
    r"^\s*Browse,\s*drag\s*&\s*drop,\s*or\s*paste\s*a\s*file.*$",

    # Helpful-vote footer fluff
    r"^\s*\d+\s+person\s+found\s+this\s+answer\s+helpful\.\s*$",
]

# These headings are usually footer sections that are NOT part of the Q/A content.
# We only cut when they appear as a standalone heading line.
FOOTER_SECTION_HEADINGS = {
    "Additional resources",
    "Training",
    "Documentation",
    "Module",
    "Certification",
    "Show 2 more",
    "Show more",
}

# Standalone "Copy" UI label (keep code below it)
COPY_LINE_RE = re.compile(r"(?m)^\s*Copy\s*$", re.IGNORECASE)


def _is_noise_line(ln: str) -> bool:
    s = ln.strip()
    if not s:
        return False

    # exact-match UI strings
    if s in NOISE_LINE_EXACT:
        return True

    # "Follow" is common; only remove when it's the entire line
    if s.lower() == "follow":
        return True

    # Apply regex line patterns
    for pat in NOISE_LINE_REGEX:
        if re.match(pat, s, flags=re.IGNORECASE):
            return True

    # Common “Sort by:” / “Sort by:” labels
    if re.match(r"^\s*Sort by:\s*$", s, flags=re.IGNORECASE):
        return True

    return False


def _cut_footer_sections(lines: list[str]) -> list[str]:
    """
    Cut noisy footer resources only if:
      - the heading appears as a standalone line, AND
      - it occurs after we've already seen meaningful content.
    This avoids deleting legit content that might contain similar words.
    """
    seen_meaningful = False
    for i, ln in enumerate(lines):
        s = ln.strip()
        if s:
            # call it "meaningful" once we hit a non-noise line
            if not _is_noise_line(s):
                seen_meaningful = True

        if seen_meaningful and s in FOOTER_SECTION_HEADINGS:
            return lines[:i]
    return lines


def clean_content(raw: str) -> str:
    """
    Goal:
      - Keep question + answers + comments + links + code.
      - Remove repeated UI-only artifacts.
      - Avoid greedy regex that can delete real text.
    """
    if not raw:
        return ""

    t = _norm_newlines(raw)

    # Remove standalone "Copy" lines (keep the code that follows)
    t = COPY_LINE_RE.sub("", t)

    # Process line-by-line so we don't nuke paragraphs/code
    lines = t.split("\n")

    # 1) Drop obvious UI-only lines
    kept: list[str] = []
    for ln in lines:
        if _is_noise_line(ln):
            continue
        kept.append(ln)

    # 2) Cut footer resource sections (optional but recommended)
    kept = _cut_footer_sections(kept)

    # 3) Some scrapes have tons of empty tab-indented lines; normalize gently
    out = "\n".join(kept)

    # 4) Cleanup residual weird whitespace sequences while preserving newlines
    out = out.replace("\t", " ")
    out = normalize_ws(out)

    return out


def trim_for_llm(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    head = text[:8000]
    tail = text[-3000:]
    return normalize_ws(head + "\n\n--- TRUNCATED ---\n\n" + tail)


def sha256_bytes(s: str) -> bytes:
    return hashlib.sha256((s or "").encode("utf-8", errors="ignore")).digest()


# ============================================================
# Datetime helpers (UTC-aware)
# ============================================================
def to_utc_aware(d: Optional[dt.datetime]) -> Optional[dt.datetime]:
    """Return timezone-aware UTC datetime (or None)."""
    if d is None:
        return None
    if d.tzinfo is None:
        # treat naive values as UTC
        return d.replace(tzinfo=UTC)
    return d.astimezone(UTC)


def utc_floor_1900() -> dt.datetime:
    return dt.datetime(1900, 1, 1, tzinfo=UTC)


# ============================================================
# SQL helpers
# ============================================================
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
        return utc_floor_1900(), 0

    last_dt = to_utc_aware(row[0]) or utc_floor_1900()
    last_qid = int(row[1] or 0)
    return last_dt, last_qid


def update_watermark(cnx: pyodbc.Connection, pipeline_name: str, last_dt: dt.datetime, last_qid: int, status: str):
    last_dt = to_utc_aware(last_dt) or utc_floor_1900()
    cur = cnx.cursor()
    cur.execute("""
        UPDATE dbo.PipelineState
        SET LastDateCreatedUtc=?, LastQuestionID=?, LastRunUtc=SYSUTCDATETIME(), Status=?
        WHERE PipelineName=?
    """, last_dt, int(last_qid), status, pipeline_name)
    cnx.commit()


def upsert_thread_clean(
    cnx: pyodbc.Connection,
    qid: int,
    created_utc: Optional[dt.datetime],
    asker: Optional[str],
    url: Optional[str],
    raw_hash: bytes,
    clean_hash: bytes,
    content_clean: str,
    content_for_llm: str
):
    created_utc = to_utc_aware(created_utc) if created_utc else None

    cur = cnx.cursor()
    cur.execute("""
    MERGE dbo.ThreadsClean AS t
    USING (SELECT ? AS QuestionID) AS s
    ON t.QuestionID = s.QuestionID
    WHEN MATCHED THEN
      UPDATE SET DateCreatedUtc=?, AskerName=?, SourceUrl=?, ContentRawHash=?, CleanHash=?,
                 ContentClean=?, ContentForLLM=?, ProcessedUtc=SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
      INSERT (QuestionID, DateCreatedUtc, AskerName, SourceUrl, ContentRawHash, CleanHash,
              ContentClean, ContentForLLM)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """,
    qid,
    created_utc, asker, url, raw_hash, clean_hash, content_clean, content_for_llm,
    qid, created_utc, asker, url, raw_hash, clean_hash, content_clean, content_for_llm
    )
    cnx.commit()


# ============================================================
# Blob + Parquet read
# ============================================================
def list_parquet_blobs(bsc: BlobServiceClient, container: str, prefix: str):
    cc = bsc.get_container_client(container)
    for blob in cc.list_blobs(name_starts_with=prefix):
        if blob.name.lower().endswith(".parquet"):
            yield blob.name


def read_parquet_blob_to_df(bsc: BlobServiceClient, container: str, blob_name: str) -> pd.DataFrame:
    bc = bsc.get_blob_client(container=container, blob=blob_name)
    data = bc.download_blob().readall()
    import io
    return pd.read_parquet(io.BytesIO(data), engine="pyarrow")


# ============================================================
# Azure Function entry
# ============================================================
def run_batch_clean(req: func.HttpRequest) -> func.HttpResponse:
    try:
        pipeline_name = os.getenv("PIPELINE_NAME", "phase1_clean_appservice_qna")
        raw_container = os.environ["RAW_CONTAINER"]
        raw_prefix = os.environ.get("RAW_PREFIX", "")
        max_rows = int(os.getenv("MAX_ROWS_PER_RUN", "2000"))
        max_chars = int(os.getenv("MAX_CONTENT_CHARS", "12000"))

        force_full = (req.params.get("full", "0") == "1")

        bsc = BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])

        processed = 0
        newest_dt = utc_floor_1900()
        newest_qid = 0

        try:
            with sql_connect() as cnx:
                last_dt, last_qid = get_watermark(cnx, pipeline_name)
                last_dt_ts = pd.Timestamp(last_dt)  # tz-aware UTC

                for blob_name in list_parquet_blobs(bsc, raw_container, raw_prefix):
                    df = read_parquet_blob_to_df(bsc, raw_container, blob_name)

                    # normalize columns
                    if "dateCreated" in df.columns:
                        df["dateCreated"] = pd.to_datetime(df["dateCreated"], errors="coerce", utc=True)
                    else:
                        df["dateCreated"] = pd.NaT

                    if "questionID" not in df.columns:
                        df["questionID"] = 0

                    # stable progress
                    try:
                        df = df.sort_values(["dateCreated", "questionID"], ascending=True, na_position="first")
                    except Exception:
                        pass

                    # incremental filter
                    if not force_full:
                        dt_series = df["dateCreated"].fillna(pd.Timestamp("1900-01-01", tz="UTC"))
                        qid_series = df["questionID"].fillna(0).astype("int64")
                        df = df[(dt_series > last_dt_ts) | (qid_series > int(last_qid))]

                    for _, row in df.iterrows():
                        if processed >= max_rows:
                            update_watermark(cnx, pipeline_name, newest_dt, newest_qid, "PartialRun_MaxRows")
                            return func.HttpResponse(
                                json.dumps({
                                    "status": "partial",
                                    "processed": processed,
                                    "watermark": {
                                        "last_dateCreated_utc": newest_dt.isoformat(),
                                        "last_questionID": newest_qid
                                    }
                                }),
                                mimetype="application/json"
                            )

                        qid = int(row.get("questionID", 0) or 0)
                        url = row.get("URL")
                        asker = row.get("AskerName")

                        created = row.get("dateCreated")
                        created_dt = None
                        if pd.notna(created):
                            created_dt = to_utc_aware(created.to_pydatetime())

                        raw = row.get("ContentRaw") or ""
                        raw_hash = sha256_bytes(raw)

                        cleaned = clean_content(raw)
                        content_for_llm = trim_for_llm(cleaned, max_chars)
                        clean_hash = sha256_bytes(cleaned)

                        upsert_thread_clean(
                            cnx, qid, created_dt, asker, url,
                            raw_hash, clean_hash, cleaned, content_for_llm
                        )

                        processed += 1
                        if created_dt and created_dt > newest_dt:
                            newest_dt = created_dt
                        if qid > newest_qid:
                            newest_qid = qid

                status = "OK" if processed > 0 else "OK_NoNewRows"
                update_watermark(
                    cnx, pipeline_name,
                    newest_dt if newest_dt.year > 1900 else last_dt,
                    newest_qid if newest_qid > 0 else last_qid,
                    status
                )

            return func.HttpResponse(
                json.dumps({
                    "status": "ok",
                    "processed": processed,
                    "watermark": {
                        "last_dateCreated_utc": newest_dt.isoformat(),
                        "last_questionID": newest_qid
                    }
                }),
                mimetype="application/json"
            )

        except Exception:
            return func.HttpResponse(
                json.dumps({"status": "error", "error": "phase1_failed", "trace": traceback.format_exc()}),
                mimetype="application/json",
                status_code=500
            )

    except Exception:
        return func.HttpResponse(
            "ERROR:\n" + traceback.format_exc(),
            status_code=500
        )
