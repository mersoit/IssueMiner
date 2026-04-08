# -*- coding: utf-8 -*-
"""
phase0_crawl_threads.py  -  Crawl Microsoft Q&A tag pages -> ThreadsClean + xlsx

Workflow
--------
1. Fetch page-1 of a tag URL, detect last page from pagination links.
2. Walk all listing pages, collect per-question links.
3. Fetch each question page: title, body, asker, date.
4. Clean via batch_cleaner, upsert into dbo.ThreadsClean.
5. Optionally export to .xlsx (avoids CSV delimiter issues with messy HTML).

Callable as:
  - Azure Function  /api/phase0_crawl?url=...&max_pages=200
  - CLI             python phase0_crawl_threads.py --url "..." --export out.xlsx
  - Dashboard       from phase0_crawl_threads import crawl_product
"""

import os
import re
import json
import html as html_mod
import time
import logging
import datetime as dt
import traceback
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests
import pyodbc
import pandas as pd

try:
    import azure.functions as func
except ImportError:
    func = None

from batch_cleaner import (
    clean_content,
    trim_for_llm,
    sha256_bytes,
    upsert_thread_clean,
    to_utc_aware,
)

# -----------------------------------------------------------
# Config
# -----------------------------------------------------------
_REQUEST_TIMEOUT = int(os.getenv("P0_REQUEST_TIMEOUT", "30"))
_REQUEST_DELAY   = float(os.getenv("P0_REQUEST_DELAY", "0.5"))
_MAX_CONTENT_CHARS = int(os.getenv("MAX_CONTENT_CHARS", "12000"))
_USER_AGENT = os.getenv(
    "P0_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
)
_HEADERS = {"User-Agent": _USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}


# -----------------------------------------------------------
# SQL helpers (mirrors batch_cleaner but reads local.settings)
# -----------------------------------------------------------
def _get_connection_string() -> str:
    cs = os.environ.get("SQL_CONNECTION_STRING")
    if cs:
        return cs
    try:
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "local.settings.json")
        with open(p, "r", encoding="utf-8-sig") as f:
            cs = json.load(f).get("Values", {}).get("SQL_CONNECTION_STRING")
            if cs:
                return cs
    except Exception:
        pass
    raise RuntimeError("SQL_CONNECTION_STRING not found")


def _sql_connect() -> pyodbc.Connection:
    return pyodbc.connect(_get_connection_string(), timeout=30)


# -----------------------------------------------------------
# HTTP fetch with retry
# -----------------------------------------------------------
def _fetch(url: str, session: Optional[requests.Session] = None) -> str:
    s = session or requests.Session()
    for attempt in range(3):
        try:
            resp = s.get(url, headers=_HEADERS, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt < 2:
                logging.warning(
                    "phase0 fetch retry %d url=%s err=%s",
                    attempt + 1, url[:120], str(e)[:200],
                )
                time.sleep(2 * (attempt + 1))
            else:
                raise
    return ""


# -----------------------------------------------------------
# Pagination detection
# -----------------------------------------------------------
def _extract_last_page(html: str) -> int:
    """
    Detect last page from pagination links.
    Mirrors the Web Scraper selector:
      a.pagination-next.pagination-link
    and also picks up numbered pagination-link anchors.
    """
    nums = re.findall(
        r'<a[^>]*class="[^"]*pagination-link[^"]*"[^>]*>\s*(\d+)\s*</a>',
        html, re.IGNORECASE,
    )
    if nums:
        return max(int(n) for n in nums)
    params = re.findall(r'[?&]page=(\d+)', html)
    if params:
        return max(int(n) for n in params)
    return 1


def _build_page_url(base_url: str, page: int) -> str:
    parsed = urlparse(base_url)
    qs = parse_qs(parsed.query)
    qs["page"] = [str(page)]
    return urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))


# -----------------------------------------------------------
# Listing-page scraping
# -----------------------------------------------------------
# Mirrors Web Scraper selector:
#   div.box.margin-bottom-xxs h2.title a[href*="/en-us/answers/questions/"]

def _extract_question_links(html: str) -> List[Dict[str, str]]:
    link_pattern = (
        r'<a\s[^>]*href="([^"]*?/en-us/answers/questions/\d+[^"]*)"'
        r'[^>]*>([^<]+)</a>'
    )
    results: List[Dict[str, str]] = []
    seen: set = set()
    for m in re.finditer(link_pattern, html, re.IGNORECASE):
        href = m.group(1).strip()
        title = html_mod.unescape(m.group(2).strip())
        if href.startswith("/"):
            href = "https://learn.microsoft.com" + href
        if href in seen:
            continue
        seen.add(href)

        # The listing page renders each question card with the
        # "asked" date in a <local-time> element in a sibling
        # column AFTER the question title link (~3000+ chars away).
        asked_date = None
        after = html[m.end():m.end() + 5000]
        dm = re.search(
            r'asked\s*<local-time[^>]+datetime="'
            r'(20\d{2}-\d{2}-\d{2}T[\d:.]+(?:\+[\d:]+|Z)?)"',
            after, re.IGNORECASE | re.DOTALL,
        )
        if dm:
            asked_date = _parse_iso_dt(dm.group(1))

        results.append({"url": href, "title": title, "asked_date": asked_date})
    return results


def _extract_question_id(url: str) -> str:
    """qa12345 format matching the rest of the pipeline."""
    m = re.search(r'/questions/(\d+)', url)
    return "qa{}".format(m.group(1)) if m else ""


# -----------------------------------------------------------
# Question-page scraping  (via JSON-LD)
# -----------------------------------------------------------
# The MS Q&A pages are JS-rendered: elements like <local-time>,
# a.profile-url, div.modular-content-container only exist after
# JavaScript runs.  However the server always embeds a JSON-LD
# <script type="application/ld+json"> with a QAPage schema that
# contains all the data we need.

def _strip_html(raw: str) -> str:
    """HTML fragment -> plain text, preserving paragraph structure."""
    t = raw
    t = re.sub(r'<script[^>]*>.*?</script>', '', t,
               flags=re.DOTALL | re.IGNORECASE)
    t = re.sub(r'<style[^>]*>.*?</style>', '', t,
               flags=re.DOTALL | re.IGNORECASE)
    t = re.sub(r'<br\s*/?>', '\n', t, flags=re.IGNORECASE)
    for tag in ('p', 'div', 'li', 'tr', 'h1', 'h2', 'h3',
                'h4', 'h5', 'h6', 'blockquote'):
        t = re.sub(
            '</{}>'.format(tag), '\n', t, flags=re.IGNORECASE,
        )
    t = re.sub(r'<[^>]+>', ' ', t)
    t = html_mod.unescape(t)
    t = re.sub(r'[ \t]+', ' ', t)
    t = re.sub(r'\n{3,}', '\n\n', t)
    return t.strip()


def _parse_iso_dt(raw: Optional[str]) -> Optional[dt.datetime]:
    """Parse an ISO datetime string, tolerating Z and >6 fractional-second digits."""
    if not raw:
        return None
    try:
        s = raw.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # Python 3.10 fromisoformat only supports up to 6 fractional
        # digits, but MS Q&A emits 7 (e.g. .9733333).  Truncate.
        s = re.sub(r'(\d{2}:\d{2}:\d{2}\.\d{6})\d+', r'\1', s)
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def _extract_jsonld(html: str) -> Optional[Dict[str, Any]]:
    """Extract the QAPage JSON-LD block from the raw HTML."""
    for m in re.finditer(
        r'<script[^>]*application/ld\+json[^>]*>(.*?)</script>',
        html, re.DOTALL,
    ):
        try:
            data = json.loads(m.group(1))
            if data.get("@type") == "QAPage":
                return data
        except Exception:
            continue
    return None


def _scrape_question_page(html: str, url: str) -> Dict[str, Any]:
    detail: Dict[str, Any] = {"source_url": url}

    jsonld = _extract_jsonld(html)
    if jsonld:
        me = jsonld.get("mainEntity", {})

        # Title
        detail["title"] = (me.get("name") or "").strip()

        # Author
        author = me.get("author")
        if isinstance(author, dict):
            detail["asker"] = (author.get("name") or "").strip()
        else:
            detail["asker"] = (str(author) if author else "").strip()

        # Body: question text + accepted/suggested answers
        q_text = me.get("text") or ""
        body_parts = [_strip_html(q_text)]
        for ans in me.get("acceptedAnswer", []) + me.get("suggestedAnswer", []):
            a_text = ans.get("text") or ""
            if a_text:
                body_parts.append(_strip_html(a_text))
        detail["body_raw"] = "\n\n---\n\n".join(
            p for p in body_parts if p.strip()
        )

        # Date: JSON-LD may have dateCreated; otherwise pick
        # earliest updatedAt from any answer; finally try <local-time>
        # in the raw HTML (the server does include it, just not as
        # a rendered DOM element).
        asked = _parse_iso_dt(me.get("dateCreated"))
        if not asked:
            asked = _parse_iso_dt(me.get("datePublished"))
        if not asked:
            answer_dates = []
            for ans in me.get("acceptedAnswer", []) + me.get("suggestedAnswer", []):
                d = _parse_iso_dt(ans.get("updatedAt")) or _parse_iso_dt(ans.get("dateCreated"))
                if d:
                    answer_dates.append(d)
            if answer_dates:
                asked = min(answer_dates)
        if not asked:
            # Fallback: first <local-time ... datetime="..."> in raw HTML
            lt = re.search(
                r'<local-time[^>]+datetime="'
                r'(20\d{2}-\d{2}-\d{2}T[\d:.]+(?:\+[\d:]+|Z)?)"',
                html, re.IGNORECASE,
            )
            if lt:
                asked = _parse_iso_dt(lt.group(1))
        detail["asked_utc"] = asked
    else:
        # Fallback: regex on raw HTML (less reliable)
        logging.warning("phase0 no JSON-LD found for %s", url[:120])

        m = re.search(r'<h1[^>]*>([^<]+)</h1>', html, re.IGNORECASE)
        detail["title"] = html_mod.unescape(m.group(1).strip()) if m else ""
        detail["asker"] = ""
        detail["asked_utc"] = None

        # Try to get any text content
        m = re.search(
            r'</h1>(.*?)(?:<footer|<div[^>]+class="[^"]*'
            r'(?:additional|related))',
            html, re.DOTALL,
        )
        detail["body_raw"] = _strip_html(m.group(1)) if m else ""

    return detail


# -----------------------------------------------------------
# Existing-ID lookup (incremental crawl)
# -----------------------------------------------------------
def _get_existing_qids(cnx: pyodbc.Connection) -> set:
    cur = cnx.cursor()
    cur.execute("SELECT QuestionID FROM dbo.ThreadsClean")
    return {str(row[0]) for row in cur.fetchall()}


# -----------------------------------------------------------
# Core crawl function
# -----------------------------------------------------------
def crawl_tag_url(
    tag_url: str,
    max_pages: int = 9999,
    session: Optional[requests.Session] = None,
    existing_qids: Optional[set] = None,
) -> List[Dict[str, Any]]:
    """
    Crawl one Microsoft Q&A tag URL end-to-end.

    1. Detect last page from pagination.
    2. Walk listing pages, collect question links (skip existing_qids).
    3. Fetch + parse each question page.

    Returns list of dicts ready for ingest_crawl_results().
    """
    sess = session or requests.Session()
    skip = existing_qids or set()
    logging.info(
        "phase0 START tag=%s max_pages=%d skip=%d",
        tag_url[:100], max_pages, len(skip),
    )

    # -- pagination --
    first_html = _fetch(_build_page_url(tag_url, 1), sess)
    last_page = min(_extract_last_page(first_html), max_pages)
    logging.info("phase0 last_page=%d", last_page)

    # -- listing pages --
    all_links: List[Dict[str, str]] = []
    seen: set = set()
    for pg in range(1, last_page + 1):
        try:
            if pg == 1:
                page_html = first_html
            else:
                time.sleep(_REQUEST_DELAY)
                page_html = _fetch(
                    _build_page_url(tag_url, pg), sess,
                )

            links = _extract_question_links(page_html)
            new_count = 0
            for lnk in links:
                if lnk["url"] in seen:
                    continue
                seen.add(lnk["url"])
                qid = _extract_question_id(lnk["url"])
                if qid in skip:
                    continue
                all_links.append(lnk)
                new_count += 1
            if pg % 10 == 0 or pg == last_page:
                logging.info(
                    "phase0 listing page=%d/%d new=%d total=%d",
                    pg, last_page, new_count, len(all_links),
                )
        except Exception as e:
            logging.error(
                "phase0 listing page=%d error: %s", pg, str(e)[:300],
            )

    logging.info(
        "phase0 question links to scrape: %d", len(all_links),
    )

    # -- question pages --
    results: List[Dict[str, Any]] = []
    for idx, lnk in enumerate(all_links):
        qid = _extract_question_id(lnk["url"])
        if not qid:
            continue
        try:
            time.sleep(_REQUEST_DELAY)
            q_html = _fetch(lnk["url"], sess)
            detail = _scrape_question_page(q_html, lnk["url"])
            detail["question_id"] = qid
            if not detail.get("title"):
                detail["title"] = lnk.get("title", "")

            # Use listing-page date if the question page didn't
            # have one (JSON-LD often omits dateCreated)
            if not detail.get("asked_utc") and lnk.get("asked_date"):
                detail["asked_utc"] = lnk["asked_date"]

            # Skip questions with empty body (matches Data Factory
            # delete-nulls step — don't waste downstream processing)
            if not (detail.get("body_raw") or "").strip():
                logging.debug(
                    "phase0 skip empty body qid=%s", qid,
                )
                continue

            results.append(detail)
        except Exception as e:
            logging.error(
                "phase0 question %s error: %s", qid, str(e)[:300],
            )
        if (idx + 1) % 50 == 0:
            logging.info(
                "phase0 scraped %d/%d", idx + 1, len(all_links),
            )

    logging.info(
        "phase0 DONE tag=%s scraped=%d", tag_url[:80], len(results),
    )
    return results


# -----------------------------------------------------------
# DB ingest (reuses batch_cleaner upsert + cleaning)
# -----------------------------------------------------------
# Mirrors what the Data Factory pipeline did:
#   1. Extract QuestionID from URL, prefix with "qa"
#   2. Drop rows where body is null/empty
#   3. Normalise datetime to UTC
#   4. Run clean_content() on the raw scraped text (strips UI noise)
#   5. Run trim_for_llm() on the cleaned text
#   6. Hash both raw and cleaned for dedup
#   7. Upsert into ThreadsClean

def ingest_crawl_results(
    results: List[Dict[str, Any]],
    cnx: Optional[pyodbc.Connection] = None,
) -> int:
    own = cnx is None
    if own:
        cnx = _sql_connect()
    count = 0
    skipped_empty = 0
    try:
        for r in results:
            qid = r.get("question_id", "")
            if not qid:
                continue

            body = (r.get("body_raw") or "").strip()

            # Skip rows with empty body (same as Data Factory delete-nulls step)
            if not body:
                skipped_empty += 1
                continue

            # Build ContentRaw: title + body, matching the Web Scraper
            # QuestionBody_Raw format that clean_content() expects
            title = (r.get("title") or "").strip()
            asker = (r.get("asker") or "").strip()
            content_raw = body  # the full scraped text is the raw content

            # ContentRawHash is on the raw scraped text (before cleaning)
            raw_hash = sha256_bytes(content_raw)

            # clean_content() strips UI noise (Follow, Add to Collections,
            # Reputation points, etc.) — exactly what batch_cleaner does
            cleaned = clean_content(content_raw)

            # Skip if cleaning left nothing
            if not cleaned.strip():
                skipped_empty += 1
                continue

            content_llm = trim_for_llm(cleaned, _MAX_CONTENT_CHARS)
            clean_hash = sha256_bytes(cleaned)

            # Parse and normalise datetime to UTC
            asked = r.get("asked_utc")
            if isinstance(asked, dt.datetime):
                asked = to_utc_aware(asked)
            else:
                asked = None

            upsert_thread_clean(
                cnx,
                qid=qid,
                created_utc=asked,
                asker=asker or None,
                url=r.get("source_url"),
                raw_hash=raw_hash,
                clean_hash=clean_hash,
                content_clean=cleaned,
                content_for_llm=content_llm,
            )
            count += 1
    finally:
        if own:
            cnx.close()
    logging.info(
        "phase0 ingested %d rows, skipped %d empty", count, skipped_empty,
    )
    return count


# -----------------------------------------------------------
# XLSX export (avoids CSV delimiter issues with HTML content)
# -----------------------------------------------------------
def export_to_xlsx(
    results: List[Dict[str, Any]], path: str,
) -> str:
    rows = []
    for r in results:
        rows.append({
            "question_id": r.get("question_id", ""),
            "title": r.get("title", ""),
            "source_url": r.get("source_url", ""),
            "asked_utc": str(r.get("asked_utc", "")),
            "asker": r.get("asker", ""),
            "body_raw": r.get("body_raw", ""),
        })
    df = pd.DataFrame(rows)
    df.to_excel(path, index=False, engine="openpyxl")
    logging.info("phase0 exported %d rows -> %s", len(rows), path)
    return path


# -----------------------------------------------------------
# High-level: crawl a product (called from dashboard)
# -----------------------------------------------------------
def crawl_product(
    tag_urls: List[str],
    max_pages: int = 9999,
    incremental: bool = True,
    export_path: Optional[str] = None,
    cnx: Optional[pyodbc.Connection] = None,
) -> Dict[str, Any]:
    """
    Crawl multiple tag URLs for one product.
    Called from the dashboard "Save & Crawl" button.
    """
    own = cnx is None
    if own:
        cnx = _sql_connect()

    existing: set = set()
    if incremental:
        existing = _get_existing_qids(cnx)
        logging.info("phase0 existing in DB: %d", len(existing))

    sess = requests.Session()
    all_results: List[Dict[str, Any]] = []

    for tag_url in tag_urls:
        results = crawl_tag_url(
            tag_url,
            max_pages=max_pages,
            session=sess,
            existing_qids=existing,
        )
        all_results.extend(results)
        for r in results:
            qid = r.get("question_id", "")
            if qid:
                existing.add(qid)

    ingested = 0
    if all_results:
        ingested = ingest_crawl_results(all_results, cnx)

    if own:
        cnx.close()

    if export_path and all_results:
        export_to_xlsx(all_results, export_path)

    return {
        "tag_urls": tag_urls,
        "questions_scraped": len(all_results),
        "ingested": ingested,
        "export_path": export_path,
    }


# -----------------------------------------------------------
# Azure Function entry point
# -----------------------------------------------------------
def run_phase0_crawl(req) -> Any:
    """
    /api/phase0_crawl?url=<tag>&max_pages=200&export=1

    url           required, comma-separated for multiple
    max_pages     default 9999
    export        0|1  (default 0)
    ingest        0|1  (default 1)
    incremental   0|1  (default 1)
    """
    try:
        raw = (req.params.get("url") or "").strip()
        if not raw:
            try:
                raw = (req.get_json() or {}).get("url", "").strip()
            except Exception:
                pass
        if not raw:
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "error": "Missing url parameter",
                }),
                status_code=400,
                mimetype="application/json",
            )

        tag_urls = [u.strip() for u in raw.split(",") if u.strip()]
        max_pages = int(req.params.get("max_pages", "9999"))
        do_export = req.params.get("export", "0") == "1"
        do_ingest = req.params.get("ingest", "1") != "0"
        incremental = req.params.get("incremental", "1") != "0"

        cnx = (
            _sql_connect() if (do_ingest or incremental) else None
        )
        existing: set = set()
        if cnx and incremental:
            existing = _get_existing_qids(cnx)

        sess = requests.Session()
        all_results: List[Dict[str, Any]] = []
        for tag_url in tag_urls:
            results = crawl_tag_url(
                tag_url,
                max_pages=max_pages,
                session=sess,
                existing_qids=existing,
            )
            all_results.extend(results)
            for r in results:
                qid = r.get("question_id", "")
                if qid:
                    existing.add(qid)

        ingested = 0
        if do_ingest and all_results and cnx:
            ingested = ingest_crawl_results(all_results, cnx)

        if cnx:
            cnx.close()

        xlsx = None
        if do_export and all_results:
            xlsx = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "crawl_export.xlsx",
            )
            export_to_xlsx(all_results, xlsx)

        return func.HttpResponse(
            json.dumps({
                "status": "ok",
                "tag_urls": tag_urls,
                "questions_scraped": len(all_results),
                "ingested": ingested,
                "export_path": xlsx,
            }),
            mimetype="application/json",
        )
    except Exception as e:
        logging.exception("phase0 crawl error")
        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "error": str(e),
                "trace": traceback.format_exc(),
            }),
            status_code=500,
            mimetype="application/json",
        )


# -----------------------------------------------------------
# CLI
# -----------------------------------------------------------
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Phase 0: Crawl Microsoft Q&A threads",
    )
    parser.add_argument(
        "--url", required=True,
        help="Tag URL (comma-separated for multiple)",
    )
    parser.add_argument("--max-pages", type=int, default=9999)
    parser.add_argument(
        "--export", default=None, help="Export xlsx path",
    )
    parser.add_argument(
        "--no-ingest", action="store_true", help="Skip DB ingest",
    )
    parser.add_argument(
        "--full", action="store_true",
        help="Non-incremental (re-scrape all)",
    )
    args = parser.parse_args()

    # bootstrap env from local.settings.json
    sp = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "local.settings.json",
    )
    if os.path.isfile(sp):
        with open(sp, "r", encoding="utf-8-sig") as fh:
            for k, v in json.load(fh).get("Values", {}).items():
                if k not in os.environ:
                    os.environ[k] = str(v)

    tag_urls = [u.strip() for u in args.url.split(",") if u.strip()]

    db_cnx = None
    existing_set: set = set()
    if not args.no_ingest or not args.full:
        db_cnx = _sql_connect()
        if not args.full:
            existing_set = _get_existing_qids(db_cnx)
            print("Existing in DB: {}".format(len(existing_set)))

    http_sess = requests.Session()
    all_res: List[Dict[str, Any]] = []
    for turl in tag_urls:
        res = crawl_tag_url(
            turl,
            max_pages=args.max_pages,
            session=http_sess,
            existing_qids=existing_set,
        )
        all_res.extend(res)
        for r in res:
            qid = r.get("question_id", "")
            if qid:
                existing_set.add(qid)

    print("Scraped {} questions".format(len(all_res)))

    if not args.no_ingest and all_res and db_cnx:
        n = ingest_crawl_results(all_res, db_cnx)
        print("Ingested {} rows".format(n))

    if db_cnx:
        db_cnx.close()

    if args.export and all_res:
        export_to_xlsx(all_res, args.export)
        print("Exported -> {}".format(args.export))
