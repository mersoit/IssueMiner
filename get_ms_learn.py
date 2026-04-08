# -*- coding: utf-8 -*-
"""
get_ms_learn.py  –  Crawl MS Learn doc TOC and extract page content to CSV.

Usage (CLI):
    python get_ms_learn.py \
        --url "https://learn.microsoft.com/en-us/azure/application-gateway/" \
        --product "Application Gateway" \
        --out "appgw_docs.csv"

Usage (from dashboard / Azure Function):
    from get_ms_learn import crawl_ms_learn_docs
    rows = crawl_ms_learn_docs(
        root_url="https://learn.microsoft.com/en-us/azure/application-gateway/",
        product="Application Gateway",
    )
    # rows is a list of dicts: Order, Product, URL, Title, Body

Output CSV columns:
    Order | Product | URL | Title | Body
"""

import argparse
import csv
import html as html_mod
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
_REQUEST_TIMEOUT = 30
_REQUEST_DELAY   = 0.05  # seconds between thread submissions — light rate limiting
_MAX_RETRIES     = 3
_CRAWL_WORKERS   = int(os.getenv("MS_LEARN_CRAWL_WORKERS", "8"))
_MSLEARN_BASE    = "https://learn.microsoft.com"


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helper
# ─────────────────────────────────────────────────────────────────────────────

def _fetch(url: str, session: requests.Session) -> Optional[str]:
    for attempt in range(_MAX_RETRIES):
        try:
            r = session.get(url, headers=_HEADERS, timeout=_REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.text
        except Exception as e:
            if attempt < _MAX_RETRIES - 1:
                wait = 2 ** (attempt + 1)
                logger.warning("Retry %d for %s: %s (wait %ds)", attempt + 1, url[:100], e, wait)
                time.sleep(wait)
            else:
                logger.error("Failed after %d retries: %s — %s", _MAX_RETRIES, url[:100], e)
                return None
    return None


# ─────────────────────────────────────────────────────────────────────────────
# TOC fetching & flattening
# ─────────────────────────────────────────────────────────────────────────────

def _toc_url(root_url: str) -> str:
    """
    Derive the toc.json URL from the root doc URL.
    e.g. https://learn.microsoft.com/en-us/azure/application-gateway/
         -> https://learn.microsoft.com/en-us/azure/application-gateway/toc.json
    """
    parsed = urlparse(root_url)
    path = parsed.path.rstrip("/")
    toc_path = path + "/toc.json"
    return f"{parsed.scheme}://{parsed.netloc}{toc_path}"


def _flatten_toc(node: Any, results: List[Dict[str, str]]) -> None:
    """Recursively flatten the TOC JSON tree into a list of {href, title} dicts."""
    if isinstance(node, list):
        for item in node:
            _flatten_toc(item, results)
    elif isinstance(node, dict):
        href  = (node.get("href")      or "").strip()
        title = (node.get("toc_title") or "").strip()
        if href:
            results.append({"href": href, "title": title})
        _flatten_toc(node.get("children", []), results)
        _flatten_toc(node.get("items",    []), results)


def _resolve_href(href: str, root_url: str) -> Optional[str]:
    """
    Convert a TOC href to an absolute learn.microsoft.com URL.

    Rules:
    - href starting with "https://"           -> keep if on learn.microsoft.com, else skip
    - href starting with "http://"            -> skip (external)
    - href starting with "/"                  -> prepend https://learn.microsoft.com
    - href starting with "./"                 -> this is the root page itself, include
    - relative path (no scheme, no leading /) -> resolve against root_url base
    """
    href = href.strip()
    if not href:
        return None

    # External full URLs
    if href.startswith("http://"):
        return None
    if href.startswith("https://"):
        if not href.startswith(_MSLEARN_BASE):
            return None
        # Strip any toc/bc query params that redirect elsewhere
        return href.split("?")[0]

    # Absolute path on learn.microsoft.com
    if href.startswith("/"):
        return _MSLEARN_BASE + href.split("?")[0]

    # Relative — resolve against root URL (strip trailing slash first)
    base = root_url.rstrip("/") + "/"
    resolved = urljoin(base, href).split("?")[0]
    if not resolved.startswith(_MSLEARN_BASE):
        return None
    return resolved


# ─────────────────────────────────────────────────────────────────────────────
# Page content extraction
# ─────────────────────────────────────────────────────────────────────────────

def _strip_html(raw: str) -> str:
    """Convert HTML fragment to clean plain text."""
    t = raw
    t = re.sub(r'<script[^>]*>.*?</script>', ' ', t, flags=re.DOTALL | re.IGNORECASE)
    t = re.sub(r'<style[^>]*>.*?</style>',  ' ', t, flags=re.DOTALL | re.IGNORECASE)
    # Block elements → newline
    for tag in ("p", "div", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6",
                "br", "blockquote", "pre", "section", "article", "aside"):
        t = re.sub(rf'</?{tag}(?:\s[^>]*)?>',  '\n', t, flags=re.IGNORECASE)
    t = re.sub(r'<[^>]+>', ' ', t)
    t = html_mod.unescape(t)
    t = re.sub(r'[ \t]+', ' ', t)
    t = re.sub(r'\n[ \t]+', '\n', t)
    t = re.sub(r'\n{3,}', '\n\n', t)
    return t.strip()


def _extract_div_by_attr(html: str, attr: str, value: str) -> str:
    """
    Extract the full inner HTML of the first div that has attr="value",
    using depth-tracking to handle nested divs correctly.
    Returns empty string if not found.
    """
    marker = f'{attr}="{value}"'
    idx = html.find(marker)
    if idx < 0:
        return ""
    # Walk back to the opening < of this div tag
    tag_start = html.rfind("<", 0, idx)
    if tag_start < 0:
        return ""
    # Find the end of the opening tag
    tag_end = html.find(">", tag_start)
    if tag_end < 0:
        return ""

    # Now depth-track from just after the opening tag
    content_start = tag_end + 1
    depth = 1
    pos = content_start
    while pos < len(html) and depth > 0:
        next_open  = html.find("<div",  pos)
        next_close = html.find("</div>", pos)
        if next_open  == -1: next_open  = len(html)
        if next_close == -1: next_close = len(html)
        if next_open < next_close:
            depth += 1
            pos = next_open + 4
        else:
            depth -= 1
            pos = next_close + 6

    return html[content_start: pos]


def _extract_title_and_body(html: str) -> Dict[str, str]:
    """
    Extract (title, body) from a MS Learn doc page.

    Title: the <h1> element.
    Body:  the div with data-bi-name="content" (depth-tracked to handle nesting).
           Falls back to everything between </h1> and the first <footer>.
    """
    # --- Title ---
    h1_m = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL | re.IGNORECASE)
    title = _strip_html(h1_m.group(1)).strip() if h1_m else ""

    # --- Body: depth-tracked content div ---
    raw = _extract_div_by_attr(html, "data-bi-name", "content")

    if not raw and h1_m:
        # Fallback: everything between </h1> and first <footer>
        after_h1 = html[h1_m.end():]
        cutoff = re.search(r'<footer', after_h1, re.IGNORECASE)
        raw = after_h1[:cutoff.start()] if cutoff else after_h1[:80000]

    body = _strip_html(raw)

    # The content div includes a toolbar header above the article text.
    # Strip everything before the first heading (h1/h2) in the body.
    body_lines = body.splitlines()
    first_real = 0
    title_lower = title.lower().strip()
    for i, line in enumerate(body_lines):
        stripped = line.strip()
        # The h1 title text appears verbatim — content starts at or after it
        if title_lower and stripped.lower() == title_lower:
            first_real = i + 1
            break
        # Or skip until we hit a non-empty line that looks like article prose
        # (after the noisy toolbar lines)
    if first_real:
        body = "\n".join(body_lines[first_real:])
        body = re.sub(r'\n{3,}', '\n\n', body).strip()

    # Clean up common UI noise that leaks in
    noise_patterns = [
        r'^\s*(Article tested with|Feedback|Was this page helpful|Yes No|In this article|Submit and view feedback)\s*$',
        r'^\s*\d+\s+min(utes?)?\s+to\s+read\s*$',
    ]
    lines = body.splitlines()
    clean_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            clean_lines.append("")
            continue
        noisy = any(re.match(p, stripped, re.IGNORECASE) for p in noise_patterns)
        if not noisy:
            clean_lines.append(stripped)

    body = "\n".join(clean_lines)
    body = re.sub(r'\n{3,}', '\n\n', body).strip()

    return {"title": title, "body": body}


# ─────────────────────────────────────────────────────────────────────────────
# Main crawler
# ─────────────────────────────────────────────────────────────────────────────

def crawl_ms_learn_docs(
    root_url: str,
    product: str = "",
    max_pages: int = 0,
    delay: float = _REQUEST_DELAY,
    skip_external: bool = True,
) -> List[Dict[str, Any]]:
    """
    Crawl all pages listed in the MS Learn TOC for a given doc root URL.

    Args:
        root_url:      Root documentation URL, e.g.
                       "https://learn.microsoft.com/en-us/azure/application-gateway/"
        product:       Product name to put in the Product column.
        max_pages:     If > 0, stop after this many pages (0 = all).
        delay:         Seconds to sleep between page fetches.
        skip_external: Skip hrefs that resolve outside learn.microsoft.com.

    Returns:
        List of dicts with keys: Order, Product, URL, Title, Body
    """
    root_url = root_url.rstrip("/") + "/"
    session  = requests.Session()

    # 1. Fetch TOC
    toc_endpoint = _toc_url(root_url)
    logger.info("Fetching TOC: %s", toc_endpoint)
    toc_text = _fetch(toc_endpoint, session)
    if not toc_text:
        raise RuntimeError(f"Could not fetch TOC from {toc_endpoint}")

    import json
    toc_data = json.loads(toc_text)

    # 2. Flatten TOC into ordered list
    toc_entries: List[Dict[str, str]] = []
    _flatten_toc(toc_data.get("items", []), toc_entries)
    logger.info("TOC entries found: %d", len(toc_entries))

    # 3. Resolve to absolute URLs, deduplicate, preserve order
    seen_urls: set = set()
    pages: List[Dict[str, str]] = []
    for entry in toc_entries:
        abs_url = _resolve_href(entry["href"], root_url)
        if not abs_url:
            continue
        if abs_url in seen_urls:
            continue
        seen_urls.add(abs_url)
        pages.append({"url": abs_url, "toc_title": entry["title"]})

    if max_pages > 0:
        pages = pages[:max_pages]

    logger.info("Unique pages to scrape: %d", len(pages))

    # 4. Fetch each page and extract content — parallel
    def _fetch_page(args: Tuple[int, Dict[str, str]]) -> Optional[Dict[str, Any]]:
        i, page = args
        url = page["url"]
        html = _fetch(url, session)
        if not html:
            logger.warning("Skipping (fetch failed): %s", url)
            return None
        extracted = _extract_title_and_body(html)
        title = extracted["title"] or page["toc_title"] or ""
        body  = extracted["body"]
        logger.info("[%d/%d] %s", i, len(pages), url)
        return {
            "Order":   i,
            "Product": product,
            "URL":     url,
            "Title":   title,
            "Body":    body,
        }

    results: List[Dict[str, Any]] = []
    workers = max(1, min(_CRAWL_WORKERS, len(pages)))

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {}
        for i, page in enumerate(pages, start=1):
            futures[ex.submit(_fetch_page, (i, page))] = i
            if delay > 0:
                time.sleep(delay)          # light stagger between submissions

        for fut in as_completed(futures):
            row = fut.result()
            if row:
                results.append(row)

    # Restore original TOC order
    results.sort(key=lambda r: r["Order"])

    logger.info("Done. %d pages scraped.", len(results))
    return results


# ─────────────────────────────────────────────────────────────────────────────
# CSV export
# ─────────────────────────────────────────────────────────────────────────────

def export_to_csv(rows: List[Dict[str, Any]], out_path: str) -> None:
    """Write rows to a UTF-8 CSV file."""
    if not rows:
        logger.warning("No rows to export.")
        return

    fieldnames = ["Order", "Product", "URL", "Title", "Body"]
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Exported %d rows to %s", len(rows), out_path)


# ─────────────────────────────────────────────────────────────────────────────
# Azure Storage upload
# ─────────────────────────────────────────────────────────────────────────────

def upload_csv_to_blob(csv_bytes: bytes, blob_name: str) -> str:
    """
    Upload CSV bytes to the 'msdocs' container in the storage account
    configured via AzureWebJobsStorage in the environment.

    Returns the blob URL (without SAS) on success.
    Raises RuntimeError if the env var is missing or the upload fails.
    """
    conn_str = os.environ.get("AzureWebJobsStorage") or os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise RuntimeError(
            "AzureWebJobsStorage not set — cannot upload CSV to blob storage."
        )

    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        raise RuntimeError(
            "azure-storage-blob is not installed. "
            "Run: pip install azure-storage-blob"
        )

    container = "msdocs"
    client = BlobServiceClient.from_connection_string(conn_str)

    # Ensure container exists
    container_client = client.get_container_client(container)
    try:
        container_client.get_container_properties()
    except Exception:
        container_client.create_container()
        logger.info("Created blob container: %s", container)

    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(csv_bytes, overwrite=True, content_settings=None)

    account_name = client.account_name
    blob_url = f"https://{account_name}.blob.core.windows.net/{container}/{blob_name}"
    logger.info("Uploaded %d bytes to %s", len(csv_bytes), blob_url)
    return blob_url


# ─────────────────────────────────────────────────────────────────────────────
# Azure AI Search — run indexer
# ─────────────────────────────────────────────────────────────────────────────

def run_search_indexer(indexer_name: str = "rag-apim-swa-indexer") -> Dict[str, Any]:
    """
    Trigger a run of the specified Azure AI Search indexer.

    Uses AZURE_SEARCH_ENDPOINT and AZURE_SEARCH_API_KEY from env.
    Returns a dict with keys: status, indexer_name, http_status, message.
    """
    endpoint = (os.environ.get("AZURE_SEARCH_ENDPOINT") or "").rstrip("/")
    api_key  = os.environ.get("AZURE_SEARCH_API_KEY") or os.environ.get("SEARCH_ADMIN_KEY")

    if not endpoint or not api_key:
        raise RuntimeError(
            "AZURE_SEARCH_ENDPOINT / AZURE_SEARCH_API_KEY not set — cannot run indexer."
        )

    api_version = os.environ.get("AZURE_SEARCH_API_VERSION", "2024-07-01")
    url = f"{endpoint}/indexers/{indexer_name}/run?api-version={api_version}"
    headers = {
        "api-key": api_key,
        "Content-Type": "application/json",
    }

    resp = requests.post(url, headers=headers, timeout=30)

    # 202 Accepted = indexer run queued successfully
    if resp.status_code in (200, 202):
        logger.info("Indexer '%s' run queued (HTTP %d).", indexer_name, resp.status_code)
        return {
            "status": "queued",
            "indexer_name": indexer_name,
            "http_status": resp.status_code,
            "message": "Indexer run queued successfully.",
        }

    logger.error(
        "Indexer run failed: indexer=%s status=%d body=%s",
        indexer_name, resp.status_code, resp.text[:500],
    )
    return {
        "status": "error",
        "indexer_name": indexer_name,
        "http_status": resp.status_code,
        "message": resp.text[:500],
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Crawl MS Learn docs TOC and export content to CSV."
    )
    parser.add_argument(
        "--url", required=True,
        help='Root doc URL, e.g. "https://learn.microsoft.com/en-us/azure/application-gateway/"',
    )
    parser.add_argument(
        "--product", default="",
        help="Product name for the Product column (e.g. 'Application Gateway')",
    )
    parser.add_argument(
        "--out", default="ms_learn_docs.csv",
        help="Output CSV file path (default: ms_learn_docs.csv)",
    )
    parser.add_argument(
        "--max-pages", type=int, default=0,
        help="Max pages to scrape (0 = all)",
    )
    parser.add_argument(
        "--delay", type=float, default=_REQUEST_DELAY,
        help=f"Seconds between requests (default: {_REQUEST_DELAY})",
    )
    args = parser.parse_args()

    rows = crawl_ms_learn_docs(
        root_url=args.url,
        product=args.product,
        max_pages=args.max_pages,
        delay=args.delay,
    )
    export_to_csv(rows, args.out)
    print(f"Done — {len(rows)} pages written to {args.out}")
