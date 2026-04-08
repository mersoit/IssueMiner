import base64
import json
import logging
import os
import threading
import time
import urllib.parse
from typing import Any, Dict, List, Optional

import requests

# Global lock to serialise ancestor page creation across threads
_ancestor_lock = threading.Lock()


def _ado_headers() -> Dict[str, str]:
    pat = os.environ["ADO_PAT"]
    token = base64.b64encode(f":{pat}".encode("utf-8")).decode("utf-8")
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
    }


def _org_url() -> str:
    return os.environ["ADO_ORG_URL"].rstrip("/")


def _project() -> str:
    # project name can contain spaces; keep raw here, URL-encode when building URLs
    return os.environ["ADO_PROJECT"]


def _project_url_encoded() -> str:
    return urllib.parse.quote(_project())


def _normalize_fields(fields: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Light normalization:
      - drop None/empty
      - convert tags list/tuple/set -> ';'-separated string
    """
    if not fields:
        return {}

    out: Dict[str, Any] = {}
    for k, v in fields.items():
        if v is None or v == "":
            continue
        if k == "System.Tags" and isinstance(v, (list, tuple, set)):
            out[k] = "; ".join([str(x).strip() for x in v if str(x).strip()])
            continue
        out[k] = v
    return out


# ---- Work Items (WIT) ----
def create_work_item(
    work_item_type: str,
    title: str,
    description_html: str,
    fields: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Creates a work item using JSON Patch.
    Returns the created work item JSON.
    """
    url = f"{_org_url()}/{_project_url_encoded()}/_apis/wit/workitems/${urllib.parse.quote(work_item_type)}?api-version=7.1-preview.3"

    patch = [
        {"op": "add", "path": "/fields/System.Title", "value": title},
        {"op": "add", "path": "/fields/System.Description", "value": description_html},
    ]

    norm = _normalize_fields(fields)
    for k, v in norm.items():
        patch.append({"op": "add", "path": f"/fields/{k}", "value": v})

    r = requests.post(
        url,
        headers={**_ado_headers(), "Content-Type": "application/json-patch+json"},
        json=patch,
        timeout=20
    )
    r.raise_for_status()
    return r.json()


def update_work_item(
    work_item_id: int,
    title: Optional[str],
    description_html: Optional[str],
    fields: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    url = f"{_org_url()}/{_project_url_encoded()}/_apis/wit/workitems/{int(work_item_id)}?api-version=7.1-preview.3"

    patch = []
    if title:
        patch.append({"op": "add", "path": "/fields/System.Title", "value": title})
    if description_html:
        patch.append({"op": "add", "path": "/fields/System.Description", "value": description_html})

    norm = _normalize_fields(fields)
    for k, v in norm.items():
        patch.append({"op": "add", "path": f"/fields/{k}", "value": v})

    r = requests.patch(
        url,
        headers={**_ado_headers(), "Content-Type": "application/json-patch+json"},
        json=patch,
        timeout=20
    )
    r.raise_for_status()
    return r.json()


# ---- Wiki ----
def upsert_wiki_page(
    wiki_id: str,
    path: str,
    markdown_content: str,
) -> Optional[Dict[str, Any]]:
    """
    Creates or updates a wiki page (and auto-creates missing ancestor pages).
    For updates, Azure DevOps may require If-Match with ETag (handled).
    If ancestors are missing, they are created as stub pages.
    Handles WikiPageAlreadyExistsException (500) by falling back to
    GET + PUT-with-ETag update flow.

    Returns the page JSON on success, or None on failure.
    """
    path = (path or "").strip()
    # sanitization
    if "?" in path:
        path = path.split("?", 1)[0]
    if "&api-version=" in path:
        path = path.split("&api-version=", 1)[0]

    if not path.startswith("/"):
        path = "/" + path

    def _url(p: str) -> str:
        query = urllib.parse.urlencode(
            {"path": p, "api-version": "7.1-preview.1"},
            quote_via=urllib.parse.quote,
        )
        return f"{_org_url()}/{_project_url_encoded()}/_apis/wiki/wikis/{urllib.parse.quote(wiki_id)}/pages?{query}"

    def _put(p: str, content: str, etag: Optional[str] = None) -> requests.Response:
        hdrs = dict(_ado_headers())
        if etag:
            hdrs["If-Match"] = etag
        return requests.put(_url(p), headers=hdrs, json={"content": content}, timeout=30)

    def _get(p: str) -> requests.Response:
        return requests.get(_url(p), headers=_ado_headers(), timeout=20)

    def _update_existing(p: str, content: str) -> Optional[requests.Response]:
        """GET existing page to obtain ETag, then PUT with If-Match."""
        g = _get(p)
        if g.status_code == 200:
            etag = g.headers.get("ETag")
            if etag:
                r2 = _put(p, content, etag=etag)
                if r2.status_code in (200, 201):
                    return r2
                # Retry once on 412 (ETag race)
                if r2.status_code == 412:
                    time.sleep(0.5)
                    g2 = _get(p)
                    if g2.status_code == 200:
                        etag2 = g2.headers.get("ETag")
                        if etag2:
                            r3 = _put(p, content, etag=etag2)
                            if r3.status_code in (200, 201):
                                return r3
                logging.warning(
                    "Wiki update failed for path=%s status=%s body=%s",
                    p,
                    r2.status_code,
                    r2.text[:500],
                )
        return None

    def _ensure_ancestors(target_path: str) -> None:
        """
        Walk the path segments and create any missing ancestor pages as stubs.
        Uses a global lock to avoid races when multiple threads push pages
        under the same parent hierarchy simultaneously.
        """
        parts = [seg for seg in target_path.split("/") if seg]
        if len(parts) <= 1:
            return  # no ancestors to create (root-level page)

        with _ancestor_lock:
            for depth in range(1, len(parts)):
                ancestor = "/" + "/".join(parts[:depth])
                g = _get(ancestor)
                if g.status_code == 200:
                    continue  # already exists
                # Create stub page
                stub_content = f"# {parts[depth - 1]}\n\n_This page was auto-created._\n"
                r = _put(ancestor, stub_content)
                if r.status_code in (200, 201):
                    logging.info("Created ancestor wiki page: %s", ancestor)
                elif r.status_code == 500 and "WikiPageAlreadyExistsException" in r.text:
                    # Race condition: another thread created it between our GET and PUT
                    logging.debug("Ancestor already exists (race): %s", ancestor)
                else:
                    logging.warning(
                        "Failed to create ancestor %s: status=%s body=%s",
                        ancestor,
                        r.status_code,
                        r.text[:500],
                    )

    # --- Main flow ---
    try:
        # 1. Ensure ancestor pages exist
        _ensure_ancestors(path)

        # 2. Attempt to create the page (PUT without ETag = create)
        resp = _put(path, markdown_content)

        if resp.status_code in (200, 201):
            return resp.json()

        # 3. If page already exists, ADO returns 500 with WikiPageAlreadyExistsException
        #    or sometimes 409. Fall back to update flow.
        if resp.status_code in (409, 500):
            body_text = resp.text or ""
            if resp.status_code == 409 or "WikiPageAlreadyExistsException" in body_text:
                logging.info(
                    "Wiki page already exists at %s, falling back to update.", path
                )
                updated = _update_existing(path, markdown_content)
                if updated:
                    return updated.json()
                logging.error("Wiki update fallback also failed for path=%s", path)
                return None

        # 4. If 404, the wiki itself might not exist or path is invalid
        if resp.status_code == 404:
            logging.error(
                "Wiki page creation returned 404 for path=%s. "
                "Check wiki_id=%s exists and path is valid. body=%s",
                path,
                wiki_id,
                resp.text[:500],
            )
            return None

        # 5. Any other error
        logging.error(
            "Wiki page creation failed for path=%s status=%s body=%s",
            path,
            resp.status_code,
            resp.text[:500],
        )
        return None

    except requests.exceptions.Timeout:
        logging.error("Wiki request timed out for path=%s", path)
        return None
    except requests.exceptions.RequestException as e:
        logging.error("Wiki request error for path=%s: %s", path, e)
        return None
    except Exception:
        logging.exception("Unexpected error in upsert_wiki_page for path=%s", path)
        return None