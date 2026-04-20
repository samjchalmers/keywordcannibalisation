from __future__ import annotations

import logging
import random
import socket
import time
from pathlib import Path

from cannibalize.db.store import Store

log = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
CREDS_DIR = Path.home() / ".cannibalize"
CLIENT_SECRETS = CREDS_DIR / "credentials.json"
TOKEN_PATH = CREDS_DIR / "token.json"

REQUEST_TIMEOUT_SECONDS = 30
MAX_ATTEMPTS = 3


def _get_service():
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    # googleapiclient uses httplib2, which reads socket defaults for timeout.
    # Set a default so no request hangs indefinitely on network stalls.
    if socket.getdefaulttimeout() is None:
        socket.setdefaulttimeout(REQUEST_TIMEOUT_SECONDS)

    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CLIENT_SECRETS.exists():
                raise FileNotFoundError(
                    f"GSC OAuth client secrets missing at {CLIENT_SECRETS}. "
                    f"Create a Google Cloud OAuth client and save the JSON there."
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRETS), SCOPES)
            creds = flow.run_local_server(port=0)
        CREDS_DIR.mkdir(parents=True, exist_ok=True)
        TOKEN_PATH.write_text(creds.to_json())
    return build("searchconsole", "v1", credentials=creds, cache_discovery=False)


def authenticate() -> None:
    _get_service()


def fetch_query_page_data(
    site_url: str,
    start_date: str,
    end_date: str,
    row_limit: int = 25000,
) -> list[dict]:
    from googleapiclient.errors import HttpError

    service = _get_service()
    all_rows: list[dict] = []
    start_row = 0

    while True:
        body = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": ["query", "page", "date"],
            "rowLimit": row_limit,
            "startRow": start_row,
        }
        for attempt in range(MAX_ATTEMPTS):
            try:
                resp = service.searchanalytics().query(siteUrl=site_url, body=body).execute()
                break
            except HttpError as e:
                status = getattr(e.resp, "status", None)
                transient = status in (429, 500, 502, 503, 504)
                if not transient or attempt == MAX_ATTEMPTS - 1:
                    raise RuntimeError(
                        f"GSC API error {status}: {e}. Check credentials, scopes, and site URL."
                    ) from e
                delay = (2 ** (attempt + 1)) + random.uniform(0, 0.5)
                log.warning(
                    "GSC transient error %s, retry %d/%d in %.1fs",
                    status,
                    attempt + 1,
                    MAX_ATTEMPTS - 1,
                    delay,
                )
                time.sleep(delay)

        rows = resp.get("rows", [])
        if not rows:
            break
        for r in rows:
            query, page, date = r["keys"]
            all_rows.append(
                {
                    "query": query,
                    "url": page,
                    "date": date,
                    "clicks": r.get("clicks", 0.0),
                    "impressions": r.get("impressions", 0.0),
                    "ctr": r.get("ctr", 0.0),
                    "position": r.get("position", 0.0),
                }
            )
        if len(rows) < row_limit:
            break
        start_row += row_limit

    return all_rows


def ingest_gsc(
    site_url: str,
    start_date: str,
    end_date: str,
    store: Store,
) -> int:
    rows = fetch_query_page_data(site_url, start_date, end_date)
    batch = [
        (
            r["query"],
            r["url"],
            r["clicks"],
            r["impressions"],
            r["ctr"],
            r["position"],
            r["date"],
        )
        for r in rows
    ]
    store.bulk_upsert_query_page_metrics(batch)
    return len(rows)
