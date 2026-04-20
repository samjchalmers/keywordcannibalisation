from __future__ import annotations

import csv
import logging
from datetime import date
from pathlib import Path

from cannibalize.db.store import Store

log = logging.getLogger(__name__)

DEFAULT_COLUMNS = {
    "query": "Top queries",
    "url": "Top pages",
    "clicks": "Clicks",
    "impressions": "Impressions",
    "ctr": "CTR",
    "position": "Position",
    "date": "Date",
}

BATCH_SIZE = 1000


def _parse_ctr(raw: str) -> float:
    val = float(raw.strip().rstrip("%"))
    return val / 100 if val > 1 else val


def import_csv(
    filepath: Path,
    store: Store,
    *,
    query_col: str | None = None,
    url_col: str | None = None,
    clicks_col: str | None = None,
    impressions_col: str | None = None,
    ctr_col: str | None = None,
    position_col: str | None = None,
) -> int:
    col_map = {
        "query": query_col or DEFAULT_COLUMNS["query"],
        "url": url_col or DEFAULT_COLUMNS["url"],
        "clicks": clicks_col or DEFAULT_COLUMNS["clicks"],
        "impressions": impressions_col or DEFAULT_COLUMNS["impressions"],
        "ctr": ctr_col or DEFAULT_COLUMNS["ctr"],
        "position": position_col or DEFAULT_COLUMNS["position"],
    }

    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"Empty CSV file: {filepath}")

        missing = [
            f"{label} (expected column: '{col}')"
            for label, col in col_map.items()
            if col not in reader.fieldnames
        ]
        if missing:
            raise ValueError(
                f"Missing columns in CSV: {', '.join(missing)}. Found columns: {reader.fieldnames}"
            )

        has_date = "Date" in reader.fieldnames or "date" in reader.fieldnames
        date_col = "Date" if "Date" in reader.fieldnames else "date"
        today = date.today().isoformat()

        batch: list[tuple[str, str, float, float, float, float, str]] = []
        count = 0
        skipped = 0

        for lineno, row in enumerate(reader, start=2):
            try:
                record = (
                    row[col_map["query"]].strip(),
                    row[col_map["url"]].strip(),
                    float(row[col_map["clicks"]]),
                    float(row[col_map["impressions"]]),
                    _parse_ctr(row[col_map["ctr"]]),
                    float(row[col_map["position"]]),
                    row[date_col].strip() if has_date else today,
                )
            except (ValueError, KeyError) as e:
                log.warning("skipping row %d: %s", lineno, e)
                skipped += 1
                continue

            batch.append(record)
            if len(batch) >= BATCH_SIZE:
                store.bulk_upsert_query_page_metrics(batch)
                count += len(batch)
                batch.clear()

        if batch:
            store.bulk_upsert_query_page_metrics(batch)
            count += len(batch)

    if skipped:
        log.warning("skipped %d malformed rows", skipped)
    return count
