from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

from cannibalize.db.store import Store

DEFAULT_COLUMNS = {
    "query": "Top queries",
    "url": "Top pages",
    "clicks": "Clicks",
    "impressions": "Impressions",
    "ctr": "CTR",
    "position": "Position",
    "date": "Date",
}


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
                f"Missing columns in CSV: {', '.join(missing)}. "
                f"Found columns: {reader.fieldnames}"
            )

        has_date = "Date" in reader.fieldnames or "date" in reader.fieldnames
        date_col = "Date" if "Date" in reader.fieldnames else "date"
        today = date.today().isoformat()
        count = 0

        for row in reader:
            ctr_raw = row[col_map["ctr"]].strip().rstrip("%")
            ctr_val = float(ctr_raw)
            if ctr_val > 1:
                ctr_val = ctr_val / 100

            row_date = row[date_col].strip() if has_date else today

            store.upsert_query_page_metrics(
                query=row[col_map["query"]].strip(),
                url=row[col_map["url"]].strip(),
                clicks=float(row[col_map["clicks"]]),
                impressions=float(row[col_map["impressions"]]),
                ctr=ctr_val,
                position=float(row[col_map["position"]]),
                date=row_date,
            )
            count += 1

    return count
