from __future__ import annotations

import csv
from pathlib import Path

from cannibalize.db.store import CannibalizationCase

COLUMNS = [
    "id",
    "query",
    "urls",
    "case_type",
    "severity_score",
    "similarity_score",
    "position_volatility",
    "estimated_click_loss",
    "keep_url",
    "recommendation",
    "status",
    "detected_at",
    "fixed_at",
]


def export_cases_csv(cases: list[CannibalizationCase], path: Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        for c in cases:
            writer.writerow(
                [
                    c.id,
                    c.query,
                    "|".join(c.urls),
                    c.case_type,
                    c.severity_score,
                    c.similarity_score,
                    c.position_volatility,
                    c.estimated_click_loss,
                    c.keep_url,
                    c.recommendation,
                    c.status,
                    c.detected_at,
                    c.fixed_at,
                ]
            )
