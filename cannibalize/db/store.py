from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

from cannibalize.db.models import SCHEMA


@dataclass
class URLMetrics:
    url: str
    clicks: float
    impressions: float
    ctr: float
    position: float


@dataclass
class QueryCluster:
    query: str
    urls: list[URLMetrics]


@dataclass
class CannibalizationCase:
    id: int | None
    query: str
    urls: list[str]
    case_type: str | None
    severity_score: float | None
    similarity_score: float | None
    position_volatility: float | None
    estimated_click_loss: float | None
    recommendation: str | None
    keep_url: str | None
    status: str = "open"
    detected_at: str | None = None
    fixed_at: str | None = None


class Store:
    def __init__(self, db_path: str) -> None:
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")

    def init_db(self) -> None:
        self.conn.executescript(SCHEMA)

    def close(self) -> None:
        self.conn.close()

    def upsert_page(
        self,
        url: str,
        title: str | None,
        h1: str | None,
        canonical: str | None,
        body_text: str | None,
    ) -> None:
        self.conn.execute(
            """INSERT INTO pages (url, title, h1, canonical, body_text, crawled_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(url) DO UPDATE SET
                   title=excluded.title, h1=excluded.h1,
                   canonical=excluded.canonical, body_text=excluded.body_text,
                   crawled_at=excluded.crawled_at""",
            (url, title, h1, canonical, body_text, _now()),
        )
        self.conn.commit()

    def upsert_query_page_metrics(
        self,
        query: str,
        url: str,
        clicks: float,
        impressions: float,
        ctr: float,
        position: float,
        date: str,
    ) -> None:
        self.conn.execute(
            """INSERT INTO query_page_metrics (query, url, clicks, impressions, ctr, position, date)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(query, url, date) DO UPDATE SET
                   clicks=excluded.clicks, impressions=excluded.impressions,
                   ctr=excluded.ctr, position=excluded.position""",
            (query, url, clicks, impressions, ctr, position, date),
        )
        self.conn.execute(
            "INSERT OR IGNORE INTO queries (query) VALUES (?)", (query,)
        )
        self.conn.commit()

    def mark_branded(self, query: str) -> None:
        self.conn.execute(
            "UPDATE queries SET is_branded = 1 WHERE query = ?", (query,)
        )
        self.conn.commit()

    def get_queries_with_multiple_urls(
        self, min_impressions: int, min_urls: int
    ) -> list[QueryCluster]:
        rows = self.conn.execute(
            """SELECT query, url,
                      SUM(clicks) as total_clicks,
                      SUM(impressions) as total_impressions,
                      AVG(ctr) as avg_ctr,
                      AVG(position) as avg_position
               FROM query_page_metrics
               GROUP BY query, url
               HAVING total_impressions >= ?""",
            (min_impressions,),
        ).fetchall()

        clusters: dict[str, list[URLMetrics]] = {}
        for row in rows:
            clusters.setdefault(row["query"], []).append(
                URLMetrics(
                    url=row["url"],
                    clicks=row["total_clicks"],
                    impressions=row["total_impressions"],
                    ctr=row["avg_ctr"],
                    position=row["avg_position"],
                )
            )

        return [
            QueryCluster(query=q, urls=urls)
            for q, urls in clusters.items()
            if len(urls) >= min_urls
        ]

    def get_daily_positions(self, query: str, url: str) -> list[float]:
        rows = self.conn.execute(
            "SELECT position FROM query_page_metrics WHERE query = ? AND url = ? ORDER BY date",
            (query, url),
        ).fetchall()
        return [row["position"] for row in rows]

    def get_page(self, url: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM pages WHERE url = ?", (url,)
        ).fetchone()
        return dict(row) if row else None

    def save_case(self, case: CannibalizationCase) -> int:
        cursor = self.conn.execute(
            """INSERT INTO cannibalization_cases
               (query, urls, case_type, severity_score, similarity_score,
                position_volatility, estimated_click_loss, recommendation,
                keep_url, status, detected_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                case.query,
                json.dumps(case.urls),
                case.case_type,
                case.severity_score,
                case.similarity_score,
                case.position_volatility,
                case.estimated_click_loss,
                case.recommendation,
                case.keep_url,
                case.status,
                _now(),
            ),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_cases(self, status: str | None = None) -> list[CannibalizationCase]:
        if status:
            rows = self.conn.execute(
                "SELECT * FROM cannibalization_cases WHERE status = ?", (status,)
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM cannibalization_cases"
            ).fetchall()
        return [_row_to_case(row) for row in rows]

    def update_case_status(self, case_id: int, status: str) -> None:
        params: list = [status]
        sql = "UPDATE cannibalization_cases SET status = ?"
        if status == "fixed":
            sql += ", fixed_at = ?"
            params.append(_now())
        sql += " WHERE id = ?"
        params.append(case_id)
        self.conn.execute(sql, params)
        self.conn.commit()

    def save_fix_tracking(
        self, case_id: int, metric: str, before: float, after: float
    ) -> None:
        self.conn.execute(
            """INSERT INTO fix_tracking (case_id, metric, value_before, value_after, measured_at)
               VALUES (?, ?, ?, ?, ?)""",
            (case_id, metric, before, after, _now()),
        )
        self.conn.commit()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_case(row: sqlite3.Row) -> CannibalizationCase:
    return CannibalizationCase(
        id=row["id"],
        query=row["query"],
        urls=json.loads(row["urls"]),
        case_type=row["case_type"],
        severity_score=row["severity_score"],
        similarity_score=row["similarity_score"],
        position_volatility=row["position_volatility"],
        estimated_click_loss=row["estimated_click_loss"],
        recommendation=row["recommendation"],
        keep_url=row["keep_url"],
        status=row["status"],
        detected_at=row["detected_at"],
        fixed_at=row["fixed_at"],
    )
