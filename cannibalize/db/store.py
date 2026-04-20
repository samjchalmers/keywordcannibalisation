from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime

from cannibalize.db.models import apply_migrations


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
        apply_migrations(self.conn)

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> Store:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def upsert_page(
        self,
        url: str,
        title: str | None,
        h1: str | None,
        canonical: str | None,
        body_text: str | None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """INSERT INTO pages (url, title, h1, canonical, body_text, crawled_at)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(url) DO UPDATE SET
                       title=excluded.title, h1=excluded.h1,
                       canonical=excluded.canonical, body_text=excluded.body_text,
                       crawled_at=excluded.crawled_at""",
                (url, title, h1, canonical, body_text, _now()),
            )

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
        with self.conn:
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

    def mark_branded(self, query: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE queries SET is_branded = 1 WHERE query = ?", (query,)
            )

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

    def get_all_urls(self) -> list[str]:
        rows = self.conn.execute(
            "SELECT DISTINCT url FROM query_page_metrics"
        ).fetchall()
        return [row["url"] for row in rows]

    def get_recently_crawled_urls(self, since_iso: str) -> set[str]:
        rows = self.conn.execute(
            "SELECT url FROM pages WHERE crawled_at >= ?", (since_iso,)
        ).fetchall()
        return {row["url"] for row in rows}

    def get_case(self, case_id: int) -> CannibalizationCase | None:
        row = self.conn.execute(
            "SELECT * FROM cannibalization_cases WHERE id = ?", (case_id,)
        ).fetchone()
        return _row_to_case(row) if row else None

    def get_query_metrics_summary(
        self, query: str, since: str | None = None, until: str | None = None
    ) -> list[URLMetrics]:
        sql = (
            "SELECT url, "
            "SUM(clicks) as clicks, "
            "SUM(impressions) as impressions, "
            "SUM(position * impressions) as pos_weight "
            "FROM query_page_metrics WHERE query = ?"
        )
        params: list = [query]
        if since:
            sql += " AND date >= ?"
            params.append(since)
        if until:
            sql += " AND date <= ?"
            params.append(until)
        sql += " GROUP BY url"
        rows = self.conn.execute(sql, params).fetchall()

        out: list[URLMetrics] = []
        for r in rows:
            clicks = r["clicks"] or 0.0
            impressions = r["impressions"] or 0.0
            pos_weight = r["pos_weight"] or 0.0
            ctr = clicks / impressions if impressions else 0.0
            position = pos_weight / impressions if impressions else 0.0
            out.append(
                URLMetrics(
                    url=r["url"],
                    clicks=clicks,
                    impressions=impressions,
                    ctr=ctr,
                    position=position,
                )
            )
        return out

    def bulk_upsert_query_page_metrics(
        self, rows: list[tuple[str, str, float, float, float, float, str]]
    ) -> None:
        if not rows:
            return
        with self.conn:
            self.conn.executemany(
                """INSERT INTO query_page_metrics
                   (query, url, clicks, impressions, ctr, position, date)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(query, url, date) DO UPDATE SET
                       clicks=excluded.clicks, impressions=excluded.impressions,
                       ctr=excluded.ctr, position=excluded.position""",
                rows,
            )
            self.conn.executemany(
                "INSERT OR IGNORE INTO queries (query) VALUES (?)",
                [(r[0],) for r in rows],
            )

    def save_case(self, case: CannibalizationCase) -> int:
        with self.conn:
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
            row_id = cursor.lastrowid
        if row_id is None:
            raise RuntimeError("save_case: INSERT did not return a rowid")
        return row_id

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

    def mark_case_fixed(
        self,
        case_id: int,
        clicks_before: float,
        position_before: float,
        ctr_before: float,
    ) -> None:
        now = _now()
        with self.conn:
            self.conn.execute(
                "UPDATE cannibalization_cases SET status = ?, fixed_at = ? WHERE id = ?",
                ("fixed", now, case_id),
            )
            for metric, value in (
                ("clicks", clicks_before),
                ("position", position_before),
                ("ctr", ctr_before),
            ):
                self.conn.execute(
                    """INSERT INTO fix_tracking (case_id, metric, value_before, value_after, measured_at)
                       VALUES (?, ?, ?, NULL, ?)
                       ON CONFLICT(case_id, metric) DO UPDATE SET
                           value_before=excluded.value_before,
                           value_after=NULL,
                           measured_at=excluded.measured_at""",
                    (case_id, metric, value, now),
                )

    def save_fix_measurement(
        self,
        case_id: int,
        metrics: dict[str, tuple[float, float]],
    ) -> None:
        now = _now()
        with self.conn:
            for metric, (before, after) in metrics.items():
                self.conn.execute(
                    """INSERT INTO fix_tracking (case_id, metric, value_before, value_after, measured_at)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT(case_id, metric) DO UPDATE SET
                           value_before=excluded.value_before,
                           value_after=excluded.value_after,
                           measured_at=excluded.measured_at""",
                    (case_id, metric, before, after, now),
                )


def _now() -> str:
    return datetime.now(UTC).isoformat()


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
