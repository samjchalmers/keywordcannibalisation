from __future__ import annotations

import sqlite3

MIGRATIONS: list[str] = [
    # v1: initial schema
    """
    CREATE TABLE IF NOT EXISTS pages (
        url TEXT PRIMARY KEY,
        title TEXT,
        h1 TEXT,
        canonical TEXT,
        body_text TEXT,
        crawled_at TEXT
    );

    CREATE TABLE IF NOT EXISTS queries (
        query TEXT PRIMARY KEY,
        is_branded INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS query_page_metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        query TEXT NOT NULL,
        url TEXT NOT NULL,
        clicks REAL,
        impressions REAL,
        ctr REAL,
        position REAL,
        date TEXT NOT NULL,
        UNIQUE(query, url, date)
    );

    CREATE TABLE IF NOT EXISTS cannibalization_cases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        query TEXT NOT NULL,
        urls TEXT NOT NULL,
        case_type TEXT,
        severity_score REAL,
        similarity_score REAL,
        position_volatility REAL,
        estimated_click_loss REAL,
        recommendation TEXT,
        keep_url TEXT,
        status TEXT DEFAULT 'open',
        detected_at TEXT,
        fixed_at TEXT
    );

    CREATE TABLE IF NOT EXISTS fix_tracking (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id INTEGER NOT NULL,
        metric TEXT NOT NULL,
        value_before REAL,
        value_after REAL,
        measured_at TEXT,
        UNIQUE(case_id, metric),
        FOREIGN KEY (case_id) REFERENCES cannibalization_cases(id)
    );
    """,
    # v2: indexes for hot-filtered columns
    """
    CREATE INDEX IF NOT EXISTS idx_pages_crawled_at
        ON pages(crawled_at);
    CREATE INDEX IF NOT EXISTS idx_qpm_query
        ON query_page_metrics(query);
    CREATE INDEX IF NOT EXISTS idx_qpm_query_url
        ON query_page_metrics(query, url);
    CREATE INDEX IF NOT EXISTS idx_qpm_date
        ON query_page_metrics(date);
    CREATE INDEX IF NOT EXISTS idx_cases_status
        ON cannibalization_cases(status);
    CREATE INDEX IF NOT EXISTS idx_fix_tracking_case
        ON fix_tracking(case_id);
    """,
]


def apply_migrations(conn: sqlite3.Connection) -> None:
    current = conn.execute("PRAGMA user_version").fetchone()[0]
    for version, sql in enumerate(MIGRATIONS, start=1):
        if current < version:
            conn.executescript(sql)
            conn.execute(f"PRAGMA user_version = {version}")
            conn.commit()
