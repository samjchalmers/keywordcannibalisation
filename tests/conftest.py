from __future__ import annotations

from pathlib import Path

import pytest

from cannibalize.config import Settings
from cannibalize.db.store import Store

FIXTURE_CSV = Path(__file__).parent / "fixtures" / "sample_gsc_data.csv"


@pytest.fixture
def settings() -> Settings:
    return Settings(
        min_impressions=10,
        min_urls_per_query=2,
        brand_terms=["example brand"],
    )


@pytest.fixture
def store(tmp_path) -> Store:
    s = Store(str(tmp_path / "test.db"))
    s.init_db()
    yield s
    s.close()


@pytest.fixture
def populated_store(store):
    from cannibalize.ingest.csv_import import import_csv

    import_csv(FIXTURE_CSV, store)
    return store
