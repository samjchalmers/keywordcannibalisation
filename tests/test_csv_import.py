import logging

import pytest

from cannibalize.ingest.csv_import import import_csv


def test_missing_columns_raises(store, tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("foo,bar\n1,2\n")
    with pytest.raises(ValueError, match="Missing columns"):
        import_csv(path, store)


def test_malformed_rows_are_skipped(store, tmp_path, caplog):
    path = tmp_path / "mixed.csv"
    path.write_text(
        "Top queries,Top pages,Clicks,Impressions,CTR,Position,Date\n"
        "good,https://x.com/a,10,100,10%,3.0,2026-03-01\n"
        "bad,https://x.com/b,not-a-number,100,10%,3.0,2026-03-01\n"
        "good2,https://x.com/c,5,50,10%,4.0,2026-03-01\n"
    )
    with caplog.at_level(logging.WARNING, logger="cannibalize.ingest.csv_import"):
        count = import_csv(path, store)
    assert count == 2
    assert "skipping row 3" in caplog.text

    urls = store.get_all_urls()
    assert "https://x.com/a" in urls
    assert "https://x.com/c" in urls
    assert "https://x.com/b" not in urls
