from datetime import date, timedelta

from cannibalize.db.store import CannibalizationCase
from cannibalize.track.tracker import mark_fixed, measure_impact


def _seed_metrics(store, query, url, days, start, clicks, impressions, position):
    for i in range(days):
        d = (date.fromisoformat(start) + timedelta(days=i)).isoformat()
        store.upsert_query_page_metrics(
            query=query,
            url=url,
            clicks=clicks,
            impressions=impressions,
            ctr=clicks / impressions if impressions else 0.0,
            position=position,
            date=d,
        )


def test_mark_fixed_and_measure_impact(store):
    q = "shoes"
    url = "https://x.com/shoes"
    # Before window (14 days of poor metrics)
    _seed_metrics(store, q, url, 14, "2026-03-01", 10, 500, 6.0)
    # After window (14 days of improved metrics starting at fix date)
    _seed_metrics(store, q, url, 14, "2026-03-15", 30, 500, 3.0)

    case = CannibalizationCase(
        id=None,
        query=q,
        urls=[url],
        case_type="SPLIT_AUTHORITY",
        severity_score=0.5,
        similarity_score=None,
        position_volatility=0.1,
        estimated_click_loss=0.0,
        recommendation="merge",
        keep_url=url,
    )
    case_id = store.save_case(case)
    mark_fixed(case_id, store)

    refreshed = store.get_case(case_id)
    assert refreshed.status == "fixed"
    # mark_fixed patch the fixed_at in the DB; stub it to the 15th for measurement
    store.conn.execute(
        "UPDATE cannibalization_cases SET fixed_at = ? WHERE id = ?",
        ("2026-03-15T00:00:00+00:00", case_id),
    )
    store.conn.commit()

    result = measure_impact(case_id, store, days_after=14)
    assert result is not None
    assert result.clicks_after > result.clicks_before
    assert result.position_after < result.position_before
    assert result.delta_pct > 0

    # fix_tracking should have exactly 3 rows (unique per metric), not duplicates
    count = store.conn.execute(
        "SELECT COUNT(*) FROM fix_tracking WHERE case_id = ?", (case_id,)
    ).fetchone()[0]
    assert count == 3
