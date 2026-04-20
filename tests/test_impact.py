from cannibalize.db.store import QueryCluster, URLMetrics
from cannibalize.detect.cluster import find_cannibalization_candidates
from cannibalize.detect.impact import (
    normalize_scores,
    score_click_loss,
    score_impact,
    score_volatility,
)


def test_click_loss_is_nonnegative(settings):
    cluster = QueryCluster(
        query="q",
        urls=[
            URLMetrics("a", clicks=50, impressions=1000, ctr=0.05, position=3.0),
            URLMetrics("b", clicks=30, impressions=900, ctr=0.033, position=5.0),
        ],
    )
    loss = score_click_loss(cluster, settings)
    assert loss >= 0


def test_click_loss_math(settings):
    cluster = QueryCluster(
        query="q",
        urls=[
            URLMetrics("a", clicks=10, impressions=1000, ctr=0.01, position=1.0),
            URLMetrics("b", clicks=0, impressions=500, ctr=0.0, position=8.0),
        ],
    )
    # Best position = 1, expected CTR = 0.319, total imp = 1500
    # expected clicks = 478.5, actual = 10 → loss = 468.5
    loss = score_click_loss(cluster, settings)
    assert abs(loss - 468.5) < 0.1


def test_volatility_uses_daily_positions(populated_store):
    clusters = find_cannibalization_candidates(
        populated_store,
        __import__("cannibalize.config", fromlist=["Settings"]).Settings(),
    )
    target = next(c for c in clusters if c.query == "best running shoes")
    vol = score_volatility(target, populated_store)
    assert 0.0 <= vol <= 1.0


def test_normalization_sets_severity(populated_store, settings):
    from cannibalize.detect.cluster import find_cannibalization_candidates

    clusters = find_cannibalization_candidates(populated_store, settings)
    scored = [
        (c, score_impact(c, populated_store, settings)) for c in clusters
    ]
    normalize_scores(scored, settings)
    for _, impact in scored:
        assert 0.0 <= impact.severity <= 1.0
