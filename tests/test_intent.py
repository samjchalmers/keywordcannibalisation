from cannibalize.db.store import QueryCluster, URLMetrics
from cannibalize.detect.intent import _token_overlap, score_similarity


def test_token_overlap():
    assert _token_overlap("best running shoes", "best running shoes") == 1.0
    assert _token_overlap("a b c", "d e f") == 0.0
    assert 0.0 < _token_overlap("best running shoes", "best hiking shoes") < 1.0


def test_similarity_returns_none_when_uncrawled(store):
    cluster = QueryCluster(
        query="q",
        urls=[
            URLMetrics("https://x.com/a", 10, 100, 0.1, 3.0),
            URLMetrics("https://x.com/b", 5, 100, 0.05, 4.0),
        ],
    )
    assert score_similarity(cluster, store) is None


def test_similarity_scores_crawled_pages(store):
    store.upsert_page(
        "https://x.com/a",
        "Running shoes",
        "Best running shoes",
        None,
        "best running shoes for marathon training and long runs",
    )
    store.upsert_page(
        "https://x.com/b",
        "Running shoes guide",
        "Running shoes guide",
        None,
        "guide to choosing running shoes for marathon and training",
    )
    cluster = QueryCluster(
        query="q",
        urls=[
            URLMetrics("https://x.com/a", 10, 100, 0.1, 3.0),
            URLMetrics("https://x.com/b", 5, 100, 0.05, 4.0),
        ],
    )
    sim = score_similarity(cluster, store)
    assert sim is not None
    assert 0.0 < sim <= 1.0
