from cannibalize.db.store import QueryCluster, URLMetrics
from cannibalize.detect.classify import _infer_page_type, classify_case
from cannibalize.detect.impact import ImpactScore


def _impact() -> ImpactScore:
    return ImpactScore(volatility=0.2, click_loss=10.0, impression_volume=1000.0, severity=0.5)


def test_infer_page_type():
    assert _infer_page_type("https://x.com/blog/foo") == "blog"
    assert _infer_page_type("https://x.com/products/shoe") == "product"
    assert _infer_page_type("https://x.com/help/how-to") == "help"
    assert _infer_page_type("https://x.com/random") == "unknown"


def test_intent_mismatch_across_page_types(populated_store):
    cluster = QueryCluster(
        query="headphones",
        urls=[
            URLMetrics("https://x.com/products/headphones", 80, 2000, 0.04, 2.1),
            URLMetrics("https://x.com/blog/headphones-review", 60, 1800, 0.033, 4.3),
        ],
    )
    result = classify_case(cluster, similarity=None, impact=_impact())
    assert result.case_type == "INTENT_MISMATCH"


def test_redundant_content_high_similarity(populated_store):
    cluster = QueryCluster(
        query="q",
        urls=[
            URLMetrics("https://x.com/a", 10, 100, 0.1, 3.0),
            URLMetrics("https://x.com/b", 5, 100, 0.05, 4.0),
        ],
    )
    result = classify_case(cluster, similarity=0.9, impact=_impact())
    assert result.case_type == "REDUNDANT_CONTENT"


def test_default_is_split_authority(populated_store):
    cluster = QueryCluster(
        query="q",
        urls=[
            URLMetrics("https://x.com/a", 10, 100, 0.1, 3.0),
            URLMetrics("https://x.com/b", 8, 100, 0.08, 3.5),
        ],
    )
    result = classify_case(cluster, similarity=None, impact=_impact())
    assert result.case_type == "SPLIT_AUTHORITY"
