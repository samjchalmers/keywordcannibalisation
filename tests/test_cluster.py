from cannibalize.detect.cluster import find_cannibalization_candidates


def test_clustering_finds_multi_url_queries(populated_store, settings):
    clusters = find_cannibalization_candidates(populated_store, settings)
    queries = {c.query for c in clusters}

    assert "best running shoes" in queries
    assert "wireless headphones review" in queries
    assert "how to clean leather boots" in queries

    # Single-URL queries should not appear
    assert "python list comprehension" not in queries
    assert "react hooks tutorial" not in queries


def test_brand_terms_filter_out(populated_store, settings):
    clusters = find_cannibalization_candidates(populated_store, settings)
    queries = {c.query for c in clusters}
    assert "example brand shoes" not in queries


def test_cluster_has_multiple_urls(populated_store, settings):
    clusters = find_cannibalization_candidates(populated_store, settings)
    for c in clusters:
        assert len(c.urls) >= 2
