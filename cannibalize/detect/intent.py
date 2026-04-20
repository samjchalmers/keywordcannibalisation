from __future__ import annotations

from itertools import combinations

from cannibalize.db.store import QueryCluster, Store


def _token_overlap(a: str | None, b: str | None) -> float:
    if not a or not b:
        return 0.0
    ta = {t for t in a.lower().split() if t}
    tb = {t for t in b.lower().split() if t}
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _body_similarity(body_a: str, body_b: str) -> float:
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity
    except ImportError:
        return _token_overlap(body_a, body_b)

    vec = TfidfVectorizer(max_features=5000, stop_words="english")
    try:
        matrix = vec.fit_transform([body_a, body_b])
    except ValueError:
        return 0.0
    sim = cosine_similarity(matrix[0:1], matrix[1:2])[0, 0]
    return float(sim)


def _pair_similarity(a: dict, b: dict) -> float:
    body = _body_similarity(a.get("body_text") or "", b.get("body_text") or "")
    title = _token_overlap(a.get("title"), b.get("title"))
    h1 = _token_overlap(a.get("h1"), b.get("h1"))
    return 0.6 * body + 0.25 * title + 0.15 * h1


def score_similarity(cluster: QueryCluster, store: Store) -> float | None:
    pages: list[dict] = []
    for url_metrics in cluster.urls:
        page = store.get_page(url_metrics.url)
        if page is None or not page.get("body_text"):
            return None
        pages.append(page)

    if len(pages) < 2:
        return None

    return max(_pair_similarity(a, b) for a, b in combinations(pages, 2))
