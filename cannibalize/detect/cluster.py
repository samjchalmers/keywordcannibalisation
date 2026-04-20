from __future__ import annotations

from cannibalize.config import Settings
from cannibalize.db.store import QueryCluster, Store


def find_cannibalization_candidates(store: Store, settings: Settings) -> list[QueryCluster]:
    clusters = store.get_queries_with_multiple_urls(
        min_impressions=settings.min_impressions,
        min_urls=settings.min_urls_per_query,
    )

    if not settings.brand_terms:
        return clusters

    brand_lower = [t.lower() for t in settings.brand_terms]
    return [c for c in clusters if not any(term in c.query.lower() for term in brand_lower)]
