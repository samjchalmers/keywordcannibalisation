from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse

from cannibalize.db.store import QueryCluster
from cannibalize.detect.impact import ImpactScore

PAGE_TYPE_PATTERNS: dict[str, list[str]] = {
    "blog": [r"/blog/", r"/articles?/", r"/posts?/", r"/news/"],
    "product": [r"/products?/", r"/shop/", r"/store/", r"/buy/"],
    "help": [r"/help/", r"/support/", r"/faq/", r"/docs/", r"/guide/"],
    "category": [r"/category/", r"/collections?/", r"/tag/"],
    "landing": [r"/lp/", r"/landing/", r"/promo/"],
}


@dataclass
class Classification:
    case_type: str
    keep_url: str
    reason: str


def _infer_page_type(url: str) -> str:
    path = urlparse(url).path.lower()
    for page_type, patterns in PAGE_TYPE_PATTERNS.items():
        if any(re.search(p, path) for p in patterns):
            return page_type
    return "unknown"


def _best_performing_url(cluster: QueryCluster) -> str:
    return max(cluster.urls, key=lambda u: (u.clicks, -u.position)).url


def classify_case(
    cluster: QueryCluster,
    similarity: float | None,
    impact: ImpactScore,
) -> Classification:
    keep_url = _best_performing_url(cluster)
    page_types = {u.url: _infer_page_type(u.url) for u in cluster.urls}
    distinct_types = set(page_types.values()) - {"unknown"}

    if similarity is not None and similarity > 0.8:
        return Classification(
            case_type="REDUNDANT_CONTENT",
            keep_url=keep_url,
            reason="Very high content similarity between competing pages",
        )

    if similarity is not None and similarity > 0.5 and len(distinct_types) <= 1:
        return Classification(
            case_type="SPLIT_AUTHORITY",
            keep_url=keep_url,
            reason="Similar content on same page type splitting ranking signals",
        )

    if len(distinct_types) > 1:
        return Classification(
            case_type="INTENT_MISMATCH",
            keep_url=keep_url,
            reason=f"Different page types competing: {', '.join(sorted(distinct_types))}",
        )

    # Check if a lower-performing URL has a better page type match
    sorted_urls = sorted(cluster.urls, key=lambda u: u.clicks, reverse=True)
    if len(sorted_urls) >= 2:
        top = sorted_urls[0]
        second = sorted_urls[1]
        if top.position > second.position + 3:
            return Classification(
                case_type="WRONG_PAGE",
                keep_url=second.url,
                reason="Higher-clicking page ranks significantly lower than competitor",
            )

    return Classification(
        case_type="SPLIT_AUTHORITY",
        keep_url=keep_url,
        reason="Multiple pages competing for the same query",
    )
