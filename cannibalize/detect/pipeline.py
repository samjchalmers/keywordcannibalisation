from __future__ import annotations

from cannibalize.config import Settings
from cannibalize.db.store import CannibalizationCase, Store
from cannibalize.detect.classify import classify_case
from cannibalize.detect.cluster import find_cannibalization_candidates
from cannibalize.detect.impact import normalize_scores, score_impact
from cannibalize.detect.intent import score_similarity
from cannibalize.recommend.actions import recommend


def run_detection(store: Store, settings: Settings) -> list[CannibalizationCase]:
    clusters = find_cannibalization_candidates(store, settings)
    if not clusters:
        return []

    similarities: dict[str, float | None] = {}
    for cluster in clusters:
        similarities[cluster.query] = score_similarity(cluster, store)

    scored: list = [
        (cluster, score_impact(cluster, store, settings)) for cluster in clusters
    ]

    normalize_scores(scored, settings, similarities=similarities)

    cases: list[CannibalizationCase] = []
    for cluster, impact in scored:
        sim = similarities[cluster.query]
        classification = classify_case(cluster, sim, impact)

        case = CannibalizationCase(
            id=None,
            query=cluster.query,
            urls=[u.url for u in cluster.urls],
            case_type=classification.case_type,
            severity_score=impact.severity,
            similarity_score=sim,
            position_volatility=impact.volatility,
            estimated_click_loss=impact.click_loss,
            recommendation=None,
            keep_url=classification.keep_url,
        )
        rec = recommend(case)
        case.recommendation = rec.action
        case.keep_url = rec.keep_url
        case_id = store.save_case(case)
        case.id = case_id
        cases.append(case)

    return cases
