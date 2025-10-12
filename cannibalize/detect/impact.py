from __future__ import annotations

import math
from dataclasses import dataclass

from cannibalize.config import Settings
from cannibalize.db.store import QueryCluster, Store


@dataclass
class ImpactScore:
    severity: float
    volatility: float
    click_loss: float
    impression_volume: float


def _std_dev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(variance)


def score_volatility(cluster: QueryCluster, store: Store) -> float:
    max_std = 0.0
    for url_metrics in cluster.urls:
        positions = store.get_daily_positions(cluster.query, url_metrics.url)
        std = _std_dev(positions)
        max_std = max(max_std, std)
    return min(max_std / 10.0, 1.0)


def score_click_loss(cluster: QueryCluster, settings: Settings) -> float:
    total_impressions = sum(u.impressions for u in cluster.urls)
    total_actual_clicks = sum(u.clicks for u in cluster.urls)
    best_position = min(u.position for u in cluster.urls)

    pos_index = max(0, min(int(best_position) - 1, len(settings.ctr_curve) - 1))
    expected_ctr = settings.ctr_curve[pos_index]
    expected_clicks = total_impressions * expected_ctr

    return max(expected_clicks - total_actual_clicks, 0.0)


def score_impact(
    cluster: QueryCluster,
    store: Store,
    settings: Settings,
    similarity: float | None = None,
) -> ImpactScore:
    volatility = score_volatility(cluster, store)
    click_loss = score_click_loss(cluster, settings)
    impression_volume = sum(u.impressions for u in cluster.urls)
    sim = similarity if similarity is not None else 0.0

    return ImpactScore(
        severity=0.0,  # normalized later across all clusters
        volatility=volatility,
        click_loss=click_loss,
        impression_volume=impression_volume,
    )


def normalize_scores(
    scores: list[tuple[QueryCluster, ImpactScore]],
    settings: Settings,
    similarities: dict[str, float | None] | None = None,
) -> None:
    if not scores:
        return

    max_click_loss = max(s.click_loss for _, s in scores) or 1.0
    max_impressions = max(s.impression_volume for _, s in scores) or 1.0

    weights = settings.severity_weights
    sims = similarities or {}

    for cluster, impact in scores:
        norm_vol = impact.volatility  # already 0-1
        norm_cl = impact.click_loss / max_click_loss
        norm_imp = impact.impression_volume / max_impressions
        sim = sims.get(cluster.query)
        norm_sim = sim if sim is not None else 0.0

        impact.severity = (
            weights["volatility"] * norm_vol
            + weights["click_dilution"] * norm_cl
            + weights["impression_volume"] * norm_imp
            + weights["similarity"] * norm_sim
        )
