from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from cannibalize.db.store import Store


@dataclass
class FixResult:
    clicks_before: float
    clicks_after: float
    position_before: float
    position_after: float
    ctr_before: float
    ctr_after: float
    delta_pct: float


def _aggregate(metrics) -> tuple[float, float, float, float]:
    clicks = sum(m.clicks for m in metrics)
    impressions = sum(m.impressions for m in metrics)
    pos_weight = sum(m.position * m.impressions for m in metrics)
    position = pos_weight / impressions if impressions else 0.0
    ctr = clicks / impressions if impressions else 0.0
    return clicks, impressions, position, ctr


def mark_fixed(case_id: int, store: Store) -> None:
    case = store.get_case(case_id)
    if case is None:
        raise ValueError(f"Case {case_id} not found")

    metrics = store.get_query_metrics_summary(case.query)
    clicks, _, position, ctr = _aggregate(metrics)

    store.mark_case_fixed(case_id, clicks, position, ctr)


def measure_impact(
    case_id: int, store: Store, days_after: int = 28
) -> FixResult | None:
    case = store.get_case(case_id)
    if case is None or case.fixed_at is None:
        return None

    fixed_date = date.fromisoformat(case.fixed_at[:10])
    after_start = fixed_date.isoformat()
    after_end = (fixed_date + timedelta(days=days_after)).isoformat()
    before_end = (fixed_date - timedelta(days=1)).isoformat()
    before_start = (fixed_date - timedelta(days=days_after)).isoformat()

    before = store.get_query_metrics_summary(
        case.query, since=before_start, until=before_end
    )
    after = store.get_query_metrics_summary(
        case.query, since=after_start, until=after_end
    )

    clicks_before, _, pos_before, ctr_before = _aggregate(before)
    clicks_after, _, pos_after, ctr_after = _aggregate(after)

    delta_pct = (
        ((clicks_after - clicks_before) / clicks_before * 100.0)
        if clicks_before
        else 0.0
    )

    store.save_fix_measurement(
        case_id,
        {
            "clicks": (clicks_before, clicks_after),
            "position": (pos_before, pos_after),
            "ctr": (ctr_before, ctr_after),
        },
    )

    return FixResult(
        clicks_before=clicks_before,
        clicks_after=clicks_after,
        position_before=pos_before,
        position_after=pos_after,
        ctr_before=ctr_before,
        ctr_after=ctr_after,
        delta_pct=delta_pct,
    )
