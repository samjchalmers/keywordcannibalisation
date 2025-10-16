from __future__ import annotations

from dataclasses import dataclass

from cannibalize.db.store import CannibalizationCase


@dataclass
class Recommendation:
    action: str
    keep_url: str
    redirect_url: str | None
    estimated_click_uplift: float
    priority: str


def _priority(severity: float | None) -> str:
    if severity is None:
        return "low"
    if severity > 0.7:
        return "high"
    if severity > 0.4:
        return "medium"
    return "low"


def recommend(case: CannibalizationCase) -> Recommendation:
    keep = case.keep_url or case.urls[0]
    others = [u for u in case.urls if u != keep]
    redirect = others[0] if others else None
    uplift = case.estimated_click_loss or 0.0
    priority = _priority(case.severity_score)

    if case.case_type == "REDUNDANT_CONTENT":
        return Recommendation(
            action=(
                f"Pages are near-duplicates. 301 redirect {redirect} to {keep}. "
                f"Remove or noindex the weaker page after redirecting."
            ),
            keep_url=keep,
            redirect_url=redirect,
            estimated_click_uplift=uplift,
            priority=priority,
        )

    if case.case_type == "WRONG_PAGE":
        return Recommendation(
            action=(
                f"The wrong page is ranking. Strengthen {keep} by adding the target "
                f"keyword to its title and H1. Improve internal linking to {keep}. "
                f"Consider adding a canonical from {redirect} to {keep}."
            ),
            keep_url=keep,
            redirect_url=redirect,
            estimated_click_uplift=uplift,
            priority=priority,
        )

    if case.case_type == "INTENT_MISMATCH":
        other_list = ", ".join(others) if others else "competing pages"
        return Recommendation(
            action=(
                f"Keep both pages but differentiate keyword targeting. "
                f"Adjust title and H1 of {keep} and {other_list} to target "
                f"distinct query variations. Review internal linking to ensure "
                f"each page is the clear target for its intended query."
            ),
            keep_url=keep,
            redirect_url=None,
            estimated_click_uplift=uplift,
            priority=priority,
        )

    # SPLIT_AUTHORITY (default)
    return Recommendation(
        action=(
            f"Consolidate ranking signals. Merge content from {redirect} into "
            f"{keep}, then 301 redirect {redirect} to {keep}."
        ),
        keep_url=keep,
        redirect_url=redirect,
        estimated_click_uplift=uplift,
        priority=priority,
    )
