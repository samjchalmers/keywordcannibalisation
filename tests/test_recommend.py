from cannibalize.db.store import CannibalizationCase
from cannibalize.recommend.actions import recommend


def _case(case_type: str, severity: float = 0.5) -> CannibalizationCase:
    return CannibalizationCase(
        id=1,
        query="q",
        urls=["https://x.com/a", "https://x.com/b"],
        case_type=case_type,
        severity_score=severity,
        similarity_score=0.5,
        position_volatility=0.2,
        estimated_click_loss=25.0,
        recommendation=None,
        keep_url="https://x.com/a",
    )


def test_redundant_suggests_redirect():
    r = recommend(_case("REDUNDANT_CONTENT"))
    assert r.redirect_url == "https://x.com/b"
    assert "301" in r.action


def test_intent_mismatch_keeps_both():
    r = recommend(_case("INTENT_MISMATCH"))
    assert r.redirect_url is None
    assert "differentiate" in r.action.lower()


def test_split_authority_merges():
    r = recommend(_case("SPLIT_AUTHORITY"))
    assert r.redirect_url == "https://x.com/b"
    assert "merge" in r.action.lower() or "consolidate" in r.action.lower()


def test_priority_thresholds():
    assert recommend(_case("SPLIT_AUTHORITY", severity=0.8)).priority == "high"
    assert recommend(_case("SPLIT_AUTHORITY", severity=0.5)).priority == "medium"
    assert recommend(_case("SPLIT_AUTHORITY", severity=0.1)).priority == "low"
