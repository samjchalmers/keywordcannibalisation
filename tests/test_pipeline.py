from cannibalize.detect.pipeline import run_detection
from cannibalize.export.csv_export import export_cases_csv
from cannibalize.export.excel import export_cases_excel


def test_full_pipeline_and_export(populated_store, settings, tmp_path):
    cases = run_detection(populated_store, settings)
    by_query = {c.query: c for c in cases}

    assert "best running shoes" in by_query
    assert "wireless headphones review" in by_query
    assert "how to clean leather boots" in by_query
    assert "example brand shoes" not in by_query
    assert "python list comprehension" not in by_query

    # The headphones case mixes /products/ and /blog/ → intent mismatch
    assert by_query["wireless headphones review"].case_type == "INTENT_MISMATCH"

    for c in cases:
        assert c.id is not None
        assert c.case_type is not None
        assert c.recommendation
        assert 0.0 <= (c.severity_score or 0.0) <= 1.0

    csv_path = tmp_path / "out.csv"
    export_cases_csv(cases, csv_path)
    assert csv_path.exists()
    assert csv_path.stat().st_size > 0

    xlsx_path = tmp_path / "out.xlsx"
    export_cases_excel(cases, xlsx_path, populated_store)
    assert xlsx_path.exists()
