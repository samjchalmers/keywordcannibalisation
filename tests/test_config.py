from cannibalize.config import Settings


def test_config_roundtrip_preserves_all_fields(tmp_path):
    original = Settings(
        db_path="custom.db",
        min_impressions=25,
        brand_terms=["acme", "widgets co"],
    )
    path = tmp_path / "cannibalize.toml"
    original.save(path)

    loaded = Settings.load(path)
    assert loaded.db_path == "custom.db"
    assert loaded.min_impressions == 25
    assert loaded.brand_terms == ["acme", "widgets co"]
    # ctr_curve must survive the roundtrip (previously broken by TOML ordering)
    assert loaded.ctr_curve == original.ctr_curve
    assert loaded.severity_weights == original.severity_weights
