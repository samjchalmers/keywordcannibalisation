from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import typer

app = typer.Typer(help="SEO keyword cannibalization detector")
ingest_app = typer.Typer(help="Import data from GSC API or CSV")
config_app = typer.Typer(help="View and update config")
app.add_typer(ingest_app, name="ingest")
app.add_typer(config_app, name="config")


@app.callback()
def _configure_logging(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging"),
) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def _settings_and_store():
    from cannibalize.config import Settings
    from cannibalize.db.store import Store

    settings = Settings.load()
    store = Store(settings.db_path)
    store.init_db()
    return settings, store


def _validate_iso_date(value: str, label: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as e:
        typer.echo(f"Invalid {label} '{value}': expected YYYY-MM-DD", err=True)
        raise typer.Exit(1) from e
    return value


@app.command()
def init() -> None:
    """Create database and generate default config."""
    from cannibalize.config import Settings
    from cannibalize.db.store import Store

    settings = Settings.load()
    with Store(settings.db_path) as store:
        store.init_db()
    if not Path("cannibalize.toml").exists():
        settings.save()
    typer.echo("Initialized database and config.")


@app.command()
def auth() -> None:
    """Run Google Search Console OAuth flow."""
    from cannibalize.ingest.gsc import authenticate

    try:
        authenticate()
        typer.echo("GSC authentication successful.")
    except Exception as e:
        typer.echo(f"Authentication failed: {e}", err=True)
        raise typer.Exit(1) from None


@ingest_app.command("csv")
def ingest_csv(
    filepath: Path,
    query_col: str = typer.Option(None),
    url_col: str = typer.Option(None),
    clicks_col: str = typer.Option(None),
    impressions_col: str = typer.Option(None),
    ctr_col: str = typer.Option(None),
    position_col: str = typer.Option(None),
) -> None:
    """Import data from a GSC CSV export."""
    from cannibalize.ingest.csv_import import import_csv

    if not filepath.exists():
        typer.echo(f"File not found: {filepath}", err=True)
        raise typer.Exit(1)

    _, store = _settings_and_store()
    try:
        count = import_csv(
            filepath,
            store,
            query_col=query_col,
            url_col=url_col,
            clicks_col=clicks_col,
            impressions_col=impressions_col,
            ctr_col=ctr_col,
            position_col=position_col,
        )
    except ValueError as e:
        typer.echo(f"CSV import failed: {e}", err=True)
        raise typer.Exit(1) from None
    finally:
        store.close()
    typer.echo(f"Imported {count} rows from {filepath}.")


@ingest_app.command("gsc")
def ingest_gsc_cmd(
    site_url: str,
    start_date: str = typer.Option(None, help="Start date YYYY-MM-DD (default: 90d ago)"),
    end_date: str = typer.Option(None, help="End date YYYY-MM-DD (default: today)"),
) -> None:
    """Fetch data from the Google Search Console API."""
    from cannibalize.ingest.gsc import ingest_gsc

    start = start_date or (date.today() - timedelta(days=90)).isoformat()
    end = end_date or date.today().isoformat()
    _validate_iso_date(start, "start_date")
    _validate_iso_date(end, "end_date")
    if start > end:
        typer.echo(f"start_date {start} is after end_date {end}", err=True)
        raise typer.Exit(1)

    _, store = _settings_and_store()
    try:
        count = ingest_gsc(site_url, start, end, store)
    except Exception as e:
        typer.echo(f"GSC ingest failed: {e}", err=True)
        raise typer.Exit(1) from None
    finally:
        store.close()
    typer.echo(f"Imported {count} rows from GSC {start} → {end}.")


@app.command()
def crawl(
    concurrency: int = typer.Option(10),
    force: bool = typer.Option(False, help="Re-crawl even if recent"),
) -> None:
    """Crawl all URLs in the database for title/h1/canonical/body."""
    from cannibalize.ingest.crawler import crawl_all

    _, store = _settings_and_store()
    try:
        ok, fail = crawl_all(store, concurrency=concurrency, force=force)
    finally:
        store.close()
    typer.echo(f"Crawled {ok} URLs ({fail} failed).")


@app.command()
def detect(
    min_impressions: int = typer.Option(None),
    min_urls: int = typer.Option(None),
    brand_terms: str = typer.Option(None, help="Comma-separated brand terms"),
) -> None:
    """Run the full detection pipeline."""
    from cannibalize.detect.pipeline import run_detection

    settings, store = _settings_and_store()
    if min_impressions is not None:
        settings.min_impressions = min_impressions
    if min_urls is not None:
        settings.min_urls_per_query = min_urls
    if brand_terms:
        settings.brand_terms = [t.strip() for t in brand_terms.split(",") if t.strip()]

    try:
        cases = run_detection(store, settings)
    finally:
        store.close()
    typer.echo(f"Detected {len(cases)} cannibalization cases.")


@app.command()
def report(
    format: str = typer.Option("table", help="table | json"),
) -> None:
    """Print a summary of cases."""
    _, store = _settings_and_store()
    try:
        cases = store.get_cases()
    finally:
        store.close()

    if format == "json":
        out = [
            {
                "id": c.id,
                "query": c.query,
                "urls": c.urls,
                "case_type": c.case_type,
                "severity": c.severity_score,
                "similarity": c.similarity_score,
                "click_loss": c.estimated_click_loss,
                "keep_url": c.keep_url,
                "status": c.status,
            }
            for c in cases
        ]
        typer.echo(json.dumps(out, indent=2))
        return

    if not cases:
        typer.echo("No cases found. Run `cannibalize detect` first.")
        return

    typer.echo(f"{'ID':<4} {'TYPE':<18} {'SEV':<6} {'LOSS':<8} QUERY")
    for c in sorted(cases, key=lambda x: x.severity_score or 0.0, reverse=True):
        sev = f"{c.severity_score:.2f}" if c.severity_score is not None else "-"
        loss = f"{c.estimated_click_loss:.0f}" if c.estimated_click_loss is not None else "-"
        typer.echo(f"{c.id:<4} {(c.case_type or '-'):<18} {sev:<6} {loss:<8} {c.query}")


@app.command()
def export(filepath: Path) -> None:
    """Export cases to Excel (.xlsx) or CSV (.csv)."""
    from cannibalize.export.csv_export import export_cases_csv
    from cannibalize.export.excel import export_cases_excel

    _, store = _settings_and_store()
    try:
        cases = store.get_cases()

        suffix = filepath.suffix.lower()
        if suffix == ".xlsx":
            export_cases_excel(cases, filepath, store)
        elif suffix == ".csv":
            export_cases_csv(cases, filepath)
        else:
            typer.echo(
                f"Unsupported export format: {suffix}. Use .xlsx or .csv.",
                err=True,
            )
            raise typer.Exit(1)
    finally:
        store.close()
    typer.echo(f"Exported {len(cases)} cases to {filepath}.")


@app.command()
def fix(case_id: int) -> None:
    """Mark a case as fixed and snapshot current metrics."""
    from cannibalize.track.tracker import mark_fixed

    _, store = _settings_and_store()
    try:
        mark_fixed(case_id, store)
    except ValueError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from None
    finally:
        store.close()
    typer.echo(f"Case {case_id} marked as fixed.")


@app.command()
def measure(
    case_id: int,
    days: int = typer.Option(28),
) -> None:
    """Measure post-fix impact for a case."""
    from cannibalize.track.tracker import measure_impact

    _, store = _settings_and_store()
    try:
        result = measure_impact(case_id, store, days_after=days)
    finally:
        store.close()

    if result is None:
        typer.echo(f"Case {case_id} has no fix recorded, or was not found.", err=True)
        raise typer.Exit(1)

    typer.echo(
        f"Case {case_id}: clicks {result.clicks_before:.0f} → "
        f"{result.clicks_after:.0f} ({result.delta_pct:+.1f}%), "
        f"position {result.position_before:.1f} → {result.position_after:.1f}"
    )


@config_app.command("show")
def config_show() -> None:
    """Print current configuration."""
    from cannibalize.config import Settings

    settings = Settings.load()
    for k, v in settings.__dict__.items():
        typer.echo(f"{k} = {v}")


@config_app.command("set")
def config_set(key: str, value: str) -> None:
    """Update a single config value."""
    from cannibalize.config import Settings

    settings = Settings.load()
    if key not in settings.__dataclass_fields__:
        typer.echo(f"Unknown config key: {key}", err=True)
        raise typer.Exit(1)

    current = getattr(settings, key)
    if isinstance(current, dict):
        typer.echo(
            f"'{key}' is a dict config. Edit cannibalize.toml directly to change its keys.",
            err=True,
        )
        raise typer.Exit(1)
    if isinstance(current, bool):
        parsed: object = value.lower() in ("1", "true", "yes", "on")
    elif isinstance(current, int):
        parsed = int(value)
    elif isinstance(current, float):
        parsed = float(value)
    elif isinstance(current, list):
        parsed = [v.strip() for v in value.split(",") if v.strip()]
    else:
        parsed = value
    setattr(settings, key, parsed)
    settings.save()
    typer.echo(f"Set {key} = {parsed}")
