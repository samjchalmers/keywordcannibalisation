from pathlib import Path

import typer

app = typer.Typer(help="SEO keyword cannibalization detector", invoke_without_command=True)
ingest_app = typer.Typer(help="Import data from GSC API or CSV")
app.add_typer(ingest_app, name="ingest")


@app.callback()
def main() -> None:
    pass


@app.command()
def init() -> None:
    """Create database and generate default config."""
    from cannibalize.config import Settings
    from cannibalize.db.store import Store

    settings = Settings.load()
    store = Store(settings.db_path)
    store.init_db()
    store.close()
    settings.save()
    typer.echo("Initialized database and config.")


@ingest_app.command("csv")
def ingest_csv(
    filepath: Path,
    query_col: str = typer.Option(None, help="Column name for queries"),
    url_col: str = typer.Option(None, help="Column name for URLs"),
    clicks_col: str = typer.Option(None, help="Column name for clicks"),
    impressions_col: str = typer.Option(None, help="Column name for impressions"),
    ctr_col: str = typer.Option(None, help="Column name for CTR"),
    position_col: str = typer.Option(None, help="Column name for position"),
) -> None:
    """Import data from a GSC CSV export."""
    from cannibalize.config import Settings
    from cannibalize.db.store import Store
    from cannibalize.ingest.csv_import import import_csv

    if not filepath.exists():
        typer.echo(f"File not found: {filepath}", err=True)
        raise typer.Exit(1)

    settings = Settings.load()
    store = Store(settings.db_path)
    store.init_db()

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
    store.close()
    typer.echo(f"Imported {count} rows from {filepath}.")
