import typer

app = typer.Typer(help="SEO keyword cannibalization detector", invoke_without_command=True)


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
