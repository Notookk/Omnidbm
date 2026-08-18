from __future__ import annotations

import json
import sys

import typer
from rich.console import Console
from rich.table import Table

from omnidbm import __version__
from omnidbm.core.errors import OmniDBMError
from omnidbm.core.models import (
    ConflictStrategy,
    ConnectorConfig,
    TableSpec,
    TransferConfig,
)
from omnidbm.core.pipeline import connect, run_transfer
from omnidbm.core.pipeline import doctor as doctor_check
from omnidbm.core.pipeline import inspect as inspect_source
from omnidbm.utils.progress import make_progress
from omnidbm.utils.prompts import run_wizard

app = typer.Typer(add_completion=False, help="omnidbm — universal database transfer engine", no_args_is_help=True)
console = Console()


def _enable_utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass


_enable_utf8()


def _parse_filter(raw: str | None) -> dict | None:
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"--filter must be valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise typer.BadParameter("--filter must be a JSON object")
    return value


def _resolve_tables(source_uri: str, dest_uri: str, tables: str | None) -> list[TableSpec]:
    if tables:
        names = [name.strip() for name in tables.split(",") if name.strip()]
        return [TableSpec(source=name, dest=name) for name in names]
    with connect(ConnectorConfig(uri=source_uri)) as src:
        names = [table.name for table in src.list_tables()]
    if not names:
        raise OmniDBMError("Source contains no tables")
    return [TableSpec(source=name, dest=name) for name in names]


def _summary_table(results) -> None:
    table = Table(title="Transfer summary", show_lines=True)
    table.add_column("Table", style="cyan")
    table.add_column("Documents", justify="right")
    table.add_column("Duration", justify="right")
    for result in results:
        table.add_row(result.table, f"{result.copied:,}", f"{result.duration:.2f}s")
    console.print(table)


@app.command()
def transfer(
    source: str = typer.Option(
        ...,
        "--source",
        "-s",
        help="Source URI (mongodb://, postgresql://, mysql://, sqlite://, redis://, csv://, jsonl://)",
    ),
    dest: str = typer.Option(..., "--dest", "-d", help="Destination URI"),
    tables: str | None = typer.Option(None, "--tables", "-t", help="Comma-separated table names (default: all)"),
    batch_size: int = typer.Option(1000, "--batch-size", min=1, help="Documents per batch"),
    drop_first: bool = typer.Option(False, "--drop-first", help="Drop destination table before transfer"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Count documents only, no writes"),
    limit: int | None = typer.Option(None, "--limit", min=1, help="Max documents per table"),
    filter_json: str | None = typer.Option(None, "--filter", help="Source-side JSON query filter"),
    conflict: ConflictStrategy = typer.Option(
        ConflictStrategy.SKIP, "--conflict", case_sensitive=False, help="Conflict handling on destination"
    ),
    copy_indexes: bool = typer.Option(True, "--copy-indexes/--no-copy-indexes", help="Copy indexes (MongoDB only)"),
    copy_options: bool = typer.Option(
        True, "--copy-options/--no-copy-options", help="Copy collection options (MongoDB only)"
    ),
) -> None:
    try:
        spec_list = _resolve_tables(source, dest, tables)
        config = TransferConfig(
            source=ConnectorConfig(uri=source),
            dest=ConnectorConfig(uri=dest),
            tables=spec_list,
            batch_size=batch_size,
            drop_first=drop_first,
            dry_run=dry_run,
            limit=limit,
            filter=_parse_filter(filter_json),
            conflict=conflict,
            copy_indexes=copy_indexes,
            copy_options=copy_options,
        )
        with connect(config.source) as src:
            counts = {table.name: table.count for table in src.list_tables()}
        progress = make_progress()
        tasks: dict[str, int] = {}
        with progress:
            for spec in config.tables:
                total = counts.get(spec.source, 0)
                if config.limit:
                    total = min(total, config.limit)
                tasks[spec.source] = progress.add_task(f"{spec.source} → {spec.dest}", total=total or None)
            results = run_transfer(
                config,
                on_progress=lambda table, copied: progress.update(tasks[table], completed=copied),
            )
        if config.dry_run:
            console.print("[yellow]Dry run completed — no data was written.[/]")
        _summary_table(results)
    except OmniDBMError as exc:
        console.print(f"[red]Error:[/] {exc}")
        raise typer.Exit(code=1) from exc


@app.command()
def wizard() -> None:
    try:
        config = run_wizard()
        if config is None:
            console.print("Aborted.")
            raise typer.Exit()
        progress = make_progress()
        with progress:
            for spec in config.tables:
                progress.add_task(f"{spec.source} → {spec.dest}", total=None)
            results = run_transfer(config)
        _summary_table(results)
    except OmniDBMError as exc:
        console.print(f"[red]Error:[/] {exc}")
        raise typer.Exit(code=1) from exc


@app.command()
def inspect(
    uri: str = typer.Argument(..., help="Connection URI to inspect"),
    sample: int = typer.Option(0, "--sample", "-n", min=0, help="Print N sample documents per table"),
) -> None:
    try:
        tables = inspect_source(ConnectorConfig(uri=uri))
        if not tables:
            console.print("[yellow]No tables found.[/]")
            raise typer.Exit()
        table = Table(title=f"Inspection of {uri}", show_lines=True)
        table.add_column("Table", style="cyan")
        table.add_column("Documents", justify="right")
        for info in tables:
            table.add_row(info.name, f"{info.count:,}")
        console.print(table)
        if sample:
            with connect(ConnectorConfig(uri=uri)) as src:
                for info in tables:
                    console.print(f"\n[bold cyan]{info.name}[/] sample:")
                    for batch in src.read_stream(info.name, batch_size=sample):
                        for doc in batch:
                            console.print(json.dumps(doc, indent=2, ensure_ascii=False))
                        break
    except OmniDBMError as exc:
        console.print(f"[red]Error:[/] {exc}")
        raise typer.Exit(code=1) from exc


@app.command()
def doctor(
    uris: list[str] | None = typer.Argument(None, help="One or more URIs to check"),
) -> None:
    from rich.prompt import Prompt

    if not uris:
        raw = Prompt.ask("URIs to check (comma-separated)")
        uris = [uri.strip() for uri in raw.split(",") if uri.strip()]
    if not uris:
        raise typer.Exit()
    table = Table(title="Connectivity check", show_lines=True)
    table.add_column("URI", style="cyan")
    table.add_column("Status")
    for uri, msg in doctor_check([ConnectorConfig(uri=u) for u in uris]).items():
        table.add_row(uri, "[green]OK[/]" if msg == "OK" else f"[red]FAILED[/] — {msg}")
    console.print(table)


@app.command()
def version() -> None:
    console.print(f"omnidbm {__version__}")


if __name__ == "__main__":
    app()
