from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt

from omnidbm.core.models import ConflictStrategy, ConnectorConfig, TableSpec, TransferConfig
from omnidbm.core.pipeline import connect

_SUPPORTED = {
    "mongo": ("MongoDB", "mongodb://user:pass@host:27017/db"),
    "postgres": ("PostgreSQL", "postgresql://user:pass@host:5432/db"),
    "mysql": ("MySQL", "mysql://user:pass@host:3306/db"),
    "sqlite": ("SQLite", "sqlite://C:/path/db.sqlite"),
    "redis": ("Redis", "redis://user:pass@host:6379/0"),
    "csv": ("CSV", "csv://C:/path/to/file.csv"),
    "jsonl": ("JSONL", "jsonl://C:/path/out.jsonl"),
}

console = Console()


def _pick_type(role: str) -> str:
    console.print(f"\n[bold cyan]Select {role} type:[/]")
    for index, name in enumerate(_SUPPORTED, start=1):
        console.print(f"  [bold]{index}[/] {_SUPPORTED[name][0]}")
    while True:
        answer = Prompt.ask("Choice", default="1")
        try:
            index = int(answer)
            return list(_SUPPORTED)[index - 1]
        except (ValueError, IndexError):
            console.print("[red]Invalid choice, try again.[/]")


def _parse_tables(answer: str, names: list[str]) -> list[str]:
    if answer.strip().lower() in ("all", ""):
        return names
    selected: list[str] = []
    for part in answer.split(","):
        part = part.strip()
        try:
            index = int(part)
            selected.append(names[index - 1])
        except (ValueError, IndexError):
            if part in names:
                selected.append(part)
    return selected


def run_wizard() -> TransferConfig | None:
    console.print(
        Panel.fit(
            "[bold cyan]omnidbm wizard[/] — interactive data transfer setup",
            border_style="cyan",
        )
    )
    source_type = _pick_type("source")
    dest_type = _pick_type("destination")
    source_uri = Prompt.ask("Source connection URI", default=_SUPPORTED[source_type][1])
    dest_uri = Prompt.ask("Destination connection URI", default=_SUPPORTED[dest_type][1])

    config = TransferConfig(
        source=ConnectorConfig(uri=source_uri),
        dest=ConnectorConfig(uri=dest_uri),
    )

    with connect(config.source) as src:
        tables = src.list_tables()
        if not tables:
            console.print("[red]No tables found in source.[/]")
            return None
        console.print("\n[bold cyan]Available tables:[/]")
        for index, table in enumerate(tables, start=1):
            console.print(f"  [bold]{index}[/] {table.name} ({table.count} rows)")
        answer = Prompt.ask("Tables to transfer (numbers, names, or 'all')", default="all")
        names = _parse_tables(answer, [table.name for table in tables])
        if not names:
            console.print("[red]No tables selected.[/]")
            return None
        config.tables = [TableSpec(source=name, dest=name) for name in names]

    config.drop_first = Confirm.ask("Drop destination tables before transfer?", default=False)
    config.dry_run = Confirm.ask("Dry run (count only, no writes)?", default=False)
    config.batch_size = int(Prompt.ask("Batch size", default="1000"))
    config.conflict = ConflictStrategy(
        Prompt.ask(
            "On conflict",
            choices=[strategy.value for strategy in ConflictStrategy],
            default=ConflictStrategy.SKIP.value,
        )
    )

    console.print(
        Panel.fit(
            f"[bold green]Ready:[/] {len(config.tables)} table(s) from [cyan]{source_uri}[/] to [cyan]{dest_uri}[/]"
            f"\nDrop first: [cyan]{config.drop_first}[/]  Dry run: [cyan]{config.dry_run}[/]  "
            f"Batch size: [cyan]{config.batch_size}[/]  On conflict: [cyan]{config.conflict.value}[/]",
            border_style="green",
        )
    )
    if not Confirm.ask("Start transfer?"):
        return None
    return config
