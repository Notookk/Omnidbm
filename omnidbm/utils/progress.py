from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TransferSpeedColumn,
)


def make_progress() -> Progress:
    return Progress(
        TextColumn("[bold cyan]{task.description}[/]"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
        TransferSpeedColumn(),
        TimeElapsedColumn(),
    )
