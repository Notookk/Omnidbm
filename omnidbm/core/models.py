from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ConflictStrategy(str, Enum):
    SKIP = "skip"
    OVERWRITE = "overwrite"
    ERROR = "error"


@dataclass
class ConnectorConfig:
    uri: str
    database: str | None = None
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class TableSpec:
    source: str
    dest: str


@dataclass
class TransferConfig:
    source: ConnectorConfig
    dest: ConnectorConfig
    tables: list[TableSpec] = field(default_factory=list)
    batch_size: int = 1000
    drop_first: bool = False
    dry_run: bool = False
    limit: int | None = None
    filter: dict[str, Any] | None = None
    conflict: ConflictStrategy = ConflictStrategy.SKIP
    copy_indexes: bool = True
    copy_options: bool = True

    def __post_init__(self) -> None:
        if isinstance(self.conflict, str):
            self.conflict = ConflictStrategy(self.conflict)


@dataclass
class TableInfo:
    name: str
    count: int = 0


@dataclass
class TransferResult:
    table: str
    copied: int
    duration: float
