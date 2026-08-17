from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from typing import Any

from omnidbm.core.errors import UnsupportedSchemeError
from omnidbm.core.models import ConflictStrategy, ConnectorConfig, TableInfo

_REGISTRY: dict[str, type[BaseConnector]] = {}


def register(*schemes: str):
    def decorator(cls: type[BaseConnector]) -> type[BaseConnector]:
        for scheme in schemes:
            _REGISTRY[scheme] = cls
        return cls

    return decorator


def get_connector(config: ConnectorConfig) -> BaseConnector:
    scheme = config.uri.split(":", 1)[0].lower()
    cls = _REGISTRY.get(scheme)
    if cls is None:
        raise UnsupportedSchemeError(f"Unsupported URI scheme '{scheme}'. Supported: {', '.join(sorted(_REGISTRY))}")
    return cls(config)


def matches(query: dict[str, Any], doc: dict[str, Any]) -> bool:
    return all(doc.get(k) == v for k, v in query.items())


class BaseConnector(ABC):
    name: str = "base"

    def __init__(self, config: ConnectorConfig):
        self.config = config

    @abstractmethod
    def connect(self) -> None: ...

    def check(self) -> None:
        self.connect()

    @abstractmethod
    def list_tables(self) -> list[TableInfo]: ...

    @abstractmethod
    def read_stream(
        self,
        table: str,
        batch_size: int = 1000,
        limit: int | None = None,
        query: dict[str, Any] | None = None,
    ) -> Iterator[list[dict[str, Any]]]: ...

    @abstractmethod
    def write_stream(
        self,
        table: str,
        batches: Iterator[list[dict[str, Any]]],
        drop_first: bool = False,
        conflict: ConflictStrategy = ConflictStrategy.SKIP,
        on_progress: Callable[[int], None] | None = None,
    ) -> int: ...

    def copy_metadata(
        self,
        dest_table: str,
        source: BaseConnector,
        source_table: str,
        copy_indexes: bool = True,
        copy_options: bool = True,
    ) -> None:
        pass

    @abstractmethod
    def close(self) -> None: ...

    def __enter__(self) -> BaseConnector:
        self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
