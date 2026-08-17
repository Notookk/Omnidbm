from __future__ import annotations

import datetime as dt
from collections.abc import Iterator

import pytest

from omnidbm.core.connector import BaseConnector
from omnidbm.core.errors import UnsupportedSchemeError
from omnidbm.core.models import (
    ConflictStrategy,
    ConnectorConfig,
    TableInfo,
    TableSpec,
    TransferConfig,
)
from omnidbm.core.pipeline import run_transfer


class MemoryConnector(BaseConnector):
    def __init__(self, data: dict[str, list[dict]] | None = None):
        super().__init__(ConnectorConfig(uri="memory://"))
        self.data = data or {}

    def connect(self) -> None:
        pass

    def list_tables(self) -> list[TableInfo]:
        return [TableInfo(name=name, count=len(docs)) for name, docs in self.data.items()]

    def read_stream(self, table, batch_size=1000, limit=None, query=None) -> Iterator[list[dict]]:
        docs = self.data.get(table, [])
        if query:
            docs = [d for d in docs if all(d.get(k) == v for k, v in query.items())]
        if limit is not None:
            docs = docs[:limit]
        for start in range(0, len(docs), batch_size):
            yield docs[start : start + batch_size]

    def write_stream(self, table, batches, drop_first=False, conflict=ConflictStrategy.SKIP, on_progress=None) -> int:
        if drop_first:
            self.data[table] = []
        copied = 0
        for batch in batches:
            self.data.setdefault(table, []).extend(batch)
            copied += len(batch)
            if on_progress:
                on_progress(copied)
        return copied

    def close(self) -> None:
        pass


def make_config(**kwargs) -> TransferConfig:
    defaults = {
        "source": ConnectorConfig(uri="memory://"),
        "dest": ConnectorConfig(uri="memory://"),
        "tables": [TableSpec(source="users", dest="users")],
    }
    defaults.update(kwargs)
    return TransferConfig(**defaults)


def test_transfer_copies_all_documents():
    source = MemoryConnector({"users": [{"_id": "1", "name": "a"}, {"_id": "2", "name": "b"}]})
    dest = MemoryConnector()
    results = run_transfer(make_config(), source=source, dest=dest)
    assert results[0].copied == 2
    assert dest.data["users"] == source.data["users"]


def test_transfer_batches_respect_batch_size():
    docs = [{"_id": str(i)} for i in range(2500)]
    source = MemoryConnector({"users": docs})
    dest = MemoryConnector()
    run_transfer(make_config(batch_size=1000), source=source, dest=dest)
    assert len(dest.data["users"]) == 2500


def test_dry_run_writes_nothing():
    source = MemoryConnector({"users": [{"_id": "1"}, {"_id": "2"}, {"_id": "3"}]})
    dest = MemoryConnector()
    results = run_transfer(make_config(dry_run=True), source=source, dest=dest)
    assert results[0].copied == 3
    assert "users" not in dest.data


def test_limit_caps_documents():
    source = MemoryConnector({"users": [{"_id": str(i)} for i in range(10)]})
    dest = MemoryConnector()
    results = run_transfer(make_config(limit=4), source=source, dest=dest)
    assert results[0].copied == 4


def test_filter_restricts_documents():
    source = MemoryConnector({"users": [{"_id": "1", "active": True}, {"_id": "2", "active": False}]})
    dest = MemoryConnector()
    results = run_transfer(make_config(filter={"active": True}), source=source, dest=dest)
    assert results[0].copied == 1
    assert dest.data["users"][0]["_id"] == "1"


def test_drop_first_clears_destination():
    source = MemoryConnector({"users": [{"_id": "1"}]})
    dest = MemoryConnector({"users": [{"_id": "old"}]})
    run_transfer(make_config(drop_first=True), source=source, dest=dest)
    assert dest.data["users"] == [{"_id": "1"}]


def test_progress_callback_fires():
    source = MemoryConnector({"users": [{"_id": str(i)} for i in range(5)]})
    dest = MemoryConnector()
    progress: list[tuple[str, int]] = []
    run_transfer(make_config(), source=source, dest=dest, on_progress=lambda t, n: progress.append((t, n)))
    assert progress[-1] == ("users", 5)


def test_multiple_tables():
    source = MemoryConnector({"a": [{"_id": "1"}], "b": [{"_id": "2"}, {"_id": "3"}]})
    dest = MemoryConnector()
    config = make_config(tables=[TableSpec(source="a", dest="a"), TableSpec(source="b", dest="b")])
    results = run_transfer(config, source=source, dest=dest)
    assert [r.copied for r in results] == [1, 2]
    assert len(dest.data["b"]) == 2


def test_values_preserved_across_transfer():
    now = dt.datetime(2024, 6, 1, tzinfo=dt.timezone.utc)
    source = MemoryConnector({"users": [{"_id": "1", "created": now, "score": 1.5}]})
    dest = MemoryConnector()
    run_transfer(make_config(), source=source, dest=dest)
    assert dest.data["users"][0]["created"] == now


def test_unsupported_scheme_raises():
    with pytest.raises(UnsupportedSchemeError):
        run_transfer(make_config(source=ConnectorConfig(uri="sqlite://x")))
