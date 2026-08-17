from __future__ import annotations

import csv
import json
import os
import re
from collections.abc import Callable, Iterator
from typing import Any

from omnidbm.core.connector import BaseConnector, matches, register
from omnidbm.core.errors import ConnectionError, TransferError
from omnidbm.core.models import ConflictStrategy, TableInfo
from omnidbm.core.typemap import csv_value

_FLOAT_RE = re.compile(r"^-?\d+\.\d+([eE][+-]?\d+)?$")


def _coerce(value: str) -> Any:
    if value == "":
        return None
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in ("null", "none"):
        return None
    if value.lstrip("-").isdigit():
        number = int(value)
        return number if str(number) == value else value
    if _FLOAT_RE.match(value):
        return float(value)
    if value[:1] in ("{", "[", '"'):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


@register("csv")
class CsvConnector(BaseConnector):
    name = "csv"

    def connect(self) -> None:
        self.path = self.config.uri[len("csv://") :]

    def check(self) -> None:
        self.connect()
        if not os.path.exists(self.path):
            raise ConnectionError(f"CSV path not found: {self.path}")

    def _resolve(self, table: str) -> str:
        if os.path.isdir(self.path):
            return os.path.join(self.path, table if table.endswith(".csv") else f"{table}.csv")
        return self.path

    def list_tables(self) -> list[TableInfo]:
        if os.path.isdir(self.path):
            names = sorted(name for name in os.listdir(self.path) if name.endswith(".csv"))
            counts = [self._count(self._resolve(name)) for name in names]
            return [TableInfo(name=name, count=count) for name, count in zip(names, counts, strict=True)]
        if os.path.exists(self.path):
            return [TableInfo(name=os.path.basename(self.path), count=self._count(self.path))]
        return []

    def _count(self, path: str) -> int:
        with open(path, newline="", encoding="utf-8") as handle:
            return sum(1 for _ in handle) - 1

    def read_stream(
        self,
        table: str,
        batch_size: int = 1000,
        limit: int | None = None,
        query: dict[str, Any] | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        path = self._resolve(table)
        if not os.path.exists(path):
            raise TransferError(f"CSV file not found: {path}")
        with open(path, newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            batch: list[dict[str, Any]] = []
            total = 0
            for row in reader:
                doc = {key: _coerce(value) for key, value in row.items()}
                if query and not matches(query, doc):
                    continue
                batch.append(doc)
                total += 1
                if len(batch) >= batch_size or (limit is not None and total >= limit):
                    yield batch
                    batch = []
                    if limit is not None and total >= limit:
                        return
            if batch:
                yield batch

    def _existing_ids(self, path: str) -> set[str]:
        if not os.path.exists(path):
            return set()
        with open(path, newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if "_id" not in (reader.fieldnames or []):
                return set()
            return {row["_id"] for row in reader}

    def write_stream(
        self,
        table: str,
        batches: Iterator[list[dict[str, Any]]],
        drop_first: bool = False,
        conflict: ConflictStrategy = ConflictStrategy.SKIP,
        on_progress: Callable[[int], None] | None = None,
    ) -> int:
        path = self._resolve(table)
        if drop_first and os.path.exists(path):
            os.remove(path)
        existing = self._existing_ids(path) if conflict != ConflictStrategy.OVERWRITE else set()
        fieldnames: list[str] = []
        seen: set[str] = set()
        copied = 0
        with open(path, "a", newline="", encoding="utf-8") as handle:
            writer = None
            for batch in batches:
                for doc in batch:
                    new_keys = [key for key in doc if key not in fieldnames]
                    if writer is not None and new_keys:
                        raise TransferError(
                            f"CSV target requires consistent keys; found new key {new_keys[0]!r} in {path}. "
                            "Use jsonl:// for heterogeneous documents."
                        )
                    fieldnames.extend(new_keys)
                    oid = doc.get("_id")
                    if oid is not None:
                        key = str(oid)
                        if key in seen or key in existing:
                            if conflict == ConflictStrategy.ERROR:
                                raise TransferError(f"Duplicate _id {oid!r} in {path}")
                            continue
                        seen.add(key)
                    if writer is None:
                        writer = csv.DictWriter(handle, fieldnames=fieldnames)
                        writer.writeheader()
                    writer.writerow({key: csv_value(value) for key, value in doc.items()})
                    copied += 1
                if on_progress:
                    on_progress(copied)
        return copied

    def close(self) -> None:
        pass
