from __future__ import annotations

import json
import os
from collections.abc import Callable, Iterator
from typing import Any

from omnidbm.core.connector import BaseConnector, matches, register
from omnidbm.core.errors import ConnectionError, TransferError
from omnidbm.core.models import ConflictStrategy, TableInfo


@register("jsonl")
class JsonlConnector(BaseConnector):
    name = "jsonl"

    def connect(self) -> None:
        self.path = self.config.uri[len("jsonl://") :]

    def check(self) -> None:
        self.connect()
        if not os.path.exists(self.path):
            raise ConnectionError(f"JSONL path not found: {self.path}")

    def _resolve(self, table: str) -> str:
        if os.path.isdir(self.path):
            return os.path.join(self.path, table if table.endswith(".jsonl") else f"{table}.jsonl")
        return self.path

    def list_tables(self) -> list[TableInfo]:
        if os.path.isdir(self.path):
            names = sorted(name for name in os.listdir(self.path) if name.endswith(".jsonl"))
            counts = [self._count(self._resolve(name)) for name in names]
            return [TableInfo(name=name, count=count) for name, count in zip(names, counts, strict=True)]
        if os.path.exists(self.path):
            return [TableInfo(name=os.path.basename(self.path), count=self._count(self.path))]
        return []

    def _count(self, path: str) -> int:
        with open(path, encoding="utf-8") as handle:
            return sum(1 for line in handle if line.strip())

    def read_stream(
        self,
        table: str,
        batch_size: int = 1000,
        limit: int | None = None,
        query: dict[str, Any] | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        path = self._resolve(table)
        if not os.path.exists(path):
            raise TransferError(f"JSONL file not found: {path}")
        with open(path, encoding="utf-8") as handle:
            batch: list[dict[str, Any]] = []
            total = 0
            for line in handle:
                if not line.strip():
                    continue
                doc = json.loads(line)
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
        ids: set[str] = set()
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                doc = json.loads(line)
                if "_id" in doc:
                    ids.add(str(doc["_id"]))
        return ids

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
        seen: set[str] = set()
        copied = 0
        with open(path, "a", encoding="utf-8") as handle:
            for batch in batches:
                for doc in batch:
                    oid = doc.get("_id")
                    if oid is not None:
                        key = str(oid)
                        if key in seen or key in existing:
                            if conflict == ConflictStrategy.ERROR:
                                raise TransferError(f"Duplicate _id {oid!r} in {path}")
                            continue
                        seen.add(key)
                    handle.write(json.dumps(doc, ensure_ascii=False) + "\n")
                    copied += 1
                if on_progress:
                    on_progress(copied)
        return copied

    def close(self) -> None:
        pass
