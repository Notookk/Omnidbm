from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from typing import Any

from omnidbm.core.connector import BaseConnector, register
from omnidbm.core.errors import ConnectionError
from omnidbm.core.models import ConflictStrategy, TableInfo
from omnidbm.core.typemap import sqlite_encode, sqlite_type, to_jsonable


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


@register("sqlite", "sqlite3")
class SQLiteConnector(BaseConnector):
    name = "sqlite"

    def connect(self) -> None:
        path = self.config.uri.split("://", 1)[1]
        try:
            self.conn = sqlite3.connect(path)
        except sqlite3.Error as exc:
            raise ConnectionError(f"SQLite connection failed: {exc}") from exc
        self.conn.row_factory = sqlite3.Row

    def list_tables(self) -> list[TableInfo]:
        with self.conn:
            names = [
                row[0]
                for row in self.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                )
            ]
            return [TableInfo(name=name, count=self._count(name)) for name in names]

    def _count(self, table: str) -> int:
        with self.conn:
            return self.conn.execute(f"SELECT COUNT(*) FROM {_quote(table)}").fetchone()[0]

    def _table_exists(self, table: str) -> bool:
        with self.conn:
            row = self.conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
            ).fetchone()
            return row is not None

    def read_stream(
        self,
        table: str,
        batch_size: int = 1000,
        limit: int | None = None,
        query: dict[str, Any] | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        sql = f"SELECT * FROM {_quote(table)}"
        params: list[Any] = []
        if query:
            clauses = " AND ".join(f"{_quote(key)} = ?" for key in query)
            sql = f"{sql} WHERE {clauses}"
            params.extend(query.values())
        if limit is not None:
            sql = f"{sql} LIMIT ?"
            params.append(limit)
        with self.conn:
            rows = self.conn.execute(sql, params)
            batch: list[dict[str, Any]] = []
            for row in rows:
                batch.append(to_jsonable(dict(row)))
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch

    def _existing_columns(self, table: str) -> set[str]:
        with self.conn:
            rows = self.conn.execute(f"PRAGMA table_info({_quote(table)})").fetchall()
            return {row[1] for row in rows}

    def _ensure_schema(self, table: str, batch: list[dict[str, Any]]) -> None:
        columns: dict[str, str] = {}
        for doc in batch:
            for key, value in doc.items():
                columns.setdefault(key, sqlite_type(value))
        with self.conn:
            if not self._table_exists(table):
                definitions = [
                    f"{_quote(key)} {column_type}" + (" PRIMARY KEY" if key == "_id" else "")
                    for key, column_type in columns.items()
                ]
                self.conn.execute(f"CREATE TABLE {_quote(table)} ({', '.join(definitions)})")
            else:
                existing = self._existing_columns(table)
                for key, column_type in columns.items():
                    if key not in existing:
                        self.conn.execute(f"ALTER TABLE {_quote(table)} ADD COLUMN {_quote(key)} {column_type}")

    def _insert_batch(
        self,
        table: str,
        batch: list[dict[str, Any]],
        conflict: ConflictStrategy,
    ) -> int:
        columns = list(batch[0].keys())
        placeholders = ", ".join("?" for _ in columns)
        values = [[sqlite_encode(doc.get(column)) for column in columns] for doc in batch]
        verb = "INSERT"
        if conflict == ConflictStrategy.SKIP:
            verb = "INSERT OR IGNORE"
        elif conflict == ConflictStrategy.OVERWRITE and "_id" in columns:
            verb = "INSERT OR REPLACE"
        sql = f"{verb} INTO {_quote(table)} ({', '.join(_quote(c) for c in columns)}) VALUES ({placeholders})"
        with self.conn:
            cursor = self.conn.executemany(sql, values)
            return cursor.rowcount

    def write_stream(
        self,
        table: str,
        batches: Iterator[list[dict[str, Any]]],
        drop_first: bool = False,
        conflict: ConflictStrategy = ConflictStrategy.SKIP,
        on_progress: Callable[[int], None] | None = None,
    ) -> int:
        with self.conn:
            if drop_first:
                self.conn.execute(f"DROP TABLE IF EXISTS {_quote(table)}")
        copied = 0
        first = True
        for batch in batches:
            if batch:
                if first:
                    self._ensure_schema(table, batch)
                    first = False
                copied += self._insert_batch(table, batch, conflict)
            if on_progress:
                on_progress(copied)
        return copied

    def close(self) -> None:
        self.conn.close()
