from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from omnidbm.core.connector import BaseConnector, register
from omnidbm.core.errors import ConnectionError, TransferError
from omnidbm.core.models import ConflictStrategy, TableInfo
from omnidbm.core.typemap import pg_encode, sql_type, to_jsonable


@register("postgres", "postgresql")
class PostgresConnector(BaseConnector):
    name = "postgres"

    def connect(self) -> None:
        try:
            self.conn = psycopg.connect(self.config.uri)
        except Exception as exc:
            raise ConnectionError(f"PostgreSQL connection failed: {exc}") from exc

    def list_tables(self) -> list[TableInfo]:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name"
            )
            names = [row[0] for row in cur.fetchall()]
        return [TableInfo(name=name, count=self._count(name)) for name in names]

    def _count(self, table: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
            return cur.fetchone()[0]

    def _table_exists(self, table: str) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s",
                (table,),
            )
            return cur.fetchone() is not None

    def read_stream(
        self,
        table: str,
        batch_size: int = 1000,
        limit: int | None = None,
        query: dict[str, Any] | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        stmt = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table))
        params: list[Any] = []
        if query:
            clauses = sql.SQL(" AND ").join(sql.SQL("{} = %s").format(sql.Identifier(key)) for key in query)
            stmt = sql.SQL("{} WHERE {}").format(stmt, clauses)
            params.extend(query.values())
        if limit is not None:
            stmt = sql.SQL("{} LIMIT %s").format(stmt)
            params.append(limit)
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(stmt, params)
            batch: list[dict[str, Any]] = []
            for row in cur:
                batch.append(to_jsonable(row))
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch

    def _ensure_schema(self, table: str, batch: list[dict[str, Any]]) -> None:
        columns: dict[str, str] = {}
        for doc in batch:
            for key, value in doc.items():
                columns.setdefault(key, sql_type(value))
        if not self._table_exists(table):
            definitions = [
                sql.SQL("{} {}").format(sql.Identifier(key), sql.SQL(column_type))
                + (sql.SQL(" PRIMARY KEY") if key == "_id" else sql.SQL(""))
                for key, column_type in columns.items()
            ]
            stmt = sql.SQL("CREATE TABLE {} ({})").format(sql.Identifier(table), sql.SQL(", ").join(definitions))
            self.conn.execute(stmt)
        else:
            for key, column_type in columns.items():
                self.conn.execute(
                    sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
                        sql.Identifier(table), sql.Identifier(key), sql.SQL(column_type)
                    )
                )
        self.conn.commit()

    def _insert_batch(
        self,
        table: str,
        batch: list[dict[str, Any]],
        conflict: ConflictStrategy,
    ) -> int:
        columns = list(batch[0].keys())
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in columns)
        values = [[pg_encode(doc.get(column)) for column in columns] for doc in batch]
        stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
            placeholders,
        )
        if conflict == ConflictStrategy.SKIP:
            stmt = sql.SQL("{} ON CONFLICT DO NOTHING").format(stmt)
        elif conflict == ConflictStrategy.OVERWRITE and "_id" in columns:
            updates = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(column), sql.Identifier(column))
                for column in columns
                if column != "_id"
            )
            stmt = sql.SQL("{} ON CONFLICT (_id) DO UPDATE SET {}").format(stmt, updates)
        try:
            with self.conn.cursor() as cur:
                cur.executemany(stmt, values)
                return cur.rowcount
        except Exception as exc:
            raise TransferError(f"PostgreSQL insert failed: {exc}") from exc

    def write_stream(
        self,
        table: str,
        batches: Iterator[list[dict[str, Any]]],
        drop_first: bool = False,
        conflict: ConflictStrategy = ConflictStrategy.SKIP,
        on_progress: Callable[[int], None] | None = None,
    ) -> int:
        if drop_first:
            self.conn.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table)))
            self.conn.commit()
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
        self.conn.commit()
        return copied

    def close(self) -> None:
        self.conn.close()
