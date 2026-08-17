from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any
from urllib.parse import unquote, urlparse

from omnidbm.core.connector import BaseConnector, register
from omnidbm.core.errors import ConnectionError, TransferError
from omnidbm.core.models import ConflictStrategy, TableInfo
from omnidbm.core.typemap import mysql_encode, mysql_type, to_jsonable


def parse_mysql_uri(uri: str) -> dict[str, str]:
    parsed = urlparse(uri)
    return {
        "host": parsed.hostname or "localhost",
        "port": str(parsed.port or 3306),
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "database": parsed.path.lstrip("/"),
    }


def _quote(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


@register("mysql")
class MySQLConnector(BaseConnector):
    name = "mysql"

    def connect(self) -> None:
        try:
            import pymysql
        except ImportError as exc:
            raise ConnectionError("PyMySQL is required for MySQL support: pip install omnidbm[mysql]") from exc
        params = parse_mysql_uri(self.config.uri)
        database = self.config.database or params["database"]
        if not database:
            raise ConnectionError("MySQL URI must include a database name")
        try:
            self.conn = pymysql.connect(
                host=params["host"],
                port=int(params["port"]),
                user=params["user"],
                password=params["password"],
                database=database,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
            )
        except Exception as exc:
            raise ConnectionError(f"MySQL connection failed: {exc}") from exc

    def list_tables(self) -> list[TableInfo]:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY table_name"
            )
            names = [row["table_name"] for row in cur.fetchall()]
        return [TableInfo(name=name, count=self._count(name)) for name in names]

    def _count(self, table: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS c FROM {_quote(table)}")
            return cur.fetchone()["c"]

    def _table_exists(self, table: str) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = %s",
                (table,),
            )
            return cur.fetchone() is not None

    def _existing_columns(self, table: str) -> set[str]:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = %s",
                (table,),
            )
            return {row["column_name"] for row in cur.fetchall()}

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
            clauses = " AND ".join(f"{_quote(key)} = %s" for key in query)
            sql = f"{sql} WHERE {clauses}"
            params.extend(query.values())
        if limit is not None:
            sql = f"{sql} LIMIT %s"
            params.append(limit)
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
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
                columns.setdefault(key, mysql_type(value))
        with self.conn.cursor() as cur:
            if not self._table_exists(table):
                definitions = []
                for key, column_type in columns.items():
                    if key == "_id":
                        column_type = "VARCHAR(191) PRIMARY KEY"
                    definitions.append(f"{_quote(key)} {column_type}")
                cur.execute(f"CREATE TABLE IF NOT EXISTS {_quote(table)} ({', '.join(definitions)})")
            else:
                existing = self._existing_columns(table)
                for key, column_type in columns.items():
                    if key not in existing:
                        cur.execute(f"ALTER TABLE {_quote(table)} ADD COLUMN {_quote(key)} {column_type}")
        self.conn.commit()

    def _insert_batch(
        self,
        table: str,
        batch: list[dict[str, Any]],
        conflict: ConflictStrategy,
    ) -> int:
        columns = list(batch[0].keys())
        placeholders = ", ".join("%s" for _ in columns)
        column_list = ", ".join(_quote(c) for c in columns)
        values = [[mysql_encode(doc.get(column)) for column in columns] for doc in batch]
        if conflict == ConflictStrategy.SKIP:
            sql = f"INSERT IGNORE INTO {_quote(table)} ({column_list}) VALUES ({placeholders})"
        elif conflict == ConflictStrategy.OVERWRITE and "_id" in columns:
            updates = ", ".join(f"{_quote(c)} = VALUES({_quote(c)})" for c in columns if c != "_id")
            sql = (
                f"INSERT INTO {_quote(table)} ({column_list}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
            )
        else:
            sql = f"INSERT INTO {_quote(table)} ({column_list}) VALUES ({placeholders})"
        try:
            with self.conn.cursor() as cur:
                return cur.executemany(sql, values).rowcount
        except Exception as exc:
            raise TransferError(f"MySQL insert failed: {exc}") from exc

    def write_stream(
        self,
        table: str,
        batches: Iterator[list[dict[str, Any]]],
        drop_first: bool = False,
        conflict: ConflictStrategy = ConflictStrategy.SKIP,
        on_progress: Callable[[int], None] | None = None,
    ) -> int:
        with self.conn.cursor() as cur:
            if drop_first:
                cur.execute(f"DROP TABLE IF EXISTS {_quote(table)}")
        self.conn.commit()
        copied = 0
        first = True
        for batch in batches:
            if batch:
                if first:
                    self._ensure_schema(table, batch)
                    first = False
                copied += self._insert_batch(table, batch, conflict)
                self.conn.commit()
            if on_progress:
                on_progress(copied)
        return copied

    def close(self) -> None:
        self.conn.close()
