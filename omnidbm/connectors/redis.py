from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

from omnidbm.core.connector import BaseConnector, matches, register
from omnidbm.core.errors import ConnectionError, TransferError
from omnidbm.core.models import ConflictStrategy, TableInfo

ALL_KEYS = "(all)"


def _read_value(client: Any, key: str) -> tuple[str, Any]:
    kind = client.type(key)
    if kind == "hash":
        return "hash", client.hgetall(key)
    if kind == "list":
        return "list", client.lrange(key, 0, -1)
    if kind == "set":
        return "set", list(client.smembers(key))
    return "string", client.get(key)


@register("redis", "rediss")
class RedisConnector(BaseConnector):
    name = "redis"

    def connect(self) -> None:
        try:
            import redis
        except ImportError as exc:
            raise ConnectionError("redis-py is required for Redis support: pip install omnidbm[redis]") from exc
        try:
            self.client = redis.Redis.from_url(self.config.uri, decode_responses=True)
            self.client.ping()
        except Exception as exc:
            raise ConnectionError(f"Redis connection failed: {exc}") from exc

    def _prefix(self, table: str) -> str:
        return "" if table == ALL_KEYS else table

    def list_tables(self) -> list[TableInfo]:
        prefixes: dict[str, int] = {}
        for key in self.client.scan_iter():
            prefix = key.split(":", 1)[0] if ":" in key else ALL_KEYS
            prefixes[prefix] = prefixes.get(prefix, 0) + 1
        if not prefixes:
            return []
        return [TableInfo(name=name, count=count) for name, count in sorted(prefixes.items(), key=lambda item: item[0])]

    def read_stream(
        self,
        table: str,
        batch_size: int = 1000,
        limit: int | None = None,
        query: dict[str, Any] | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        prefix = self._prefix(table)
        pattern = f"{prefix}:*" if prefix else "*"
        batch: list[dict[str, Any]] = []
        for key in self.client.scan_iter(match=pattern):
            _, value = _read_value(self.client, key)
            doc: dict[str, Any] = {"_key": key, "_value": value}
            ttl = self.client.ttl(key)
            if ttl is not None and ttl >= 0:
                doc["_ttl"] = ttl
            if query and not matches(query, doc):
                continue
            batch.append(doc)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _key_exists(self, key: str) -> bool:
        return bool(self.client.exists(key))

    def _write_doc(self, key: str, doc: dict[str, Any]) -> None:
        value = doc.get("_value")
        if value is None:
            raise TransferError(f"Redis target requires '_value' in document for key {key!r}")
        if isinstance(value, dict):
            self.client.delete(key)
            self.client.hset(key, mapping=value)
        elif isinstance(value, list):
            self.client.delete(key)
            if value:
                self.client.rpush(key, *value)
        elif isinstance(value, str):
            self.client.set(key, value)
        else:
            self.client.set(key, str(value))
        ttl = doc.get("_ttl")
        if ttl is not None:
            self.client.expire(key, int(ttl))

    def write_stream(
        self,
        table: str,
        batches: Iterator[list[dict[str, Any]]],
        drop_first: bool = False,
        conflict: ConflictStrategy = ConflictStrategy.SKIP,
        on_progress: Callable[[int], None] | None = None,
    ) -> int:
        prefix = self._prefix(table)
        if drop_first:
            self._drop_prefix(prefix)
        copied = 0
        for batch in batches:
            for doc in batch:
                key = str(doc.get("_key", ""))
                if not key:
                    raise TransferError("Redis target requires '_key' in document")
                if prefix and not key.startswith(f"{prefix}:"):
                    key = f"{prefix}:{key}"
                if conflict == ConflictStrategy.ERROR and self._key_exists(key):
                    raise TransferError(f"Key already exists in Redis: {key}")
                if conflict == ConflictStrategy.SKIP and self._key_exists(key):
                    continue
                self._write_doc(key, doc)
                copied += 1
            if on_progress:
                on_progress(copied)
        return copied

    def _drop_prefix(self, prefix: str) -> None:
        pattern = f"{prefix}:*" if prefix else "*"
        pipeline = self.client.pipeline()
        for key in self.client.scan_iter(match=pattern):
            pipeline.delete(key)
        pipeline.execute()

    def close(self) -> None:
        self.client.close()
