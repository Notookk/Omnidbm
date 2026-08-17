from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import pymongo
from pymongo.errors import BulkWriteError, CollectionInvalid, OperationFailure
from pymongo.uri_parser import parse_uri

from omnidbm.core.connector import BaseConnector, register
from omnidbm.core.errors import ConnectionError
from omnidbm.core.models import ConflictStrategy, TableInfo
from omnidbm.core.typemap import from_jsonable, to_jsonable


@register("mongodb", "mongodb+srv")
class MongoConnector(BaseConnector):
    name = "mongo"

    def connect(self) -> None:
        try:
            self.client = pymongo.MongoClient(self.config.uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command("ping")
        except Exception as exc:
            raise ConnectionError(f"MongoDB connection failed: {exc}") from exc
        database = self.config.database or parse_uri(self.config.uri).get("database")
        if not database:
            raise ConnectionError("MongoDB URI must include a database name")
        self.db = self.client[database]

    def list_tables(self) -> list[TableInfo]:
        return [
            TableInfo(name=name, count=self.db[name].estimated_document_count())
            for name in self.db.list_collection_names()
        ]

    def read_stream(
        self,
        table: str,
        batch_size: int = 1000,
        limit: int | None = None,
        query: dict[str, Any] | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        cursor = self.db[table].find(query or {}).batch_size(5000)
        if limit is not None:
            cursor = cursor.limit(limit)
        batch: list[dict[str, Any]] = []
        for doc in cursor:
            batch.append(to_jsonable(doc))
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def write_stream(
        self,
        table: str,
        batches: Iterator[list[dict[str, Any]]],
        drop_first: bool = False,
        conflict: ConflictStrategy = ConflictStrategy.SKIP,
        on_progress: Callable[[int], None] | None = None,
    ) -> int:
        collection = self.db[table]
        if drop_first:
            collection.drop()
        copied = 0
        for batch in batches:
            docs = [from_jsonable(doc) for doc in batch]
            if conflict == ConflictStrategy.OVERWRITE:
                ops = []
                for doc in docs:
                    oid = doc.get("_id")
                    if oid is not None:
                        body = dict(doc)
                        body.pop("_id", None)
                        ops.append(pymongo.UpdateOne({"_id": oid}, {"$set": body}, upsert=True))
                    else:
                        ops.append(pymongo.InsertOne(doc))
                if ops:
                    result = collection.bulk_write(ops, ordered=False)
                    copied += result.upserted_count
            elif conflict == ConflictStrategy.ERROR:
                result = collection.insert_many(docs, ordered=True)
                copied += len(result.inserted_ids)
            else:
                try:
                    result = collection.insert_many(docs, ordered=False)
                    copied += len(result.inserted_ids)
                except BulkWriteError as exc:
                    copied += exc.details.get("nInserted", 0)
            if on_progress:
                on_progress(copied)
        return copied

    def copy_metadata(
        self,
        dest_table: str,
        source: BaseConnector,
        source_table: str,
        copy_indexes: bool = True,
        copy_options: bool = True,
    ) -> None:
        if not isinstance(source, MongoConnector):
            return
        src_collection = source.db[source_table]
        dst_collection = self.db[dest_table]
        if copy_options:
            options = src_collection.options()
            try:
                self.db.create_collection(dest_table, **options)
            except (CollectionInvalid, OperationFailure):
                pass
        if copy_indexes:
            for name, spec in src_collection.index_information().items():
                if name == "_id_":
                    continue
                spec = dict(spec)
                keys = spec.pop("key")
                spec.pop("ns", None)
                spec.pop("v", None)
                dst_collection.create_index(keys, name=name, **spec)

    def close(self) -> None:
        self.client.close()
