from __future__ import annotations

import base64
import datetime as dt
from decimal import Decimal
from typing import Any

TAG_DATETIME = "$omni:datetime"
TAG_OBJECTID = "$omni:objectid"
TAG_BINARY = "$omni:binary"
TAG_DECIMAL = "$omni:decimal"

_TAGGED_TYPES = {TAG_DATETIME, TAG_OBJECTID, TAG_BINARY, TAG_DECIMAL}


def _tag(key: str, value: str) -> dict[str, str]:
    return {key: value}


def _is_named(value: Any, name: str) -> bool:
    return type(value).__name__ == name and type(value).__module__.startswith("bson")


def to_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        if len(value) == 1 and next(iter(value)) in _TAGGED_TYPES:
            return value
        return {k: to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_jsonable(v) for v in value]
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return _tag(TAG_DATETIME, value.astimezone(dt.timezone.utc).isoformat())
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return _tag(TAG_BINARY, base64.b64encode(bytes(value)).decode("ascii"))
    if isinstance(value, Decimal):
        return _tag(TAG_DECIMAL, str(value))
    if _is_named(value, "ObjectId"):
        return _tag(TAG_OBJECTID, str(value))
    if _is_named(value, "Decimal128"):
        return _tag(TAG_DECIMAL, str(value.to_decimal()))
    return value


def from_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        if len(value) == 1 and next(iter(value)) in _TAGGED_TYPES:
            tag, raw = next(iter(value.items()))
            if tag == TAG_DATETIME:
                return dt.datetime.fromisoformat(raw)
            if tag == TAG_OBJECTID:
                from bson import ObjectId

                return ObjectId(raw)
            if tag == TAG_BINARY:
                return base64.b64decode(raw)
            if tag == TAG_DECIMAL:
                return Decimal(raw)
        return {k: from_jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [from_jsonable(v) for v in value]
    return value


def _is_tag(value: Any) -> bool:
    return isinstance(value, dict) and len(value) == 1 and next(iter(value)) in _TAGGED_TYPES


def _tag_pair(value: dict[str, Any]) -> tuple[str, str]:
    return next(iter(value.items()))


def sql_type(value: Any) -> str:
    if isinstance(value, dict) and len(value) == 1:
        if TAG_DATETIME in value:
            return "timestamptz"
        if TAG_OBJECTID in value:
            return "text"
        if TAG_BINARY in value:
            return "bytea"
        if TAG_DECIMAL in value:
            return "numeric"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "bigint"
    if isinstance(value, float):
        return "double precision"
    if isinstance(value, str):
        return "text"
    if isinstance(value, (dict, list)):
        return "jsonb"
    return "text"


def pg_encode(value: Any) -> Any:
    if isinstance(value, dict):
        if _is_tag(value):
            tag, raw = _tag_pair(value)
            if tag == TAG_DATETIME:
                return dt.datetime.fromisoformat(raw)
            if tag == TAG_OBJECTID:
                return raw
            if tag == TAG_BINARY:
                return base64.b64decode(raw)
            if tag == TAG_DECIMAL:
                return Decimal(raw)
        from psycopg.types.json import Jsonb

        return Jsonb(value)
    if isinstance(value, list):
        from psycopg.types.json import Jsonb

        return Jsonb(value)
    return value


def mysql_type(value: Any) -> str:
    if isinstance(value, dict) and len(value) == 1:
        if TAG_DATETIME in value:
            return "DATETIME"
        if TAG_OBJECTID in value:
            return "VARCHAR(32)"
        if TAG_BINARY in value:
            return "LONGBLOB"
        if TAG_DECIMAL in value:
            return "DECIMAL(38, 18)"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE"
    if isinstance(value, str):
        return "TEXT"
    if isinstance(value, (dict, list)):
        return "JSON"
    return "TEXT"


def mysql_encode(value: Any) -> Any:
    if isinstance(value, dict):
        if _is_tag(value):
            tag, raw = _tag_pair(value)
            if tag == TAG_DATETIME:
                return dt.datetime.fromisoformat(raw)
            if tag == TAG_OBJECTID:
                return raw
            if tag == TAG_BINARY:
                return base64.b64decode(raw)
            if tag == TAG_DECIMAL:
                return Decimal(raw)
        import json

        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        import json

        return json.dumps(value, ensure_ascii=False)
    return value


def sqlite_type(value: Any) -> str:
    if isinstance(value, dict) and len(value) == 1:
        if TAG_BINARY in value:
            return "BLOB"
        return "TEXT"
    if isinstance(value, bool):
        return "INTEGER"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "REAL"
    if isinstance(value, str):
        return "TEXT"
    if isinstance(value, (dict, list)):
        return "TEXT"
    return "TEXT"


def sqlite_encode(value: Any) -> Any:
    if isinstance(value, dict):
        if _is_tag(value):
            tag, raw = _tag_pair(value)
            if tag == TAG_BINARY:
                return base64.b64decode(raw)
            return raw
        import json

        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        import json

        return json.dumps(value, ensure_ascii=False)
    return value


def csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        if len(value) == 1 and next(iter(value)) in _TAGGED_TYPES:
            return next(iter(value.values()))
        import json

        return json.dumps(value)
    if isinstance(value, (list, tuple)):
        import json

        return json.dumps(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
