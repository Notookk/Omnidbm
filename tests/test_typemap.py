from __future__ import annotations

import datetime as dt
from decimal import Decimal

from omnidbm.core.typemap import csv_value, from_jsonable, pg_encode, sql_type, to_jsonable


def test_datetime_round_trip():
    value = dt.datetime(2024, 5, 1, 12, 30, 0, tzinfo=dt.timezone.utc)
    encoded = to_jsonable(value)
    assert "$omni:datetime" in encoded
    assert from_jsonable(encoded) == value
    assert sql_type(encoded) == "timestamptz"


def test_naive_datetime_normalized_to_utc():
    value = dt.datetime.fromisoformat("2024-05-01T12:30:00")
    encoded = to_jsonable(value)
    assert from_jsonable(encoded).tzinfo is not None


def test_bytes_round_trip():
    value = b"\x00\x01\xffdata"
    encoded = to_jsonable(value)
    assert "$omni:binary" in encoded
    assert from_jsonable(encoded) == value
    assert sql_type(encoded) == "bytea"


def test_decimal_round_trip():
    value = Decimal("12345.6789")
    encoded = to_jsonable(value)
    assert "$omni:decimal" in encoded
    assert from_jsonable(encoded) == value
    assert sql_type(encoded) == "numeric"


def test_objectid_round_trip():
    from bson import ObjectId

    value = ObjectId("507f1f77bcf86cd799439011")
    encoded = to_jsonable(value)
    assert "$omni:objectid" in encoded
    assert from_jsonable(encoded) == value
    assert sql_type(encoded) == "text"


def test_nested_documents():
    value = {"a": [1, 2.5, "x"], "b": {"c": dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)}}
    encoded = to_jsonable(value)
    assert from_jsonable(encoded) == value


def test_sql_types():
    assert sql_type(True) == "boolean"
    assert sql_type(42) == "bigint"
    assert sql_type(1.5) == "double precision"
    assert sql_type("x") == "text"
    assert sql_type({"k": 1}) == "jsonb"
    assert sql_type([1, 2]) == "jsonb"
    assert sql_type(None) == "text"


def test_pg_encode():
    assert pg_encode(1) == 1
    assert pg_encode("x") == "x"
    assert pg_encode({"$omni:objectid": "abc"}) == "abc"
    assert pg_encode({"$omni:datetime": "2024-01-01T00:00:00+00:00"}) == dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
    assert pg_encode({"k": 1}).__class__.__name__ == "Jsonb"


def test_csv_value():
    assert csv_value(None) == ""
    assert csv_value(True) == "true"
    assert csv_value(12) == "12"
    assert csv_value({"$omni:objectid": "abc"}) == "abc"
    assert csv_value({"nested": 1}) == '{"nested": 1}'
