from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from omnidbm.connectors.mysql import parse_mysql_uri
from omnidbm.core.connector import get_connector
from omnidbm.core.errors import UnsupportedSchemeError
from omnidbm.core.models import ConnectorConfig
from omnidbm.core.typemap import (
    TAG_BINARY,
    TAG_DATETIME,
    TAG_DECIMAL,
    TAG_OBJECTID,
    mysql_encode,
    mysql_type,
    sqlite_encode,
    sqlite_type,
)

DATETIME_TAG = {TAG_DATETIME: "2024-06-01T12:30:00+00:00"}
OBJECTID_TAG = {TAG_OBJECTID: "507f1f77bcf86cd799439011"}
BINARY_TAG = {TAG_BINARY: "AAEC"}
DECIMAL_TAG = {TAG_DECIMAL: "12345.6789"}


def test_mysql_types():
    assert mysql_type(DATETIME_TAG) == "DATETIME"
    assert mysql_type(OBJECTID_TAG) == "VARCHAR(32)"
    assert mysql_type(BINARY_TAG) == "LONGBLOB"
    assert mysql_type(DECIMAL_TAG) == "DECIMAL(38, 18)"
    assert mysql_type(True) == "BOOLEAN"
    assert mysql_type(42) == "BIGINT"
    assert mysql_type(1.5) == "DOUBLE"
    assert mysql_type("x") == "TEXT"
    assert mysql_type({"k": 1}) == "JSON"
    assert mysql_type([1]) == "JSON"
    assert mysql_type(None) == "TEXT"


def test_mysql_encode():
    assert mysql_encode(DATETIME_TAG) == dt.datetime.fromisoformat("2024-06-01T12:30:00+00:00")
    assert mysql_encode(OBJECTID_TAG) == "507f1f77bcf86cd799439011"
    assert mysql_encode(BINARY_TAG) == b"\x00\x01\x02"
    assert mysql_encode(DECIMAL_TAG) == Decimal("12345.6789")
    assert mysql_encode({"k": 1}) == '{"k": 1}'
    assert mysql_encode([1, "a"]) == '[1, "a"]'
    assert mysql_encode("x") == "x"


def test_sqlite_types():
    assert sqlite_type(DATETIME_TAG) == "TEXT"
    assert sqlite_type(OBJECTID_TAG) == "TEXT"
    assert sqlite_type(BINARY_TAG) == "BLOB"
    assert sqlite_type(DECIMAL_TAG) == "TEXT"
    assert sqlite_type(True) == "INTEGER"
    assert sqlite_type(42) == "INTEGER"
    assert sqlite_type(1.5) == "REAL"
    assert sqlite_type("x") == "TEXT"
    assert sqlite_type({"k": 1}) == "TEXT"


def test_sqlite_encode():
    assert sqlite_encode(DATETIME_TAG) == "2024-06-01T12:30:00+00:00"
    assert sqlite_encode(OBJECTID_TAG) == "507f1f77bcf86cd799439011"
    assert sqlite_encode(BINARY_TAG) == b"\x00\x01\x02"
    assert sqlite_encode(DECIMAL_TAG) == "12345.6789"
    assert sqlite_encode({"k": [1, 2]}) == '{"k": [1, 2]}'
    assert sqlite_encode(3.14) == 3.14


def test_parse_mysql_uri():
    params = parse_mysql_uri("mysql://alice:p%40ss@db.example.com:3307/mydb")
    assert params == {
        "host": "db.example.com",
        "port": "3307",
        "user": "alice",
        "password": "p@ss",
        "database": "mydb",
    }


def test_parse_mysql_uri_defaults():
    params = parse_mysql_uri("mysql://root@localhost/mydb")
    assert params["host"] == "localhost"
    assert params["port"] == "3306"
    assert params["user"] == "root"
    assert params["password"] == ""


def test_registry_new_schemes():
    assert get_connector(ConnectorConfig(uri="mysql://user@host/db")).name == "mysql"
    assert get_connector(ConnectorConfig(uri="sqlite:///tmp/x.db")).name == "sqlite"
    assert get_connector(ConnectorConfig(uri="sqlite3:///tmp/x.db")).name == "sqlite"
    assert get_connector(ConnectorConfig(uri="redis://host:6379/0")).name == "redis"
    assert get_connector(ConnectorConfig(uri="rediss://host:6379/0")).name == "redis"


def test_registry_unknown_scheme():
    with pytest.raises(UnsupportedSchemeError):
        get_connector(ConnectorConfig(uri="mssql://host/db"))
