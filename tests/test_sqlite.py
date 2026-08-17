from __future__ import annotations

import json
import sqlite3

from omnidbm.core.models import ConnectorConfig, TableSpec, TransferConfig
from omnidbm.core.pipeline import connect, inspect, run_transfer
from omnidbm.core.typemap import TAG_BINARY, TAG_DATETIME, TAG_DECIMAL, TAG_OBJECTID


def make_config(source_uri, dest_uri, source_table, dest_table, **kwargs) -> TransferConfig:
    defaults = {
        "source": ConnectorConfig(uri=source_uri),
        "dest": ConnectorConfig(uri=dest_uri),
        "tables": [TableSpec(source=source_table, dest=dest_table)],
    }
    defaults.update(kwargs)
    return TransferConfig(**defaults)


def test_sqlite_round_trip(tmp_path):
    db_path = tmp_path / "src.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE users (_id TEXT PRIMARY KEY, name TEXT, age INTEGER, score REAL)")
    conn.executemany(
        "INSERT INTO users VALUES (?, ?, ?, ?)",
        [("1", "Alice", 30, 10.5), ("2", "Bob", None, None)],
    )
    conn.commit()
    conn.close()

    dest_path = tmp_path / "out.sqlite"
    config = make_config(f"sqlite://{db_path}", f"sqlite://{dest_path}", "users", "users")
    results = run_transfer(config)
    assert results[0].copied == 2

    with connect(config.dest) as connector:
        docs = [doc for batch in connector.read_stream("users") for doc in batch]
    assert docs[0] == {"_id": "1", "name": "Alice", "age": 30, "score": 10.5}
    assert docs[1] == {"_id": "2", "name": "Bob", "age": None, "score": None}


def test_sqlite_typed_values_round_trip(tmp_path):
    src = tmp_path / "in.jsonl"
    docs = [
        {
            "_id": "1",
            "created": {TAG_DATETIME: "2024-06-01T12:00:00+00:00"},
            "data": {TAG_BINARY: "AAEC"},
            "price": {TAG_DECIMAL: "99.95"},
            "oid": {TAG_OBJECTID: "507f1f77bcf86cd799439011"},
            "nested": {"k": [1, 2]},
        }
    ]
    with open(src, "w", encoding="utf-8") as f:
        f.writelines(json.dumps(doc) + "\n" for doc in docs)

    dest = tmp_path / "out.sqlite"
    config = make_config(f"jsonl://{src}", f"sqlite://{dest}", "in.jsonl", "items")
    results = run_transfer(config)
    assert results[0].copied == 1

    conn = sqlite3.connect(dest)
    row = conn.execute("SELECT * FROM items").fetchone()
    assert row[1] == "2024-06-01T12:00:00+00:00"
    assert row[2] == b"\x00\x01\x02"
    assert row[3] == "99.95"
    assert row[4] == "507f1f77bcf86cd799439011"
    assert row[5] == '{"k": [1, 2]}'
    conn.close()

    with connect(config.dest) as connector:
        batch = next(connector.read_stream("items"))
    assert batch[0]["created"] == "2024-06-01T12:00:00+00:00"
    assert batch[0]["data"] == {TAG_BINARY: "AAEC"}
    assert batch[0]["price"] == "99.95"
    assert batch[0]["oid"] == "507f1f77bcf86cd799439011"
    assert batch[0]["nested"] == '{"k": [1, 2]}'


def test_sqlite_drop_first(tmp_path):
    src = tmp_path / "a.sqlite"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (_id TEXT PRIMARY KEY)")
    conn.execute("INSERT INTO t VALUES ('1')")
    conn.commit()
    conn.close()

    dest = tmp_path / "b.sqlite"
    conn = sqlite3.connect(dest)
    conn.execute("CREATE TABLE t (_id TEXT PRIMARY KEY)")
    conn.execute("INSERT INTO t VALUES ('old')")
    conn.commit()
    conn.close()

    config = make_config(f"sqlite://{src}", f"sqlite://{dest}", "t", "t", drop_first=True)
    run_transfer(config)
    with connect(config.dest) as connector:
        docs = [doc for batch in connector.read_stream("t") for doc in batch]
    assert docs == [{"_id": "1"}]


def test_sqlite_skip_duplicates(tmp_path):
    src = tmp_path / "in.jsonl"
    with open(src, "w", encoding="utf-8") as f:
        f.write(json.dumps({"_id": "1", "v": 1}) + "\n")
        f.write(json.dumps({"_id": "1", "v": 2}) + "\n")

    dest = tmp_path / "out.sqlite"
    config = make_config(f"jsonl://{src}", f"sqlite://{dest}", "in.jsonl", "t")
    results = run_transfer(config)
    assert results[0].copied == 1

    config = make_config(f"jsonl://{src}", f"sqlite://{dest}", "in.jsonl", "t")
    results = run_transfer(config)
    assert results[0].copied == 0


def test_sqlite_overwrite(tmp_path):
    src = tmp_path / "in.jsonl"
    with open(src, "w", encoding="utf-8") as f:
        f.write(json.dumps({"_id": "1", "v": 2}) + "\n")

    from omnidbm.core.models import ConflictStrategy

    dest = tmp_path / "out.sqlite"
    config = make_config(f"jsonl://{src}", f"sqlite://{dest}", "in.jsonl", "t")
    run_transfer(config)
    config = make_config(f"jsonl://{src}", f"sqlite://{dest}", "in.jsonl", "t", conflict=ConflictStrategy.OVERWRITE)
    run_transfer(config)
    with connect(config.dest) as connector:
        docs = [doc for batch in connector.read_stream("t") for doc in batch]
    assert len(docs) == 1


def test_sqlite_filter_and_limit(tmp_path):
    src = tmp_path / "in.jsonl"
    with open(src, "w", encoding="utf-8") as f:
        f.writelines(json.dumps({"_id": str(i), "active": i % 2 == 0}) + "\n" for i in range(10))

    dest = tmp_path / "out.sqlite"
    config = make_config(f"jsonl://{src}", f"sqlite://{dest}", "in.jsonl", "t", filter={"active": True}, limit=3)
    results = run_transfer(config)
    assert results[0].copied == 3

    src_db = tmp_path / "src2.sqlite"
    conn = sqlite3.connect(src_db)
    conn.execute("CREATE TABLE t (_id TEXT, active INTEGER)")
    for i in range(10):
        conn.execute("INSERT INTO t VALUES (?, ?)", (str(i), int(i % 2 == 0)))
    conn.commit()
    conn.close()

    dest2 = tmp_path / "out2.sqlite"
    config = make_config(f"sqlite://{src_db}", f"sqlite://{dest2}", "t", "t", filter={"active": 1})
    results = run_transfer(config)
    assert results[0].copied == 5


def test_sqlite_inspect(tmp_path):
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE a (_id TEXT)")
    conn.execute("CREATE TABLE b (_id TEXT)")
    conn.executemany("INSERT INTO a VALUES (?)", [("1",), ("2",)])
    conn.commit()
    conn.close()

    tables = inspect(ConnectorConfig(uri=f"sqlite://{db_path}"))
    assert sorted(t.name for t in tables) == ["a", "b"]
    assert {t.count for t in tables} == {2, 0}


def test_sqlite_memory(tmp_path):
    src = tmp_path / "in.jsonl"
    with open(src, "w", encoding="utf-8") as f:
        f.write(json.dumps({"_id": "1"}) + "\n")
    config = make_config(f"jsonl://{src}", "sqlite://:memory:", "in.jsonl", "t")
    run_transfer(config)


def test_sqlite_append_new_columns(tmp_path):
    src = tmp_path / "src.sqlite"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (_id TEXT)")
    conn.execute("INSERT INTO t VALUES ('1')")
    conn.commit()
    conn.close()

    dest = tmp_path / "out.sqlite"
    config = make_config(f"sqlite://{src}", f"sqlite://{dest}", "t", "t")
    run_transfer(config)

    conn = sqlite3.connect(src)
    conn.execute("ALTER TABLE t ADD COLUMN extra TEXT")
    conn.execute("INSERT INTO t VALUES ('2', 'x')")
    conn.commit()
    conn.close()

    config = make_config(f"sqlite://{src}", f"sqlite://{dest}", "t", "t")
    results = run_transfer(config)
    assert results[0].copied == 1
    with connect(config.dest) as connector:
        docs = [doc for batch in connector.read_stream("t") for doc in batch]
    assert docs == [{"_id": "1", "extra": None}, {"_id": "2", "extra": "x"}]


def test_sqlite_to_jsonl(tmp_path):
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t (_id TEXT PRIMARY KEY, name TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [("1", "a"), ("2", "b")])
    conn.commit()
    conn.close()

    out = tmp_path / "out.jsonl"
    config = make_config(f"sqlite://{db_path}", f"jsonl://{out}", "t", "out.jsonl")
    results = run_transfer(config)
    assert results[0].copied == 2
    with open(out, encoding="utf-8") as f:
        docs = [json.loads(line) for line in f if line.strip()]
    assert docs == [{"_id": "1", "name": "a"}, {"_id": "2", "name": "b"}]
