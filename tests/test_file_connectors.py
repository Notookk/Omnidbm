from __future__ import annotations

import datetime as dt
import json
import os

import pytest

from omnidbm.core.errors import TransferError
from omnidbm.core.models import ConnectorConfig, TableSpec, TransferConfig
from omnidbm.core.pipeline import connect, inspect, run_transfer


@pytest.fixture
def data_dir(tmp_path):
    return tmp_path


def test_csv_round_trip(data_dir):
    src_path = os.path.join(data_dir, "users.csv")
    with open(src_path, "w", newline="", encoding="utf-8") as f:
        f.write("_id,name,age,active,balance\n")
        f.write("1,Alice,30,true,10.5\n")
        f.write("2,Bob,,false,\n")

    dest_path = os.path.join(data_dir, "out.csv")
    config = TransferConfig(
        source=ConnectorConfig(uri=f"csv://{src_path}"),
        dest=ConnectorConfig(uri=f"csv://{dest_path}"),
        tables=[TableSpec(source=os.path.basename(src_path), dest=os.path.basename(dest_path))],
    )
    results = run_transfer(config)
    assert results[0].copied == 2

    config.source = ConnectorConfig(uri=f"csv://{dest_path}")
    config.dest = ConnectorConfig(uri=f"csv://{data_dir}/final.csv")
    config.tables = [TableSpec(source=os.path.basename(dest_path), dest="final.csv")]
    results = run_transfer(config)
    assert results[0].copied == 2

    with connect(config.dest) as connector:
        docs = [doc for batch in connector.read_stream("final.csv") for doc in batch]
    assert docs[0] == {"_id": 1, "name": "Alice", "age": 30, "active": True, "balance": 10.5}
    assert docs[1] == {"_id": 2, "name": "Bob", "age": None, "active": False, "balance": None}


def test_csv_directory_transfer(data_dir):
    (data_dir / "a.csv").write_text("_id,v\n1,x\n", encoding="utf-8")
    (data_dir / "b.csv").write_text("_id,v\n2,y\n", encoding="utf-8")
    config = ConnectorConfig(uri=f"csv://{data_dir}")
    tables = inspect(config)
    assert sorted(t.name for t in tables) == ["a.csv", "b.csv"]
    assert {t.count for t in tables} == {1}


def test_csv_inconsistent_keys_raise(data_dir):
    src_path = os.path.join(data_dir, "bad.jsonl")
    dest_path = os.path.join(data_dir, "bad.csv")
    with open(src_path, "w", encoding="utf-8") as f:
        f.write(json.dumps({"_id": "1", "a": 1}) + "\n")
        f.write(json.dumps({"_id": "2", "b": 2}) + "\n")
    config = TransferConfig(
        source=ConnectorConfig(uri=f"jsonl://{src_path}"),
        dest=ConnectorConfig(uri=f"csv://{dest_path}"),
        tables=[TableSpec(source="bad.jsonl", dest="bad.csv")],
    )
    with pytest.raises(TransferError):
        run_transfer(config)


def test_jsonl_round_trip_with_types(data_dir):
    src_path = os.path.join(data_dir, "in.jsonl")
    now = dt.datetime.fromisoformat("2024-03-01T12:00:00+00:00")
    docs = [
        {"_id": "1", "name": "Alice", "created": now.isoformat(), "tags": ["a", "b"], "meta": {"k": 1}},
        {"_id": "2", "name": "Bob", "created": None, "tags": [], "meta": {}},
    ]
    with open(src_path, "w", encoding="utf-8") as f:
        f.writelines(json.dumps(doc) + "\n" for doc in docs)

    dest_path = os.path.join(data_dir, "out.jsonl")
    config = TransferConfig(
        source=ConnectorConfig(uri=f"jsonl://{src_path}"),
        dest=ConnectorConfig(uri=f"jsonl://{dest_path}"),
        tables=[TableSpec(source="in.jsonl", dest="out.jsonl")],
    )
    results = run_transfer(config)
    assert results[0].copied == 2
    with open(dest_path, encoding="utf-8") as f:
        assert [json.loads(line) for line in f if line.strip()] == docs


def test_jsonl_skip_duplicates(data_dir):
    src_path = os.path.join(data_dir, "src.jsonl")
    dest_path = os.path.join(data_dir, "dst.jsonl")
    with open(src_path, "w", encoding="utf-8") as f:
        f.write(json.dumps({"_id": "1", "v": 1}) + "\n")
        f.write(json.dumps({"_id": "1", "v": 2}) + "\n")
        f.write(json.dumps({"_id": "3", "v": 3}) + "\n")
    config = TransferConfig(
        source=ConnectorConfig(uri=f"jsonl://{src_path}"),
        dest=ConnectorConfig(uri=f"jsonl://{dest_path}"),
        tables=[TableSpec(source="src.jsonl", dest="dst.jsonl")],
    )
    results = run_transfer(config)
    assert results[0].copied == 2

    config.dest = ConnectorConfig(uri=f"jsonl://{dest_path}")
    results = run_transfer(config)
    assert results[0].copied == 0


def test_jsonl_append_without_drop(data_dir):
    src_path = os.path.join(data_dir, "a.jsonl")
    dest_path = os.path.join(data_dir, "b.jsonl")
    with open(src_path, "w", encoding="utf-8") as f:
        f.write(json.dumps({"_id": "1"}) + "\n")
    with open(dest_path, "w", encoding="utf-8") as f:
        f.write(json.dumps({"_id": "existing"}) + "\n")
    config = TransferConfig(
        source=ConnectorConfig(uri=f"jsonl://{src_path}"),
        dest=ConnectorConfig(uri=f"jsonl://{dest_path}"),
        tables=[TableSpec(source="a.jsonl", dest="b.jsonl")],
    )
    run_transfer(config)
    with open(dest_path, encoding="utf-8") as f:
        lines = [json.loads(line) for line in f if line.strip()]
    assert len(lines) == 2

    config.drop_first = True
    run_transfer(config)
    with open(dest_path, encoding="utf-8") as f:
        lines = [json.loads(line) for line in f if line.strip()]
    assert len(lines) == 1
