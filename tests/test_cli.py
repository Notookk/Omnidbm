from __future__ import annotations

import json
import os

from typer.testing import CliRunner

from omnidbm.cli import app

runner = CliRunner()


def test_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "omnidbm" in result.output


def test_transfer_csv_to_jsonl(tmp_path):
    src = os.path.join(tmp_path, "in.csv")
    dst = os.path.join(tmp_path, "out.jsonl")
    with open(src, "w", newline="", encoding="utf-8") as f:
        f.write("_id,name\n")
        f.write("1,Alice\n")
        f.write("2,Bob\n")
    result = runner.invoke(
        app,
        [
            "transfer",
            "-s",
            f"csv://{src}",
            "-d",
            f"jsonl://{dst}",
            "-t",
            os.path.basename(src),
            "--conflict",
            "overwrite",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "2" in result.output
    with open(dst, encoding="utf-8") as f:
        docs = [json.loads(line) for line in f if line.strip()]
    assert len(docs) == 2
    assert docs[0]["name"] == "Alice"


def test_transfer_dry_run_writes_nothing(tmp_path):
    src = os.path.join(tmp_path, "in.jsonl")
    dst = os.path.join(tmp_path, "out.jsonl")
    with open(src, "w", encoding="utf-8") as f:
        f.write(json.dumps({"_id": "1"}) + "\n")
    result = runner.invoke(
        app,
        ["transfer", "-s", f"jsonl://{src}", "-d", f"jsonl://{dst}", "--dry-run"],
    )
    assert result.exit_code == 0, result.output
    assert not os.path.exists(dst)


def test_inspect_csv(tmp_path):
    src = os.path.join(tmp_path, "in.csv")
    with open(src, "w", newline="", encoding="utf-8") as f:
        f.write("_id,v\n1,x\n2,y\n")
    result = runner.invoke(app, ["inspect", f"csv://{src}"])
    assert result.exit_code == 0
    assert "in.csv" in result.output
    assert "2" in result.output


def test_invalid_filter_rejected(tmp_path):
    src = os.path.join(tmp_path, "in.jsonl")
    with open(src, "w", encoding="utf-8") as f:
        f.write(json.dumps({"_id": "1"}) + "\n")
    result = runner.invoke(
        app,
        ["transfer", "-s", f"jsonl://{src}", "-d", f"jsonl://{tmp_path}/o.jsonl", "--filter", "not-json"],
    )
    assert result.exit_code != 0


def test_unknown_scheme(tmp_path):
    result = runner.invoke(
        app,
        ["transfer", "-s", "mssql://x", "-d", f"jsonl://{tmp_path}/o.jsonl"],
    )
    assert result.exit_code == 1
    assert "Unsupported URI scheme" in result.output


def test_doctor_csv(tmp_path):
    src = os.path.join(tmp_path, "in.csv")
    with open(src, "w", newline="", encoding="utf-8") as f:
        f.write("_id\n1\n")
    result = runner.invoke(app, ["doctor", f"csv://{src}", f"csv://{tmp_path}/missing.csv"])
    assert result.exit_code == 0
    assert "OK" in result.output
    assert "FAILED" in result.output
