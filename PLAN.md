Here's the plan for **omnidbm**:

## 1. Package structure

```
omnidbm/
├── pyproject.toml              # hatchling, deps, CLI entry point
├── omnidbm/
│   ├── __init__.py             # version + public API (transfer(), inspect())
│   ├── cli.py                  # Typer app: transfer / wizard / inspect / doctor
│   ├── core/
│   │   ├── models.py           # TransferConfig, ConnectorConfig dataclasses
│   │   ├── connector.py        # BaseConnector abstract class (the contract)
│   │   ├── pipeline.py         # streaming batch engine (memory-safe)
│   │   ├── typemap.py          # BSON/JSON/SQL type converter registry
│   │   └── errors.py
│   ├── connectors/
│   │   ├── mongo.py            # pymongo
│   │   ├── postgres.py         # psycopg3
│   │   ├── csv.py
│   │   └── jsonl.py
│   └── utils/
│       ├── progress.py         # Rich progress bars / status
│       └── prompts.py          # interactive wizard prompts
└── tests/
```

## 2. Core architecture — "record bus"

- Everything is normalized to **`dict` records + schema metadata** in the middle.
  - Mongo docs are already dicts; Postgres rows → dicts via cursor; CSV/JSONL → dicts.
  - This is what makes *any→any* possible: `Mongo→CSV`, `CSV→Postgres`, `Postgres→Mongo` all work with zero special-casing.
- `BaseConnector` contract: `connect()`, `get_schema()`, `read_stream(batch_size)`, `write_stream(batches, ...)`, `close()`. A connector registry (`get_connector("mongo")`) lets developers plug in custom connectors — that's the "library for developers" part.
- **typemap.py** handles the hard part: ObjectId↔str↔uuid, datetime↔iso string↔timestamptz, Decimal128↔numeric, Binary↔bytea.

## 3. CLI (Typer + Rich)

```
omnidbm transfer --source mongodb://... --dest postgresql://... \
                 --tables users,orders --batch-size 1000 --drop-first --dry-run

omnidbm wizard              # fully interactive: pick source → URI → tables → dest → confirm → run
omnidbm inspect mongodb://...   # list tables, row counts, sample data
omnidbm doctor              # connectivity preflight checks
```

- URI scheme determines connector: `mongodb://`, `postgresql://`, `csv://path`, `jsonl://path`.
- No config file, no hardcoding — everything via flags or prompts (only constants are defaults like batch size).
- Runs show live progress bars, per-table stats, and a final summary table (Rich).

## 4. Feature set (v1)

- Full transfer + schema/index/settings copy (Mongo→Mongo, like your script)
- `--limit`, `--filter` (source-side query), `--batch-size`, `--dry-run`, `--drop-first`
- `--on-conflict` (skip / overwrite / error) for dest keys
- Python API for developers: `from omnidbm import transfer, inspect`

## 5. Publishing

- `pyproject.toml` (hatchling), Python ≥3.10, deps: `typer`, `rich`, `pymongo`, `psycopg[binary]`
- GitHub repo + Actions (ruff lint, pytest, `twine`/trusted-publisher publish on tags)

## 6. Roadmap

- **v0.1**: the above (Mongo, Postgres, CSV/JSONL, any↔any)
- **v0.2**: more DBs (MySQL, SQLite, Redis), resume/checkpoint, config profiles file
- **v0.3**: parallel workers, delta/sync mode

Questions before we start: save this as `PLAN.md` in the repo, and should v0.1 also include Mongo↔Postgres direct transfer (the hardest type-mapping case), or keep that for v0.2 after Mongo↔Mongo + file transfers are solid?