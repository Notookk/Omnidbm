<p align="center">
  <img src="https://raw.githubusercontent.com/Notookk/Omnidbm/master/assets/omnidbm-banner.svg" alt="omnidbm — universal database-to-database transfer engine" width="100%">
</p>

<p align="center">
  <a href="https://pypi.org/project/omnidbm/"><img src="https://img.shields.io/pypi/v/omnidbm?style=flat&color=8b5cf6" alt="PyPI version"></a>
  <a href="https://pypi.org/project/omnidbm/"><img src="https://img.shields.io/pypi/pyversions/omnidbm?style=flat&color=22d3ee" alt="Python versions"></a>
  <a href="https://pypi.org/project/omnidbm/"><img src="https://img.shields.io/pypi/l/omnidbm?style=flat&color=10b981" alt="License"></a>
  <a href="https://pypi.org/project/omnidbm/"><img src="https://img.shields.io/pypi/dm/omnidbm?style=flat&color=6366f1" alt="Downloads"></a>
  <a href="https://pypi.org/project/omnidbm/"><img src="https://img.shields.io/pypi/wheel/omnidbm?style=flat&color=38bdf8" alt="Wheel"></a>
</p>

---

**omnidbm** moves data between any supported database and any other. MongoDB, PostgreSQL, MySQL, SQLite, Redis, CSV, JSONL — pick a source, pick a destination, transfer. One package, one CLI, one lossless data bus.

## Features

- **Any-to-any transfers** — every connector speaks both directions; no source/destination pairing limits
- **Lossless type bus** — datetimes, ObjectIds, Binary, and Decimals survive round trips via JSON-safe type tags
- **Zero hardcoding** — everything is configured per-run with CLI flags; nothing is baked in
- **Automatic schema** — PostgreSQL, MySQL, and SQLite destinations are created and typed for you
- **Interactive wizard** — guided setup for every supported database
- **Preflight checks** — `doctor` verifies connectivity before you start
- **Battle-tested basics** — batch size, per-table limits, filters, drop-first, conflict policies, dry runs, index copy

## Install

```bash
pip install omnidbm

pip install "omnidbm[mysql]"   # MySQL support
pip install "omnidbm[redis]"   # Redis support
pip install "omnidbm[all]"     # everything
```

Requires Python 3.10+. MongoDB (pymongo) and PostgreSQL (psycopg) drivers are included; MySQL and Redis drivers are optional extras.

## Quick start

```bash
# Transfer every table, schema included
omnidbm transfer -s mongodb://user:pass@host:27017/db -d postgresql://user:pass@host:5432/db

# Selective: specific tables, bigger batches, wipe the destination first
omnidbm transfer -s mongodb://... -d postgresql://... -t users,orders --batch-size 2000 --drop-first

# Dry run: count only, no writes
omnidbm transfer -s csv://./data -d mongodb://... --dry-run

# Guided interactive setup
omnidbm wizard

# Inspect a source before moving anything
omnidbm inspect mongodb://user:pass@host:27017/db --sample 3

# Connectivity preflight
omnidbm doctor mongodb://... postgresql://...
```

### Options

| Flag | Description |
| --- | --- |
| `-s, --source` | Source URI |
| `-d, --dest` | Destination URI |
| `-t, --tables` | Comma-separated tables (default: all) |
| `--batch-size` | Documents per batch (default 1000) |
| `--drop-first` | Drop destination table before transferring |
| `--dry-run` | Count documents only, no writes |
| `--limit` | Max documents per table |
| `--filter` | Source-side JSON filter, e.g. `{"status":"active"}` |
| `--conflict` | `skip` (default), `overwrite`, or `error` |
| `--copy-indexes` | Copy indexes (MongoDB only) |
| `--copy-options` | Copy collection options (MongoDB only) |

### URI schemes

| Scheme | Example |
| --- | --- |
| MongoDB | `mongodb://user:pass@host:27017/db` · `mongodb+srv://...` |
| PostgreSQL | `postgresql://user:pass@host:5432/db` |
| MySQL | `mysql://user:pass@host:3306/db` |
| SQLite | `sqlite://C:/path/db.sqlite` · `sqlite://:memory:` |
| Redis | `redis://user:pass@host:6379/0` |
| CSV | `csv://C:/path/file.csv` · `csv://C:/data/` (directory = multiple tables) |
| JSONL | `jsonl://C:/path/out.jsonl` · `jsonl://C:/data/` |

Redis treats key prefixes (split on `:`) as tables; `(all)` targets every key. Documents use the shape `{"_key", "_value", "_ttl"}` and support strings, hashes, lists, and sets.

## Python API

```python
from omnidbm import transfer, TransferConfig, ConnectorConfig, TableSpec

config = TransferConfig(
    source=ConnectorConfig(uri="mongodb://user:pass@host:27017/db"),
    dest=ConnectorConfig(uri="postgresql://user:pass@host:5432/db"),
    tables=[TableSpec(source="users", dest="users")],
    drop_first=True,
)
results = transfer(config)
```

### Custom connectors

Subclass `BaseConnector`, implement `connect`, `list_tables`, `read_stream`, `write_stream`, and `close`, then register:

```python
from omnidbm.core.connector import BaseConnector, register

@register("myproto")
class MyConnector(BaseConnector):
    ...
```

## How the type bus works

Every document is normalized to JSON-safe values on the wire, tagged so nothing is lost in transit:

| Type | Tag |
| --- | --- |
| datetime | `{"$omni:datetime": "2026-08-17T12:00:00+00:00"}` |
| ObjectId | `{"$omni:objectid": "507f1f77bcf86cd799439011"}` |
| Binary | `{"$omni:binary": "<base64>"}` |
| Decimal | `{"$omni:decimal": "99.95"}` |

## Development

```bash
git clone <repo-url>
pip install -e ".[dev]"

pytest          # run the test suite
ruff check .    # lint
ruff format .   # format
```

## Roadmap

- Resumable transfers with checkpoints
- Saved config profiles
- Parallel table transfers
- Incremental / delta sync

## License

MIT — see [LICENSE](LICENSE).