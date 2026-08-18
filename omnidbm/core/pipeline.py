from __future__ import annotations

import time
from collections.abc import Callable

from omnidbm import connectors  # noqa: F401
from omnidbm.core.connector import BaseConnector, get_connector
from omnidbm.core.models import ConnectorConfig, TableInfo, TransferConfig, TransferResult


def connect(config: ConnectorConfig) -> BaseConnector:
    connector = get_connector(config)
    connector.connect()
    return connector


def run_transfer(
    cfg: TransferConfig,
    source: BaseConnector | None = None,
    dest: BaseConnector | None = None,
    on_progress: Callable[[str, int], None] | None = None,
) -> list[TransferResult]:
    src = source or connect(cfg.source)
    dst = dest or connect(cfg.dest)
    results: list[TransferResult] = []
    try:
        for spec in cfg.tables:
            start = time.monotonic()
            batches = src.read_stream(spec.source, cfg.batch_size, cfg.limit, cfg.filter)
            copied = 0
            if cfg.dry_run:
                for batch in batches:
                    copied += len(batch)
                    if on_progress:
                        on_progress(spec.source, copied)
            else:
                table_name = spec.source

                def progress(count: int, table: str = table_name) -> None:
                    if on_progress:
                        on_progress(table, count)

                copied = dst.write_stream(spec.dest, batches, cfg.drop_first, cfg.conflict, progress)
                if cfg.copy_indexes or cfg.copy_options:
                    dst.copy_metadata(spec.dest, src, spec.source, cfg.copy_indexes, cfg.copy_options)
            results.append(TransferResult(spec.source, copied, time.monotonic() - start))
    finally:
        if source is None:
            src.close()
        if dest is None:
            dst.close()
    return results


def inspect(config: ConnectorConfig) -> list[TableInfo]:
    with connect(config) as connector:
        return connector.list_tables()


def doctor(configs: list[ConnectorConfig]) -> dict[str, str]:
    status: dict[str, str] = {}
    for config in configs:
        try:
            with connect(config) as connector:
                connector.check()
            status[config.uri] = "OK"
        except Exception as exc:  # noqa: BLE001
            status[config.uri] = str(exc)
    return status
