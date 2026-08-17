from omnidbm.core.errors import ConnectionError, OmniDBMError, TableNotFoundError, TransferError, UnsupportedSchemeError
from omnidbm.core.models import (
    ConflictStrategy,
    ConnectorConfig,
    TableInfo,
    TableSpec,
    TransferConfig,
    TransferResult,
)
from omnidbm.core.pipeline import connect, doctor, inspect, run_transfer

__version__ = "0.1.0"

__all__ = [
    "OmniDBMError",
    "ConnectionError",
    "UnsupportedSchemeError",
    "TableNotFoundError",
    "TransferError",
    "ConflictStrategy",
    "ConnectorConfig",
    "TableInfo",
    "TableSpec",
    "TransferConfig",
    "TransferResult",
    "connect",
    "doctor",
    "inspect",
    "run_transfer",
    "__version__",
]
