class OmniDBMError(Exception):
    pass


class ConnectionError(OmniDBMError):
    pass


class UnsupportedSchemeError(OmniDBMError):
    pass


class TableNotFoundError(OmniDBMError):
    pass


class TransferError(OmniDBMError):
    pass
