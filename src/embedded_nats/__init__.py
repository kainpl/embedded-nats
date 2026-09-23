"""Managed local NATS server, bundled in a platform wheel."""

from .server import (
    EmbeddedNatsError,
    NatsServer,
    RecoveryRequired,
    StoreInUse,
    binary_path,
    get_server,
    server_version,
)

__all__ = [
    "EmbeddedNatsError",
    "NatsServer",
    "RecoveryRequired",
    "StoreInUse",
    "binary_path",
    "get_server",
    "server_version",
]
