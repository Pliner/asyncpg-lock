import sys
from importlib.metadata import version as _get_version

from .guard import AdvisoryLockGuard, ConnectFunc, connect_func

__all__: tuple[str, ...] = (
    # guard.py
    "ConnectFunc",
    "AdvisoryLockGuard",
    "connect_func",
)

__version__ = _get_version("asyncpg-lock")

version = f"{__version__}, Python {sys.version}"
