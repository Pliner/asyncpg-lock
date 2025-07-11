import collections
import re
import sys

from .guard import AdvisoryLockGuard, ConnectFunc, connect_func  # noqa

__all__: tuple[str, ...] = (
    # guard.py
    "ConnectFunc",
    "AdvisoryLockGuard",
    "connect_func",
)

__version__ = "0.0.1"

version = f"{__version__}, Python {sys.version}"

VersionInfo = collections.namedtuple("VersionInfo", "major minor micro release_level serial")


def _parse_version(v: str) -> VersionInfo:
    version_re = r"^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<micro>\d+)" r"((?P<release_level>[a-z]+)(?P<serial>\d+)?)?$"
    match = re.match(version_re, v)
    if not match:
        raise ImportError(f"Invalid package version {v}")
    try:
        major = int(match.group("major"))
        minor = int(match.group("minor"))
        micro = int(match.group("micro"))
        levels = {"rc": "candidate", "a": "alpha", "b": "beta", None: "final"}
        release_level = levels[match.group("release_level")]
        serial = int(match.group("serial")) if match.group("serial") else 0
        return VersionInfo(major, minor, micro, release_level, serial)
    except Exception as e:
        raise ImportError(f"Invalid package version {v}") from e


version_info = _parse_version(__version__)
