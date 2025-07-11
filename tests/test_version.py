import pytest

from asyncpg_lock import _parse_version


def test_alpha() -> None:
    assert (0, 1, 2, "alpha", 2) == _parse_version("0.1.2a2")
    assert (1, 2, 3, "alpha", 0) == _parse_version("1.2.3a")


def test_beta() -> None:
    assert (0, 1, 2, "beta", 2) == _parse_version("0.1.2b2")
    assert (0, 1, 2, "beta", 0) == _parse_version("0.1.2b")


def test_rc() -> None:
    assert (0, 1, 2, "candidate", 5) == _parse_version("0.1.2rc5")
    assert (0, 1, 2, "candidate", 0) == _parse_version("0.1.2rc")


def test_final() -> None:
    assert (0, 1, 2, "final", 0) == _parse_version("0.1.2")


def test_invalid() -> None:
    pytest.raises(ImportError, _parse_version, "0.1")
    pytest.raises(ImportError, _parse_version, "0.1.1.2")
    pytest.raises(ImportError, _parse_version, "0.1.1z2")
