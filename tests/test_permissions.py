"""POSIX store permissions are repaired without changing unrelated data."""

import os
import stat
from pathlib import Path

import pytest

from embedded_nats import EmbeddedNatsError, NatsServer
from embedded_nats.server import _private_directory

pytestmark = pytest.mark.skipif(os.name == "nt", reason="POSIX directory permissions")


@pytest.mark.parametrize("mode", [0o755, 0o777, 0o750, 0o500])
def test_owned_directory_permissions_are_repaired(tmp_path: Path, mode: int) -> None:
    store = tmp_path / "store"
    store.mkdir()
    payload = store / "payload"
    payload.write_bytes(b"preserve me")
    payload.chmod(0o640)
    store.chmod(mode)

    assert _private_directory(store) == store.resolve()
    assert stat.S_IMODE(store.stat().st_mode) == 0o700
    assert payload.read_bytes() == b"preserve me"
    assert stat.S_IMODE(payload.stat().st_mode) == 0o640


def test_directory_symlink_is_not_chmodded(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir(mode=0o755)
    target.chmod(0o755)
    link = tmp_path / "logs"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(EmbeddedNatsError):
        _private_directory(link)
    assert stat.S_IMODE(target.stat().st_mode) == 0o755


def test_foreign_owner_is_not_chmodded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = tmp_path / "store"
    store.mkdir(mode=0o755)
    store.chmod(0o755)
    monkeypatch.setattr(os, "geteuid", lambda: store.stat().st_uid + 1)
    with pytest.raises(EmbeddedNatsError, match="owned"):
        _private_directory(store)
    assert stat.S_IMODE(store.stat().st_mode) == 0o755


@pytest.mark.parametrize("failure", ["denied", "ignored"])
def test_unsuccessful_chmod_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str) -> None:
    store = tmp_path / "store"
    store.mkdir(mode=0o755)
    store.chmod(0o755)

    def chmod(_fd: int, _mode: int) -> None:
        if failure == "denied":
            raise PermissionError("chmod denied")

    monkeypatch.setattr(os, "fchmod", chmod)
    with pytest.raises(EmbeddedNatsError, match="0700"):
        _private_directory(store)


@pytest.mark.integration
def test_broker_starts_and_restarts_with_existing_public_directories(tmp_path: Path) -> None:
    store = tmp_path / "broker"
    paths = [store, store / ".runtime", store / "logs"]
    for path in paths:
        path.mkdir(mode=0o755)
        path.chmod(0o755)
    for _ in range(2):
        with NatsServer(store) as server:
            assert server.pid > 0
            for path in paths:
                assert stat.S_IMODE(path.stat().st_mode) == 0o700
