"""Fault tests for opt-in recovery; no process or listener heuristics."""

import asyncio
import json
import os
import signal
import shutil
import socket
import time

import nats
import pytest

from embedded_nats import NatsServer, RecoveryRequired, StoreInUse
from embedded_nats._lease import LifetimeLease


def crash(server):
    server._process.kill()
    server._process.wait(timeout=5)
    with pytest.raises(RecoveryRequired):
        server.stop()


def test_recovery_preserves_persistent_data_and_default_stays_closed(tmp_path):
    first = NatsServer(tmp_path / "store").start()

    async def data(server, write):
        nc = await nats.connect(server.url, token=server.auth_token)
        try:
            js = nc.jetstream()
            if write:
                kv = await js.create_key_value(bucket="evidence")
                await kv.put("key", b"durable value")
                objects = await js.create_object_store(bucket="objects")
                await objects.put("object", b"durable object")
            else:
                kv = await js.key_value("evidence")
                assert (await kv.get("key")).value == b"durable value"
                objects = await js.object_store("objects")
                assert (await objects.get("object")).data == b"durable object"
        finally:
            await nc.close()

    try:
        asyncio.run(data(first, True))
    finally:
        crash(first)
    marker = first.recovery_marker_path.read_bytes()
    generation = json.loads(marker)["generation"]
    with pytest.raises(RecoveryRequired) as failure:
        NatsServer(first.store_dir).start()
    assert failure.value.reason == "opt_in_required"
    assert failure.value.marker_path == first.recovery_marker_path
    assert first.recovery_marker_path.read_bytes() == marker
    with NatsServer(first.store_dir, recover_stale=True) as recovered:
        assert recovered.recovered_generation == generation
        assert not (first.store_dir / ".runtime" / generation).exists()
        asyncio.run(data(recovered, False))
        with pytest.raises(StoreInUse):
            NatsServer(first.store_dir, recover_stale=True).start()
    with NatsServer(first.store_dir, recover_stale=True) as clean:
        assert clean.recovered_generation is None


@pytest.mark.parametrize("change", ["legacy", "corrupt", "large", "generation", "identity", "missing_lease"])
def test_unknown_evidence_is_left_untouched(tmp_path, change):
    server = NatsServer(tmp_path / "store").start()
    crash(server)
    path = server.recovery_marker_path
    record = json.loads(path.read_text())
    if change == "legacy":
        del record["schema"]
    elif change == "generation":
        record["generation"] = "../../escape"
    elif change == "identity":
        record["lease_identity"][1] += 1
    elif change == "missing_lease":
        (server.store_dir / ".broker.lease").unlink()  # Test-owned, reaped child only.
    path.write_text("{" if change == "corrupt" else (" " * 8193 if change == "large" else json.dumps(record)))
    before = path.read_bytes()
    with pytest.raises(RecoveryRequired):
        NatsServer(server.store_dir, recover_stale=True).start()
    assert path.read_bytes() == before
    assert (server.store_dir / "jetstream").is_dir()


@pytest.mark.parametrize("pid", [None, 1])
def test_alive_child_wins_even_if_pid_missing_or_reused(tmp_path, pid):
    server = NatsServer(tmp_path / "store").start()
    # Model parent death without losing the test's exact child handle for cleanup.
    server._store_lock.release()
    record = json.loads(server.recovery_marker_path.read_text())
    record["child_pid"] = pid
    server.recovery_marker_path.write_text(json.dumps(record))
    before = server.recovery_marker_path.read_bytes()
    try:
        with pytest.raises(RecoveryRequired) as failure:
            NatsServer(server.store_dir, recover_stale=True).start()
        assert failure.value.reason == "lease_held"
        assert server.recovery_marker_path.read_bytes() == before
        assert server._process.poll() is None
    finally:
        server.stop()


def test_permissions_are_unknown_not_dead(tmp_path, monkeypatch):
    server = NatsServer(tmp_path / "store").start()
    crash(server)
    before = server.recovery_marker_path.read_bytes()

    def refused(*args, **kwargs):
        raise PermissionError("access denied")

    monkeypatch.setattr(LifetimeLease, "acquire", refused)
    with pytest.raises(RecoveryRequired) as failure:
        NatsServer(server.store_dir, recover_stale=True).start()
    assert failure.value.reason == "lease_unavailable"
    assert server.recovery_marker_path.read_bytes() == before


def test_crash_before_spawn_and_before_child_pid_record(tmp_path, monkeypatch):
    from embedded_nats import server as module

    # A pre-spawn marker with no child is recoverable only after its fence is free.
    first = NatsServer(tmp_path / "store")
    original = first._write_marker

    def before_spawn(child_pid=None):
        original(child_pid)
        raise KeyboardInterrupt()  # bypass ordinary startup-failure cleanup

    monkeypatch.setattr(first, "_write_marker", before_spawn)
    with pytest.raises(KeyboardInterrupt):
        first.start()
    first._lease.close()
    first._store_lock.release()
    with NatsServer(first.store_dir, recover_stale=True):
        pass

    second = NatsServer(first.store_dir)
    original = second._write_marker

    def after_spawn(child_pid=None):
        if child_pid:
            raise KeyboardInterrupt()
        original()

    monkeypatch.setattr(second, "_write_marker", after_spawn)
    with pytest.raises(KeyboardInterrupt):
        second.start()
    second._store_lock.release()
    try:
        assert json.loads(second.recovery_marker_path.read_text())["child_pid"] is None
        with pytest.raises(RecoveryRequired, match="lease_held"):
            module.NatsServer(second.store_dir, recover_stale=True).start()
    finally:
        second.stop()


@pytest.mark.skipif(os.name == "nt", reason="NATS lame duck signal is POSIX")
def test_lame_duck_closed_listener_does_not_allow_recovery(tmp_path):
    server = NatsServer(tmp_path / "store").start()
    server._store_lock.release()
    # With no clients NATS exits immediately. Keep an authenticated client so
    # lame duck has a real drain window after closing the accept socket.
    client = socket.create_connection((server.host, server.port), timeout=5)
    try:
        assert client.recv(16384).startswith(b"INFO ")
        client.sendall(b"CONNECT " + json.dumps({"auth_token": server.auth_token}).encode() + b"\r\nPING\r\n")
        response = b""
        while b"PONG\r\n" not in response:
            response += client.recv(16384)
        server._process.send_signal(signal.SIGUSR2)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with socket.socket() as probe:
                if probe.connect_ex((server.host, server.port)) != 0:
                    break
            time.sleep(0.05)
        else:
            pytest.fail("lame duck listener stayed open")
        assert server._process.poll() is None
        with pytest.raises(RecoveryRequired, match="lease_held"):
            NatsServer(server.store_dir, recover_stale=True).start()
    finally:
        try:
            server.stop()
        finally:
            client.close()


def test_unrelated_listener_is_not_consulted(tmp_path):
    server = NatsServer(tmp_path / "store").start()
    port = server.port
    crash(server)
    with socket.socket() as unrelated:
        unrelated.bind(("127.0.0.1", port))
        unrelated.listen()
        with NatsServer(server.store_dir, recover_stale=True) as again:
            assert again.port != port


def test_replaced_lease_is_rejected(tmp_path):
    server = NatsServer(tmp_path / "store").start()
    crash(server)
    lease = server.store_dir / ".broker.lease"
    lease.rename(lease.with_suffix(".old"))
    lease.touch()
    with pytest.raises(RecoveryRequired, match="lease_identity_changed"):
        NatsServer(server.store_dir, recover_stale=True).start()


def test_cleanup_refuses_unexpected_directory_without_partial_deletion(tmp_path):
    server = NatsServer(tmp_path / "store").start()
    crash(server)
    marker = server.recovery_marker_path.read_bytes()
    runtime = server.store_dir / ".runtime" / json.loads(marker)["generation"]
    (runtime / "unexpected").mkdir()
    with pytest.raises(RecoveryRequired, match="unsafe_runtime_entry"):
        NatsServer(server.store_dir, recover_stale=True).start()
    assert (runtime / "nats.conf").exists()
    assert server.recovery_marker_path.read_bytes() == marker


def test_copied_store_requires_manual_recovery(tmp_path):
    server = NatsServer(tmp_path / "original").start()
    crash(server)
    copy = tmp_path / "copy"
    shutil.copytree(server.store_dir, copy)
    with pytest.raises(RecoveryRequired, match="lease_identity_changed"):
        NatsServer(copy, recover_stale=True).start()
    with NatsServer(server.store_dir, recover_stale=True):
        pass


def test_runtime_link_cannot_delete_external_files(tmp_path):
    server = NatsServer(tmp_path / "store").start()
    crash(server)
    generation = json.loads(server.recovery_marker_path.read_text())["generation"]
    runtime = server.store_dir / ".runtime" / generation
    saved = tmp_path / "saved"
    runtime.rename(saved)
    try:
        runtime.symlink_to(saved, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation unavailable")
    with pytest.raises(RecoveryRequired, match="unsafe_runtime_path"):
        NatsServer(server.store_dir, recover_stale=True).start()
    assert (saved / "nats.conf").exists()
    assert server.recovery_marker_path.exists()


def test_recovery_cleanup_interruption_is_retryable(tmp_path, monkeypatch):
    from pathlib import Path

    server = NatsServer(tmp_path / "store").start()
    crash(server)
    original = Path.unlink

    def fail_marker(path, *args, **kwargs):
        if path == server.recovery_marker_path:
            raise PermissionError("injected marker deletion failure")
        return original(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "unlink", fail_marker)
        with pytest.raises(RecoveryRequired, match="cleanup_failed"):
            NatsServer(server.store_dir, recover_stale=True).start()
    assert server.recovery_marker_path.exists()
    with NatsServer(server.store_dir, recover_stale=True) as recovered:
        assert recovered.recovered_generation
