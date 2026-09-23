"""Acceptance tests against the actual bundled server, not a mock broker."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import socket
import subprocess
import sys
import time
import tracemalloc
from pathlib import Path

import nats
import pytest
from nats.js.api import StorageType

from embedded_nats import EmbeddedNatsError, NatsServer, RecoveryRequired, StoreInUse, server_version

pytestmark = pytest.mark.integration


def test_lifecycle_auth_and_lock(tmp_path: Path) -> None:
    store = tmp_path / "Привіт spaces"
    first = NatsServer(store)
    with first as server:
        assert server is first
        assert server.start() is first
        assert server_version() == "2.15.0"
        assert server.pid > 0
        assert server.port > 0
        assert server.auth_token not in server.url
        assert server.url.startswith("nats://127.0.0.1:")
        with pytest.raises(StoreInUse):
            NatsServer(store).start()

        async def check() -> None:
            client = await nats.connect(server.url, token=server.auth_token, allow_reconnect=False)
            await client.flush()
            await client.jetstream().account_info()
            await client.close()
            with pytest.raises(nats.errors.Error):
                await nats.connect(server.url, token="wrong-token-for-test", allow_reconnect=False, connect_timeout=1)

        asyncio.run(check())
    assert first.stop() == "not-started"
    assert not (store / "managed-runtime.json").exists()
    with NatsServer(store) as again:
        assert again.pid > 0


def test_fixed_port_conflict_rolls_back(tmp_path: Path) -> None:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        server = NatsServer(tmp_path / "store", port=port, startup_timeout=2)
        with pytest.raises(EmbeddedNatsError):
            server.start()
        assert not (server.store_dir / "managed-runtime.json").exists()
        assert NatsServer(server.store_dir).start().stop() in ("graceful", "forced")


def test_invalid_version_rolls_back_without_spawn(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("embedded_nats.server.server_version", lambda: "0.0.0")
    store = tmp_path / "store"
    with pytest.raises(EmbeddedNatsError, match="version"):
        NatsServer(store).start()
    assert not (store / "managed-runtime.json").exists()
    assert not (store / "jetstream").exists()


def test_invalid_security_options_fail_before_start(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="loopback"):
        NatsServer(tmp_path / "store", host="0.0.0.0")
    with pytest.raises(ValueError, match="auth_token"):
        NatsServer(tmp_path / "store", auth_token="")
    assert not (tmp_path / "store").exists()


def test_readiness_failure_reaps_child_and_keeps_store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_probe(_url: str, _token: str, _jetstream: bool, _timeout: float) -> None:
        raise TimeoutError("injected readiness timeout")

    monkeypatch.setattr("embedded_nats.server._probe_in_thread", fail_probe)
    store = tmp_path / "store"
    server = NatsServer(store, startup_timeout=1)
    with pytest.raises(EmbeddedNatsError, match="readiness timed out"):
        server.start()
    assert server._process is None
    assert not (store / "managed-runtime.json").exists()
    assert (store / "jetstream").is_dir()


def test_child_crash_requires_manual_recovery(tmp_path: Path) -> None:
    store = tmp_path / "store"
    server = NatsServer(store)
    server.start()
    process = server._process
    assert process is not None
    process.kill()
    process.wait(timeout=5)
    with pytest.raises(RecoveryRequired):
        server.stop()
    marker = store / "managed-runtime.json"
    assert marker.exists()
    with pytest.raises(RecoveryRequired):
        NatsServer(store).start()
    assert process.poll() is not None
    marker.unlink()  # Test-only manual recovery after proving our exact child is gone.
    with NatsServer(store):
        pass


def test_stale_pid_is_never_adopted(tmp_path: Path) -> None:
    store = tmp_path / "store"
    store.mkdir()
    if os.name != "nt":
        store.chmod(0o700)
    (store / "managed-runtime.json").write_text(json.dumps({"owner_pid": os.getpid(), "generation": "old"}))
    with pytest.raises(RecoveryRequired):
        NatsServer(store).start()
    assert (store / "managed-runtime.json").exists()


def test_cross_process_store_lock(tmp_path: Path) -> None:
    store = tmp_path / "shared store"
    ready = tmp_path / "ready"
    script = """
import sys
from pathlib import Path
from embedded_nats import NatsServer
with NatsServer(sys.argv[1]):
    Path(sys.argv[2]).write_text('ready')
    sys.stdin.readline()
"""
    owner = subprocess.Popen(
        [sys.executable, "-c", script, str(store), str(ready)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 20
        while not ready.exists() and owner.poll() is None and time.monotonic() < deadline:
            time.sleep(0.05)
        assert ready.exists(), "owner did not start"
        with pytest.raises(StoreInUse):
            NatsServer(store).start()
        assert owner.stdin is not None
        owner.stdin.write(b"\n")
        owner.stdin.flush()
        _stdout, stderr = owner.communicate(timeout=20)
        assert owner.returncode == 0, stderr.decode(errors="replace")
    finally:
        if owner.poll() is None:
            if owner.stdin is not None:
                owner.stdin.close()
            owner.wait(timeout=20)
    with NatsServer(store):
        pass


def test_symlink_store_alias_does_not_bypass_lock(tmp_path: Path) -> None:
    store = tmp_path / "store"
    alias = tmp_path / "alias"
    with NatsServer(store):
        try:
            alias.symlink_to(store, target_is_directory=True)
        except (NotImplementedError, OSError):
            pytest.skip("Creating a directory symlink is unavailable")
        with pytest.raises(StoreInUse):
            NatsServer(alias).start()


def test_parent_crash_leaves_fail_closed_marker(tmp_path: Path) -> None:
    store = tmp_path / "crashed owner"
    status_path = tmp_path / "child.json"
    script = """
import json
import os
import sys
from pathlib import Path
from embedded_nats import NatsServer
server = NatsServer(sys.argv[1]).start()
Path(sys.argv[2]).write_text(json.dumps({'pid': server.pid, 'port': server.port}))
os._exit(0)
"""
    owner = subprocess.run(
        [sys.executable, "-c", script, str(store), str(status_path)],
        capture_output=True,
        timeout=20,
        check=True,
    )
    assert owner.returncode == 0
    status = json.loads(status_path.read_text())
    child_pid = status["pid"]
    try:
        marker = json.loads((store / "managed-runtime.json").read_text())
        assert marker["child_pid"] == child_pid
        with pytest.raises(RecoveryRequired):
            NatsServer(store).start()
        with pytest.raises(RecoveryRequired, match="lease_held"):
            NatsServer(store, recover_stale=True).start()
        with socket.create_connection(("127.0.0.1", status["port"]), timeout=2):
            pass
    finally:
        os.kill(child_pid, signal.SIGTERM)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            recovered = NatsServer(store, recover_stale=True).start()
        except RecoveryRequired:
            pass
        else:
            assert recovered.recovered_generation == marker["generation"]
            recovered.stop()
            break
        time.sleep(0.05)
    else:
        pytest.fail("orphan lifetime lease stayed unavailable after exact-PID test cleanup")
    with NatsServer(store):
        pass


def test_context_exception_still_stops_owned_child(tmp_path: Path) -> None:
    store = tmp_path / "store"
    with pytest.raises(ValueError, match="consumer failed"), NatsServer(store) as server:
        assert server.pid > 0
        raise ValueError("consumer failed")
    assert not (store / "managed-runtime.json").exists()
    with NatsServer(store):
        pass


def test_core_jetstream_kv_object_persist(tmp_path: Path) -> None:
    store = tmp_path / "store"

    async def write(url: str, token: str) -> None:
        client = await nats.connect(url, token=token)
        try:
            sub = await client.subscribe("test.events")
            await client.flush()
            await client.publish("test.events", b"hello")
            assert (await sub.next_msg(timeout=2)).data == b"hello"
            responder = await client.subscribe("test.request")

            async def reply_once() -> None:
                message = await responder.next_msg(timeout=2)
                await message.respond(b"reply")

            reply_task = asyncio.create_task(reply_once())
            assert (await client.request("test.request", b"request", timeout=2)).data == b"reply"
            await reply_task
            js = client.jetstream()
            await js.add_stream(name="TASKS", subjects=["tasks.*"])
            await js.publish("tasks.one", b"one")
            messages = await js.pull_subscribe("tasks.*", durable="worker", stream="TASKS")
            message = (await messages.fetch(1, timeout=2))[0]
            assert message.data == b"one"
            await message.ack_sync(timeout=2)
            await js.publish("tasks.retry", b"retry")
            retry = (await messages.fetch(1, timeout=2))[0]
            assert retry.data == b"retry"
            await retry.nak()
            redelivered = (await messages.fetch(1, timeout=2))[0]
            assert redelivered.data == b"retry"
            await redelivered.ack_sync(timeout=2)
            kv = await js.create_key_value(bucket="state")
            await kv.put("one", b"value")
            assert (await kv.get("one")).value == b"value"
            watcher = await kv.watch("one")
            try:
                initial = await watcher.updates(timeout=2)
                assert initial is not None and initial.value == b"value"
                assert await watcher.updates(timeout=2) is None  # Initial snapshot boundary.
                await kv.put("one", b"updated")
                update = await watcher.updates(timeout=2)
                assert update is not None and update.value == b"updated"
            finally:
                await watcher.stop()
            objects = await js.create_object_store(bucket="files", storage=StorageType.FILE, max_bytes=64 * 1024 * 1024)
            with (tmp_path / "source.bin").open("wb") as out:
                out.write(b"small-object")
            with (tmp_path / "source.bin").open("rb") as source:
                info = await objects.put("immutable-key", source)
            assert info.size == 12
            with (tmp_path / "result.bin").open("wb") as result:
                await objects.get("immutable-key", writeinto=result)
            assert (tmp_path / "result.bin").read_bytes() == b"small-object"
            await objects.put("another-key", b"second")
            assert {item.name for item in await objects.list()} == {"immutable-key", "another-key"}
            await objects.put("another-key", b"replacement")
            assert (await objects.get("another-key")).data == b"replacement"
            await objects.delete("another-key")
            with pytest.raises(nats.errors.Error):
                await objects.get_info("another-key")
        finally:
            await client.close()

    async def read(url: str, token: str) -> None:
        client = await nats.connect(url, token=token)
        try:
            js = client.jetstream()
            kv = await js.key_value("state")
            assert (await kv.get("one")).value == b"updated"
            objects = await js.object_store("files")
            assert (await objects.get_info("immutable-key")).size == 12
            with (tmp_path / "after-restart.bin").open("wb") as result:
                await objects.get("immutable-key", writeinto=result)
            assert (tmp_path / "after-restart.bin").read_bytes() == b"small-object"
            await js.delete_object_store("files")
        finally:
            await client.close()

    with NatsServer(store) as server:
        asyncio.run(write(server.url, server.auth_token))
    with NatsServer(store) as server:
        asyncio.run(read(server.url, server.auth_token))


@pytest.mark.slow
def test_object_larger_than_max_payload_is_streamed(tmp_path: Path) -> None:
    source_path = tmp_path / "large.bin"
    expected = hashlib.sha256()
    with source_path.open("wb") as file:
        for index in range(32):
            block = hashlib.shake_256(str(index).encode()).digest(1024 * 1024)
            expected.update(block)
            file.write(block)

    async def transfer(url: str, token: str) -> None:
        client = await nats.connect(url, token=token)
        try:
            js = client.jetstream()
            objects = await js.create_object_store(bucket="large", storage=StorageType.FILE, max_bytes=64 * 1024 * 1024)
            with source_path.open("rb") as source:
                info = await objects.put("large-object", source)
            assert info.size == 32 * 1024 * 1024
            destination = tmp_path / "large.download"
            with destination.open("wb") as output:
                await objects.get("large-object", writeinto=output)
            digest = hashlib.sha256()
            with destination.open("rb") as file:
                while block := file.read(1024 * 1024):
                    digest.update(block)
            assert digest.digest() == expected.digest()
        finally:
            await client.close()

    with NatsServer(tmp_path / "store", max_payload=1024 * 1024) as server:
        tracemalloc.start()
        try:
            asyncio.run(transfer(server.url, server.auth_token))
            _current, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        assert peak < 16 * 1024 * 1024, f"32 MiB transfer buffered too much Python memory: {peak} bytes"


def test_interrupted_object_transfer_is_not_published(tmp_path: Path) -> None:
    class FailingReader:
        calls = 0

        def readinto(self, buffer: bytearray) -> int:
            self.calls += 1
            if self.calls > 1:
                raise OSError("injected reader failure")
            buffer[: len(buffer)] = b"a" * len(buffer)
            return len(buffer)

    class FailingWriter:
        def __init__(self, output: object) -> None:
            self.output = output
            self.calls = 0

        def write(self, data: bytes) -> int:
            self.calls += 1
            if self.calls > 1:
                raise OSError("injected writer failure")
            return self.output.write(data)

    with NatsServer(tmp_path / "store") as server:

        async def check() -> None:
            client = await nats.connect(server.url, token=server.auth_token)
            try:
                objects = await client.jetstream().create_object_store(bucket="interrupt", storage=StorageType.FILE)
                with pytest.raises(OSError, match="injected reader"):
                    await objects.put("incomplete", FailingReader())
                with pytest.raises(nats.errors.Error):
                    await objects.get_info("incomplete")
                await objects.put("complete", b"b" * (256 * 1024))
                temp_output = tmp_path / "download.tmp"
                try:
                    with temp_output.open("wb") as output, pytest.raises(OSError, match="injected writer"):
                        await objects.get("complete", writeinto=FailingWriter(output))
                    assert temp_output.stat().st_size > 0
                finally:
                    temp_output.unlink(missing_ok=True)
                assert not temp_output.exists()  # The consumer discards its incomplete staging file.
                assert (await objects.get_info("complete")).size == 256 * 1024
            finally:
                await client.close()

        asyncio.run(check())


def test_core_only_disables_jetstream(tmp_path: Path) -> None:
    with NatsServer(tmp_path / "store", jetstream=False) as server:

        async def check() -> None:
            client = await nats.connect(server.url, token=server.auth_token)
            try:
                await client.flush()
                with pytest.raises((nats.errors.Error, TimeoutError)):
                    await asyncio.wait_for(client.jetstream().account_info(), timeout=0.5)
            finally:
                await client.close()

        asyncio.run(check())


def test_forced_stop_keeps_acknowledged_object(tmp_path: Path) -> None:
    store = tmp_path / "store"
    server = NatsServer(store)
    server.start()

    async def put() -> None:
        client = await nats.connect(server.url, token=server.auth_token)
        try:
            objects = await client.jetstream().create_object_store(bucket="recovery", storage=StorageType.FILE)
            await objects.put("acknowledged", b"persistent-bytes")
        finally:
            await client.close()

    asyncio.run(put())
    process = server._process
    assert process is not None
    process.kill()
    process.wait(timeout=5)
    with pytest.raises(RecoveryRequired):
        server.stop()
    (store / "managed-runtime.json").unlink()  # Exact child was reaped; deliberate test recovery.

    with NatsServer(store) as restored:

        async def get() -> None:
            client = await nats.connect(restored.url, token=restored.auth_token)
            try:
                objects = await client.jetstream().object_store("recovery")
                assert (await objects.get("acknowledged")).data == b"persistent-bytes"
            finally:
                await client.close()

        asyncio.run(get())


def test_object_quota_and_ttl(tmp_path: Path) -> None:
    with NatsServer(tmp_path / "store") as server:

        async def check() -> None:
            client = await nats.connect(server.url, token=server.auth_token)
            try:
                js = client.jetstream()
                limited = await js.create_object_store(bucket="limited", storage=StorageType.FILE, max_bytes=256 * 1024)
                with pytest.raises(nats.errors.Error):
                    await limited.put("too-large", b"x" * (512 * 1024))
                with pytest.raises(nats.errors.Error):
                    await limited.get_info("too-large")
                expiring = await js.create_object_store(bucket="expiring", storage=StorageType.FILE, ttl=1)
                await expiring.put("temporary", b"short-lived")
                await asyncio.sleep(1.5)
                with pytest.raises(nats.errors.Error):
                    await expiring.get_info("temporary")
            finally:
                await client.close()

        asyncio.run(check())


def test_global_file_store_limit_rejects_oversized_bucket(tmp_path: Path) -> None:
    with NatsServer(tmp_path / "store", max_file_store="1MB") as server:

        async def check() -> None:
            client = await nats.connect(server.url, token=server.auth_token)
            try:
                with pytest.raises(nats.errors.Error):
                    await client.jetstream().create_object_store(
                        bucket="too_large", storage=StorageType.FILE, max_bytes=2 * 1024 * 1024
                    )
            finally:
                await client.close()

        asyncio.run(check())
