"""Own one loopback nats-server process and one persistent JetStream store."""

from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import socket
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Self
from urllib.parse import urlparse

import nats

SERVER_VERSION = "2.15.0"
_LIMIT = re.compile(r"^[1-9][0-9]*(?:KB|MB|GB)$")
_TOKEN = re.compile(r"^[A-Za-z0-9_-]{16,256}$")
_SYNC = re.compile(r"^(?:always|[1-9][0-9]*(?:ms|s|m|h))$")
_CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


class EmbeddedNatsError(RuntimeError):
    """The managed server could not start, operate, or stop safely."""


class StoreInUse(EmbeddedNatsError):
    """Another manager holds the store lock."""


class RecoveryRequired(EmbeddedNatsError):
    """An earlier process may still own the persistent store."""


def binary_path() -> Path:
    name = "nats-server.exe" if os.name == "nt" else "nats-server"
    path = Path(__file__).resolve().parent / "bin" / name
    if not path.is_file():
        raise EmbeddedNatsError(f"Bundled nats-server is missing: {path}")
    if os.name != "nt" and not os.access(path, os.X_OK):
        raise EmbeddedNatsError(f"Bundled nats-server is not executable: {path}")
    return path


def server_version() -> str:
    try:
        result = subprocess.run(
            [str(binary_path()), "--version"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
            creationflags=_CREATE_NO_WINDOW,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise EmbeddedNatsError("Unable to execute bundled nats-server") from exc
    match = re.search(r"\bv?(\d+\.\d+\.\d+)\b", result.stdout)
    if not match:
        raise EmbeddedNatsError("Bundled nats-server returned no recognizable version")
    return match.group(1)


class _StoreLock:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._file = None

    def acquire(self) -> None:
        fd = self._path.open("a+b")
        try:
            fd.seek(0, os.SEEK_END)
            if fd.tell() == 0:
                fd.write(b"\0")
                fd.flush()
            fd.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(fd.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError) as exc:
            fd.close()
            raise StoreInUse(f"Store is already managed: {self._path.parent}") from exc
        self._file = fd

    def release(self) -> None:
        if self._file is None:
            return
        try:
            self._file.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        finally:
            self._file.close()
            self._file = None


def _private_directory(path: Path) -> Path:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    resolved = path.resolve(strict=True)
    if os.name != "nt" and resolved.stat().st_mode & 0o077:
        raise EmbeddedNatsError(f"Store directory must be private (mode 0700): {resolved}")
    return resolved


def _redact(value: str, token: str) -> str:
    return value.replace(token, "[REDACTED]")


def _quoted(value: str) -> str:
    # NATS accepts quoted UTF-8, but not JSON's \uXXXX escape syntax.
    return json.dumps(value, ensure_ascii=False)


def _log_tail(path: Path, token: str, limit: int = 8192) -> str:
    try:
        with path.open("rb") as stream:
            stream.seek(0, os.SEEK_END)
            stream.seek(max(0, stream.tell() - limit))
            data = stream.read(limit)
        return _redact(data.decode("utf-8", errors="replace"), token)
    except OSError:
        return ""


def _read_info(host: str, port: int, timeout: float) -> dict:
    with socket.create_connection((host, port), timeout=timeout) as stream:
        stream.settimeout(timeout)
        data = bytearray()
        while b"\r\n" not in data and len(data) < 16384:
            part = stream.recv(512)
            if not part:
                raise OSError("Server closed before INFO")
            data.extend(part)
    if not data.startswith(b"INFO ") or b"\r\n" not in data:
        raise ValueError("No valid NATS INFO line")
    result = json.loads(data[5 : data.index(b"\r\n")].decode("utf-8"))
    if not isinstance(result, dict):
        raise TypeError("INFO is not an object")
    return result


async def _authenticated_probe(url: str, token: str, jetstream: bool, timeout: float) -> None:
    client = await nats.connect(
        url,
        token=token,
        allow_reconnect=False,
        connect_timeout=timeout,
        max_reconnect_attempts=0,
    )
    try:
        await asyncio.wait_for(client.flush(), timeout=timeout)
        if jetstream:
            await asyncio.wait_for(client.jetstream().account_info(), timeout=timeout)
    finally:
        await client.close()


def _probe_in_thread(url: str, token: str, jetstream: bool, timeout: float) -> None:
    outcome: list[BaseException] = []

    def run() -> None:
        try:
            asyncio.run(asyncio.wait_for(_authenticated_probe(url, token, jetstream, timeout), timeout))
        except Exception as exc:  # noqa: BLE001 - propagate every readiness failure to the owner thread.
            outcome.append(exc)

    worker = threading.Thread(target=run, name="embedded-nats-ready", daemon=True)
    worker.start()
    worker.join(timeout + 0.25)
    if worker.is_alive():
        raise TimeoutError("NATS client readiness probe did not finish")
    if outcome:
        raise outcome[0]


class NatsServer:
    """A managed child process; the store survives clean and forced stops."""

    def __init__(
        self,
        store_dir: os.PathLike[str] | str,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        jetstream: bool = True,
        auth_token: str | None = None,
        max_memory_store: str = "64MB",
        max_file_store: str = "1GB",
        max_payload: int = 1024 * 1024,
        sync_interval: str = "always",
        startup_timeout: float = 15,
        shutdown_timeout: float = 10,
    ) -> None:
        if host not in ("127.0.0.1", "::1"):
            raise ValueError("Only explicit loopback hosts are supported")
        if not isinstance(port, int) or not 0 <= port <= 65535:
            raise ValueError("Port must be between 0 and 65535")
        if not _LIMIT.fullmatch(max_memory_store) or not _LIMIT.fullmatch(max_file_store):
            raise ValueError("JetStream limits must be positive KB, MB, or GB amounts")
        if not isinstance(max_payload, int) or not 1024 <= max_payload <= 8 * 1024 * 1024:
            raise ValueError("max_payload must be 1 KiB through 8 MiB")
        if not _SYNC.fullmatch(sync_interval):
            raise ValueError("sync_interval must be 'always' or a positive duration")
        if startup_timeout <= 0 or shutdown_timeout <= 0:
            raise ValueError("Time limits must be positive")
        token = secrets.token_urlsafe(32) if auth_token is None else auth_token
        if not _TOKEN.fullmatch(token):
            raise ValueError("auth_token must be 16-256 URL-safe ASCII characters")
        self.store_dir = Path(store_dir).expanduser().resolve()
        self.host = host
        self.requested_port = port
        self.jetstream = jetstream
        self.auth_token = token
        self.max_memory_store = max_memory_store
        self.max_file_store = max_file_store
        self.max_payload = max_payload
        self.sync_interval = sync_interval
        self.startup_timeout = startup_timeout
        self.shutdown_timeout = shutdown_timeout
        self._mutex = threading.RLock()
        self._store_lock: _StoreLock | None = None
        self._process: subprocess.Popen[bytes] | None = None
        self._generation: str | None = None
        self._runtime_dir: Path | None = None
        self._port: int | None = None

    @property
    def port(self) -> int:
        if self._port is None:
            raise EmbeddedNatsError("Server is not running")
        return self._port

    @property
    def pid(self) -> int:
        if self._process is None:
            raise EmbeddedNatsError("Server is not running")
        return self._process.pid

    @property
    def url(self) -> str:
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"nats://{host}:{self.port}"

    @property
    def log_path(self) -> Path:
        return self.store_dir / "logs" / "nats.log"

    def _config(self) -> str:
        assert self._runtime_dir is not None
        listen_host = f"[{self.host}]" if ":" in self.host else self.host
        lines = [
            f"listen: {_quoted(f'{listen_host}:{self.requested_port or -1}')}",
            f"authorization: {{ token: {_quoted(self.auth_token)} }}",
            f"ports_file_dir: {_quoted(str(self._runtime_dir))}",
            f"log_file: {_quoted(str(self.log_path))}",
            "log_size_limit: 10485760",
            "log_max_num: 3",
            f"max_payload: {self.max_payload}",
        ]
        if self.jetstream:
            lines += [
                "jetstream {",
                f"  store_dir: {_quoted(str(self.store_dir / 'jetstream'))}",
                f"  max_memory_store: {self.max_memory_store}",
                f"  max_file_store: {self.max_file_store}",
                f"  sync_interval: {_quoted(self.sync_interval)}",
                "}",
            ]
        return "\n".join(lines) + "\n"

    def _marker_path(self) -> Path:
        return self.store_dir / "managed-runtime.json"

    def _write_marker(self, child_pid: int | None = None) -> None:
        assert self._runtime_dir is not None and self._generation is not None
        temporary = self._runtime_dir / "managed-runtime.json.tmp"
        metadata = {"generation": self._generation, "owner_pid": os.getpid(), "child_pid": child_pid}
        temporary.write_text(json.dumps(metadata), encoding="utf-8")
        if os.name != "nt":
            temporary.chmod(0o600)
        os.replace(temporary, self._marker_path())

    def _read_port(self) -> int | None:
        assert self._process is not None and self._runtime_dir is not None
        path = self._runtime_dir / f"{binary_path().name}_{self._process.pid}.ports"
        try:
            if path.stat().st_size > 8192:
                raise EmbeddedNatsError("NATS ports file is unexpectedly large")
            data = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        urls = data.get("nats", [])
        if not isinstance(urls, list) or len(urls) != 1:
            raise EmbeddedNatsError("NATS ports file has an unexpected listener set")
        parsed = urlparse(urls[0])
        if parsed.scheme != "nats" or parsed.hostname != self.host or not parsed.port:
            raise EmbeddedNatsError("NATS ports file does not match the managed listener")
        return parsed.port

    def _cleanup_runtime(self) -> None:
        if self._runtime_dir is None or self._generation is None:
            return
        marker = self._marker_path()
        if marker.exists():
            try:
                recorded = json.loads(marker.read_text(encoding="utf-8"))
            except ValueError:
                return
            if recorded.get("generation") != self._generation:
                return
        for path in self._runtime_dir.iterdir():
            if path.is_file() and path.parent == self._runtime_dir:
                path.unlink()
        self._runtime_dir.rmdir()
        marker.unlink(missing_ok=True)
        self._runtime_dir = None
        self._generation = None

    def _stop_child(self) -> str:
        process = self._process
        if process is None:
            return "not-started"
        if process.poll() is not None:
            return "crashed"
        mode = "forced" if os.name == "nt" else "graceful"
        process.terminate()
        try:
            process.wait(timeout=self.shutdown_timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=self.shutdown_timeout)
            mode = "forced"
        return mode

    def start(self) -> NatsServer:
        with self._mutex:
            if self._process is not None:
                if self._process.poll() is None:
                    return self
                raise RecoveryRequired("Managed NATS child exited unexpectedly; inspect the store")
            self.store_dir = _private_directory(self.store_dir)
            lock = _StoreLock(self.store_dir / ".managed.lock")
            lock.acquire()
            self._store_lock = lock
            if self._marker_path().exists():
                lock.release()
                self._store_lock = None
                raise RecoveryRequired("Previous managed runtime is unresolved; inspect managed-runtime.json")
            try:
                self._generation = uuid.uuid4().hex
                _private_directory(self.store_dir / ".runtime")
                self._runtime_dir = _private_directory(self.store_dir / ".runtime" / self._generation)
                _private_directory(self.store_dir / "logs")
                if server_version() != SERVER_VERSION:
                    raise EmbeddedNatsError("Bundled server version does not match this package")
                config_path = self._runtime_dir / "nats.conf"
                config_path.write_text(self._config(), encoding="utf-8")
                if os.name != "nt":
                    config_path.chmod(0o600)
                checked = subprocess.run(
                    [str(binary_path()), "-t", "-c", str(config_path)],
                    capture_output=True,
                    text=True,
                    timeout=5,
                    creationflags=_CREATE_NO_WINDOW,
                    check=False,
                )
                if checked.returncode != 0:
                    raise EmbeddedNatsError(
                        "Bundled server rejected generated config: " + _redact(checked.stderr, self.auth_token)
                    )
                self._write_marker()
                self._process = subprocess.Popen(
                    [str(binary_path()), "-c", str(config_path)],
                    cwd=self._runtime_dir,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    creationflags=_CREATE_NO_WINDOW,
                )
                self._write_marker(self._process.pid)
                deadline = time.monotonic() + self.startup_timeout
                last_error: BaseException | None = None
                while time.monotonic() < deadline:
                    if self._process.poll() is not None:
                        raise EmbeddedNatsError("NATS child exited during startup")
                    try:
                        port = self._read_port()
                    except (OSError, ValueError, TypeError) as exc:
                        last_error = exc
                        time.sleep(min(0.1, max(0, deadline - time.monotonic())))
                        continue
                    if port:
                        self._port = port
                        try:
                            timeout = max(0.1, min(1.0, deadline - time.monotonic()))
                            info = _read_info(self.host, port, timeout)
                            if info.get("version") != SERVER_VERSION:
                                raise EmbeddedNatsError("NATS INFO reports an unexpected version")
                            _probe_in_thread(self.url, self.auth_token, self.jetstream, timeout)
                            return self
                        except (nats.errors.Error, TimeoutError, OSError, ValueError, TypeError) as exc:
                            last_error = exc
                    time.sleep(min(0.1, max(0, deadline - time.monotonic())))
                raise EmbeddedNatsError(
                    f"NATS readiness timed out: {type(last_error).__name__ if last_error else 'no port'}"
                )
            except Exception as exc:
                try:
                    self._stop_child()
                    if self._process is None or self._process.poll() is not None:
                        self._cleanup_runtime()
                except Exception as cleanup_exc:
                    raise RecoveryRequired("NATS startup failed and child ownership remains uncertain") from cleanup_exc
                finally:
                    if self._process is None or self._process.poll() is not None:
                        lock.release()
                        self._store_lock = None
                        self._process = None
                        self._port = None
                detail = _log_tail(self.log_path, self.auth_token)
                raise EmbeddedNatsError(f"NATS startup failed: {_redact(str(exc), self.auth_token)}\n{detail}") from exc

    def stop(self) -> str:
        with self._mutex:
            if self._process is None:
                return "not-started"
            try:
                mode = self._stop_child()
                if mode == "crashed":
                    raise RecoveryRequired("NATS child had already exited; runtime marker retained")
                self._cleanup_runtime()
                return mode
            finally:
                if self._process.poll() is not None:
                    self._process = None
                    self._port = None
                    if self._store_lock is not None:
                        self._store_lock.release()
                        self._store_lock = None

    def __enter__(self) -> Self:
        return self.start()

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.stop()


def get_server(store_dir: os.PathLike[str] | str, **kwargs: object) -> NatsServer:
    return NatsServer(store_dir, **kwargs)
