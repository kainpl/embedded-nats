# embedded-nats

Platform wheels containing [nats-server](https://github.com/nats-io/nats-server)
and a small Python manager. JetStream, KV, and Object Store are provided by
that one server. The project targets Python 3.12+ on Windows x64, Linux
x86_64/aarch64/armv7l, and macOS x86_64/arm64 (macOS 12+).

The package version tracks the server version plus a packaging revision:
`2.15.0.1` bundles NATS `2.15.0`, packaging revision 1. No Go toolchain is
needed to install a wheel. The only Python runtime dependency is pinned
`nats-py` 2.16.0.

This is independent, unofficial packaging of the NATS™ server; it is not
produced, sponsored, or endorsed by the NATS project or the Linux Foundation.
The maintainer has chosen to retain the `embedded-nats` name. This choice is
not a claim of affiliation or trademark permission.

## Example

```python
import asyncio
import os
from pathlib import Path

import nats
from embedded_nats import get_server


async def put_example(url: str, token: str) -> None:
    client = await nats.connect(url, token=token)
    try:
        js = client.jetstream()
        store = await js.create_object_store(bucket="render_results", max_bytes=64 * 1024 * 1024)
        with Path("preview.png").open("rb") as source:
            await store.put("render/immutable-run-id.png", source)
        temporary = Path("preview.download.tmp")
        try:
            with temporary.open("wb") as destination:
                await store.get("render/immutable-run-id.png", writeinto=destination)
            os.replace(temporary, "preview.download.png")
        finally:
            temporary.unlink(missing_ok=True)
        await js.delete_object_store("render_results")  # only for this disposable example
    finally:
        await client.close()


with get_server("./nats-data") as server:
    print(server.url)  # URL never includes the token
    asyncio.run(put_example(server.url, server.auth_token))
```

The synchronous manager should be started outside an application's asyncio
event loop (for example with `await asyncio.to_thread(server.start)`). It
starts a child, waits for an authenticated NATS roundtrip and JetStream API,
then owns that child until `stop()`. A second manager cannot control the same
store. By default a previous process marker requires manual recovery; the package does
not adopt or kill a process identified only by an old PID.

### Opt-in recovery after a crash

`NatsServer(store, recover_stale=True)` can recover a runtime created by this
version after its broker has exited. A separate kernel-held lifetime fence is
inherited by the broker itself: `flock` on Linux/macOS, an exclusive file handle
on Windows. Recovery must acquire the **same physical fence file** while holding
the manager lock. Neither a closed port, PID reuse, a clock adjustment nor a
different container PID namespace is treated as evidence of death. No orphan
process is killed or adopted. `server.recovered_generation` identifies a recovered
generation, or is `None` for a clean start.

Use a **local filesystem**, not NFS/SMB or a store shared across machines. Never
delete or replace `.broker.lease`: the persistent file binds the evidence to its
kernel object. Legacy/corrupt markers, missing/replaced/copied fence files,
permission errors and live holders still require manual recovery. A copied
unclean store is not automatically trusted. A reboot releases the kernel fence;
it does not erase or repair JetStream data. The package removes only old runtime
configuration/port files and the marker, never `jetstream/` or buckets.

`RecoveryRequired.reason` is a diagnostic code; `RecoveryRequired.marker_path`
and `server.recovery_marker_path` are public diagnostic paths. These fields contain
no credentials. A broker that dies during the manager's lifetime still makes
`stop()` report `RecoveryRequired`; a subsequent opt-in start can recover it.

This proves broker ownership only. Consumers must independently contain their
workers and must not delete old staging files just because broker recovery
succeeded. There is no automatic replay of consumer commands.

## Data and security

The server listens on loopback with token authentication. The token is kept
separately from `server.url`; avoid logging it. Store data remains after a
clean restart. The default JetStream `sync_interval` is `always`; relaxing it
increases the loss window after a crash. Windows managed stop terminates the
child, so recovery is tested as a crash, not as graceful server shutdown.

Store directories should be private to the OS user. On POSIX the manager
requires mode 0700; on Windows choose a directory with a user-private ACL,
for example inside the user's application data directory. The generated
configuration contains the token. The JetStream file budget defaults to 1 GB,
memory store to 64 MB, and NATS message payload to 1 MiB. These do not cap
the process RSS or all disk overhead. Configure per-bucket max bytes and TTL
according to each consumer's artifact lifetime. Object Store is not S3 and
does not provide a transaction with your database or physical printer.
Download to a temporary file and publish it with an atomic rename only after
`get()` completes and verifies the digest; discard partial temporary files
after an error. An interrupted upload is not a committed object; consumers
must not infer completion from orphan chunks. Bucket TTL/quota and periodic
consumer cleanup remain necessary for abandoned artifacts.

If a process crashes, inspect `managed-runtime.json` (including `child_pid`
when present), verify that its old NATS process is gone by executable and
store path rather than PID alone, and only then remove the marker before
restarting. A crash between spawn and marker update can leave `child_pid`
null; inspect the old generation's ports file and OS process list in that case.
Keep `jetstream/` and all bucket data. A live copy of that directory is not
a consistent backup; integration with application backup belongs to the
consumer. The package neither installs an OS service nor deletes store data.

## Build and verification

`scripts/build_wheels.py` builds six target binaries from the pinned upstream
tag and commit with Go 1.26.8 (source minimum 1.26), then packages each in a
fresh staging tree. The exact commit, Go version, target, binary SHA256, and
Go module inventory are inside every wheel. Linux wheels are tagged
`manylinux_2_17` because the Go binary is built with CGO disabled; minimum
kernel compatibility is checked separately. macOS uses a 12.0 deployment
floor because Go 1.26 does not support macOS 11. Windows requires Windows 10+
or Server 2016+ under that toolchain. These are packaging targets, not a
claim of native tests on every platform from one workstation.

```text
python scripts/build_wheels.py all
python -m pytest tests/ -v
```

Releases are triggered by `v<package-version>` tags after full wheel and
runtime acceptance. The workflow uses the exact tested wheels for both GitHub
Releases and PyPI Trusted Publishing. Creating the repository or committing
code does not publish a package.

The bundled server is Apache-2.0. See [NOTICE](NOTICE),
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md), and each wheel's build
manifest for the pinned source and dependency inventory.
