# Changelog

## 2.15.0.3 — Unreleased

- Rebuild the pinned NATS server with Go 1.26.10 to include standard-library
  security fixes through that release. Keep NATS, its module dependencies,
  the managed-server API and the macOS 12 platform minimum unchanged.

## 2.15.0.2 — 2026-10-07

- Repair POSIX permissions on existing store, runtime and log directories owned
  by the current OS user before starting NATS. A store created with mode 0755
  no longer prevents dependent services from starting.
- Verify mode 0700 after repair, refuse foreign-owned directories and final
  symlinks during repair, and preserve stored data and its permissions.

## 2.15.0.0 — 2026-09-23

Initial package: six platform wheels for nats-server 2.15.0, a managed
loopback server with JetStream, and Object Store acceptance coverage.

The wheels target macOS 12+, but macOS runtime acceptance ran on macOS 15
(Intel and Apple Silicon); macOS 12 itself has not been tested.
# 2.15.0.1

- Add opt-in stale runtime recovery using a kernel-held lifetime fence inherited
  by the actual broker, across Windows, Linux and macOS. Keep fail-closed as the
  default and for legacy, missing, copied or uncertain ownership evidence.
- Preserve JetStream data during recovery; expose diagnostic reasons, marker
  paths and the recovered generation. Never infer process death from a PID or port.
- Test inherited ownership, orphan brokers, closed listeners, crash windows,
  copied stores, interrupted cleanup and persistent KV/Object Store recovery.
