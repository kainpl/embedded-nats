# Changelog

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
