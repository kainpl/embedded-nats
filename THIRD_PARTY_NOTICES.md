# Bundled software

The wheel includes a binary built from [nats-server](https://github.com/nats-io/nats-server),
licensed under Apache-2.0. Its source LICENSE is reproduced in this package's
`LICENSE`. Each wheel's `embedded_nats/build_manifest.json` records the
exact source commit and the Go modules linked into its binary. The corresponding
upstream license texts are bundled under `embedded_nats/licenses/` and
identified with checksums in that manifest.
The Go standard library license is bundled there as `go_standard_library.txt`.

The Python runtime client [nats-py](https://github.com/nats-io/nats.py) is
installed as a separate Python distribution with its own license metadata.
