"""Fail closed if the release inventory is incomplete or mislabeled."""

from __future__ import annotations

import hashlib
import json
import struct
import sys
import tomllib
import zipfile
from pathlib import Path

from build_wheels import TARGETS, UPSTREAM_COMMIT, UPSTREAM_TAG

with (Path(__file__).resolve().parents[1] / "pyproject.toml").open("rb") as metadata:
    EXPECTED_VERSION = tomllib.load(metadata)["project"]["version"]
if not EXPECTED_VERSION.startswith(UPSTREAM_TAG.removeprefix("v") + "."):
    raise ValueError("Package version must extend the pinned server version")
ELF_MACHINES = {"linux_amd64": 62, "linux_arm64": 183, "linux_armv7": 40}
MACH_CPUS = {"darwin_amd64": 0x01000007, "darwin_arm64": 0x0100000C}


def verify_binary(target: str, contents: bytes) -> None:
    if target in ELF_MACHINES:
        assert contents[:4] == b"\x7fELF"
        assert struct.unpack_from("<H", contents, 18)[0] == ELF_MACHINES[target]
    elif target in MACH_CPUS:
        assert contents[:4] == b"\xcf\xfa\xed\xfe"
        assert struct.unpack_from("<I", contents, 4)[0] == MACH_CPUS[target]
    else:
        assert contents[:2] == b"MZ"
        pe_offset = struct.unpack_from("<I", contents, 0x3C)[0]
        assert contents[pe_offset : pe_offset + 4] == b"PE\0\0"
        assert struct.unpack_from("<H", contents, pe_offset + 4)[0] == 0x8664


def main() -> None:
    directory = Path(sys.argv[1] if len(sys.argv) > 1 else "dist")
    wheels = sorted(directory.glob("*.whl"))
    if len(wheels) != len(TARGETS):
        raise SystemExit(f"Expected {len(TARGETS)} wheels, got {len(wheels)}")
    checksums = []
    for target, (_, _, _, platform) in TARGETS.items():
        matches = [
            wheel for wheel in wheels if wheel.name == f"embedded_nats-{EXPECTED_VERSION}-py3-none-{platform}.whl"
        ]
        if len(matches) != 1:
            raise SystemExit(f"Missing or duplicate wheel for {target}")
        wheel = matches[0]
        with zipfile.ZipFile(wheel) as archive:
            names = archive.namelist()
            binaries = [name for name in names if name.startswith("embedded_nats/bin/nats-server")]
            if len(binaries) != 1:
                raise SystemExit(f"Wrong binary inventory in {wheel.name}")
            binary_name = binaries[0]
            if (target == "windows_amd64") != binary_name.endswith(".exe"):
                raise SystemExit(f"Wrong binary suffix in {wheel.name}")
            binary = archive.read(binary_name)
            verify_binary(target, binary)
            manifest = json.loads(archive.read("embedded_nats/build_manifest.json"))
            if (
                manifest["commit"] != UPSTREAM_COMMIT
                or manifest["tag"] != UPSTREAM_TAG
                or manifest["target"] != target
                or manifest["platform_tag"] != platform
                or manifest["binary_sha256"] != hashlib.sha256(binary).hexdigest()
            ):
                raise SystemExit(f"Manifest mismatch in {wheel.name}")
            if not manifest["go_modules"]:
                raise SystemExit(f"Missing Go module inventory in {wheel.name}")
            if (
                hashlib.sha256(archive.read("embedded_nats/licenses/go_standard_library.txt")).hexdigest()
                != manifest["go_license_sha256"]
            ):
                raise SystemExit(f"Go standard library license mismatch in {wheel.name}")
            for module in manifest["go_modules"]:
                license_data = archive.read("embedded_nats/" + module["license_file"])
                if hashlib.sha256(license_data).hexdigest() != module["license_sha256"]:
                    raise SystemExit(f"Linked module license mismatch in {wheel.name}: {module['module']}")
            wheel_metadata = archive.read(next(name for name in names if name.endswith(".dist-info/WHEEL"))).decode()
            package_metadata = archive.read(
                next(name for name in names if name.endswith(".dist-info/METADATA"))
            ).decode()
            if (
                f"Tag: py3-none-{platform}" not in wheel_metadata
                or f"Version: {EXPECTED_VERSION}" not in package_metadata
                or "Requires-Python: >=3.12" not in package_metadata
                or "Requires-Dist: nats-py==2.16.0" not in package_metadata
            ):
                raise SystemExit(f"Wheel metadata mismatch in {wheel.name}")
            if not any(name.endswith("/LICENSE") for name in names):
                raise SystemExit(f"Missing license in {wheel.name}")
            if target != "windows_amd64":
                mode = archive.getinfo(binary_name).external_attr >> 16
                if not mode & 0o111:
                    raise SystemExit(f"Executable bit lost in {wheel.name}")
        checksums.append(f"{hashlib.sha256(wheel.read_bytes()).hexdigest()}  {wheel.name}")
        print(f"OK {target}: {wheel.name}")
    (directory / "SHA256SUMS").write_text("\n".join(checksums) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
