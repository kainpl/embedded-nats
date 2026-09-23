"""Build each target from the pinned upstream source into an isolated wheel tree."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM_TAG = "v2.15.0"
UPSTREAM_COMMIT = "eb763679aa3c24a40dcd3012aa046ad1996d851c"
GO_VERSION = "go1.26.8"
TARGETS = {
    "linux_amd64": ("linux", "amd64", "", "manylinux_2_17_x86_64"),
    "linux_arm64": ("linux", "arm64", "", "manylinux_2_17_aarch64"),
    "linux_armv7": ("linux", "arm", "7", "manylinux_2_17_armv7l"),
    "darwin_amd64": ("darwin", "amd64", "", "macosx_12_0_x86_64"),
    "darwin_arm64": ("darwin", "arm64", "", "macosx_12_0_arm64"),
    "windows_amd64": ("windows", "amd64", "", "win_amd64"),
}


def checked(args: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(args, cwd=cwd, env=env, text=True, check=True, capture_output=True)
    return result.stdout.strip()


def upstream_source(path: Path) -> Path:
    if not path.exists():
        subprocess.run(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "--branch",
                UPSTREAM_TAG,
                "https://github.com/nats-io/nats-server.git",
                str(path),
            ],
            check=True,
        )
    commit = checked(["git", "rev-parse", "HEAD"], cwd=path)
    if commit != UPSTREAM_COMMIT:
        raise RuntimeError(f"Upstream source is {commit}, expected {UPSTREAM_COMMIT}")
    if checked(["git", "status", "--porcelain", "--untracked-files=no"], cwd=path):
        raise RuntimeError("Pinned upstream source has tracked modifications")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("targets", nargs="+", choices=[*TARGETS, "all"])
    parser.add_argument("--source", type=Path, default=ROOT / ".upstream-nats")
    parser.add_argument("--output", type=Path, default=ROOT / "dist")
    args = parser.parse_args()
    targets = list(TARGETS) if "all" in args.targets else args.targets
    source = upstream_source(args.source.resolve())
    go_version = checked(["go", "version"], cwd=source)
    if GO_VERSION not in go_version:
        raise RuntimeError(f"Expected {GO_VERSION}, got {go_version}")
    args.output.mkdir(parents=True, exist_ok=True)

    for target in targets:
        goos, goarch, goarm, platform = TARGETS[target]
        with tempfile.TemporaryDirectory(prefix=f"embedded-nats-{target}-") as temp:
            stage = Path(temp)
            for filename in ("pyproject.toml", "setup.py", "README.md", "LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md"):
                shutil.copy2(ROOT / filename, stage / filename)
            package = stage / "src" / "embedded_nats"
            shutil.copytree(
                ROOT / "src" / "embedded_nats",
                package,
                ignore=shutil.ignore_patterns("bin", "build_manifest.json", "__pycache__"),
            )
            bindir = package / "bin"
            bindir.mkdir()
            binary = bindir / ("nats-server.exe" if goos == "windows" else "nats-server")
            env = os.environ.copy()
            env.update(
                {
                    "CGO_ENABLED": "0",
                    "GOOS": goos,
                    "GOARCH": goarch,
                    "GOARM": goarm,
                    "GOTOOLCHAIN": "local",
                    "GOMAXPROCS": "4",
                }
            )
            ldflags = (
                "-s -w "
                f"-X github.com/nats-io/nats-server/v2/server.gitCommit={UPSTREAM_COMMIT[:7]} "
                f"-X github.com/nats-io/nats-server/v2/server.serverVersion={UPSTREAM_TAG}"
            )
            print(f"Building {target} with {go_version}", flush=True)
            subprocess.run(
                [
                    "go",
                    "build",
                    "-p",
                    "4",
                    "-trimpath",
                    "-buildvcs=false",
                    f"-ldflags={ldflags}",
                    "-o",
                    str(binary),
                    ".",
                ],
                cwd=source,
                env=env,
                check=True,
            )
            if goos != "windows":
                binary.chmod(0o755)
            expected_magic = b"MZ" if goos == "windows" else b"\x7fELF" if goos == "linux" else b"\xcf\xfa\xed\xfe"
            if binary.open("rb").read(4)[: len(expected_magic)] != expected_magic:
                raise RuntimeError(f"Wrong binary header for {target}")
            build_info = checked(["go", "version", "-m", str(binary)], cwd=source)
            if "v2.15.0+dirty" in build_info or "vcs.modified=true" in build_info:
                raise RuntimeError(f"The {target} binary was built from a dirty source checkout")
            modules = []
            license_dir = package / "licenses"
            license_dir.mkdir()
            go_license = Path(checked(["go", "env", "GOROOT"], cwd=source)) / "LICENSE"
            shutil.copy2(go_license, license_dir / "go_standard_library.txt")
            for line in build_info.splitlines():
                fields = line.strip().split("\t")
                if len(fields) < 4 or fields[0] != "dep":
                    continue
                module, version, checksum = fields[1:4]
                module_dir = Path(
                    json.loads(checked(["go", "mod", "download", "-json", f"{module}@{version}"], cwd=source))["Dir"]
                )
                license_files = sorted(
                    path
                    for path in module_dir.iterdir()
                    if path.is_file() and path.name.upper().startswith(("LICENSE", "COPYING"))
                )
                if not license_files:
                    raise RuntimeError(f"No root license file found for linked Go module {module}@{version}")
                license_name = module.replace("/", "_").replace(".", "_") + ".txt"
                shutil.copy2(license_files[0], license_dir / license_name)
                modules.append(
                    {
                        "module": module,
                        "version": version,
                        "checksum": checksum,
                        "license_file": f"licenses/{license_name}",
                        "license_sha256": hashlib.sha256((license_dir / license_name).read_bytes()).hexdigest(),
                    }
                )
            digest = hashlib.sha256(binary.read_bytes()).hexdigest()
            (package / "build_manifest.json").write_text(
                json.dumps(
                    {
                        "source": "https://github.com/nats-io/nats-server",
                        "tag": UPSTREAM_TAG,
                        "commit": UPSTREAM_COMMIT,
                        "go": go_version,
                        "target": target,
                        "platform_tag": platform,
                        "binary_sha256": digest,
                        "go_license_sha256": hashlib.sha256(
                            (license_dir / "go_standard_library.txt").read_bytes()
                        ).hexdigest(),
                        "go_modules": modules,
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            build_env = os.environ.copy()
            build_env["EMBEDDED_NATS_TARGET"] = target
            subprocess.run(
                [sys.executable, "-m", "build", "--wheel", "--no-isolation", "--outdir", str(args.output.resolve())],
                cwd=stage,
                env=build_env,
                check=True,
            )
            wheels = list(args.output.glob(f"embedded_nats-*-{platform}.whl"))
            if len(wheels) != 1:
                raise RuntimeError(f"Expected one {platform} wheel, found {len(wheels)}")
            with zipfile.ZipFile(wheels[0]) as archive:
                embedded = [name for name in archive.namelist() if "/bin/nats-server" in name]
                if len(embedded) != 1:
                    raise RuntimeError(f"Wheel {wheels[0]} has {len(embedded)} server binaries")
            print(f"Built {wheels[0].name}: {digest}", flush=True)


if __name__ == "__main__":
    main()
