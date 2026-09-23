"""Test a downloaded wheel from a clean interpreter, away from the source tree."""

from __future__ import annotations

import importlib.metadata
import os
import subprocess
import sys
from pathlib import Path

from build_wheels import TARGETS


def main() -> None:
    target = sys.argv[1]
    if target not in TARGETS:
        raise SystemExit(f"Unknown target {target}")
    platform = TARGETS[target][3]
    matches = list(Path("dist").glob(f"embedded_nats-*-py3-none-{platform}.whl"))
    if len(matches) != 1:
        raise SystemExit(f"Expected one wheel for {target}, found {len(matches)}")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--force-reinstall",
            str(matches[0]),
            "pytest>=8,<10",
            "pytest-timeout>=2,<3",
        ],
        check=True,
    )
    import embedded_nats

    location = Path(embedded_nats.__file__).resolve()
    if "site-packages" not in location.parts:
        raise SystemExit(f"Imported source package instead of installed wheel: {location}")
    print(f"Installed {importlib.metadata.version('embedded-nats')} at {location}", flush=True)
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env["EMBEDDED_NATS_EXPECT_INSTALLED"] = "1"
    subprocess.run(
        [sys.executable, "-m", "pytest", "tests", "-q", "--timeout=120"],
        check=True,
        env=env,
    )


if __name__ == "__main__":
    main()
