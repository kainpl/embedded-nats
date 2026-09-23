"""Stage only verified wheels for the PyPI publishing action."""

from __future__ import annotations

import shutil
from pathlib import Path

from verify_wheels import main as verify


def main() -> None:
    verify()
    destination = Path("pypi-dist")
    if destination.exists():
        raise SystemExit("pypi-dist already exists; use a clean job")
    destination.mkdir()
    for wheel in Path("dist").glob("*.whl"):
        shutil.copy2(wheel, destination / wheel.name)


if __name__ == "__main__":
    main()
