"""The wheel contains a native executable, but no Python extension."""

import os
from pathlib import Path

from setuptools import Distribution, setup
from setuptools.command.bdist_wheel import bdist_wheel as _bdist_wheel

TARGET_TAGS = {
    "linux_amd64": "manylinux_2_17_x86_64",
    "linux_arm64": "manylinux_2_17_aarch64",
    "linux_armv7": "manylinux_2_17_armv7l",
    "darwin_amd64": "macosx_12_0_x86_64",
    "darwin_arm64": "macosx_12_0_arm64",
    "windows_amd64": "win_amd64",
}


class BinaryDistribution(Distribution):
    def has_ext_modules(self) -> bool:
        return True


class PlatformWheel(_bdist_wheel):
    def finalize_options(self) -> None:
        super().finalize_options()
        self.root_is_pure = False

    def get_tag(self) -> tuple[str, str, str]:
        target = os.environ.get("EMBEDDED_NATS_TARGET", "")
        if target not in TARGET_TAGS:
            raise RuntimeError("Set EMBEDDED_NATS_TARGET to one of: " + ", ".join(TARGET_TAGS))
        binary = "nats-server.exe" if target.startswith("windows_") else "nats-server"
        if not (Path(__file__).parent / "src" / "embedded_nats" / "bin" / binary).is_file():
            raise RuntimeError(f"Missing staged binary for {target}")
        return "py3", "none", TARGET_TAGS[target]


setup(distclass=BinaryDistribution, cmdclass={"bdist_wheel": PlatformWheel})
