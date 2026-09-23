"""Real inherited kernel fence, independent of PID and listener availability."""

import os
import subprocess
import time

import pytest

from embedded_nats import binary_path
from embedded_nats._lease import LeaseHeld, LifetimeLease


def test_actual_broker_retains_lease_after_parent_closes_its_copy(tmp_path):
    config = tmp_path / "nats.conf"
    config.write_text('listen: "127.0.0.1:-1"\n')
    path = tmp_path / "broker.lease"
    lease = LifetimeLease(path)
    lease.acquire(create=True)
    identity = lease.identity
    process = None
    try:
        with lease.inheritance() as kwargs:
            process = subprocess.Popen(
                [str(binary_path()), "-c", str(config)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                **kwargs,
            )
        lease.close()
        # Check both immediately after CreateProcess/exec and after Go startup.
        for _ in range(10):
            assert process.poll() is None
            with pytest.raises(LeaseHeld):
                LifetimeLease(path).acquire()
            time.sleep(0.05)
    finally:
        lease.close()
        if process:
            process.kill()
            process.wait(timeout=5)
    replacement = LifetimeLease(path)
    replacement.acquire()
    try:
        assert replacement.identity == identity
    finally:
        replacement.close()


def test_missing_or_linked_fence_never_means_dead(tmp_path):
    with pytest.raises(OSError):
        LifetimeLease(tmp_path / "missing").acquire()
    target = tmp_path / "original"
    target.touch()
    alias = tmp_path / "alias"
    os.link(target, alias)
    with pytest.raises(OSError, match="unique"):
        LifetimeLease(alias).acquire()
