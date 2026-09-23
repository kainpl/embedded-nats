"""A kernel-held lifetime fence inherited by the actual broker.

Unlike a PID or listener probe, reacquiring the SAME local file proves that
no process retains its previous open description. Never unlink this file and
never LOCK_UN a duplicated POSIX descriptor: close only our own reference.
"""

from __future__ import annotations

import errno
import os
import stat
import subprocess
from contextlib import contextmanager
from pathlib import Path


class LeaseHeld(OSError):
    pass


class LifetimeLease:
    def __init__(self, path: Path):
        self.path = path
        self.fd: int | None = None
        self.identity: list[int] | None = None

    def acquire(self, *, create: bool = False) -> None:
        if self.fd is not None:
            raise RuntimeError("Lease is already acquired")
        if self.path.is_symlink() or self.path.is_junction():
            raise OSError("Lease must be a regular local file")
        if os.name == "nt":
            import ctypes
            import msvcrt
            from ctypes import wintypes

            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            create_file = kernel.CreateFileW
            create_file.argtypes = [
                wintypes.LPCWSTR,
                wintypes.DWORD,
                wintypes.DWORD,
                ctypes.c_void_p,
                wintypes.DWORD,
                wintypes.DWORD,
                wintypes.HANDLE,
            ]
            create_file.restype = wintypes.HANDLE
            # Share mode zero, not LockFileEx (byte locks are process-owned).
            handle = create_file(str(self.path), 0xC0000000, 0, None, 4 if create else 3, 0x00200080, None)
            if handle == wintypes.HANDLE(-1).value:
                error = ctypes.get_last_error()
                if error == 32:
                    raise LeaseHeld("Broker lifetime lease is held")
                raise ctypes.WinError(error)
            try:
                fd = msvcrt.open_osfhandle(handle, os.O_RDWR)
            except BaseException:
                kernel.CloseHandle.argtypes = [wintypes.HANDLE]
                kernel.CloseHandle(handle)
                raise
        else:
            import fcntl

            fd = os.open(self.path, os.O_RDWR | os.O_NOFOLLOW | (os.O_CREAT if create else 0), 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                os.close(fd)
                if exc.errno in (errno.EACCES, errno.EAGAIN):
                    raise LeaseHeld("Broker lifetime lease is held") from exc
                raise
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or not info.st_ino:
                raise OSError("Lease identity is not a unique regular file")
            self.identity = [info.st_dev, info.st_ino]
            os.set_inheritable(fd, False)
            self.fd = fd
        except BaseException:
            os.close(fd)
            raise

    @contextmanager
    def inheritance(self):
        """Use only around Popen; other children must not inherit the fence."""
        assert self.fd is not None
        if os.name == "nt":
            import msvcrt

            handle = msvcrt.get_osfhandle(self.fd)
            info = subprocess.STARTUPINFO()
            info.lpAttributeList = {"handle_list": [handle]}
            os.set_handle_inheritable(handle, True)
            try:
                yield {"startupinfo": info, "close_fds": True}
            finally:
                os.set_handle_inheritable(handle, False)
        else:
            yield {"pass_fds": (self.fd,), "close_fds": True}

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
