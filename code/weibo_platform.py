"""OS helpers shared by collectors and return/export tools.

Keep active SQLite databases and media on a local, non-synchronized disk.
Locks are advisory and require every writer/exporter to use this module.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import plistlib
import socket
import subprocess
import sys
import uuid


@contextmanager
def file_lock(path):
    """Acquire the same nonblocking OS lock in every entry point.

    The lock file is retained. Its existence is not evidence of a live holder;
    process exit releases the OS lock even if a checkpoint was interrupted.
    """
    path = Path(path)
    stream = path.open("a+b")
    try:
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise RuntimeError("Another collector, worker or exporter holds " + str(path)) from error
        try:
            yield
        finally:
            if os.name == "nt":
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
    finally:
        stream.close()


def local_directory(value):
    """Resolve symlinks and reject known cloud-sync paths.

    This conservative path check does not detect every network volume or
    custom synchronization setup. Select a local non-synchronized worker root.
    """
    path = Path(value).expanduser().resolve()
    if os.name == "nt" and (str(path).startswith("\\\\") or path.drive.upper() == "G:"):
        raise ValueError("Use a local disk outside synchronized folders and network shares")
    forbidden = {
        "onedrive", "google drive", "my drive", "dropbox", "icloud drive",
        "cloudstorage", "mobile documents", "com~apple~clouddocs",
    }
    for part in path.parts:
        name = part.casefold()
        if name in forbidden or name.startswith(("onedrive - ", "googledrive-", "dropbox (")):
            raise ValueError("Worker databases must be outside cloud-synchronized folders")
    return path


def machine_fingerprint():
    """Return a hashed machine identity; never save a raw Mac hardware UUID.

    macOS uses IOPlatformUUID rather than the mutable hostname. A failure to
    obtain it is fatal: silently falling back would weaken the host binding.
    Windows keeps the original MachineGuid/hostname scheme for compatibility.
    """
    if sys.platform == "darwin":
        try:
            result = subprocess.run(
                ["/usr/sbin/ioreg", "-rd1", "-c", "IOPlatformExpertDevice", "-a"],
                check=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=5,
            )
            devices = plistlib.loads(result.stdout)
            if not isinstance(devices, list) or len(devices) != 1:
                raise ValueError("Unexpected platform identity response")
            raw_uuid = devices[0].get("IOPlatformUUID")
            if not isinstance(raw_uuid, str):
                raise ValueError("Missing platform UUID")
            canonical_uuid = str(uuid.UUID(raw_uuid))
            if canonical_uuid == str(uuid.UUID(int=0)):
                raise ValueError("Invalid platform UUID")
        except (OSError, subprocess.SubprocessError, plistlib.InvalidFileException,
                ValueError, TypeError, AttributeError) as error:
            raise RuntimeError("Cannot determine a stable Mac identity; worker binding was not created") from error
        machine_key = "darwin:" + canonical_uuid
    else:
        machine_key = os.name + ":" + socket.gethostname().casefold()
        if os.name == "nt":
            import winreg
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Cryptography") as key:
                machine_key += ":" + str(winreg.QueryValueEx(key, "MachineGuid")[0])
    return hashlib.sha256(machine_key.encode("utf-8")).hexdigest()
