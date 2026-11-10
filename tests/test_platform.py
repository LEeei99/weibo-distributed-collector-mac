"""Offline OS-boundary tests. No requests to Weibo or browser launches."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import plistlib
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "code"))
sys.path.insert(0, str(ROOT / "tools"))
import collect
import worker
import weibo_platform as platform


PROBE = """
from pathlib import Path
import os
import sys
sys.path.insert(0, str(Path(sys.argv[1]) / "code"))
sys.path.insert(0, str(Path(sys.argv[1]) / "tools"))
from weibo_platform import file_lock
from collect import run_lock
from worker import file_lock as worker_lock
root = Path(sys.argv[2])
kind = sys.argv[3]
lock = run_lock(root) if kind == "collector" else (
    worker_lock(root / "collector.lock") if kind == "worker"
    else file_lock(root / "collector.lock"))
try:
    with lock:
        if kind == "crash":
            os._exit(17)
except RuntimeError:
    sys.exit(23)
"""


class PlatformTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def probe(self, kind):
        return subprocess.run(
            [sys.executable, "-B", "-c", PROBE, str(ROOT), str(self.root), kind],
            cwd=ROOT, capture_output=True, text=True, timeout=15,
        )

    def test_collector_worker_and_exporter_share_one_os_lock(self):
        holders = {
            "collector": lambda: collect.run_lock(self.root),
            "worker": lambda: worker.file_lock(self.root / "collector.lock"),
            "exporter": lambda: platform.file_lock(self.root / "collector.lock"),
        }
        for holder, acquire in holders.items():
            with acquire():
                for probe in ("collector", "worker", "exporter"):
                    with self.subTest(holder=holder, probe=probe):
                        result = self.probe(probe)
                        self.assertEqual(result.returncode, 23, result.stderr)
            result = self.probe("collector")
            self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((self.root / "collector.lock").exists())

    def test_process_exit_releases_lock_without_deleting_lock_file(self):
        result = self.probe("crash")
        self.assertEqual(result.returncode, 17, result.stderr)
        with collect.run_lock(self.root):
            self.assertTrue((self.root / "collector.lock").is_file())

    def test_known_sync_paths_are_rejected_by_every_entry_point(self):
        for parts in (
            ("Library", "CloudStorage", "GoogleDrive-user", "My Drive", "worker"),
            ("Library", "Mobile Documents", "com~apple~CloudDocs", "worker"),
            ("iCloud Drive", "worker"),
            ("OneDrive - University", "worker"),
            ("GoogleDrive-user", "worker"),
            ("Dropbox", "worker"),
        ):
            for check in (platform.local_directory, worker.local_directory, collect.local_path):
                with self.subTest(parts=parts, check=check.__name__):
                    with self.assertRaisesRegex(ValueError, "cloud-synchronized"):
                        check(self.root.joinpath(*parts))
        self.assertEqual(platform.local_directory(self.root / "local-work"),
                         (self.root / "local-work").resolve())

    def test_symlink_into_sync_folder_is_rejected(self):
        synced = self.root / "Library" / "CloudStorage" / "provider"
        synced.mkdir(parents=True)
        link = self.root / "shortcut"
        try:
            link.symlink_to(synced, target_is_directory=True)
        except (OSError, NotImplementedError):
            self.skipTest("Symlinks unavailable")
        with self.assertRaisesRegex(ValueError, "cloud-synchronized"):
            platform.local_directory(link / "worker")

    def mac_identity(self, value):
        return subprocess.CompletedProcess(
            args=[], returncode=0, stdout=plistlib.dumps([{"IOPlatformUUID": value}])
        )

    def test_mac_identity_is_hashed_stable_and_distinguishes_hosts(self):
        first_uuid = "01234567-89AB-CDEF-0123-456789ABCDEF"
        second_uuid = "12345678-9ABC-DEF0-1234-56789ABCDEF0"
        with patch.object(platform.sys, "platform", "darwin"), patch.object(
            platform.subprocess, "run",
            side_effect=[self.mac_identity(first_uuid), self.mac_identity(first_uuid),
                         self.mac_identity(second_uuid)],
        ) as run, patch.object(platform.socket, "gethostname", side_effect=AssertionError(
                "Mac binding must not depend on the hostname")):
            first = platform.machine_fingerprint()
            self.assertEqual(first, platform.machine_fingerprint())
            self.assertNotEqual(first, platform.machine_fingerprint())
        self.assertEqual(first, hashlib.sha256(
            ("darwin:" + first_uuid.lower()).encode("utf-8")).hexdigest())
        self.assertNotIn(first_uuid, first)
        args, kwargs = run.call_args
        self.assertEqual(args[0], ["/usr/sbin/ioreg", "-rd1", "-c",
                                  "IOPlatformExpertDevice", "-a"])
        self.assertTrue(kwargs["check"])
        self.assertEqual(kwargs["timeout"], 5)
        self.assertNotIn("shell", kwargs)

    def test_mac_identity_failure_never_falls_back_to_hostname(self):
        bad_results = [
            subprocess.CompletedProcess([], 0, stdout=b"not a plist"),
            subprocess.CompletedProcess([], 0, stdout=plistlib.dumps([])),
            self.mac_identity("not-a-uuid"),
            self.mac_identity("00000000-0000-0000-0000-000000000000"),
        ]
        for result in bad_results:
            with self.subTest(result=result.stdout), patch.object(
                platform.sys, "platform", "darwin"
            ), patch.object(platform.subprocess, "run", return_value=result), patch.object(
                platform.socket, "gethostname", side_effect=AssertionError("No fallback")
            ):
                with self.assertRaisesRegex(RuntimeError, "stable Mac identity"):
                    platform.machine_fingerprint()
        with patch.object(platform.sys, "platform", "darwin"), patch.object(
            platform.subprocess, "run", side_effect=subprocess.TimeoutExpired("ioreg", 5)
        ):
            with self.assertRaisesRegex(RuntimeError, "stable Mac identity"):
                platform.machine_fingerprint()

    def test_worker_root_cannot_move_to_another_mac_identity(self):
        worker_root = self.root / "worker-root"
        worker_root.mkdir()
        with patch.object(worker, "machine_fingerprint", return_value="a" * 64):
            first = worker.own_host(worker_root)
            self.assertEqual(first, worker.own_host(worker_root))
        with patch.object(worker, "machine_fingerprint", return_value="b" * 64):
            with self.assertRaisesRegex(ValueError, "another machine"):
                worker.own_host(worker_root)
        saved = json.loads((worker_root / "host_identity.json").read_text())
        self.assertEqual(saved["machine_fingerprint"], "a" * 64)
        self.assertEqual(saved["host_id"], first)


if __name__ == "__main__":
    unittest.main(verbosity=2)
