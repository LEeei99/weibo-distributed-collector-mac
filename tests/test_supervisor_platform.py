"""Offline tests for ownership-scoped browser supervision; no network or Edge."""
from functools import partial
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "code"))
import weibo_supervisor as supervisor


class ImmediateBackend:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def fetch_post(self, url):
        return {"status": "success", "source_url": url, "post_id": "offline",
                "media": [], "locations": []}


class HangingDescendantBackend:
    def __init__(self, marker):
        self.marker = marker

    def __enter__(self):
        program = ("import os,pathlib,signal,time;"
                   "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
                   "pathlib.Path(" + repr(self.marker) + ").write_text(str(os.getpid()));"
                   "time.sleep(60)")
        self.child = subprocess.Popen([sys.executable, "-c", program],
                                      stdout=subprocess.DEVNULL,
                                      stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 5
        while not Path(self.marker).exists():
            if time.monotonic() >= deadline:
                raise RuntimeError("fixture did not start")
            time.sleep(.01)
        return self

    def __exit__(self, *args):
        pass

    def fetch_post(self, url):
        time.sleep(60)


def stalled_start(*args):
    time.sleep(60)


def pid_running(pid):
    result = subprocess.run(["ps", "-p", str(pid), "-o", "stat="],
                            capture_output=True, text=True, check=False)
    state = result.stdout.strip()
    return bool(state) and not state.startswith("Z")


class SupervisionTests(unittest.TestCase):
    def test_offline_spawn_roundtrip_and_close(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(supervisor, "_TERMINATE_GRACE", .15):
            backend = supervisor.SupervisedBrowserBackend(
                work=folder, backend_factory=ImmediateBackend, startup_timeout=10)
            try:
                result = backend.fetch_post("https://example.invalid/offline")
                self.assertEqual(result["status"], "success")
                self.assertTrue(supervisor._running(backend._process))
            finally:
                backend.close()
            self.assertIsNone(backend._process)
            self.assertIsNone(backend._connection)

    @unittest.skipIf(os.name == "nt", "POSIX sessions only")
    def test_worker_owns_session_separate_from_parent(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(supervisor, "_TERMINATE_GRACE", .15):
            backend = supervisor.SupervisedBrowserBackend(
                work=folder, backend_factory=ImmediateBackend, startup_timeout=10)
            try:
                backend._start()
                self.assertEqual(backend._pgid, backend._process.pid)
                self.assertEqual(os.getpgid(backend._process.pid), backend._process.pid)
                self.assertNotEqual(backend._pgid, os.getpgrp())
            finally:
                backend.close()

    @unittest.skipIf(os.name == "nt", "POSIX sessions only")
    def test_timeout_kills_stubborn_descendant_and_preserves_unrelated_process(self):
        unrelated = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(60)"])
        child_pid = None
        try:
            with tempfile.TemporaryDirectory() as folder, patch.object(supervisor, "_TERMINATE_GRACE", .2):
                marker = str(Path(folder) / "descendant.pid")
                backend = supervisor.SupervisedBrowserBackend(
                    work=folder, backend_factory=partial(HangingDescendantBackend, marker),
                    wall_timeout=.2, startup_timeout=10)
                try:
                    result = backend.fetch_post("https://example.invalid/offline")
                    child_pid = int(Path(marker).read_text())
                    self.assertEqual(result["status"], "timeout")
                    deadline = time.monotonic() + 3
                    while pid_running(child_pid) and time.monotonic() < deadline:
                        time.sleep(.02)
                    self.assertFalse(pid_running(child_pid), "browser descendant remains running")
                    self.assertIsNone(unrelated.poll(), "unrelated process was terminated")
                finally:
                    backend.close()
        finally:
            if child_pid and pid_running(child_pid):
                os.kill(child_pid, signal.SIGKILL)
            unrelated.terminate()
            unrelated.wait(timeout=5)

    @unittest.skipIf(os.name == "nt", "POSIX startup race")
    def test_startup_timeout_before_handshake_does_not_signal_parent_group(self):
        unrelated = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(60)"])
        try:
            with tempfile.TemporaryDirectory() as folder, patch.object(supervisor, "_TERMINATE_GRACE", .15), patch.object(supervisor, "_worker", stalled_start):
                backend = supervisor.SupervisedBrowserBackend(
                    work=folder, backend_factory=ImmediateBackend, startup_timeout=.1)
                try:
                    with self.assertRaisesRegex(RuntimeError, "browser_startup_timeout"):
                        backend._start()
                    self.assertIsNone(backend._process)
                    self.assertIsNone(unrelated.poll())
                finally:
                    backend.close()
        finally:
            unrelated.terminate()
            unrelated.wait(timeout=5)

    @unittest.skipIf(os.name == "nt", "POSIX process group ownership")
    def test_never_signals_collectors_own_group(self):
        fake = type("Owned", (), {"pid": os.getpid()})()
        with patch.object(supervisor.os, "killpg") as kill_group:
            self.assertFalse(supervisor._signal_owned_group(fake, os.getpgrp(), signal.SIGTERM))
            kill_group.assert_not_called()

    def test_cleanup_wait_survives_second_keyboard_interrupt(self):
        fake = type("Process", (), {"sentinel": 123})()
        with patch.object(supervisor, "wait", side_effect=[KeyboardInterrupt(), [123]]):
            self.assertTrue(supervisor._wait_uninterrupted(fake, 1))


if __name__ == "__main__":
    unittest.main()
