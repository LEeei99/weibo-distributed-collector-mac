"""Opt-in real Chromium smoke test using local HTML only, no Weibo requests."""
from functools import partial
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "code"))
import psutil
import weibo_supervisor as supervisor


class LocalBrowser:
    def __init__(self, marker):
        self.marker = marker

    def __enter__(self):
        from weibo_browser import BrowserBackend
        self.backend = BrowserBackend(channel="chromium", worker_mode=True).__enter__()
        page = self.backend.context.new_page()
        page.set_content("<title>Offline browser supervision</title><p>synthetic fixture</p>")
        assert page.title() == "Offline browser supervision"
        children = psutil.Process().children(recursive=True)
        Path(self.marker).write_text(json.dumps([
            [process.pid, process.create_time()] for process in children
        ]), encoding="utf-8")
        return self

    def fetch_post(self, url):
        # Simulate a stalled browser RPC; no network or page navigation.
        time.sleep(60)

    def __exit__(self, *args):
        # Deliberately broken cleanup to exercise the parent's ownership checks.
        pass


def same_process_alive(pid, created):
    try:
        process = psutil.Process(pid)
        return (process.create_time() == created and process.is_running()
                and process.status() != psutil.STATUS_ZOMBIE)
    except psutil.NoSuchProcess:
        return False


@unittest.skipUnless(os.environ.get("WEIBO_BROWSER_SMOKE") == "1", "opt-in browser installation")
class BrowserSmokeTests(unittest.TestCase):
    def test_local_chromium_timeout_leaves_no_owned_browser_processes(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(supervisor, "_TERMINATE_GRACE", .5):
            marker = Path(folder) / "owned-pids.json"
            backend = supervisor.SupervisedBrowserBackend(
                work=folder, backend_factory=partial(LocalBrowser, str(marker)),
                startup_timeout=40, wall_timeout=.3)
            identities = []
            try:
                result = backend.fetch_post("https://example.invalid/local-only")
                self.assertEqual(result["status"], "timeout")
                identities = json.loads(marker.read_text(encoding="utf-8"))
                self.assertTrue(identities, "fixture did not launch real browser processes")
                deadline = time.monotonic() + 5
                while any(same_process_alive(*item) for item in identities) and time.monotonic() < deadline:
                    time.sleep(.05)
                self.assertFalse([item for item in identities if same_process_alive(*item)],
                                 "owned browser process survived timeout cleanup")
            finally:
                backend.close()
                # Only identity-checked synthetic fixture descendants can be removed.
                for pid, created in identities:
                    if same_process_alive(pid, created):
                        try:
                            psutil.Process(pid).kill()
                        except psutil.NoSuchProcess:
                            pass


if __name__ == "__main__":
    unittest.main()
