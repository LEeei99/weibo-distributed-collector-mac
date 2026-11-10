"""Exercise CLI forwarding with a fake Python; never starts a collector."""
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ("Setup-Mac.sh", "Run-Worker.sh", "Monitor-Worker.sh", "Pack-Return.sh")


@unittest.skipIf(os.name == "nt" or not shutil.which("bash"), "POSIX shell wrappers")
class ScriptTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.repo = self.root / "repo with spaces"
        self.repo.mkdir()
        self.cwd = self.root / "caller with spaces"
        self.cwd.mkdir()
        for name in SCRIPTS:
            shutil.copyfile(ROOT / name, self.repo / name)
        self.capture = self.root / "arguments.json"
        probe = self.root / "capture.py"
        probe.write_text(
            "import json,os,sys\n"
            "from pathlib import Path\n"
            "Path(os.environ['WEIBO_TEST_CAPTURE']).write_text("
            "json.dumps({'args':sys.argv[1:],'cwd':os.getcwd()}))\n",
            encoding="utf-8")
        python = self.repo / ".venv" / "bin" / "python"
        python.parent.mkdir(parents=True)
        python.write_text("#!/bin/sh\nexec " + shlex.quote(sys.executable)
                          + " " + shlex.quote(str(probe)) + ' "$@"\n', encoding="utf-8")
        python.chmod(0o755)

    def tearDown(self):
        self.temp.cleanup()

    def invoke(self, script, *args):
        subprocess.run(["bash", str(self.repo / script), *args],
                       cwd=self.cwd,
                       env={**os.environ, "WEIBO_TEST_CAPTURE": str(self.capture)},
                       check=True, capture_output=True, text=True, timeout=15)
        result = json.loads(self.capture.read_text())
        self.assertEqual(Path(result["cwd"]), self.cwd)
        return result["args"]

    def test_shell_syntax(self):
        for script in SCRIPTS:
            subprocess.run(["bash", "-n", str(self.repo / script)],
                           check=True, capture_output=True, timeout=15)

    def test_default_run_is_bounded_and_preserves_quoted_paths(self):
        args = self.invoke("Run-Worker.sh", "run", "--package", "inbox with spaces/P.jsonl",
                           "--worker-id", "MAC-A", "--worker-root", "local work")
        self.assertEqual(args[:4], ["-u", "-B", str(self.repo / "tools" / "worker.py"), "run"])
        self.assertEqual(args[4:10], ["--max-posts", "2", "--max-batches", "1", "--request-limit", "2"])
        self.assertIn("inbox with spaces/P.jsonl", args)
        self.assertIn("local work", args)

    def test_full_run_requires_explicit_flag_and_overrides_are_forwarded(self):
        args = self.invoke("Run-Worker.sh", "run", "--until-finished", "--package", "P.jsonl")
        self.assertNotIn("--request-limit", args)
        args = self.invoke("Run-Worker.sh", "run", "--request-limit", "100", "--max-posts", "100")
        self.assertEqual(args[-4:], ["--request-limit", "100", "--max-posts", "100"])

    def test_prepare_monitor_and_pack_forward_without_collection(self):
        args = self.invoke("Run-Worker.sh", "prepare", "--package", "P.jsonl")
        self.assertEqual(args[3], "prepare")
        self.assertNotIn("--request-limit", args)
        args = self.invoke("Monitor-Worker.sh", "--out", "a b", "--once")
        self.assertEqual(args[3:], ["monitor", "--out", "a b", "--once"])
        args = self.invoke("Pack-Return.sh", "--package", "P.jsonl", "--bundle", "a b")
        self.assertEqual(args[:4], ["-u", "-B", str(self.repo / "tools" / "returns.py"), "pack"])


if __name__ == "__main__":
    unittest.main()
