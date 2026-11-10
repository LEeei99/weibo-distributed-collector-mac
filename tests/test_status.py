"""Offline status tests: merged progress is distinct from local/unreturned work."""
import hashlib
from contextlib import closing
import importlib.util
from pathlib import Path
import sqlite3
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("coordinator_status", Path(__file__).resolve().parents[1] / "tools/status.py")
status = importlib.util.module_from_spec(spec)
spec.loader.exec_module(status)


class StatusTest(unittest.TestCase):
    def fixture(self, root, mode="complete"):
        data = root / "data"
        data.mkdir()
        central = data / "central.sqlite"
        with closing(sqlite3.connect(central)) as db, db:
            db.executescript("""
                CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT);
                CREATE TABLE packages(package_id TEXT PRIMARY KEY,snapshot_id TEXT,count INTEGER,sha256 TEXT);
                CREATE TABLE assignments(post_id TEXT PRIMARY KEY,package_id TEXT);
            """)
            db.execute("INSERT INTO meta VALUES('snapshot_id','snapshot1')")
            db.executemany("INSERT INTO packages VALUES(?,?,?,?)",
                           [("B01-S01", "snapshot1", 2, "a" * 64), ("B02-S01", "snapshot1", 1, "b" * 64)])
            db.executemany("INSERT INTO assignments VALUES(?,?)",
                           [("1", "B01-S01"), ("2", "B01-S01"), ("3", "B02-S01")])
        registry = data / "dispatch.sqlite"
        with closing(sqlite3.connect(registry)) as db, db:
            db.execute("CREATE TABLE assignments(snapshot_id TEXT,package_id TEXT,sha256 TEXT,worker_id TEXT,assignment_id TEXT)")
            db.execute("INSERT INTO assignments VALUES(?,?,?,?,?)",
                       ("snapshot1", "B01-S01", "a" * 64, "laptop1", "aid1"))
        merge = root / "merge"
        merge.mkdir()
        with closing(sqlite3.connect(merge / "state.sqlite")) as db, db:
            db.executescript("""
                CREATE TABLE distribution_base(snapshot_id TEXT PRIMARY KEY,mode TEXT);
                CREATE TABLE returned_posts(post_id TEXT PRIMARY KEY,assignment_id TEXT);
                CREATE TABLE posts(post_id TEXT PRIMARY KEY,status TEXT);
                CREATE TABLE returned_packages(payload_sha TEXT,assignment_id TEXT,imported_at TEXT);
            """)
            db.execute("INSERT INTO distribution_base VALUES(?,?)", ("snapshot1", mode))
            # Baseline success is deliberately excluded from returned_posts.
            db.executemany("INSERT INTO posts VALUES(?,?)", [("1", "success"), ("999", "success")])
            db.execute("INSERT INTO returned_posts VALUES('1','aid1')")
            db.execute("INSERT INTO returned_packages VALUES('payload1','aid1','2026-11-04T00:00:00+00:00')")
        workers = root / "workers"
        work = workers / "B01-S01"
        work.mkdir(parents=True)
        with closing(sqlite3.connect(work / "state.sqlite")) as db, db:
            db.executescript("""
                CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT);
                CREATE TABLE posts(post_id TEXT PRIMARY KEY,status TEXT);
                CREATE TABLE request_history(attempt_id INTEGER PRIMARY KEY,attempted_at TEXT,status TEXT);
            """)
            meta = {
                "distributed_snapshot_id": "snapshot1", "distributed_package_id": "B01-S01",
                "distributed_package_sha256": "a" * 64, "distributed_package_count": "2",
                "distributed_import_complete": "1", "distributed_worker_id": "laptop1",
                "distributed_assignment_id": "aid1",
            }
            db.executemany("INSERT INTO meta VALUES(?,?)", meta.items())
            db.executemany("INSERT INTO posts VALUES(?,?)", [("1", "success"), ("2", "unavailable")])
            db.execute("INSERT INTO request_history VALUES(2,'2026-11-04T01:00:00+00:00','unavailable')")
        return data, registry, merge, workers

    def test_merged_unique_and_local_unreturned_are_separate_and_readonly(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            data, registry, merge, workers = self.fixture(root)
            files = list(root.rglob("*.sqlite"))
            before = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
            report = status.summarize(data, merge_dir=merge, worker_root=workers)
            self.assertEqual(report["production_merged_unique_posts"], 1)
            self.assertEqual(report["merged_unique_posts_selected"], 1)
            self.assertEqual(report["not_in_selected_merge"], 2)
            self.assertEqual(report["local_processed_unique_posts"], 2)
            self.assertEqual(report["local_processed_not_in_selected_merge"], 1)
            self.assertEqual(report["packages"][0]["central_state"], "partial")
            self.assertEqual(report["packages"][1]["central_state"], "unassigned")
            self.assertEqual(report["big_packages"][0]["owners"], ["laptop1"])
            after = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
            self.assertEqual(before, after)

    def test_pilot_merge_never_counts_as_production(self):
        with tempfile.TemporaryDirectory() as td:
            data, registry, merge, workers = self.fixture(Path(td), mode="db_test_copy")
            report = status.summarize(data, merge_dir=merge)
            self.assertEqual(report["pilot_merged_unique_posts"], 1)
            self.assertEqual(report["production_merged_unique_posts"], 0)
            self.assertIn("试验", status.render(report))
            self.assertIn("远端", status.render(report))

    def test_no_merge_means_local_return_boundary_unknown(self):
        with tempfile.TemporaryDirectory() as td:
            data, registry, merge, workers = self.fixture(Path(td))
            report = status.summarize(data, worker_root=workers)
            self.assertEqual(report["local_processed_unique_posts"], 2)
            self.assertIsNone(report["local_processed_not_in_selected_merge"])
            self.assertEqual(report["packages"][0]["central_state"], "assigned_no_return")

    def test_wrong_merge_snapshot_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            data, registry, merge, workers = self.fixture(Path(td))
            with closing(sqlite3.connect(merge / "state.sqlite")) as db, db:
                db.execute("UPDATE distribution_base SET snapshot_id='different'")
            with self.assertRaisesRegex(ValueError, "another snapshot"):
                status.summarize(data, merge_dir=merge)

    def test_missing_files_are_never_created_and_registry_is_unknown(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            with self.assertRaises(ValueError):
                status.summarize(root / "missing")
            self.assertFalse((root / "missing").exists())
            data, registry, merge, workers = self.fixture(root)
            absent = root / "absent.sqlite"
            report = status.summarize(data, registry=absent)
            self.assertFalse(absent.exists())
            self.assertFalse(report["registry_available"])
            self.assertEqual(report["packages"][0]["central_state"], "dispatch_unknown")


if __name__ == "__main__":
    unittest.main()
