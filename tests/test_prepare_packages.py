"""Meaningful offline package tests; no network access or production database."""
from __future__ import annotations
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

MODULE = Path(__file__).resolve().parents[1] / "tools" / "prepare_packages.py"
spec = importlib.util.spec_from_file_location("prepare_packages", MODULE)
prep = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prep)


class PackagesTest(unittest.TestCase):
    def setUp(self):
        patch = mock.patch.object(prep, "emit", lambda *args, **kwargs: None)
        patch.start()
        self.addCleanup(patch.stop)

    def make_source(self, root):
        source = root / "source.sqlite"
        db = sqlite3.connect(source)
        db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE posts(post_id TEXT PRIMARY KEY,ordinal INTEGER UNIQUE,source_url TEXT,status TEXT,attempts INTEGER);
            CREATE TABLE sources(source_id INTEGER PRIMARY KEY,workbook TEXT,sheet TEXT,excel_row INTEGER,post_id TEXT,input_error TEXT,row_json TEXT);
            CREATE TABLE request_history(attempt_id INTEGER PRIMARY KEY,post_id TEXT,attempted_at TEXT,status TEXT,error TEXT);
        """)
        source_id = 0
        for ordinal in range(1, 404):
            pid = str(5000000000000000 + ordinal)
            status = "success" if ordinal <= 2 else "pending"
            db.execute("INSERT INTO posts VALUES(?,?,?,?,?)",
                       (pid, ordinal, "https://weibo.com/123456/" + str(ordinal), status, int(status != "pending")))
            if status == "success":
                db.execute("INSERT INTO request_history VALUES(?,?,?,?,?)",
                           (ordinal, pid, "2026-10-30T00:00:00+00:00", status, None))
            region = "京津冀" if ordinal % 2 else "粤港澳"
            date = "2025-06-15 12:00:00" if ordinal % 3 else "2025-07-01"
            refs = [(region, date)]
            if ordinal % 7 == 0:
                refs.append(("长三角", "2025-08-02"))
            if ordinal == 400:
                refs = [("unknown", None)]
            for region, date in refs:
                source_id += 1
                value = {"_region": region, "日期": date, "标题/微博内容": "PRIVATE ORIGINAL CONTENT"}
                db.execute("INSERT INTO sources VALUES(?,?,?,?,?,?,?)",
                           (source_id, "/private/" + region + "/source.xlsx", "sheet1", source_id + 1,
                            pid, None, json.dumps(value, ensure_ascii=False)))
        db.commit()
        return source, db

    def test_wal_snapshot_and_160_package_exact_coverage_balance_privacy(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source, source_db = self.make_source(root)
            out = root / "private"
            out.mkdir()
            wal_before = Path(str(source) + "-wal").read_bytes()
            source_before = source.read_bytes()
            snapshot, frozen = prep.freeze_source(source, out)
            self.assertEqual(source_before, source.read_bytes())
            self.assertEqual(wal_before, Path(str(source) + "-wal").read_bytes())
            self.assertEqual(snapshot["counts"]["post_statuses"]["pending"], 401)
            self.assertTrue(snapshot["source_files_unchanged"])
            ids = prep.package_ids(16, 10)
            central = prep.build_central(snapshot, frozen, out, ids)
            entries = prep.build_packages(snapshot, central, out, ids)
            report = prep.audit_packages(snapshot, central, out, ids, entries)
            self.assertEqual(report["status"], "passed")
            self.assertEqual(report["task_count"], 401)
            self.assertLessEqual(report["small_package_count_difference"], 1)
            self.assertLessEqual(report["max_small_package_stratum_difference"], 1)
            self.assertLessEqual(report["max_big_package_stratum_difference"], 1)
            self.assertEqual(report["unknown_month_posts"], 1)
            self.assertGreater(report["source_month_conflict_posts"], 0)
            self.assertEqual(report["excluded_nonpending_count"], 2)
            found = set()
            for entry in entries:
                content = (out / entry["relative_path"]).read_text("utf-8")
                self.assertNotIn("PRIVATE ORIGINAL", content)
                self.assertNotIn("source.xlsx", content)
                for line in content.splitlines():
                    row = json.loads(line)
                    self.assertEqual(set(row), prep.ALLOWED_FIELDS)
                    self.assertNotIn(row["post_id"], found)
                    self.assertGreater(row["ordinal"], 2)
                    found.add(row["post_id"])
            self.assertEqual(len(found), 401)
            self.assertFalse(Path(str(frozen) + "-wal").exists())
            first = out / entries[0]["relative_path"]
            first.write_bytes(first.read_bytes() + b"\\n")
            with self.assertRaisesRegex(ValueError, "checksum"):
                prep.audit_packages(snapshot, central, out, ids, entries)
            source_db.close()

    def test_schema_rejects_original_source_fields_and_bad_urls(self):
        base = {"post_id": "123", "ordinal": 1, "source_url": "https://weibo.com/1/abc"}
        prep.validate_task(base)
        for value in [{**base, "excel_row": 2},
                      {**base, "source_url": "https://example.com/abc"},
                      {**base, "ordinal": True},
                      {**base, "post_id": 123}]:
            with self.assertRaises(ValueError):
                prep.validate_task(value)

    def test_month_key_unknown_is_preserved(self):
        self.assertEqual(prep.month_key("2025/6/15"), "2025-06")
        self.assertEqual(prep.month_key(None), "unknown")
        self.assertEqual(prep.month_key("2025-19-01"), "unknown")


if __name__ == "__main__":
    unittest.main()
