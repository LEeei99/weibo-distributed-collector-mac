"""Integration checks for handoff, repeated returns, media integrity, and resume."""
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/"tools"),str(ROOT/"code")]
from assign import assign, digest, write_json
from returns import apply_return, init_central, pack
from weibo_queue import Queue
from collect import run_lock
from worker import file_lock


class ReturnTests(unittest.TestCase):
    def test_partial_resume_and_replay(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            base=Queue(root/"base.sqlite")
            records=[]
            for i in range(3):
                pid=str(5000000000000000+i)
                url="https://m.weibo.cn/detail/"+pid
                base.add_source({"source_url":url,"_source_filepath":"fixture","_sheet":"Sheet1","_excel_row":i+2})
                records.append({"post_id":pid,"ordinal":i+1,"source_url":url})
            base.save_result(records[2]["post_id"],{"status":"unavailable"},attempted=True)
            base.close()
            pkg=root/"B01-S01.jsonl"
            pkg.write_text("".join(json.dumps(r)+"\n" for r in records[:2]),encoding="utf-8")
            write_json(pkg.with_suffix(".manifest.json"),dict(snapshot_id="fixture",package_id=pkg.stem,count=2,sha256=digest(pkg)))
            reg=root/"registry.sqlite"
            handoff=assign(pkg,"HOST-A",reg,root/"pickup")
            with self.assertRaises(ValueError):
                assign(pkg,"HOST-B",reg,root/"pickup")
            claim=json.loads(handoff.with_suffix(".assignment.json").read_text())
            work=root/"work"
            work.mkdir()
            worker=Queue(work/"state.sqlite")
            for i,r in enumerate(records[:2]):
                worker.add_source({"source_url":r["source_url"],"_source_filepath":"worker","_sheet":"pkg","_excel_row":i+1})
            meta=dict(distributed_package_id=pkg.stem,distributed_package_sha256=claim["sha256"],distributed_package_count="2",
                      distributed_worker_id="HOST-A",distributed_snapshot_id="fixture",distributed_assignment_id=claim["assignment_id"])
            worker.db.executemany("INSERT INTO meta VALUES(?,?)",meta.items())
            worker.save_result(records[0]["post_id"],dict(status="session_required",media=[],locations=[]),attempted=True)
            worker.close()
            central=root/"joined"
            init_central(root/"base.sqlite",None,central,"fixture",True)
            for lock in (file_lock(work/"worker.lock"), run_lock(work)):
                with lock:
                    with self.assertRaises(RuntimeError):
                        pack(pkg,work,root/"blocked")
                self.assertFalse((root/"blocked").exists())
            partial=root/"first"
            pack(pkg,work,partial)
            self.assertEqual(apply_return(pkg,reg,partial,central)["new_posts"],1)
            worker=Queue(work/"state.sqlite")
            media=work/"media/test.jpg"
            media.parent.mkdir()
            media.write_bytes(b"\xff\xd8\xff"+b"fixture-test")
            checksum=digest(media)
            rec=dict(status="success",post_id=records[1]["post_id"],locations=[{"place":"test"}],
                     media=[dict(kind="image",media_id="test",url="https://example.invalid/test.jpg",
                     download=dict(file="media/test.jpg",bytes=media.stat().st_size,sha256=checksum,status="downloaded"))])
            worker.save_result(records[1]["post_id"],rec,attempted=True)
            worker.close()
            final=root/"second"
            pack(pkg,work,final)
            returned=next((final/"media").glob("*"))
            original=returned.read_bytes()
            returned.write_bytes(b"corrupted")
            with self.assertRaises(ValueError):
                apply_return(pkg,reg,final,central)
            returned.write_bytes(original)
            self.assertEqual(apply_return(pkg,reg,final,central)["new_attempts"],1)
            self.assertTrue(apply_return(pkg,reg,final,central)["replayed"])
            self.assertTrue(apply_return(pkg,reg,partial,central)["replayed"])
            with closing(sqlite3.connect(central/"state.sqlite")) as conn:
                self.assertEqual(conn.execute("SELECT count(*) FROM request_history").fetchone()[0],3)
                self.assertEqual(conn.execute("SELECT count(*) FROM sources").fetchone()[0],3)
                self.assertEqual(conn.execute("SELECT count(*) FROM returned_posts").fetchone()[0],2)
                self.assertEqual(conn.execute("SELECT count(*) FROM assets").fetchone()[0],1)
                success=json.loads(conn.execute("SELECT result_json FROM posts WHERE status='success'").fetchone()[0])
                self.assertEqual(digest(central/success["media"][0]["download"]["file"]),checksum)
            with closing(sqlite3.connect(root/"base.sqlite")) as conn:
                self.assertEqual(conn.execute("SELECT count(*) FROM posts WHERE status='pending'").fetchone()[0],2)


if __name__=="__main__":
    unittest.main()

