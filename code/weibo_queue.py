"""Persistent row provenance and numeric-post-ID queue for local collection."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import sqlite3
from urllib.parse import urlsplit, parse_qs
from datetime import datetime, timezone

BASE62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def canonical_identity(url):
    from weibo_backend import post_token
    raw = str(url or "").strip()
    parsed = urlsplit(raw)
    if parsed.username or parsed.password:
        raise ValueError("unsupported_url")
    token = post_token(raw)
    if token.isdigit():
        post_id = str(int(token))
    else:
        pieces, rest = [], token
        while rest:
            group, rest = rest[-4:], rest[:-4]
            n = 0
            for char in group:
                n = n * 62 + BASE62.index(char)
            pieces.append(str(n).zfill(7) if rest else str(n))
        post_id = str(int("".join(reversed(pieces))))
    host = (parsed.hostname or "").lower().removeprefix("www.")
    path = parsed.path.rstrip("/")
    query = f"?id={post_id}" if path.endswith(("show", "show.json")) or not path else ""
    return post_id, f"https://{host}{path}{query}"

class Queue:
    def __init__(self, path):
        self.path = Path(path)
        self.db = sqlite3.connect(self.path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=30000")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS assets(asset_key TEXT PRIMARY KEY,result_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS posts(
          post_id TEXT PRIMARY KEY,ordinal INTEGER NOT NULL UNIQUE,source_url TEXT NOT NULL,
          status TEXT NOT NULL,result_json TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,
          updated_at TEXT,last_error TEXT,last_attempt_json TEXT);
        CREATE TABLE IF NOT EXISTS sources(
          source_id INTEGER PRIMARY KEY,workbook TEXT NOT NULL,sheet TEXT NOT NULL,
          excel_row INTEGER NOT NULL,post_id TEXT NOT NULL,input_error TEXT,row_json TEXT NOT NULL,
          UNIQUE(workbook,sheet,excel_row),FOREIGN KEY(post_id) REFERENCES posts(post_id));
        CREATE INDEX IF NOT EXISTS sources_post ON sources(post_id,source_id);
        CREATE INDEX IF NOT EXISTS posts_status ON posts(status,ordinal);
        CREATE TABLE IF NOT EXISTS ingestion(
          workbook TEXT PRIMARY KEY,size INTEGER NOT NULL,mtime_ns INTEGER NOT NULL,
          last_sheet TEXT,last_excel_row INTEGER,completed INTEGER NOT NULL DEFAULT 0,
          rows_seen INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS request_history(
          attempt_id INTEGER PRIMARY KEY,post_id TEXT NOT NULL,attempted_at TEXT NOT NULL,
          status TEXT,error TEXT,FOREIGN KEY(post_id) REFERENCES posts(post_id));
        """)
        self.db.commit()
        self.next_ordinal = self.db.execute("SELECT COALESCE(MAX(ordinal),0)+1 FROM posts").fetchone()[0]

    def add_source(self, row):
        raw = str(row.get("原文/评论链接") or row.get("source_url") or "").strip()
        error = None
        try:
            post_id, url = canonical_identity(raw)
        except (ValueError, OverflowError):
            error = "unsupported_article" if "/ttarticle/" in raw else "unsupported_url"
            post_id = error + ":" + hashlib.sha256(raw.encode("utf-8")).hexdigest()
            url = raw
        record = {"queue_id": post_id, "source_url": url, "post_id": None,
                  "status": error or "pending", "media": [], "locations": [], "error": error}
        inserted = self.db.execute(
            "INSERT OR IGNORE INTO posts(post_id,ordinal,source_url,status,result_json) VALUES(?,?,?,?,?)",
            (post_id, self.next_ordinal, url, error or "pending", json.dumps(record, ensure_ascii=False)))
        if inserted.rowcount:
            self.next_ordinal += 1
        enriched = {**row, "_queue_id": post_id}
        cursor = self.db.execute(
            "INSERT OR IGNORE INTO sources(workbook,sheet,excel_row,post_id,input_error,row_json) VALUES(?,?,?,?,?,?)",
            (str(row["_source_filepath"]), row["_sheet"], row["_excel_row"], post_id, error,
             json.dumps(enriched, ensure_ascii=False)))
        return bool(cursor.rowcount)

    def save_result(self, queue_id, record, *, attempted=False, attempt_result=None):
        previous = self.db.execute("SELECT result_json,status,source_url,ordinal,last_attempt_json,last_error FROM posts WHERE post_id=?", (queue_id,)).fetchone()
        if previous is None:
            raise KeyError(queue_id)
        latest = attempt_result or record
        # A temporary failed refresh must not erase the last accessible post.
        old = json.loads(previous["result_json"])
        if attempted and old.get("status") == "success" and record.get("status") != "success":
            record = {**old, "last_attempt": {"status": latest.get("status"), "error": latest.get("error"), "at": utc_now()}}
        record = {**record, "queue_id": queue_id, "source_url": previous["source_url"],
                  "ordinal": previous["ordinal"], "retrieved_at": record.get("retrieved_at") or utc_now()}
        self.db.execute(
            "UPDATE posts SET status=?,result_json=?,attempts=attempts+?,updated_at=?,last_error=?,last_attempt_json=? WHERE post_id=?",
            (record.get("status", "unknown"), json.dumps(record, ensure_ascii=False), int(attempted), utc_now(),
             latest.get("error") if attempted else previous["last_error"],
             json.dumps({"status": latest.get("status"), "error": latest.get("error"), "at": utc_now()}) if attempted else previous["last_attempt_json"],
             queue_id))
        if attempted:
            self.db.execute("INSERT INTO request_history(post_id,attempted_at,status,error) VALUES(?,?,?,?)",
                            (queue_id, utc_now(), latest.get("status"), latest.get("error")))
        self.db.commit()
        return record

    def records(self):
        for r in self.db.execute("SELECT post_id,ordinal,source_url,status,result_json,last_attempt_json FROM posts ORDER BY ordinal"):
            record = json.loads(r["result_json"])
            record.update(queue_id=r["post_id"], ordinal=r["ordinal"], source_url=r["source_url"], status=r["status"])
            if r["last_attempt_json"]:
                record["last_attempt"] = json.loads(r["last_attempt_json"])
            first = self.db.execute("SELECT row_json FROM sources WHERE post_id=? ORDER BY source_id LIMIT 1",(r["post_id"],)).fetchone()
            record["source_refs"] = [json.loads(first[0])] if first else []
            yield record

    def source_rows(self):
        for r in self.db.execute("SELECT row_json FROM sources ORDER BY source_id"):
            yield json.loads(r[0])

    def sources_for_record(self, record):
        key = record.get("queue_id") or record.get("post_id")
        for r in self.db.execute("SELECT row_json FROM sources WHERE post_id=? ORDER BY source_id", (key,)):
            yield json.loads(r[0])

    def stats(self):
        return {"source_rows": self.db.execute("SELECT COUNT(*) FROM sources").fetchone()[0],
                "unique_queue_entries": self.db.execute("SELECT COUNT(*) FROM posts").fetchone()[0],
                "post_statuses": dict(self.db.execute("SELECT status,COUNT(*) FROM posts GROUP BY status").fetchall()),
                "metadata_requests_total": self.db.execute("SELECT COUNT(*) FROM request_history").fetchone()[0]}

    def close(self):
        self.db.commit()
        self.db.close()

