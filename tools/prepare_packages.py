"""Freeze a read-only collection and create balanced, mutually exclusive task packages.

Packages contain exactly post_id, ordinal and source_url. Original source references
remain in the private frozen SQLite database and central.sqlite, never in packages.
This tool makes no network requests and never modifies the source database.
"""
from __future__ import annotations
import argparse
import collections
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
from urllib.parse import urlsplit

PACKAGE_SCHEMA = "weibo-package-v1"
ALLOWED_FIELDS = {"post_id", "ordinal", "source_url"}
REGIONS = ("JJJ", "GBA", "YRD", "UNK")
REGION_LABELS = {"JJJ": "京津冀", "GBA": "粤港澳", "YRD": "长三角", "UNK": "未知"}
REGION_NAMES = {"京津冀": "JJJ", "粤港澳": "GBA", "长三角": "YRD"}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def emit(event, **values):
    print(json.dumps({"event": event, "at": utc_now(), **values}, ensure_ascii=True), flush=True)


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for part in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            result.update(part)
    return result.hexdigest()


def write_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def readonly(path):
    db = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    return db


def file_identity(path):
    path = Path(path)
    if not path.exists():
        return None
    stat = path.stat()
    return {"bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def month_key(value):
    value = str(value or "").strip()
    match = re.match(r"^(\d{4})[-/](\d{1,2})(?:[-/ T]|$)", value)
    if match and 1 <= int(match[2]) <= 12:
        return f"{int(match[1]):04d}-{int(match[2]):02d}"
    return "unknown"


def region_key(value, workbook):
    if value in REGION_NAMES:
        return REGION_NAMES[value]
    for name, key in REGION_NAMES.items():
        if name in str(workbook):
            return key
    return "UNK"


def package_ids(big_count, small_count):
    # Interleave big packages so every region/month stratum is balanced at both
    # the big-package and small-package levels, not just across 160 small files.
    return [f"B{big:02d}-S{small:02d}"
            for small in range(1, small_count + 1)
            for big in range(1, big_count + 1)]


def validate_task(value):
    if set(value) != ALLOWED_FIELDS:
        raise ValueError("Package row fields differ from the three-field privacy schema")
    if not isinstance(value["post_id"], str) or not value["post_id"].isdigit():
        raise ValueError("A task post_id is not a numeric string")
    ordinal = value["ordinal"]
    if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 1:
        raise ValueError("A task ordinal is invalid")
    parsed = urlsplit(value["source_url"])
    if (parsed.scheme not in {"http", "https"} or parsed.username or parsed.password
            or (parsed.hostname or "").lower() not in
            {"weibo.com", "www.weibo.com", "weibo.cn", "www.weibo.cn", "m.weibo.cn"}):
        raise ValueError("A task source_url is not an allowed Weibo URL")


def freeze_source(source, out, resume=False):
    record_path = out / "snapshot.json"
    if record_path.exists():
        if not resume:
            raise FileExistsError("Private output already has a snapshot; use --resume")
        record = json.loads(record_path.read_text("utf-8"))
        backup = out / record["snapshot_relative_path"]
        if digest(backup) != record["sha256"]:
            raise ValueError("Frozen database checksum does not match snapshot.json")
        emit("snapshot_reused", snapshot_id=record["snapshot_id"])
        return record, backup
    source = Path(source).resolve()
    if source == (out / "central.sqlite").resolve() or out in source.parents:
        raise ValueError("Source database must be outside the private output directory")
    snapshots = out / "snapshots"
    snapshots.mkdir(parents=True, exist_ok=True)
    started = utc_now()
    temporary = snapshots / ("freeze-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + ".sqlite")
    before = {str(path): file_identity(path) for path in [source, Path(str(source) + "-wal")]}
    progress_at = [0.0]
    def progress(status, remaining, total):
        now = time.monotonic()
        if now - progress_at[0] >= 15 or remaining == 0:
            emit("snapshot_copy", pages_remaining=remaining, pages_total=total)
            progress_at[0] = now
    with closing(readonly(source)) as src, closing(sqlite3.connect(temporary)) as dst:
        src.execute("BEGIN")
        src.execute("SELECT COUNT(*) FROM request_history").fetchone()
        src.backup(dst, pages=4096, progress=progress)
        src.rollback()
        dst.commit()
        dst.execute("PRAGMA journal_mode=DELETE")
        check = [row[0] for row in dst.execute("PRAGMA quick_check")]
        if check != ["ok"]:
            raise ValueError("Frozen SQLite quick_check failed: " + repr(check))
        counts = {
            "source_rows": dst.execute("SELECT COUNT(*) FROM sources").fetchone()[0],
            "unique_queue_entries": dst.execute("SELECT COUNT(*) FROM posts").fetchone()[0],
            "metadata_requests_total": dst.execute("SELECT COUNT(*) FROM request_history").fetchone()[0],
            "post_statuses": dict(dst.execute("SELECT status,COUNT(*) FROM posts GROUP BY status")),
        }
        latest = dst.execute(
            "SELECT attempt_id,post_id,attempted_at,status,error FROM request_history ORDER BY attempt_id DESC LIMIT 1"
        ).fetchone()
        columns = ["attempt_id", "post_id", "attempted_at", "status", "error"]
        latest = dict(zip(columns, latest)) if latest else None
    emit("snapshot_hashing", bytes=temporary.stat().st_size)
    sha = digest(temporary)
    snapshot_id = "weibo-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + sha[:12]
    target = snapshots / (snapshot_id + ".sqlite")
    os.replace(temporary, target)
    after = {str(path): file_identity(path) for path in [source, Path(str(source) + "-wal")]}
    record = {
        "schema": "weibo-snapshot-v1", "snapshot_id": snapshot_id,
        "snapshot_relative_path": target.relative_to(out).as_posix(),
        "sha256": sha, "bytes": target.stat().st_size,
        "started_at": started, "completed_at": utc_now(),
        "method": "SQLite online backup from an explicit read transaction; committed WAL included; standalone destination",
        "source_database": str(source), "source_file_identity_before": before,
        "source_file_identity_after": after, "source_files_unchanged": before == after,
        "sqlite_quick_check": "ok", "counts": counts, "cutoff": latest,
    }
    write_json(record_path, record)
    emit("snapshot_ready", snapshot_id=snapshot_id, counts=counts)
    return record, target


def build_central(snapshot, frozen, out, ids):
    central = out / "central.sqlite"
    temporary = out / ("central-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + ".sqlite")
    with closing(readonly(frozen)) as src, closing(sqlite3.connect(temporary)) as db:
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute("PRAGMA synchronous=FULL")
        db.executescript("""
            CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE queue_posts(post_id TEXT PRIMARY KEY,ordinal INTEGER UNIQUE NOT NULL,
                source_url TEXT NOT NULL,status TEXT NOT NULL,attempts INTEGER NOT NULL);
            CREATE TABLE source_refs(source_id INTEGER PRIMARY KEY,post_id TEXT NOT NULL,
                workbook TEXT NOT NULL,sheet TEXT NOT NULL,excel_row INTEGER NOT NULL,
                input_error TEXT,region TEXT NOT NULL,reported_date TEXT,month TEXT NOT NULL);
            CREATE TABLE assignments(post_id TEXT PRIMARY KEY,ordinal INTEGER UNIQUE NOT NULL,
                source_url TEXT NOT NULL,package_id TEXT,region_combo TEXT NOT NULL,
                month TEXT NOT NULL,source_count INTEGER NOT NULL,source_month_count INTEGER NOT NULL);
            CREATE TABLE packages(package_id TEXT PRIMARY KEY,snapshot_id TEXT NOT NULL,
                relative_path TEXT NOT NULL,count INTEGER NOT NULL,sha256 TEXT NOT NULL,bytes INTEGER NOT NULL);
            CREATE TABLE observed(post_id TEXT PRIMARY KEY,ordinal INTEGER UNIQUE NOT NULL,
                source_url TEXT NOT NULL,package_id TEXT NOT NULL);
        """)
        for key, value in {"snapshot_id": snapshot["snapshot_id"], "snapshot_sha256": snapshot["sha256"],
                           "snapshot_relative_path": snapshot["snapshot_relative_path"],
                           "source_mapping_note": "source_id joins to full row_json in frozen SQLite; source_refs is private",
                           "package_schema": PACKAGE_SCHEMA}.items():
            db.execute("INSERT INTO meta VALUES(?,?)", (key, value))
        rows = src.execute("SELECT post_id,ordinal,source_url,status,attempts FROM posts ORDER BY ordinal")
        while batch := rows.fetchmany(10000):
            db.executemany("INSERT INTO queue_posts VALUES(?,?,?,?,?)", [tuple(row) for row in batch])
        db.commit()
        emit("queue_indexed", count=snapshot["counts"]["unique_queue_entries"])
        source_rows = src.execute("""SELECT source_id,post_id,workbook,sheet,excel_row,input_error,
            json_extract(row_json,'$._region') AS region,
            json_extract(row_json,'$.日期') AS reported_date FROM sources ORDER BY source_id""")
        indexed = 0
        while batch := source_rows.fetchmany(10000):
            prepared = []
            for row in batch:
                date = None if row["reported_date"] is None else str(row["reported_date"])
                prepared.append((row["source_id"], row["post_id"], row["workbook"], row["sheet"],
                                 row["excel_row"], row["input_error"],
                                 region_key(row["region"], row["workbook"]), date, month_key(date)))
            db.executemany("INSERT INTO source_refs VALUES(?,?,?,?,?,?,?,?,?)", prepared)
            indexed += len(prepared)
            if indexed % 200000 == 0:
                db.commit()
                emit("source_mapping_indexed", count=indexed)
        db.execute("CREATE INDEX source_refs_post ON source_refs(post_id,source_id)")
        db.execute("CREATE INDEX queue_posts_status ON queue_posts(status,ordinal)")
        db.commit()
        invalid_pending = db.execute("SELECT COUNT(*) FROM queue_posts WHERE status='pending' AND attempts<>0").fetchone()[0]
        history_pending = src.execute("""SELECT COUNT(*) FROM posts p WHERE status='pending'
            AND post_id IN (SELECT post_id FROM request_history)""").fetchone()[0]
        if invalid_pending or history_pending:
            raise ValueError("Pending rows have previous attempts/history; require explicit reconciliation before assignment")
        emit("balancing_features")
        db.execute("""INSERT INTO assignments
            SELECT p.post_id,p.ordinal,p.source_url,NULL,
                trim(
                    CASE WHEN MAX(s.region='JJJ') THEN 'JJJ|' ELSE '' END ||
                    CASE WHEN MAX(s.region='GBA') THEN 'GBA|' ELSE '' END ||
                    CASE WHEN MAX(s.region='YRD') THEN 'YRD|' ELSE '' END ||
                    CASE WHEN MAX(s.region='UNK') THEN 'UNK|' ELSE '' END,'|'),
                COALESCE(MIN(CASE WHEN s.month<>'unknown' THEN s.month END),'unknown'),
                COUNT(*),COUNT(DISTINCT CASE WHEN s.month<>'unknown' THEN s.month END)
            FROM queue_posts p JOIN source_refs s ON s.post_id=p.post_id
            WHERE p.status='pending' AND p.attempts=0 GROUP BY p.post_id""")
        db.execute("CREATE INDEX assignment_strata ON assignments(region_combo,month,ordinal)")
        db.commit()
        cursor = db.execute("SELECT post_id FROM assignments ORDER BY region_combo,month,ordinal")
        sequence = 0
        while batch := cursor.fetchmany(10000):
            updates = []
            for row in batch:
                updates.append((ids[sequence % len(ids)], row[0]))
                sequence += 1
            db.executemany("UPDATE assignments SET package_id=? WHERE post_id=?", updates)
        db.execute("CREATE INDEX assignments_package ON assignments(package_id,ordinal)")
        db.commit()
        expected = snapshot["counts"]["post_statuses"].get("pending", 0)
        if sequence != expected:
            raise ValueError(f"Source coverage mismatch: assigned {sequence}, pending {expected}")
        check = db.execute("PRAGMA quick_check").fetchone()[0]
        if check != "ok":
            raise ValueError("Central database quick_check failed")
    if central.exists():
        previous = out / ("central.previous-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + ".sqlite")
        os.replace(central, previous)
    os.replace(temporary, central)
    emit("central_ready", count=sequence, path=str(central))
    return central


def build_packages(snapshot, central, out, ids):
    package_root = out / "packages"
    package_root.mkdir(exist_ok=True)
    entries = []
    with closing(sqlite3.connect(central)) as db:
        db.row_factory = sqlite3.Row
        for package_id in sorted(ids):
            directory = package_root / package_id.split("-")[0]
            directory.mkdir(exist_ok=True)
            path = directory / (package_id + ".jsonl")
            temporary = path.with_suffix(".jsonl.tmp")
            count, low, high = 0, None, None
            sha = hashlib.sha256()
            with temporary.open("wb") as stream:
                for row in db.execute(
                    "SELECT post_id,ordinal,source_url FROM assignments WHERE package_id=? ORDER BY ordinal",
                    (package_id,)):
                    value = dict(row)
                    validate_task(value)
                    line = (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
                    stream.write(line)
                    sha.update(line)
                    count += 1
                    low = row["ordinal"] if low is None else min(low, row["ordinal"])
                    high = row["ordinal"] if high is None else max(high, row["ordinal"])
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            strata = {row[0] + "/" + row[1]: row[2] for row in db.execute(
                "SELECT region_combo,month,COUNT(*) FROM assignments WHERE package_id=? GROUP BY region_combo,month",
                (package_id,))}
            manifest = {
                "schema": PACKAGE_SCHEMA, "package_id": package_id,
                "big_package_id": package_id.split("-")[0],
                "snapshot_id": snapshot["snapshot_id"], "count": count,
                "sha256": sha.hexdigest(), "bytes": path.stat().st_size,
                "file_name": path.name, "relative_path": path.relative_to(out).as_posix(),
                "ordinal_min": low, "ordinal_max": high,
                "row_fields": sorted(ALLOWED_FIELDS),
                "region_month_counts": strata,
                "month_definition": "Earliest valid source-reported calendar month for the post; unknown preserved",
                "region_definition": "Union of source workbook regions; not image capture location",
                "assignment_method": "Deterministic stratified interleaved round-robin over region-combination/month and original ordinal",
                "scope": "Snapshot pending posts with zero attempts; all previously processed/nonpending statuses excluded",
                "created_at": utc_now(),
            }
            write_json(path.with_suffix(".manifest.json"), manifest)
            db.execute("INSERT INTO packages VALUES(?,?,?,?,?,?)",
                       (package_id, snapshot["snapshot_id"], manifest["relative_path"],
                        count, manifest["sha256"], manifest["bytes"]))
            db.commit()
            entries.append({key: manifest[key] for key in
                            ["package_id", "snapshot_id", "relative_path", "count", "sha256", "bytes"]})
            emit("package_ready", **entries[-1])
    write_json(package_root / "index.json", {
        "schema": "weibo-package-index-v1", "snapshot_id": snapshot["snapshot_id"],
        "package_count": len(entries), "task_count": sum(x["count"] for x in entries),
        "packages": entries,
    })
    return entries


def audit_packages(snapshot, central, out, ids, entries):
    with closing(sqlite3.connect(central)) as db:
        db.row_factory = sqlite3.Row
        for index, package_id in enumerate(sorted(ids)):
            path = out / "packages" / package_id.split("-")[0] / (package_id + ".jsonl")
            manifest = json.loads(path.with_suffix(".manifest.json").read_text("utf-8"))
            if (manifest["package_id"] != package_id or manifest["snapshot_id"] != snapshot["snapshot_id"]
                    or manifest["sha256"] != digest(path)):
                raise ValueError("Package identity/checksum mismatch: " + package_id)
            rows, count = [], 0
            with path.open("r", encoding="utf-8") as stream:
                for line in stream:
                    value = json.loads(line)
                    validate_task(value)
                    rows.append((value["post_id"], value["ordinal"], value["source_url"], package_id))
                    count += 1
                    if len(rows) >= 10000:
                        db.executemany("INSERT INTO observed VALUES(?,?,?,?)", rows)
                        rows.clear()
            if rows:
                db.executemany("INSERT INTO observed VALUES(?,?,?,?)", rows)
            if count != manifest["count"]:
                raise ValueError("Package line count mismatch: " + package_id)
            db.commit()
            if (index + 1) % 16 == 0:
                emit("packages_verified", count=index + 1)
        checks = {
            "missing_assigned_posts": db.execute("SELECT COUNT(*) FROM assignments a LEFT JOIN observed o USING(post_id) WHERE o.post_id IS NULL").fetchone()[0],
            "unexpected_or_mismatched_rows": db.execute("""SELECT COUNT(*) FROM observed o LEFT JOIN assignments a USING(post_id)
                WHERE a.post_id IS NULL OR o.ordinal<>a.ordinal OR o.source_url<>a.source_url OR o.package_id<>a.package_id""").fetchone()[0],
            "nonpending_or_previously_attempted_in_packages": db.execute("""SELECT COUNT(*) FROM observed o JOIN queue_posts p USING(post_id)
                WHERE p.status<>'pending' OR p.attempts<>0""").fetchone()[0],
            "source_refs_without_queue_post": db.execute("SELECT COUNT(*) FROM source_refs s LEFT JOIN queue_posts p USING(post_id) WHERE p.post_id IS NULL").fetchone()[0],
            "queue_posts_without_source_refs": db.execute("SELECT COUNT(*) FROM queue_posts p LEFT JOIN source_refs s USING(post_id) WHERE s.source_id IS NULL").fetchone()[0],
        }
        if any(checks.values()):
            raise ValueError("Package reconciliation failed: " + repr(checks))
        strata = [tuple(row) for row in db.execute("SELECT region_combo,month,COUNT(*) FROM assignments GROUP BY region_combo,month")]
        small_imbalance, big_imbalance = 0, 0
        big_ids = sorted({p.split("-")[0] for p in ids})
        for region, month, total in strata:
            small = dict(db.execute("SELECT package_id,COUNT(*) FROM assignments WHERE region_combo=? AND month=? GROUP BY package_id", (region, month)))
            big = collections.Counter()
            for key, value in small.items():
                big[key.split("-")[0]] += value
            sv = [small.get(key, 0) for key in ids]
            bv = [big.get(key, 0) for key in big_ids]
            small_imbalance = max(small_imbalance, max(sv) - min(sv))
            big_imbalance = max(big_imbalance, max(bv) - min(bv))
        counts = [x["count"] for x in entries]
        actual = db.execute("SELECT COUNT(*) FROM observed").fetchone()[0]
        pending = snapshot["counts"]["post_statuses"].get("pending", 0)
        if (actual != pending or max(counts) - min(counts) > 1
                or small_imbalance > 1 or big_imbalance > 1):
            raise ValueError("Equal-size or per-stratum balance invariant failed")
        report = {
            "schema": "weibo-package-validation-v1", "status": "passed",
            "snapshot_id": snapshot["snapshot_id"], "snapshot_sha256": snapshot["sha256"],
            "validated_at": utc_now(), "snapshot_counts": snapshot["counts"],
            "cutoff": snapshot["cutoff"], "big_package_count": len(big_ids),
            "small_package_count": len(ids), "task_count": actual,
            "small_package_min_count": min(counts), "small_package_max_count": max(counts),
            "small_package_count_difference": max(counts) - min(counts),
            "max_small_package_stratum_difference": small_imbalance,
            "max_big_package_stratum_difference": big_imbalance,
            "stratum_count": len(strata),
            "source_month_conflict_posts": db.execute("SELECT COUNT(*) FROM assignments WHERE source_month_count>1").fetchone()[0],
            "unknown_month_posts": db.execute("SELECT COUNT(*) FROM assignments WHERE month='unknown'").fetchone()[0],
            "excluded_nonpending_count": snapshot["counts"]["unique_queue_entries"] - pending,
            "source_duplicate_rows_beyond_unique_queue": snapshot["counts"]["source_rows"] - snapshot["counts"]["unique_queue_entries"],
            "checks_zero_expected": checks,
            "uniqueness": "All post_id and ordinal values inserted under SQLite PRIMARY KEY/UNIQUE constraints",
            "source_mapping": "central.sqlite source_refs links source_id to frozen SQLite sources.row_json; no original rows in package files",
            "privacy_fields_exact": sorted(ALLOWED_FIELDS),
            "region_labels": REGION_LABELS,
            "strata_counts": {r + "/" + m: n for r, m, n in strata},
            "big_package_counts": dict(db.execute("SELECT substr(package_id,1,3),SUM(count) FROM packages GROUP BY substr(package_id,1,3)")),
            "packages": entries,
        }
    write_json(out / "validation_report.json", report)
    text = [
        "Weibo distributed package validation: PASSED",
        "Snapshot: " + snapshot["snapshot_id"],
        "Cutoff metadata request: " + str((snapshot["cutoff"] or {}).get("attempt_id")),
        f"Source rows: {snapshot['counts']['source_rows']:,}",
        f"Unique queue entries: {snapshot['counts']['unique_queue_entries']:,}",
        f"Pending tasks packaged: {actual:,}; excluded nonpending: {report['excluded_nonpending_count']:,}",
        f"Packages: {len(big_ids)} big x {len(ids)//len(big_ids)} small = {len(ids)}",
        f"Small package count range: {min(counts):,}-{max(counts):,}",
        f"Max per-region/month difference: small={small_imbalance}; big={big_imbalance}",
        "All checksums, exact task coverage, identity/ordinal uniqueness and processed-post exclusions passed.",
        "Only post_id, ordinal and source_url are distributed.",
        "Original Excel references and full row data remain in the private frozen database; source_refs indexes them.",
        "Regions mean source workbook membership. Month is earliest valid source-reported month, not verified capture date.",
        "No collection/network request was performed.",
    ]
    (out / "validation_report.txt").write_text("\n".join(text) + "\n", encoding="utf-8")
    emit("validation_passed", snapshot_id=snapshot["snapshot_id"], count=actual, packages=len(ids))
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-db", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--big-packages", type=int, default=16)
    parser.add_argument("--small-per-big", type=int, default=10)
    parser.add_argument("--resume", action="store_true", help="Reuse the verified frozen snapshot; rebuild deterministic packages")
    args = parser.parse_args(argv)
    if not 1 <= args.big_packages <= 99 or not 1 <= args.small_per_big <= 99:
        parser.error("Package dimensions must be 1-99")
    out = args.out.expanduser().resolve()
    out.mkdir(parents=True, exist_ok=True)
    snapshot, frozen = freeze_source(args.source_db, out, args.resume)
    ids = package_ids(args.big_packages, args.small_per_big)
    central = build_central(snapshot, frozen, out, ids)
    entries = build_packages(snapshot, central, out, ids)
    audit_packages(snapshot, central, out, ids, entries)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
