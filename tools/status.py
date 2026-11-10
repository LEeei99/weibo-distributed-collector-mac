"""Read-only coordinator progress; never starts collection or changes databases.

Usage:
  python tools/status.py --data-root D:\\WeiboShardData
  python tools/status.py --data-root D:\\WeiboShardData --details
  python tools/status.py --data-root DATA --merge-dir MERGE --worker-root WORKER --json

Only returned_posts counts as merged progress. A worker that has not returned may
already have processed posts. --worker-root reveals local processed posts absent
from the selected merge ledger; it cannot reveal other computers' live progress.
A db_test_copy merge is labelled as a pilot and never counted as production.
"""
from __future__ import annotations
import argparse
import collections
from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys


def readonly(path):
    path = Path(path).expanduser().resolve()
    if not path.is_file():
        raise ValueError("Database does not exist: " + str(path))
    db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=3)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    return db


def scalar(db, query, args=()):
    return db.execute(query, args).fetchone()[0]


def batches(values, size=500):
    for start in range(0, len(values), size):
        yield values[start:start + size]


def read_registry(path, snapshot_id, packages):
    if path is None or not Path(path).is_file():
        return {}, False
    with closing(readonly(path)) as db:
        claims = [dict(row) for row in db.execute(
            "SELECT snapshot_id,package_id,sha256,worker_id,assignment_id FROM assignments WHERE snapshot_id=?",
            (snapshot_id,))]
    result = {}
    for row in claims:
        pid = row["package_id"]
        if pid not in packages or row["sha256"] != packages[pid]["sha256"]:
            raise ValueError("Dispatch registry does not match package " + str(pid))
        result[pid] = row
    return result, True


def local_progress(root, package, snapshot_id, claim, db, merge_available):
    state = Path(root) / package["package_id"] / "state.sqlite"
    if not state.is_file():
        return None
    with closing(readonly(state)) as local:
        local.execute("BEGIN")
        meta = dict(local.execute("SELECT key,value FROM meta"))
        expected = {
            "distributed_snapshot_id": snapshot_id,
            "distributed_package_id": package["package_id"],
            "distributed_package_sha256": package["sha256"],
            "distributed_package_count": str(package["count"]),
            "distributed_import_complete": "1",
        }
        if claim:
            expected.update(distributed_worker_id=claim["worker_id"],
                            distributed_assignment_id=claim["assignment_id"])
        if any(str(meta.get(key)) != str(value) for key, value in expected.items()):
            raise ValueError("Local worker identity/import metadata differs: " + package["package_id"])
        processed = [dict(row) for row in local.execute(
            "SELECT post_id,status FROM posts WHERE status<>'pending'")]
        total = scalar(local, "SELECT COUNT(*) FROM posts")
        if total != package["count"]:
            raise ValueError("Local worker task count differs: " + package["package_id"])
        latest = local.execute(
            "SELECT attempted_at,status FROM request_history ORDER BY attempt_id DESC LIMIT 1").fetchone()
        local.rollback()
    ids = [row["post_id"] for row in processed]
    found = set()
    merged = set()
    for part in batches(ids):
        marks = ",".join("?" for _ in part)
        members = db.execute(
            f"SELECT post_id,package_id FROM assignments WHERE post_id IN ({marks})", part).fetchall()
        if any(row["package_id"] != package["package_id"] for row in members):
            raise ValueError("Local worker contains another package's result")
        found.update(row["post_id"] for row in members)
        if merge_available:
            merged.update(row[0] for row in db.execute(
                f"SELECT post_id FROM selected_merge.returned_posts WHERE post_id IN ({marks})", part))
    if len(found) != len(ids):
        raise ValueError("Local worker contains an unassigned result")
    return {
        "worker_id": meta.get("distributed_worker_id"),
        "processed_unique_posts": len(ids),
        "pending_local_posts": total - len(ids),
        "processed_statuses": dict(collections.Counter(row["status"] for row in processed)),
        "already_in_selected_merge": len(merged) if merge_available else None,
        "processed_not_in_selected_merge": len(ids) - len(merged) if merge_available else None,
        "latest_attempt": dict(latest) if latest else None,
        "path": str(state),
    }


def summarize(data_root, registry=None, merge_dir=None, worker_root=None):
    data_root = Path(data_root).expanduser().resolve()
    central = data_root / "central.sqlite"
    registry = Path(registry) if registry is not None else data_root / "dispatch.sqlite"
    with closing(readonly(central)) as db:
        meta = dict(db.execute("SELECT key,value FROM meta"))
        snapshot_id = meta.get("snapshot_id")
        if not snapshot_id:
            raise ValueError("central.sqlite has no snapshot_id")
        package_rows = [dict(row) for row in db.execute(
            "SELECT package_id,snapshot_id,count,sha256 FROM packages ORDER BY package_id")]
        packages = {row["package_id"]: row for row in package_rows}
        if not packages or any(row["snapshot_id"] != snapshot_id for row in package_rows):
            raise ValueError("Package inventory belongs to another snapshot or is empty")
        claims, registry_available = read_registry(registry, snapshot_id, packages)
        merge_available = merge_dir is not None
        merge_mode = "not_selected"
        merge_info = None
        returned = collections.defaultdict(collections.Counter)
        if merge_available:
            merge_path = Path(merge_dir).expanduser().resolve()
            if merge_path.is_dir():
                merge_path /= "state.sqlite"
            if not merge_path.is_file():
                raise ValueError("Selected merge database does not exist")
            db.execute("ATTACH DATABASE ? AS selected_merge", (merge_path.as_uri() + "?mode=ro",))
        db.execute("BEGIN")
        if merge_available:
            base = db.execute("SELECT snapshot_id,mode FROM selected_merge.distribution_base").fetchall()
            if len(base) != 1 or base[0]["snapshot_id"] != snapshot_id:
                raise ValueError("Selected merge database belongs to another snapshot")
            merge_mode = base[0]["mode"]
            if merge_mode not in {"complete", "db_test_copy"}:
                raise ValueError("Unknown selected merge mode")
            rows = db.execute("""SELECT a.package_id,p.status,COUNT(*) AS count
                FROM selected_merge.returned_posts r
                LEFT JOIN assignments a ON a.post_id=r.post_id
                LEFT JOIN selected_merge.posts p ON p.post_id=r.post_id
                GROUP BY a.package_id,p.status""").fetchall()
            for row in rows:
                if row["package_id"] is None or row["status"] in {None, "pending"}:
                    raise ValueError("Selected merge ledger contains an unassigned or pending result")
                returned[row["package_id"]][row["status"]] += row["count"]
            merge_info = {
                "mode": merge_mode,
                "path": str(merge_path),
                "is_production": merge_mode == "complete",
                "returned_batch_count": scalar(db, "SELECT COUNT(*) FROM selected_merge.returned_packages"),
                "latest_import_at": scalar(db, "SELECT MAX(imported_at) FROM selected_merge.returned_packages"),
            }
        report_packages = []
        for package in package_rows:
            pid = package["package_id"]
            claim = claims.get(pid)
            statuses = dict(returned[pid])
            merged_count = sum(statuses.values())
            if merged_count > package["count"]:
                raise ValueError("Merged count exceeds package membership")
            local = (local_progress(worker_root, package, snapshot_id, claim, db, merge_available)
                     if worker_root is not None else None)
            if merged_count == package["count"]:
                state = "complete"
            elif merged_count:
                state = "partial"
            elif claim:
                state = "assigned_no_return"
            elif registry_available:
                state = "unassigned"
            else:
                state = "dispatch_unknown"
            report_packages.append({
                "package_id": pid, "big_package_id": pid.split("-")[0],
                "owner": claim["worker_id"] if claim else None,
                "assignment_id": claim["assignment_id"] if claim else None,
                "task_count": package["count"], "central_state": state,
                "merged_unique_posts": merged_count, "merged_statuses": statuses,
                "not_in_selected_merge": package["count"] - merged_count,
                "local_worker": local,
            })
        db.rollback()
    big = {}
    for package in report_packages:
        row = big.setdefault(package["big_package_id"], {
            "big_package_id": package["big_package_id"], "task_count": 0,
            "merged_unique_posts": 0, "small_package_count": 0, "assigned_small_packages": 0,
            "complete_small_packages": 0, "partial_small_packages": 0,
            "owners": set(), "local_processed_not_in_selected_merge": 0,
        })
        row["task_count"] += package["task_count"]
        row["merged_unique_posts"] += package["merged_unique_posts"]
        row["small_package_count"] += 1
        row["assigned_small_packages"] += package["owner"] is not None
        row["complete_small_packages"] += package["central_state"] == "complete"
        row["partial_small_packages"] += package["central_state"] == "partial"
        if package["owner"]:
            row["owners"].add(package["owner"])
        local = package["local_worker"]
        if local and local["processed_not_in_selected_merge"] is not None:
            row["local_processed_not_in_selected_merge"] += local["processed_not_in_selected_merge"]
    for row in big.values():
        row["owners"] = sorted(row["owners"])
        row["not_in_selected_merge"] = row["task_count"] - row["merged_unique_posts"]
        if not merge_available or worker_root is None:
            row["local_processed_not_in_selected_merge"] = None
    states = collections.Counter(p["central_state"] for p in report_packages)
    merged_total = sum(p["merged_unique_posts"] for p in report_packages)
    total = sum(p["task_count"] for p in report_packages)
    locals_found = [p["local_worker"] for p in report_packages if p["local_worker"] is not None]
    return {
        "schema": "weibo-central-status-v1", "observed_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_id": snapshot_id, "total_tasks": total, "package_count": len(report_packages),
        "big_package_count": len(big), "registry_available": registry_available,
        "selected_merge": merge_info,
        "merged_unique_posts_selected": merged_total,
        "production_merged_unique_posts": merged_total if merge_mode == "complete" else 0,
        "pilot_merged_unique_posts": merged_total if merge_mode == "db_test_copy" else 0,
        "not_in_selected_merge": total - merged_total,
        "package_states": dict(states), "local_worker_databases_found": len(locals_found),
        "local_processed_unique_posts": sum(x["processed_unique_posts"] for x in locals_found) if worker_root else None,
        "local_processed_not_in_selected_merge": (
            sum(x["processed_not_in_selected_merge"] for x in locals_found)
            if worker_root and merge_available else None),
        "progress_boundary": (
            "Only distinct returned_posts in the selected merge database are counted as merged. "
            "Not in selected merge includes unattempted posts and completed work not yet returned/imported; "
            "remote live progress is unknown. Local processed-but-not-merged refers only to --worker-root. "
            "db_test_copy is a pilot and contributes zero to production completion."),
        "big_packages": list(big.values()), "packages": report_packages,
    }


STATE_LABELS = {
    "complete": "已合并完成", "partial": "部分已合并",
    "assigned_no_return": "已分派，未回传/远端未知",
    "unassigned": "未分派", "dispatch_unknown": "派发状态未知",
}


def render(report, details=False):
    merge = report["selected_merge"]
    scope = "正式" if merge and merge["is_production"] else "试验" if merge else "未选择"
    lines = [
        "微博分布式进度（只读）",
        "Snapshot: " + report["snapshot_id"],
        f"总任务 {report['total_tasks']:,} | 大包 {report['big_package_count']} | 小包 {report['package_count']}",
        f"正式已合并去重 {report['production_merged_unique_posts']:,} | 试验已合并去重 {report['pilot_merged_unique_posts']:,}",
        f"选定合并库: {scope} | 此库未记录 {report['not_in_selected_merge']:,}",
        "未回传不等于未采集；远端电脑可能已有本机进度，中央尚不可见。",
    ]
    if not merge:
        lines.append("未选择合并库：无法核对是否已回传，当前合并计数仅表示未提供证据。")
    if report["local_processed_unique_posts"] is not None:
        value = report["local_processed_not_in_selected_merge"]
        lines.append(f"本机已处理 {report['local_processed_unique_posts']:,} | 本机已处理但未入此合并库 "
                     + (f"{value:,}" if value is not None else "未知（未选择合并库）"))
    lines += ["", "大包 | 分派小包 | 已合并小包 | 部分小包 | 已合并/任务 | Owner"]
    for row in report["big_packages"]:
        lines.append(f"{row['big_package_id']} | {row['assigned_small_packages']}/{row['small_package_count']} | "
                     f"{row['complete_small_packages']} | {row['partial_small_packages']} | "
                     f"{row['merged_unique_posts']:,}/{row['task_count']:,} | {','.join(row['owners']) or '-'}")
    lines += ["", "小包 | Owner | 已合并/任务 | 状态 | 本机已处理未入此库"]
    shown = 0
    for row in report["packages"]:
        if not details and not (row["owner"] or row["merged_unique_posts"] or row["local_worker"]):
            continue
        local = row["local_worker"]
        missing = local["processed_not_in_selected_merge"] if local else None
        lines.append(f"{row['package_id']} | {row['owner'] or '-'} | "
                     f"{row['merged_unique_posts']:,}/{row['task_count']:,} | "
                     f"{STATE_LABELS[row['central_state']]} | {missing if missing is not None else '-'}")
        shown += 1
    if shown < report["package_count"]:
        lines.append(f"其余 {report['package_count'] - shown} 个小包未在本视图逐项列出；--details 显示全部。")
    return "\n".join(lines)


def main(argv=None):
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-root", type=Path, required=True, help="Private directory containing central.sqlite")
    parser.add_argument("--registry", type=Path, help="Defaults to DATA/dispatch.sqlite; absent registry remains unknown")
    parser.add_argument("--merge-dir", type=Path, help="Selected coordinator directory containing state.sqlite; pilot mode is labelled")
    parser.add_argument("--worker-root", type=Path, help="Optional local worker root containing PACKAGE/state.sqlite")
    parser.add_argument("--details", action="store_true", help="List every small package")
    parser.add_argument("--json", action="store_true", help="Print complete machine-readable status to stdout")
    args = parser.parse_args(argv)
    try:
        report = summarize(args.data_root, args.registry, args.merge_dir, args.worker_root)
    except (sqlite3.Error, OSError, ValueError, KeyError) as exc:
        parser.error(str(exc))
    print(json.dumps(report, ensure_ascii=False, indent=2) if args.json else render(report, args.details))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
