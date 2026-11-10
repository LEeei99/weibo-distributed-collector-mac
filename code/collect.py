"""Local prepared-shard batches; raw workbook ingestion is unavailable."""
from __future__ import annotations
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import sys
import time

from weibo_queue import Queue, utc_now
from weibo_media import MediaStore, valid_media, allowed_media_url, cached_media_needs_repair, repair_cached_media
from weibo_locations import has_coordinates
from weibo_worker_gate import PLATFORM_GATES, annotate_gate
from weibo_platform import file_lock, local_directory

LOCAL_ROOT = Path(__file__).resolve().parent.parent
STOP_STATUSES = {"rate_limited", "captcha_required"}
ACCESS_GATES = {"visitor_gate","login_required","session_required","browser_session_required"}
LOGIN_GATES = {"login_required", "session_required"}
RETRY_STATUSES = {"timeout", "network_error", "http_error", "parse_error", "unknown", "visitor_gate", "login_required", "session_required", "browser_session_required", "server_error", "unexpected_response", "rate_limited", "captcha_required"}

@contextmanager
def run_lock(out):
    """Use the same OS lock for the collector and return/export tools."""
    with file_lock(Path(out) / "collector.lock"):
        yield


def local_path(path):
    return local_directory(path)


def preserve_media_retry_state(fresh, old):
    def key(a):
        return (a.get("kind"),a.get("media_id") or a.get("url"),a.get("source_post_id"),a.get("role"))
    prior={key(a):a for a in old.get("media",[])}
    for a in fresh.get("media",[]):
        found=prior.get(key(a),{})
        for field in ("repair_attempts","repair_status","prior_download","download"):
            if field in found:
                a[field]=found[field]
        old_urls={found.get("url")} | {x.get("url") for x in found.get("candidates",[])}
        new_urls={a.get("url")} | {x.get("url") for x in a.get("candidates",[])}
        if found.get("repair_status")=="retry_exhausted" and new_urls-old_urls-{None}:
            a["repair_attempts"]=0
            a.pop("repair_status",None)
    return fresh

def media_failed(record):
    return any((a.get("download") or {}).get("status") == "download_failed" for a in record.get("media", []))

def run_batch(queue, out, backend_factory, *, max_posts=100, max_attempts=3, delay=2,
              metadata_only=False, retry_failed=False, max_file_mb=30, max_run_mb=150,
              min_free_gb=5, verify_media=True, login_gate_limit=5, worker_mode=False):
    if isinstance(login_gate_limit, bool) or not isinstance(login_gate_limit, int) or login_gate_limit < 1:
        raise ValueError('login_gate_limit must be a positive integer')
    store = MediaStore(out, queue.db, max_file_mb=max_file_mb, max_run_mb=max_run_mb)
    backend = None
    stats = {"metadata_requests": 0, "cached_posts_checked": 0, "cached_media_repair_posts": 0,
             "resumed_without_requests": 0, "stop_reason": None,
             "login_gate_limit": login_gate_limit, "consecutive_login_gates": 0,
             "worker_mode": bool(worker_mode), "login_skipped": 0, "unknown_login_skipped": 0,
             "health_probe_count": 0, "health_requests": 0, "health_checks": []}
    started = time.monotonic()
    consecutive_gate = 0
    consecutive_login_gate = 0
    consecutive_network_failures = 0
    consecutive_unknown_login = 0
    def fetch(url):
        nonlocal backend, consecutive_gate, consecutive_login_gate, consecutive_network_failures, consecutive_unknown_login
        if backend is None:
            backend = backend_factory()
            if hasattr(backend, "__enter__"):
                backend.__enter__()
        result = backend.fetch_post(url)
        if worker_mode:
            result = annotate_gate(result)
        if hasattr(backend, "make_download_opener"):
            store.opener = backend.make_download_opener()
        stats["metadata_requests"] += 1
        consecutive_gate = consecutive_gate+1 if result.get("status") in ACCESS_GATES else 0
        consecutive_login_gate = consecutive_login_gate+1 if result.get("status") in LOGIN_GATES else 0
        stats["consecutive_login_gates"] = consecutive_login_gate
        if worker_mode:
            status=result.get('status')
            unknown_login=status in LOGIN_GATES and result.get('gate_scope')!='post'
            consecutive_unknown_login=consecutive_unknown_login+1 if unknown_login else 0
            if status in LOGIN_GATES:
                stats['login_skipped']+=1
                stats['unknown_login_skipped']+=int(unknown_login)
            if status in PLATFORM_GATES:
                stats['stop_reason']=status
                stats['stop_scope']='session_or_environment'
            elif consecutive_unknown_login>=login_gate_limit and not stats['health_probe_count']:
                stats['health_probe_count']+=1
                probe=getattr(backend,'probe_public_health',None)
                try:
                    if callable(probe) and delay:
                        time.sleep(delay)
                    health=probe() if callable(probe) else {'status':'health_unknown','requests':0,'error':'no_same_session_health_interface'}
                    if not isinstance(health,dict):
                        health={'status':'health_unknown','requests':0,'error':'invalid_health_response'}
                except Exception as error:
                    health={'status':'health_unknown','requests':0,'error':'health_probe_'+type(error).__name__}
                health=annotate_gate(health)
                # Diagnostics are not post attempts and never update any post,
                # attempt counter or request_history entry. One probe per batch.
                health={key:health.get(key) for key in ('status','gate_scope','gate_evidence','request_source','http_status','requests','error') if key in health}
                health.update(request_role='session_health_probe',recorded_at=utc_now())
                requests=health.get('requests',0)
                stats['health_requests']+=requests if isinstance(requests,int) and not isinstance(requests,bool) and 0<=requests<=1 else 0
                stats['health_checks'].append(health)
                print(json.dumps({'event':'worker_health_probe',**health},ensure_ascii=False),flush=True)
                if health.get('status') in PLATFORM_GATES:
                    stats['stop_reason']=health['status']
                    stats['stop_scope']='session_or_environment'
                # A generic login, failed probe, or lack of a control does not
                # prove a platform gate and does not force a worker pause.
        elif result.get("status") in STOP_STATUSES:
            stats["stop_reason"]=result["status"]
        elif consecutive_login_gate >= login_gate_limit:
            stats["stop_reason"]="repeated_access_gate"
        elif consecutive_gate >= 3 and consecutive_login_gate < consecutive_gate:
            # Visitor/browser gates still stop at three, including mixed gate
            # sequences. Only a pure login-required sequence uses the new limit.
            stats["stop_reason"]="repeated_access_gate"
        consecutive_network_failures = consecutive_network_failures+1 if result.get("status") in {"timeout","network_error"} else 0
        if consecutive_network_failures>=3 and not stats["stop_reason"]:
            stats["stop_reason"]="repeated_network_failure"
        if delay:
            time.sleep(delay)
        return result
    try:
        # A bounded rolling audit prevents hashing all prior media on every batch.
        if verify_media and not metadata_only:
            cursor = queue.db.execute("SELECT value FROM meta WHERE key='integrity_cursor'").fetchone()
            start_ordinal = int(cursor[0]) if cursor else 0
            successes = queue.db.execute(
                "SELECT * FROM posts WHERE status='success' AND ordinal>? ORDER BY ordinal LIMIT ?",
                (start_ordinal,max_posts)).fetchall()
            if not successes and start_ordinal:
                successes = queue.db.execute("SELECT * FROM posts WHERE status='success' ORDER BY ordinal LIMIT ?",(max_posts,)).fetchall()
            for item in successes:
                old = json.loads(item["result_json"])
                stats["cached_posts_checked"] += 1
                if cached_media_needs_repair(old,out,max_attempts=max_attempts):
                    fixed = repair_cached_media(old,store,item["source_url"],max_attempts=max_attempts)
                    queue.save_result(item["post_id"],fixed)
                    stats["cached_media_repair_posts"] += 1
                    if media_failed(fixed) and retry_failed and item["attempts"] < max_attempts and stats["metadata_requests"] < max_posts:
                        fresh = fetch(item["source_url"])
                        if fresh.get("status") == "success":
                            fresh = preserve_media_retry_state(fresh,fixed)
                            queue.save_result(item["post_id"],fresh,attempted=True)
                            fresh = repair_cached_media(fresh,store,item["source_url"],metadata_only=metadata_only,max_attempts=max_attempts)
                            queue.save_result(item["post_id"],fresh)
                        else:
                            queue.save_result(item["post_id"],fresh,attempted=True)
                        if fresh.get("status") in STOP_STATUSES:
                            stats["stop_reason"] = fresh["status"]
                            break
                else:
                    stats["resumed_without_requests"] += 1
                queue.db.execute("INSERT OR REPLACE INTO meta VALUES('integrity_cursor',?)",(str(item["ordinal"]),))
                queue.db.commit()
                if stats["stop_reason"]:
                    break
                if store.new_bytes >= store.max_run:
                    stats["stop_reason"] = "media_budget"
                    break
        candidates = queue.db.execute("SELECT * FROM posts WHERE status='pending' ORDER BY ordinal LIMIT ?",(max_posts,)).fetchall()
        if retry_failed:
            marks = ",".join("?" for _ in RETRY_STATUSES)
            retries = queue.db.execute(
                f"SELECT * FROM posts WHERE status IN ({marks}) AND attempts<? ORDER BY ordinal LIMIT ?",
                (*sorted(RETRY_STATUSES),max_attempts,max_posts)).fetchall()
            candidates.extend(retries)
            candidates.sort(key=lambda item:item["ordinal"])
        for item in candidates:
            if stats["stop_reason"] or stats["metadata_requests"] >= max_posts:
                break
            if shutil.disk_usage(out).free < min_free_gb*1024**3:
                stats["stop_reason"] = "disk_space"
                break
            result = fetch(item["source_url"])
            if result.get("status") == "success":
                result = preserve_media_retry_state(result,json.loads(item["result_json"]))
                queue.save_result(item["post_id"],result,attempted=True)
                result = repair_cached_media(result,store,item["source_url"],metadata_only=metadata_only,max_attempts=max_attempts)
                queue.save_result(item["post_id"],result)
            else:
                queue.save_result(item["post_id"],result,attempted=True)
            print(json.dumps({"ordinal":item["ordinal"],"status":result.get("status"),
                              "error":result.get("error"), "recorded_at":utc_now(),
                               "login_gate_streak":consecutive_login_gate,"login_gate_limit":login_gate_limit,
                               "gate_scope":result.get('gate_scope'),"worker_mode":bool(worker_mode),
                              "images":sum(a.get("kind")=="image" for a in result.get("media",[])),
                              "videos":sum(a.get("kind")=="video" for a in result.get("media",[])),
                              "locations":len(result.get("locations",[])),
                              "new_media_mb":round(store.new_bytes/1024**2,2)},ensure_ascii=False),flush=True)
            if result.get("status") in STOP_STATUSES:
                stats["stop_reason"] = result["status"]
                break
            if stats["stop_reason"]:
                break
            if not metadata_only and store.new_bytes >= store.max_run:
                stats["stop_reason"] = "media_budget"
                break
    finally:
        if backend is not None and hasattr(backend,"__exit__"):
            backend.__exit__(None,None,None)
        stats.update(new_media_bytes=store.new_bytes,seconds=round(time.monotonic()-started,2))
        stats['request_calls_total']=stats['metadata_requests']+stats['health_requests']
    return stats

def export_results(queue,out,run_stats,*,page_size=100,export=True):
    from weibo_exports import write_streaming_outputs
    linked = write_streaming_outputs(queue.records,out,has_coordinates,
                                    sources_for_record=queue.sources_for_record,sources_factory=queue.source_rows,page_size=page_size) if export else None
    stats = queue.stats()
    summary = {"generated_at":utc_now(),**stats,"linked_outputs":linked,"run":run_stats,
               "scope":"Bounded feasibility pilot or persistent queue; recovery rates are not population estimates."}
    summary["media_download_statuses"] = {}
    summary["explicit_location_entries"] = summary["explicit_coordinate_entries"] = 0
    for record in (queue.records() if export else ()):
        summary["explicit_location_entries"] += len(record.get("locations",[]))
        summary["explicit_coordinate_entries"] += sum(has_coordinates(l) for l in record.get("locations",[]))
        for asset in record.get("media",[]):
            status = (asset.get("download") or {}).get("status","not_attempted")
            summary["media_download_statuses"][status] = summary["media_download_statuses"].get(status,0)+1
    unique = {"images":0,"videos":0,"bytes":0}
    for row in (queue.db.execute("SELECT result_json FROM assets") if export else ()):
        asset = json.loads(row[0])
        relative = asset.get("file","")
        if asset.get("status") == "downloaded" and valid_media(relative,"video" if Path(relative).name.startswith("video_") else "image",{"bytes":asset.get("bytes")},out=out):
            unique["videos" if Path(relative).name.startswith("video_") else "images"] += 1
            unique["bytes"] += asset.get("bytes",0)
    summary["unique_saved_files"] = unique if export else None
    if not export:
        summary["media_download_statuses"]=None
        summary["explicit_location_entries"]=None
        summary["explicit_coordinate_entries"]=None
        summary["export_note"]="Complete views deferred; run weibo_exports.py --out this-folder at a deliberate checkpoint."
    (out/"summary.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding="utf-8")
    with (out/"run_history.jsonl").open("a",encoding="utf-8") as stream:
        stream.write(json.dumps(summary,ensure_ascii=False)+"\n")
    # Keep the old sample filename portable without assembling a full-run list.
    if stats["source_rows"] <= 10000:
        with (out/"samples.json").open("w",encoding="utf-8") as stream:
            stream.write("[")
            for i,row in enumerate(queue.source_rows()):
                stream.write((",\n" if i else "\n")+json.dumps(row,ensure_ascii=False))
            stream.write("\n]\n")
    else:
        (out/"samples.json").write_text(json.dumps({"format":"source_manifest_v2","source_file":"sources.jsonl","row_count":stats["source_rows"]},indent=2),encoding="utf-8")
    return summary

def main(argv=None):
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root",type=Path)
    parser.add_argument("--out",type=Path,default=LOCAL_ROOT/"outputs"/"pilot")
    parser.add_argument("--prepare-only",action="store_true")
    parser.add_argument("--max-posts",type=int,default=100,help="Maximum metadata requests per invocation.")
    parser.add_argument("--max-attempts",type=int,default=3)
    parser.add_argument("--login-gate-limit",type=int,default=5,
                        help="Stop after this many consecutive login/session-required responses; other protections remain.")
    parser.add_argument("--delay",type=float,default=2)
    parser.add_argument("--max-file-mb",type=float,default=30)
    parser.add_argument("--max-run-mb",type=float,default=150)
    parser.add_argument("--min-free-gb",type=float,default=5)
    parser.add_argument("--metadata-only",action="store_true")
    parser.add_argument("--retry-failed",action="store_true")
    parser.add_argument("--worker-mode",action="store_true",help="Record and skip login requirements; pause immediately on platform gates. login-gate-limit triggers one diagnostic, never a login-count stop.")
    parser.add_argument("--backend",choices=["browser","http"],default="browser")
    parser.add_argument("--channel",default="msedge")
    parser.add_argument("--page-size",type=int,default=100)
    parser.add_argument("--export",action="store_true",help="Regenerate complete views even for large queues.")
    parser.add_argument("--no-export",action="store_true",help="Retain DB progress and summary without full export.")
    args = parser.parse_args(argv)
    if not 1<=args.max_posts<=1000 or args.max_attempts<1 or args.login_gate_limit<1 or args.delay<0 or min(args.max_file_mb,args.max_run_mb,args.min_free_gb,args.page_size)<=0:
        parser.error("Counts and budgets must be positive; max-posts must be 1–1000; delay cannot be negative.")
    if args.input_root is not None:
        parser.error("Raw workbook ingestion is disabled; import a prepared shard package instead.")
    out=local_path(args.out)
    out.mkdir(parents=True,exist_ok=True)
    os.chdir(LOCAL_ROOT)
    with run_lock(out):
        queue=Queue(out/"state.sqlite")
        run={"prepared_only":args.prepare_only,"metadata_requests":0}
        try:
            if queue.stats()["source_rows"]==0:
                parser.error("A prepared shard queue with source references is required.")
            if not args.prepare_only:
                if args.backend=="browser":
                    from weibo_supervisor import SupervisedBrowserBackend
                    factory=lambda:SupervisedBrowserBackend(channel=args.channel,work=LOCAL_ROOT/'work',worker_mode=args.worker_mode)
                else:
                    from weibo_backend import WeiboBackend
                    factory=lambda:WeiboBackend(delay=0)
                run=run_batch(queue,out,factory,max_posts=args.max_posts,max_attempts=args.max_attempts,
                              delay=args.delay,metadata_only=args.metadata_only,retry_failed=args.retry_failed,
                              max_file_mb=args.max_file_mb,max_run_mb=args.max_run_mb,min_free_gb=args.min_free_gb,
                               login_gate_limit=args.login_gate_limit,worker_mode=args.worker_mode)
        except KeyboardInterrupt:
            run["stop_reason"]="interrupted"
            print("Interrupted; persisted queue/checkpoints are retained.",flush=True)
        finally:
            summary=export_results(queue,out,run,page_size=args.page_size,export=args.export or (not args.no_export and queue.stats()["source_rows"]<=10000))
            queue.close()
            print(json.dumps({"summary":summary},ensure_ascii=False),flush=True)
    return 0

if __name__=="__main__":
    raise SystemExit(main())

