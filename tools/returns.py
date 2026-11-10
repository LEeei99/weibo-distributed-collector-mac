"""Freeze a stopped worker's return, or import it into an isolated coordinator copy."""
import argparse
from contextlib import closing, contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import tempfile

from assign import digest, load_manifest, read_json, write_json
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "code"))
from weibo_media import MediaStore, valid_media
from collect import run_lock
from worker import file_lock

SCHEMA = """
CREATE TABLE distribution_base(snapshot_id TEXT PRIMARY KEY, mode TEXT NOT NULL);
CREATE TABLE returned_packages(payload_sha TEXT, assignment_id TEXT, imported_at TEXT,
 PRIMARY KEY(payload_sha,assignment_id));
CREATE TABLE returned_posts(post_id TEXT PRIMARY KEY, assignment_id TEXT, row_sha TEXT, updated_at TEXT);
CREATE TABLE returned_attempts(assignment_id TEXT, local_id INTEGER, post_id TEXT, raw TEXT,
 PRIMARY KEY(assignment_id,local_id));
CREATE TABLE distributed_contents(sha TEXT PRIMARY KEY,file TEXT NOT NULL);
CREATE TABLE distributed_cache_variants(asset_key TEXT, sha TEXT, result_json TEXT,
 PRIMARY KEY(asset_key,sha));
"""


def readonly(path):
    return sqlite3.connect(path.resolve().as_uri()+"?mode=ro",uri=True)


def safe_file(root, name):
    if not isinstance(name,str) or not re.fullmatch(r"media/[A-Za-z0-9_.-]+",name) or name.endswith(("/.","/..")):
        raise ValueError("Invalid local media reference")
    target=(root/name).resolve()
    if not target.is_relative_to(root.resolve()):
        raise ValueError("Media reference leaves bundle")
    return target


def transfer(source,target,expected):
    if not re.fullmatch(r"[0-9a-f]{64}",expected):
        raise ValueError("Invalid media checksum")
    if target.exists():
        if digest(target)!=expected:
            raise ValueError("Existing media checksum differs")
        return
    target.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=target.parent,delete=False) as stream:
        temp=Path(stream.name)
    try:
        shutil.copyfile(source,temp)
        if digest(temp)!=expected:
            raise ValueError("Returned media checksum differs")
        os.replace(temp,target)
    finally:
        temp.unlink(missing_ok=True)


def links(value):
    if isinstance(value,dict):
        if isinstance(value.get("file"),str) and value["file"]:
            yield value
        for child in value.values():
            yield from links(child)
    elif isinstance(value,list):
        for child in value:
            yield from links(child)


def init_central(base,media,central,snapshot_id,test_only):
    if central.exists():
        raise ValueError("Choose a new directory for the coordinator")
    if not test_only and (not media or not media.is_dir()):
        raise ValueError("Provide the frozen baseline media directory")
    central.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(dir=central.parent) as tmp:
        stage=Path(tmp)/"central"
        stage.mkdir()
        (stage/"media").mkdir()
        with closing(readonly(base)) as original, closing(sqlite3.connect(stage/"state.sqlite")) as db:
            original.backup(db)
            db.executescript(SCHEMA)
            db.execute("INSERT INTO distribution_base VALUES(?,?)",(snapshot_id,"db_test_copy" if test_only else "complete"))
            if not test_only:
                for file in media.iterdir():
                    if not file.is_file() or file.is_symlink():
                        raise ValueError("Unexpected baseline media entry")
                    target=stage/"media"/file.name
                    shutil.copyfile(file,target)
                db.execute("""INSERT OR IGNORE INTO distributed_contents
                  SELECT json_extract(result_json,'$.sha256'),json_extract(result_json,'$.file') FROM assets
                  WHERE json_extract(result_json,'$.sha256') IS NOT NULL AND json_extract(result_json,'$.file') IS NOT NULL""")
            db.commit()
        stage.rename(central)


def package_rows(package):
    manifest=load_manifest(package)
    with package.open(encoding="utf-8-sig") as stream:
        rows=[json.loads(line) for line in stream]
    index={str(r["post_id"]):r for r in rows}
    if len(index)!=len(rows) or len(rows)!=manifest["count"]:
        raise ValueError("Package membership mismatch")
    return manifest,index


def check_saved(asset, folder):
    saved=asset.get("download") or {}
    if saved.get("status") in {"downloaded","reused"}:
        if not saved.get("file") or not re.fullmatch(r"[0-9a-f]{64}",str(saved.get("sha256",""))):
            raise ValueError("Saved attachment is missing its file reference or SHA-256")
        if not valid_media(saved["file"],asset["kind"],saved,out=folder):
            raise ValueError("Saved attachment failed file type, size or checksum validation")


def pack(package,work,target):
    manifest,members=package_rows(package)
    if target.exists():
        raise ValueError("Use a new directory for each return")
    target.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(dir=target.parent) as tmp, file_lock(work/"worker.lock"), run_lock(work), closing(readonly(work/"state.sqlite")) as db:
        db.row_factory=sqlite3.Row
        db.execute("BEGIN")
        meta=dict(db.execute("SELECT key,value FROM meta"))
        for key in ("package_id","snapshot_id","sha256","count"):
            if str(manifest[key])!=meta["distributed_"+{"sha256":"package_sha256","count":"package_count"}.get(key,key)]:
                raise ValueError("Wrong worker or package metadata")
        stage=Path(tmp)/"return"
        stage.mkdir()
        histories={}
        for attempt in db.execute("SELECT * FROM request_history ORDER BY attempt_id"):
            histories.setdefault(attempt["post_id"],[]).append(dict(attempt))
        count=0
        with (stage/"posts.jsonl").open("w",encoding="utf-8",newline="\n") as dst:
            for row in db.execute("SELECT * FROM posts WHERE status!='pending' ORDER BY ordinal"):
                member=members.get(row["post_id"])
                if member is None or row["ordinal"]!=member["ordinal"] or row["source_url"]!=member["source_url"]:
                    raise ValueError("Worker contains an unassigned result")
                result=json.loads(row["result_json"])
                for asset in result.get("media",[]):
                    check_saved(asset,work)
                    for download in links(asset.get("download",{})):
                        file=safe_file(work,download["file"])
                        sha=download.get("sha256") or digest(file)
                        name="media/"+asset["kind"]+"_"+sha+file.suffix.lower()
                        transfer(file,safe_file(stage,name),sha)
                        download.update(file=name,sha256=sha)
                attempts=histories.get(row["post_id"],[])
                if not attempts or len(attempts)!=row["attempts"]:
                    raise ValueError("Attempt history does not reconcile")
                item=dict(post_id=row["post_id"],result=result,attempts=attempts,updated_at=row["updated_at"],
                          last_attempt_json=row["last_attempt_json"],last_error=row["last_error"])
                dst.write(json.dumps(item,ensure_ascii=False,separators=(",",":"))+"\n")
                count+=1
        header=dict(package_id=manifest["package_id"],snapshot_id=manifest["snapshot_id"],sha256=manifest["sha256"],
                    count=count,worker_id=meta["distributed_worker_id"],assignment_id=meta["distributed_assignment_id"],
                    payload_sha=digest(stage/"posts.jsonl"))
        write_json(stage/"return.json",header)
        stage.rename(target)
        return header


def apply_return(package,registry,bundle,central):
    m,members=package_rows(package)
    h=read_json(bundle/"return.json")
    payload=bundle/"posts.jsonl"
    if any(m[k]!=h[k] for k in ("snapshot_id","package_id","sha256")) or h["payload_sha"]!=digest(payload):
        raise ValueError("Return manifest mismatch")
    aid=h["assignment_id"]
    with closing(readonly(registry)) as reg:
        claim=reg.execute("SELECT worker_id,sha256,assignment_id FROM assignments WHERE snapshot_id=? AND package_id=?",
                          (m["snapshot_id"],m["package_id"])).fetchone()
    if claim!=(h["worker_id"],m["sha256"],aid):
        raise ValueError("Return does not match the coordinator's signed-out package")
    if not (central/"state.sqlite").is_file():
        raise ValueError("Initialize separate coordinator database first")
    with run_lock(central), closing(sqlite3.connect(central/"state.sqlite")) as db:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("BEGIN IMMEDIATE")
        if not db.execute("SELECT 1 FROM distribution_base WHERE snapshot_id=?",(m["snapshot_id"],)).fetchone():
            raise ValueError("Coordinator belongs to a different snapshot")
        if db.execute("SELECT 1 FROM returned_packages WHERE payload_sha=? AND assignment_id=?",(h["payload_sha"],aid)).fetchone():
            verified=set()
            with payload.open(encoding="utf-8") as stream:
                for line in stream:
                    for media in json.loads(line)["result"].get("media",[]):
                        for saved in links(media.get("download",{})):
                            sha=saved["sha256"]
                            if sha in verified:
                                continue
                            stored=db.execute("SELECT file FROM distributed_contents WHERE sha=?",(sha,)).fetchone()
                            if not stored:
                                raise ValueError("Previously merged media is missing from the central ledger")
                            source=safe_file(bundle,saved["file"])
                            if digest(source)!=sha:
                                raise ValueError("Corrupt return media on replay")
                            transfer(source,safe_file(central,stored[0]),sha)
                            verified.add(sha)
            return dict(replayed=True,new_posts=0,new_attempts=0,verified_media=len(verified))
        seen=set()
        imported=events=0
        with payload.open(encoding="utf-8") as stream:
            for line in stream:
                item=json.loads(line)
                pid=item["post_id"]
                r=item["result"]
                member=members.get(pid)
                if (pid in seen or member is None or r.get("queue_id")!=pid or r["ordinal"]!=member["ordinal"]
                    or r["source_url"]!=member["source_url"] or r["status"]=="pending"
                    or (r["status"]=="success" and str(r.get("post_id"))!=pid)):
                    raise ValueError("Invalid or duplicate returned post")
                seen.add(pid)
                original=db.execute("SELECT status,ordinal,source_url FROM posts WHERE post_id=?",(pid,)).fetchone()
                if not original or original[1:]!=(member["ordinal"],member["source_url"]):
                    raise ValueError("Baseline identity differs")
                prior=db.execute("SELECT assignment_id,row_sha,updated_at FROM returned_posts WHERE post_id=?",(pid,)).fetchone()
                row_sha=hashlib.sha256(line.encode("utf-8")).hexdigest()
                if (prior and prior[0]!=aid) or (not prior and original[0]!="pending"):
                    raise ValueError("Refusing to overwrite a previously acquired post")
                if prior and prior[1]==row_sha:
                    continue
                if prior and item["updated_at"]<=prior[2]:
                    if item["updated_at"]==prior[2]:
                        raise ValueError("Conflicting equal-time observation")
                    continue
                if original[0]=="success" and r["status"]!="success":
                    raise ValueError("Refusing to downgrade an existing success")
                for asset in r.get("media",[]):
                    check_saved(asset,bundle)
                    for download in links(asset.get("download",{})):
                        name=download["file"]
                        sha=download["sha256"]
                        source=safe_file(bundle,name)
                        if digest(source)!=sha:
                            raise ValueError("Corrupt or missing media in return")
                        existing=db.execute("SELECT file FROM distributed_contents WHERE sha=?",(sha,)).fetchone()
                        if existing:
                            name=existing[0]
                        target=safe_file(central,name)
                        transfer(source,target,sha)
                        download["file"]=name
                        db.execute("INSERT OR IGNORE INTO distributed_contents VALUES(?,?)",(sha,name))
                    saved=asset.get("download") or {}
                    if saved.get("file") and saved.get("sha256"):
                        key=MediaStore._asset_key(asset,asset.get("url") or "")
                        serialized=json.dumps(saved,ensure_ascii=False)
                        db.execute("INSERT OR IGNORE INTO assets VALUES(?,?)",(key,serialized))
                        db.execute("INSERT OR IGNORE INTO distributed_cache_variants VALUES(?,?,?)",(key,saved["sha256"],serialized))
                new_attempts=0
                if not item["attempts"]:
                    raise ValueError("Missing acquisition history")
                local_ids=set()
                for a in item["attempts"]:
                    local_id=a["attempt_id"]
                    if type(local_id) is not int or local_id<1 or local_id in local_ids:
                        raise ValueError("Invalid or repeated worker attempt ID")
                    local_ids.add(local_id)
                    if a["post_id"]!=pid:
                        raise ValueError("Attempt belongs to another post")
                    raw=json.dumps(a,sort_keys=True)
                    prev=db.execute("SELECT raw FROM returned_attempts WHERE assignment_id=? AND local_id=?",(aid,a["attempt_id"])).fetchone()
                    if prev and prev[0]!=raw:
                        raise ValueError("Conflicting attempt identity")
                    if not prev:
                        db.execute("INSERT INTO returned_attempts VALUES(?,?,?,?)",(aid,a["attempt_id"],pid,raw))
                        db.execute("INSERT INTO request_history(post_id,attempted_at,status,error) VALUES(?,?,?,?)",
                                   (pid,a["attempted_at"],a["status"],a["error"]))
                        new_attempts+=1
                db.execute("""UPDATE posts SET status=?,result_json=?,attempts=attempts+?,updated_at=?,last_error=?,
                          last_attempt_json=? WHERE post_id=?""",(r["status"],json.dumps(r,ensure_ascii=False),new_attempts,
                          item["updated_at"],item["last_error"],item["last_attempt_json"],pid))
                db.execute("INSERT OR REPLACE INTO returned_posts VALUES(?,?,?,?)",(pid,aid,row_sha,item["updated_at"]))
                imported+=int(not prior)
                events+=new_attempts
        if len(seen)!=h["count"]:
            raise ValueError("Return count differs")
        db.execute("INSERT INTO returned_packages VALUES(?,?,?)",(h["payload_sha"],aid,datetime.now(timezone.utc).isoformat()))
        db.commit()
        return dict(replayed=False,new_posts=imported,new_attempts=events)


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    subs=parser.add_subparsers(dest="command",required=True)
    init=subs.add_parser("init")
    init.add_argument("--base-db",type=Path,required=True)
    init.add_argument("--base-media",type=Path)
    init.add_argument("--central",type=Path,required=True)
    init.add_argument("--snapshot-id",required=True)
    init.add_argument("--test-db-only",action="store_true",help="Isolated merge test; historical media stays in the original baseline.")
    for command in ("pack","merge"):
        p=subs.add_parser(command)
        p.add_argument("--package",type=Path,required=True)
        p.add_argument("--bundle",type=Path,required=True)
        if command=="pack":
            p.add_argument("--work",type=Path,required=True)
        else:
            p.add_argument("--registry",type=Path,required=True)
            p.add_argument("--central",type=Path,required=True)
    args=parser.parse_args()
    if args.command=="init":
        receipt=read_json(args.base_db.resolve().parent.parent/"snapshot.json")
        if receipt["snapshot_id"]!=args.snapshot_id or digest(args.base_db)!=receipt["sha256"]:
            raise ValueError("Frozen database does not match snapshot receipt")
        init_central(args.base_db,args.base_media,args.central,args.snapshot_id,args.test_db_only)
        print("Coordinator initialized")
    elif args.command=="pack":
        print(json.dumps(pack(args.package,args.work,args.bundle)))
    else:
        print(json.dumps(apply_return(args.package,args.registry,args.bundle,args.central)))
