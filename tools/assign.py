"""Coordinator-only package assignment. Keep the registry on one local machine."""
from contextlib import closing
import argparse
import hashlib
import json
import re
import shutil
import sqlite3
import tempfile
import uuid
from pathlib import Path


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_manifest(package):
    manifest = read_json(package.with_suffix(".manifest.json"))
    if (not re.fullmatch(r"B\d{2}-S\d{2}", package.stem)
            or package.stem != manifest["package_id"] or digest(package) != manifest["sha256"]):
        raise ValueError("Package ID or digest mismatch")
    return manifest


def assign(package, worker_id, registry, destination):
    manifest = load_manifest(package)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,59}", worker_id):
        raise ValueError("Use a short machine label with letters, digits, dot, dash or underscore")
    registry.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(registry)) as db, db:
        db.execute("""CREATE TABLE IF NOT EXISTS assignments(
            snapshot_id TEXT, package_id TEXT, sha256 TEXT, worker_id TEXT, assignment_id TEXT,
            PRIMARY KEY(snapshot_id,package_id))""")
        identity = (manifest["snapshot_id"], manifest["package_id"])
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT sha256,worker_id,assignment_id FROM assignments WHERE snapshot_id=? AND package_id=?",identity).fetchone()
        if row and row[:2] != (manifest["sha256"], worker_id):
            raise ValueError("This package is assigned already. Reconcile before handoff.")
        assignment_id = row[2] if row else uuid.uuid4().hex
        if not row:
            db.execute("INSERT INTO assignments VALUES(?,?,?,?,?)", (*identity,manifest["sha256"],worker_id,assignment_id))
    receipt = dict(zip(("snapshot_id","package_id","sha256","worker_id","assignment_id"), (*identity,manifest["sha256"],worker_id,assignment_id)))
    target = destination / package.stem
    destination.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if read_json(target / package.with_suffix(".assignment.json").name) != receipt or load_manifest(target / package.name) != manifest:
            raise ValueError("Existing handoff directory differs")
    else:
        with tempfile.TemporaryDirectory(dir=destination) as tmp:
            stage = Path(tmp) / package.stem
            stage.mkdir()
            for path in (package, package.with_suffix(".manifest.json")):
                shutil.copyfile(path, stage/path.name)
            write_json(stage/package.with_suffix(".assignment.json").name, receipt)
            stage.rename(target)
    return target / package.name


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--package",type=Path,required=True)
    p.add_argument("--worker-id",required=True)
    p.add_argument("--registry",type=Path,required=True)
    p.add_argument("--destination",type=Path,required=True)
    p.add_argument("--catalog",type=Path,help="Authoritative partition catalog; defaults to the package plan's central.sqlite")
    a=p.parse_args()
    catalog=a.catalog or (a.package.resolve().parents[2]/"central.sqlite")
    manifest=load_manifest(a.package)
    with closing(sqlite3.connect(catalog.resolve().as_uri()+"?mode=ro",uri=True)) as authority:
        frozen=authority.execute("SELECT snapshot_id,sha256,count FROM packages WHERE package_id=?",(manifest["package_id"],)).fetchone()
    if frozen!=(manifest["snapshot_id"],manifest["sha256"],manifest["count"]):
        raise ValueError("Package does not match the frozen central catalog")
    print(assign(a.package,a.worker_id,a.registry,a.destination))
