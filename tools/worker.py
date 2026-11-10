"""Portable, package-bound Weibo worker. No Excel ingestion or shared database."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import time
import uuid

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'code'))
from weibo_queue import Queue, canonical_identity
from weibo_monitor import process_identity, read_snapshot, render_board
from weibo_platform import file_lock, local_directory, machine_fingerprint

SCHEMA = 'weibo-worker-v1'
SAFE_ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$')
CONTINUE_STOPS = {None, 'media_budget'}
CODE_VERSION_SCHEME = 'weibo-worker-code-v1'
CODE_NORMALIZATION = 'utf8-bom-removed;crlf-to-lf-v1'
# Fixed paths, including the gate and process supervisor; never hash data,
# machine paths, timestamps, .git, environments, or downloaded media.
CODE_VERSION_PATHS = (
    'code/collect.py',
    'code/requirements.txt',
    'code/weibo_access_export.py',
    'code/weibo_backend.py',
    'code/weibo_browser.py',
    'code/weibo_exports.py',
    'code/weibo_inputs.py',
    'code/weibo_locations.py',
    'code/weibo_media.py',
    'code/weibo_monitor.py',
    'code/weibo_platform.py',
    'code/weibo_queue.py',
    'code/weibo_supervisor.py',
    'code/weibo_worker_gate.py',
    'requirements.txt',
    'tools/worker.py',
)


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with temporary.open('w', encoding='utf-8', newline='\n') as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        # Short-lived readers can hold a Windows file handle across replacement.
        for attempt in range(10):
            try:
                temporary.replace(path)
                break
            except PermissionError:
                if attempt == 9:
                    raise
                time.sleep(.05)
    finally:
        if temporary.exists():
            temporary.unlink()


def read_json(path):
    value = json.loads(Path(path).read_text(encoding='utf-8-sig'))
    if not isinstance(value, dict):
        raise ValueError('Expected a JSON object: ' + str(path))
    return value


def code_version(root=ROOT):
    """Git-independent, reproducible source manifest, computed once per run.

    Hash the UTF-8 canonical JSON {scheme,normalization,files}, with
    sort_keys=True and separators=(',', ':'). File SHA-256 uses UTF-8 text
    after removing an optional BOM and converting CRLF to LF. Git checkout
    line-ending conversion and source ZIP extraction produce the same version.
    """
    files = []
    for relative in CODE_VERSION_PATHS:
        source = (Path(root) / relative).read_bytes().decode('utf-8-sig')
        canonical = source.replace('\r\n', '\n').encode('utf-8')
        digest = hashlib.sha256(canonical).hexdigest()
        files.append({'path': relative, 'sha256': digest})
    manifest = {'scheme': CODE_VERSION_SCHEME, 'normalization': CODE_NORMALIZATION, 'files': files}
    serialized = json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode('utf-8')
    digest = hashlib.sha256(serialized).hexdigest()
    return {**manifest, 'sha256': digest, 'version': 'ww1-' + digest[:16]}


def save_run_receipt(out, runtime, requests_total):
    """Retain a short, data-free receipt per run and a reusable code manifest."""
    version = runtime.get('code_version')
    if version is None:  # Offline prepare is not a collection run.
        return
    versions = out / 'code_versions'
    versions.mkdir(exist_ok=True)
    manifest_path = versions / (version['sha256'] + '.json')
    if manifest_path.exists():
        if read_json(manifest_path) != version:
            raise ValueError('Existing code-version manifest differs')
    else:
        atomic_json(manifest_path, version)
    receipts = out / 'run_receipts'
    receipts.mkdir(exist_ok=True)
    atomic_json(receipts / (runtime['run_id'] + '.json'), {
        'code_version': version['version'],
        'code_manifest_sha256': version['sha256'],
        'package_id': runtime['package_id'],
        'started_at_utc': runtime['started_at_utc'],
        'finished_at_utc': runtime.get('finished_at_utc'),
        'requests': requests_total - runtime['requests_at_start'],
        'stop_reason': runtime.get('stop_reason'),
    })


def checked_id(value, name):
    if not isinstance(value, str) or not SAFE_ID.fullmatch(value):
        raise ValueError(name + ' must be a short identifier using letters, numbers, dot, underscore or hyphen')
    return value


def load_package(path):
    path = Path(path).expanduser().resolve()
    manifest_path = path.with_suffix('.manifest.json')
    manifest = read_json(manifest_path)
    if manifest.get('schema') != 'weibo-package-v1':
        raise ValueError('Unsupported package manifest schema')
    package_id = checked_id(manifest.get('package_id'), 'package_id')
    snapshot_id = checked_id(manifest.get('snapshot_id'), 'snapshot_id')
    if path.stem != package_id:
        raise ValueError('Package filename must match package_id')
    digest = hashlib.sha256()
    rows, ids, ordinals = [], set(), set()
    with path.open('rb') as stream:
        for number, line in enumerate(stream, 1):
            if len(line) > 65536 or not line.strip():
                raise ValueError('Invalid package line ' + str(number))
            digest.update(line)
            raw = json.loads(line.decode('utf-8'))
            if not isinstance(raw, dict) or set(raw) != {'post_id', 'ordinal', 'source_url'}:
                raise ValueError('Package lines must contain exactly post_id, ordinal, source_url')
            post_id, ordinal, url = raw['post_id'], raw['ordinal'], raw['source_url']
            if not isinstance(post_id, str) or not re.fullmatch(r'[1-9][0-9]{0,29}', post_id):
                raise ValueError('Invalid numeric post_id at line ' + str(number))
            if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 1:
                raise ValueError('Invalid ordinal at line ' + str(number))
            if not isinstance(url, str) or len(url) > 4096:
                raise ValueError('Invalid source_url at line ' + str(number))
            identity, unused = canonical_identity(url)
            if identity != post_id:
                raise ValueError('source_url does not match post_id at line ' + str(number))
            if post_id in ids or ordinal in ordinals:
                raise ValueError('Duplicate post_id or ordinal within package')
            ids.add(post_id)
            ordinals.add(ordinal)
            rows.append(raw)
    count = manifest.get('count')
    if isinstance(count, bool) or not isinstance(count, int) or count != len(rows) or count < 1:
        raise ValueError('Package count does not match manifest')
    if digest.hexdigest() != manifest.get('sha256'):
        raise ValueError('Package SHA-256 does not match manifest')
    return manifest, rows


def load_assignment(package, manifest, worker_id):
    assignment = read_json(Path(package).with_suffix('.assignment.json'))
    expected = {'snapshot_id': manifest['snapshot_id'], 'package_id': manifest['package_id'],
                'sha256': manifest['sha256'], 'worker_id': worker_id}
    if any(assignment.get(key) != value for key, value in expected.items()):
        raise ValueError('Assignment does not match this worker/package/snapshot/hash')
    checked_id(assignment.get('assignment_id'), 'assignment_id')
    return assignment


def binding(manifest, worker_id, assignment_id):
    return {
        'distributed_schema': SCHEMA,
        'distributed_worker_id': worker_id,
        'distributed_package_id': manifest['package_id'],
        'distributed_snapshot_id': manifest['snapshot_id'],
        'distributed_package_sha256': manifest['sha256'],
        'distributed_package_count': str(manifest['count']),
        'distributed_assignment_id': assignment_id,
    }


def import_package(out, manifest, rows, worker_id, assignment_id):
    """One transaction, preserving central ordinals and no original Excel fields."""
    expected = binding(manifest, worker_id, assignment_id)
    queue = Queue(out / 'state.sqlite')
    try:
        old = dict(queue.db.execute('SELECT key,value FROM meta'))
        if old.get('distributed_import_complete') == '1':
            for key, value in expected.items():
                if old.get(key) != value:
                    raise ValueError('Existing worker checkpoint has a different ' + key)
            count = queue.db.execute('SELECT COUNT(*) FROM posts').fetchone()[0]
            source_count = queue.db.execute('SELECT COUNT(*) FROM sources').fetchone()[0]
            if count != len(rows) or source_count != len(rows):
                raise ValueError('Imported worker checkpoint has unexpected queue/source counts')
            # Validate the package mapping, including original ordinals, on every resume.
            actual = {(r['post_id'], r['ordinal'], r['source_url']) for r in
                      queue.db.execute('SELECT post_id,ordinal,source_url FROM posts')}
            wanted = {(r['post_id'], r['ordinal'], r['source_url']) for r in rows}
            if actual != wanted:
                raise ValueError('Worker checkpoint membership differs from verified package')
            return queue.stats(), False
        if old or queue.db.execute('SELECT COUNT(*) FROM posts').fetchone()[0]:
            raise ValueError('Refusing to import into a nonempty or incompletely bound checkpoint')
        source_name = 'package://' + manifest['snapshot_id'] + '/' + manifest['package_id']
        with queue.db:
            for row in rows:
                record = {'queue_id': row['post_id'], 'source_url': row['source_url'],
                          'post_id': None, 'status': 'pending', 'media': [], 'locations': [],
                          'error': None, 'ordinal': row['ordinal']}
                queue.db.execute('INSERT INTO posts(post_id,ordinal,source_url,status,result_json) VALUES(?,?,?,?,?)',
                                 (row['post_id'], row['ordinal'], row['source_url'], 'pending',
                                  json.dumps(record, ensure_ascii=False)))
                source = {'_source_filepath': source_name, '_sheet': 'worker_lookup',
                          '_excel_row': row['ordinal'], '_queue_id': row['post_id'],
                          'source_url': row['source_url'], 'post_id': row['post_id'],
                          'package_id': manifest['package_id'], 'snapshot_id': manifest['snapshot_id']}
                queue.db.execute('INSERT INTO sources(workbook,sheet,excel_row,post_id,row_json) VALUES(?,?,?,?,?)',
                                 (source_name, 'worker_lookup', row['ordinal'], row['post_id'],
                                  json.dumps(source, ensure_ascii=False)))
            for key, value in {**expected, 'distributed_import_complete': '1'}.items():
                queue.db.execute('INSERT INTO meta VALUES(?,?)', (key, value))
        return queue.stats(), True
    finally:
        queue.close()


def source_counts(out):
    db = sqlite3.connect((out / 'state.sqlite').as_uri() + '?mode=ro', uri=True, timeout=1)
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        return {'requests_total': db.execute('SELECT COUNT(*) FROM request_history').fetchone()[0],
                'post_statuses': dict(db.execute('SELECT status,COUNT(*) FROM posts GROUP BY status'))}
    finally:
        db.close()


def own_host(worker_root):
    fingerprint = machine_fingerprint()
    with file_lock(worker_root / 'host_identity.lock'):
        path = worker_root / 'host_identity.json'
        if path.exists():
            saved = read_json(path)
            if saved.get('machine_fingerprint') != fingerprint:
                raise ValueError('Worker root belongs to another machine; controller migration is required')
            return saved['host_id']
        host_id = uuid.uuid4().hex
        atomic_json(path, {'schema': SCHEMA, 'host_id': host_id,
                          'machine_fingerprint': fingerprint, 'created_at_utc': utcnow()})
        return host_id


def publish(out, runtime):
    runtime['heartbeat_at_utc'] = utcnow()
    snapshot = read_snapshot(out)
    counts = source_counts(out)
    progress = {
        'schema': SCHEMA, 'worker_id': runtime['worker_id'], 'host_id': runtime['host_id'],
        'package_id': runtime['package_id'], 'snapshot_id': runtime['snapshot_id'],
        'package_sha256': runtime['package_sha256'], 'package_count': runtime['package_count'],
        'run_id': runtime['run_id'], 'heartbeat_at_utc': runtime['heartbeat_at_utc'],
        'state': runtime['state'], 'active': runtime['active'], 'stop_reason': runtime.get('stop_reason'),
        'requests_this_invocation': counts['requests_total'] - runtime['requests_at_start'],
        'requests_total': counts['requests_total'], 'post_statuses': counts['post_statuses'],
        'latest_request': snapshot.get('latest_request'), 'runtime': runtime,
    }
    atomic_json(out / 'worker_runtime.json', runtime)
    atomic_json(out / 'worker_progress.json', progress)
    return progress


def collector_command(out, args, max_posts):
    command = [sys.executable, '-u', '-B', str(ROOT / 'code' / 'collect.py'),
               '--out', str(out), '--worker-mode', '--max-posts', str(max_posts),
               '--delay', str(args.delay), '--login-gate-limit', str(args.login_gate_limit),
               '--max-file-mb', str(args.max_file_mb), '--max-run-mb', str(args.max_run_mb),
               '--min-free-gb', str(args.min_free_gb), '--backend', args.backend,
               '--channel', args.channel, '--no-export']
    if args.metadata_only:
        command.append('--metadata-only')
    return command


def execute_batch(out, args, max_posts, runtime):
    """Child shares the console group so Ctrl+C reaches both safely."""
    log_path = out / ('collection_' + runtime['run_id'] + '.log')
    runtime['log_file'] = str(log_path)
    with log_path.open('a', encoding='utf-8') as log:
        process = subprocess.Popen(collector_command(out, args, max_posts), cwd=ROOT,
                                   stdout=log, stderr=subprocess.STDOUT,
                                   env={**os.environ, 'PYTHONIOENCODING': 'utf-8'})
        runtime['collector_pid'] = process.pid
        runtime['collector_identity'] = process_identity(process.pid)
        next_heartbeat = 0.0
        try:
            while process.poll() is None:
                if time.monotonic() >= next_heartbeat:
                    publish(out, runtime)
                    next_heartbeat = time.monotonic() + args.heartbeat
                time.sleep(.5)
        except KeyboardInterrupt:
            runtime['state'] = 'Stopping'
            runtime['stop_reason'] = 'interrupted'
            publish(out, runtime)
            # The console delivered Ctrl+C to the child too. Never kill a PID.
            print('Waiting for the collector to save its checkpoint. Do not close this window.', flush=True)
            while process.poll() is None:
                try:
                    process.wait(timeout=1)
                except (subprocess.TimeoutExpired, KeyboardInterrupt):
                    continue
            raise
        finally:
            runtime['collector_exit_code'] = process.poll()
    return process.returncode


def run_worker(args, *, batch_runner=execute_batch):
    manifest, rows = load_package(args.package)
    worker_id = checked_id(args.worker_id, 'worker_id')
    assignment = load_assignment(args.package, manifest, worker_id)
    worker_root = local_directory(args.worker_root)
    worker_root.mkdir(parents=True, exist_ok=True)
    host_id = own_host(worker_root)
    out = worker_root / manifest['package_id']
    out.mkdir(parents=True, exist_ok=True)
    with file_lock(out / 'worker.lock'):
        receipt_path = out / 'receipt.json'
        if receipt_path.exists():
            receipt = read_json(receipt_path)
            if any(receipt.get(key) != value for key, value in
                   {'worker_id': worker_id, 'host_id': host_id, 'package_id': manifest['package_id'],
                    'snapshot_id': manifest['snapshot_id'], 'package_sha256': manifest['sha256'],
                    'assignment_id': assignment['assignment_id']}.items()):
                raise ValueError('Existing receipt belongs to another worker/host/package')
        with file_lock(out / 'collector.lock'):
            stats, imported = import_package(out, manifest, rows, worker_id, assignment['assignment_id'])
        if not receipt_path.exists():
            atomic_json(receipt_path, {'schema': SCHEMA, 'worker_id': worker_id, 'host_id': host_id,
                        'package_id': manifest['package_id'], 'snapshot_id': manifest['snapshot_id'],
                        'package_sha256': manifest['sha256'], 'package_count': manifest['count'],
                        'assignment_id': assignment['assignment_id'],
                        'accepted_at_utc': utcnow(), 'synthetic_sources_only': True,
                        'assignment_note': 'Local acknowledgement; central assignment prevents cross-host duplicate claims'})
        runtime = {'schema': SCHEMA, 'worker_id': worker_id, 'host_id': host_id,
                   'package_id': manifest['package_id'], 'snapshot_id': manifest['snapshot_id'],
                   'package_sha256': manifest['sha256'], 'package_count': manifest['count'],
                   'assignment_id': assignment['assignment_id'],
                   'run_id': uuid.uuid4().hex, 'pid': os.getpid(),
                   'process_identity': process_identity(os.getpid()), 'started_at_utc': utcnow(),
                   'requests_at_start': stats['metadata_requests_total'],
                   'state': 'Prepared' if args.command == 'prepare' else 'Running',
                   'active': args.command != 'prepare', 'stop_reason': None,
                   'batches_completed': 0, 'imported_now': imported}
        runtime['code_version'] = code_version() if args.command == 'run' else None
        # Standard local monitor compatibility. Its PID is explicitly this local worker.
        pipeline_runtime = {'active': runtime['active'], 'pid': os.getpid(),
                            'process_identity': runtime['process_identity'], 'run_id': runtime['run_id'],
                            'started_at': runtime['started_at_utc'], 'login_gate_limit': args.login_gate_limit,
                            'delay': args.delay, 'stop_reason': None}
        atomic_json(out / 'pipeline_runtime.json', pipeline_runtime)
        save_run_receipt(out, runtime, runtime['requests_at_start'])
        exit_code = 0
        try:
            publish(out, runtime)
            print(json.dumps({'worker_id': worker_id, 'package_id': manifest['package_id'],
                              'out': str(out), 'state': runtime['state'],
                              'requests_at_start': runtime['requests_at_start'],
                              'code_version': (runtime['code_version'] or {}).get('version'),
                              'note': 'Ctrl+C in this worker console saves progress; monitor in a separate window'},
                             ensure_ascii=False), flush=True)
            if args.command == 'prepare':
                runtime['stop_reason'] = 'prepared_only'
            else:
                while True:
                    counts = source_counts(out)
                    used = counts['requests_total'] - runtime['requests_at_start']
                    if counts['post_statuses'].get('pending', 0) == 0:
                        runtime['stop_reason'] = 'package_complete'
                        break
                    if args.request_limit is not None and used >= args.request_limit:
                        runtime['stop_reason'] = 'invocation_request_limit'
                        break
                    if not args.until_finished and runtime['batches_completed'] >= args.max_batches:
                        runtime['stop_reason'] = 'batch_limit'
                        break
                    limit = min(args.max_posts, args.request_limit - used) if args.request_limit is not None else args.max_posts
                    before = counts['requests_total']
                    code = batch_runner(out, args, limit, runtime)
                    runtime['batches_completed'] += 1
                    summary = read_json(out / 'summary.json')
                    reason = (summary.get('run') or {}).get('stop_reason')
                    publish(out, runtime)
                    if code:
                        runtime['stop_reason'] = reason or 'collector_error'
                        exit_code = code if 0 < code < 126 else 1
                        break
                    if reason not in CONTINUE_STOPS:
                        runtime['stop_reason'] = reason
                        break
                    after = source_counts(out)['requests_total']
                    if after == before:
                        runtime['stop_reason'] = 'no_progress'
                        break
        except KeyboardInterrupt:
            runtime['stop_reason'] = 'interrupted'
            exit_code = 130
        except Exception as error:
            runtime['stop_reason'] = 'worker_error'
            runtime['error'] = type(error).__name__ + ': ' + str(error)
            print(runtime['error'], file=sys.stderr, flush=True)
            exit_code = 1
        finally:
            runtime['active'] = False
            runtime['state'] = ('Complete' if runtime['stop_reason'] == 'package_complete' else
                                'Prepared' if runtime['stop_reason'] == 'prepared_only' else 'Stopped')
            runtime['finished_at_utc'] = utcnow()
            pipeline_runtime.update(active=False, finished_at=runtime['finished_at_utc'],
                                    stop_reason=runtime['stop_reason'], exit_code=exit_code)
            atomic_json(out / 'pipeline_runtime.json', pipeline_runtime)
            progress = publish(out, runtime)
            save_run_receipt(out, runtime, progress['requests_total'])
            print(json.dumps({'out': str(out), 'worker_id': worker_id, 'package_id': manifest['package_id'],
                              'state': runtime['state'], 'stop_reason': runtime['stop_reason'],
                              'requests_this_invocation': progress['requests_this_invocation'],
                              'requests_total': progress['requests_total'],
                              'post_statuses': progress['post_statuses']}, ensure_ascii=False), flush=True)
        return exit_code


def monitor(args):
    out = Path(args.out).expanduser().resolve()
    while True:
        progress = read_json(out / 'worker_progress.json')
        stamp = datetime.fromisoformat(progress['heartbeat_at_utc'])
        age = max(0, (datetime.now(timezone.utc) - stamp).total_seconds())
        state = progress['state']
        if progress.get('active') and age > 90:
            state = 'Unknown (heartbeat stale; do not assume stopped)'
        if sys.stdout.isatty():
            print('\033[2J\033[H', end='')
        print('Weibo distributed worker - read only\n'
              + 'Worker: ' + progress['worker_id'] + ' | Package: ' + progress['package_id']
              + '\nState: ' + state + ' | Source heartbeat age: ' + str(round(age, 1)) + ' seconds'
              + '\nRequests total: ' + str(progress['requests_total'])
              + ' | This invocation: ' + str(progress['requests_this_invocation'])
              + '\nCounts: ' + json.dumps(progress['post_statuses'], ensure_ascii=False)
              + '\nStop reason: ' + str(progress.get('stop_reason'))
              + '\nCtrl+C closes only this monitor. It never probes remote process IDs.', flush=True)
        if args.once:
            return 0
        time.sleep(args.interval)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('prepare', 'run'):
        cmd = commands.add_parser(name)
        cmd.add_argument('--package', required=True, type=Path)
        cmd.add_argument('--worker-id', required=True)
        cmd.add_argument('--worker-root', type=Path, default=ROOT / 'runtime' / 'worker')
        cmd.add_argument('--max-posts', type=int, default=100)
        cmd.add_argument('--max-batches', type=int, default=1)
        cmd.add_argument('--request-limit', type=int)
        cmd.add_argument('--until-finished', action='store_true')
        cmd.add_argument('--delay', type=float, default=5)
        cmd.add_argument('--login-gate-limit', type=int, default=5)
        cmd.add_argument('--max-file-mb', type=float, default=100)
        cmd.add_argument('--max-run-mb', type=float, default=1024)
        cmd.add_argument('--min-free-gb', type=float, default=10)
        cmd.add_argument('--heartbeat', type=float, default=15)
        cmd.add_argument('--backend', choices=('browser', 'http'), default='browser')
        cmd.add_argument('--channel', default='msedge')
        cmd.add_argument('--metadata-only', action='store_true')
    cmd = commands.add_parser('monitor')
    cmd.add_argument('--out', type=Path, required=True)
    cmd.add_argument('--interval', type=float, default=5)
    cmd.add_argument('--once', action='store_true')
    args = parser.parse_args(argv)
    if args.command == 'monitor':
        if not math.isfinite(args.interval) or args.interval < 1:
            parser.error('--interval must be at least 1 second')
        return monitor(args)
    if (not 1 <= args.max_posts <= 1000 or args.max_batches < 1
            or args.request_limit is not None and args.request_limit < 1
            or args.login_gate_limit < 1 or not math.isfinite(args.delay) or args.delay < 0
            or any(not math.isfinite(value) or value <= 0 for value in
                   (args.max_file_mb, args.max_run_mb, args.min_free_gb, args.heartbeat))):
        parser.error('Counts/budgets must be positive; max-posts 1-1000; delay nonnegative')
    return run_worker(args)


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print('Monitor closed. Collection was not changed.', flush=True)
        raise SystemExit(0)
    except (OSError, ValueError, RuntimeError) as error:
        print(type(error).__name__ + ': ' + str(error), file=sys.stderr, flush=True)
        raise SystemExit(2)
