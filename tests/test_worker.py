"""Offline worker protocol tests. Fake batches never access Weibo."""
from __future__ import annotations
import argparse
from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('worker', ROOT / 'tools' / 'worker.py')
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.package = self.base / 'B01-S01.jsonl'
        self.rows = [{'post_id': str(1000000000000000+i), 'ordinal': 10+i,
                      'source_url': 'https://weibo.com/detail/' + str(1000000000000000+i)}
                     for i in range(5)]
        self.write_package()
        self.assignment = {'snapshot_id': 'snapshot-test', 'package_id': 'B01-S01',
                           'sha256': self.manifest['sha256'], 'worker_id': 'PC-A',
                           'assignment_id': 'assignment-test'}
        worker.atomic_json(self.package.with_suffix('.assignment.json'), self.assignment)
        self.args = argparse.Namespace(command='run', package=self.package, worker_id='PC-A',
                      worker_root=self.base / 'local-work', max_posts=2, max_batches=1,
                      request_limit=2, until_finished=False, delay=5., login_gate_limit=5,
                      max_file_mb=100., max_run_mb=1024., min_free_gb=10., heartbeat=15.,
                      backend='browser', channel='msedge', metadata_only=False)

    def tearDown(self):
        self.temp.cleanup()

    def write_package(self):
        data = ''.join(json.dumps(row, separators=(',', ':'))+'\n' for row in self.rows).encode()
        self.package.write_bytes(data)
        self.manifest = {'schema': 'weibo-package-v1', 'package_id': 'B01-S01',
                         'snapshot_id': 'snapshot-test', 'count': len(self.rows),
                         'sha256': hashlib.sha256(data).hexdigest()}
        worker.atomic_json(self.package.with_suffix('.manifest.json'), self.manifest)

    def fake_batch(self, out, args, max_posts, runtime):
        queue = worker.Queue(out / 'state.sqlite')
        rows = list(queue.db.execute("SELECT post_id FROM posts WHERE status='pending' ORDER BY ordinal LIMIT ?", (max_posts,)))
        for row in rows:
            queue.save_result(row['post_id'], {'post_id': row['post_id'], 'status': 'success',
                              'media': [], 'locations': []}, attempted=True)
        stats = queue.stats()
        queue.close()
        worker.atomic_json(out / 'summary.json', {**stats, 'generated_at': worker.utcnow(),
                           'run': {'stop_reason': None, 'metadata_requests': len(rows)}})
        return 0

    def run_silent(self, runner=None):
        with redirect_stdout(io.StringIO()):
            return worker.run_worker(self.args, batch_runner=runner or self.fake_batch)

    def test_verified_package_and_assignment(self):
        manifest, rows = worker.load_package(self.package)
        self.assertEqual(manifest['count'], 5)
        self.assertEqual(rows, self.rows)
        self.assertEqual(worker.load_assignment(self.package, manifest, 'PC-A'), self.assignment)

    def test_hash_mismatch_rejected(self):
        with self.package.open('ab') as stream:
            stream.write(b' ')
        with self.assertRaises(ValueError):
            worker.load_package(self.package)

    def test_duplicate_and_original_row_fields_rejected(self):
        self.rows[1] = dict(self.rows[0])
        self.write_package()
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            worker.load_package(self.package)
        self.rows = [dict(self.rows[0], original_text='private source')]
        self.write_package()
        with self.assertRaisesRegex(ValueError, 'exactly'):
            worker.load_package(self.package)

    def test_link_identity_mismatch_rejected(self):
        self.rows[0]['source_url'] = self.rows[1]['source_url']
        self.write_package()
        with self.assertRaisesRegex(ValueError, 'match post_id'):
            worker.load_package(self.package)

    def test_assignment_required_and_bound(self):
        with self.assertRaises(ValueError):
            worker.load_assignment(self.package, self.manifest, 'PC-B')
        self.package.with_suffix('.assignment.json').unlink()
        with self.assertRaises(FileNotFoundError):
            self.run_silent()
        self.assertFalse(self.args.worker_root.exists())

    def test_prepare_synthetic_sources_and_stable_meta(self):
        self.args.command = 'prepare'
        def forbidden(*unused):
            self.fail('prepare must never invoke a collector')
        self.assertEqual(self.run_silent(forbidden), 0)
        out = self.args.worker_root / 'B01-S01'
        db = sqlite3.connect(out / 'state.sqlite')
        try:
            meta = dict(db.execute('SELECT key,value FROM meta'))
            self.assertEqual(meta['distributed_assignment_id'], 'assignment-test')
            self.assertEqual(meta['distributed_worker_id'], 'PC-A')
            self.assertEqual(meta['distributed_import_complete'], '1')
            self.assertEqual([r[0] for r in db.execute('SELECT ordinal FROM posts ORDER BY ordinal')], list(range(10,15)))
            sources = [json.loads(r[0]) for r in db.execute('SELECT row_json FROM sources')]
            self.assertTrue(all(s['_source_filepath'].startswith('package://') for s in sources))
            self.assertTrue(all('original_text' not in s and '标题/微博内容' not in s for s in sources))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM request_history').fetchone()[0], 0)
        finally:
            db.close()
        progress = worker.read_json(out / 'worker_progress.json')
        self.assertEqual(progress['state'], 'Prepared')
        self.assertFalse(progress['active'])

    def test_two_bounded_rounds_resume_then_complete(self):
        self.assertEqual(self.run_silent(), 0)
        out = self.args.worker_root / 'B01-S01'
        self.assertEqual(worker.source_counts(out)['requests_total'], 2)
        receipt_bytes = (out / 'receipt.json').read_bytes()
        self.assertEqual(self.run_silent(), 0)
        self.assertEqual(worker.source_counts(out)['requests_total'], 4)
        self.assertEqual((out / 'receipt.json').read_bytes(), receipt_bytes)
        self.args.request_limit = None
        self.args.until_finished = True
        self.assertEqual(self.run_silent(), 0)
        self.assertEqual(worker.source_counts(out)['requests_total'], 5)
        self.assertEqual(worker.read_json(out / 'worker_progress.json')['state'], 'Complete')
        self.assertEqual(self.run_silent(), 0)
        self.assertEqual(worker.source_counts(out)['requests_total'], 5)

    def test_gate_stops_without_next_batch(self):
        calls = []
        self.args.until_finished = True
        self.args.request_limit = None
        def gated(out, args, limit, runtime):
            calls.append(limit)
            self.fake_batch(out, args, 1, runtime)
            summary = worker.read_json(out / 'summary.json')
            summary['run']['stop_reason'] = 'captcha_required'
            worker.atomic_json(out / 'summary.json', summary)
            return 0
        self.assertEqual(self.run_silent(gated), 0)
        self.assertEqual(calls, [2])
        report = worker.read_json(self.args.worker_root / 'B01-S01' / 'worker_progress.json')
        self.assertEqual(report['stop_reason'], 'captcha_required')
        self.assertEqual(report['post_statuses']['pending'], 4)

    def test_assignment_change_cannot_reset_existing_checkpoint(self):
        self.run_silent()
        self.assignment['assignment_id'] = 'other-assignment'
        worker.atomic_json(self.package.with_suffix('.assignment.json'), self.assignment)
        with self.assertRaisesRegex(ValueError, 'receipt'):
            self.run_silent()

    def test_nonblocking_lock_retained_and_reusable(self):
        lock = self.base / 'worker.lock'
        with worker.file_lock(lock):
            with self.assertRaises(RuntimeError):
                with worker.file_lock(lock):
                    pass
        self.assertTrue(lock.exists())
        with worker.file_lock(lock):
            pass

    def test_foreign_machine_identity_rejected(self):
        self.args.worker_root.mkdir()
        worker.atomic_json(self.args.worker_root / 'host_identity.json',
                           {'host_id': 'other', 'machine_fingerprint': 'other'})
        with self.assertRaisesRegex(ValueError, 'another machine'):
            self.run_silent()

    def test_collector_command_uses_worker_mode_and_no_input(self):
        command = worker.collector_command(self.base, self.args, 2)
        self.assertIn('--worker-mode', command)
        self.assertIn('--no-export', command)
        self.assertNotIn('--input-root', command)
        self.assertNotIn('--retry-failed', command)

    def test_source_version_is_deterministic_recomputable_and_sensitive(self):
        root = self.base / 'code-fixture'
        for relative in worker.CODE_VERSION_PATHS:
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(('fixture:' + relative + '\n').encode())
        first = worker.code_version(root)
        self.assertEqual(first, worker.code_version(root))
        payload = {'scheme': first['scheme'], 'normalization': first['normalization'], 'files': first['files']}
        expected = hashlib.sha256(json.dumps(payload, sort_keys=True,
                           separators=(',', ':')).encode('utf-8')).hexdigest()
        self.assertEqual(first['sha256'], expected)
        self.assertEqual(first['version'], 'ww1-' + expected[:16])
        self.assertEqual([row['path'] for row in first['files']], list(worker.CODE_VERSION_PATHS))
        for relative in worker.CODE_VERSION_PATHS:
            path = root / relative
            path.write_bytes(b'\xef\xbb\xbf' + path.read_bytes().replace(b'\n', b'\r\n'))
        self.assertEqual(first, worker.code_version(root))
        (root / 'code' / 'weibo_worker_gate.py').write_bytes(b'changed gate code\n')
        self.assertNotEqual(first['version'], worker.code_version(root)['version'])

    def test_run_receipts_are_retained_and_version_is_not_hashed_per_heartbeat(self):
        with patch.object(worker, 'code_version', wraps=worker.code_version) as version:
            self.run_silent()
            self.assertEqual(version.call_count, 1)
            out = self.args.worker_root / 'B01-S01'
            runtime = worker.read_json(out / 'worker_runtime.json')
            first_id = runtime['run_id']
            first_receipt = worker.read_json(out / 'run_receipts' / (first_id + '.json'))
            self.assertEqual(first_receipt['requests'], 2)
            self.assertEqual(first_receipt['code_version'], runtime['code_version']['version'])
            self.assertIsNotNone(first_receipt['finished_at_utc'])
            self.assertEqual(set(first_receipt), {'code_version', 'code_manifest_sha256',
                         'package_id', 'started_at_utc', 'finished_at_utc', 'requests', 'stop_reason'})
            manifest_path = out / 'code_versions' / (first_receipt['code_manifest_sha256'] + '.json')
            self.assertEqual(worker.read_json(manifest_path), runtime['code_version'])
            worker.publish(out, runtime)
            worker.publish(out, runtime)
            self.assertEqual(version.call_count, 1)
            self.run_silent()
            self.assertEqual(version.call_count, 2)
            self.assertEqual(worker.read_json(out / 'run_receipts' / (first_id + '.json')), first_receipt)
            self.assertEqual(len(list((out / 'run_receipts').glob('*.json'))), 2)
            self.assertEqual(len(list((out / 'code_versions').glob('*.json'))), 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
