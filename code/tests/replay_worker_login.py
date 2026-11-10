"""Replay seven real package jobs with offline responses in a disposable DB.

The package is read only. Job identities only appear in the explicit report,
which must be outside the published repository. No real worker DB is opened.
"""
from contextlib import redirect_stdout
from datetime import datetime, timezone
import argparse
import hashlib
import io
import itertools
import json
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import collect
from weibo_queue import Queue, canonical_identity


def sha(path):
    with Path(path).open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--package-file',type=Path,required=True)
    parser.add_argument('--report',type=Path,required=True)
    args=parser.parse_args(argv)
    report=args.report.resolve();repo=Path(__file__).resolve().parents[2]
    if report.is_relative_to(repo):parser.error('The private replay report must be outside the published repository.')
    package=args.package_file.resolve();before=sha(package)
    with package.open(encoding='utf-8') as stream:jobs=[json.loads(line) for line in itertools.islice(stream,7)]
    if len(jobs)!=7:raise ValueError('At least seven package jobs are needed.')
    jobs.sort(key=lambda row:row['ordinal'])
    if any(canonical_identity(row['source_url'])[0]!=row['post_id'] for row in jobs):raise ValueError('Package post identity mismatch.')
    manifest_path=package.with_suffix('.manifest.json')
    manifest=json.loads(manifest_path.read_text(encoding='utf-8')) if manifest_path.is_file() else {}
    if manifest.get('sha256') and manifest['sha256']!=before:raise ValueError('Package checksum mismatch.')
    class OfflineBackend:
        calls=0
        def fetch_post(self,url):
            self.calls+=1;status='login_required' if self.calls<=6 else 'success'
            return {'source_url':url,'post_id':canonical_identity(url)[0],'status':status,'media':[],'locations':[],
                    'error':'offline_fixture_explicit_login_requirement' if status!='success' else None}
        def probe_public_health(self):
            return {'status':'health_unknown','gate_scope':'unknown','requests':1,'error':'offline_fixture_generic_login_scope_unknown'}
    report.parent.mkdir(parents=True,exist_ok=True)
    output=io.StringIO()
    with tempfile.TemporaryDirectory(prefix='gate-replay-',dir=report.parent) as directory:
        out=Path(directory);queue=Queue(out/'state.sqlite');backend=OfflineBackend()
        try:
            for index,job in enumerate(jobs):
                queue.add_source({'_source_filepath':'package:'+str(manifest.get('package_id','fixture')),'_sheet':'jobs','_excel_row':index+1,'source_url':job['source_url']})
                queue.db.execute('UPDATE posts SET ordinal=? WHERE post_id=?',(job['ordinal'],job['post_id']))
            queue.db.commit()
            with patch('urllib.request.OpenerDirector.open',side_effect=AssertionError('Network is forbidden in offline replay')),patch('socket.create_connection',side_effect=AssertionError('Network is forbidden in offline replay')),redirect_stdout(output):
                run=collect.run_batch(queue,out,lambda:backend,max_posts=7,delay=0,min_free_gb=0,verify_media=False,worker_mode=True,login_gate_limit=5)
            rows=[dict(x) for x in queue.db.execute('SELECT post_id,ordinal,status,attempts,last_error FROM posts ORDER BY ordinal')]
            history=[dict(x) for x in queue.db.execute('SELECT post_id,status,error FROM request_history ORDER BY attempt_id')]
            assert run['stop_reason'] is None and run['metadata_requests']==7
            assert [r['status'] for r in rows]==['login_required']*6+['success']
            assert len(history)==7 and all(r['attempts']==1 for r in rows)
            assert run['health_probe_count']==1 and run['health_requests']==1
            disposable_db=str(out/'state.sqlite')
        finally:queue.close()
    assert sha(package)==before
    result={'generated_at_utc':datetime.now(timezone.utc).isoformat(),'offline':True,'real_network_requests':0,
            'real_worker_db_opened':False,'disposable_db_removed':not Path(disposable_db).exists(),
            'package_id':manifest.get('package_id'),'snapshot_id':manifest.get('snapshot_id'),
            'package_sha256':before,'package_bytes_unchanged':True,'collector_code_sha256':sha(collect.__file__),
            'scenario':'Same package seven jobs: six simulated login requirements, one simulated success; unknown landing diagnostic does not force pause.',
            'worker_summary':run,'post_attempts':rows,'request_history':history,
            'diagnostic_does_not_increment_post_attempts':len(history)==7 and sum(x['attempts'] for x in rows)==7,
            'events':[json.loads(line) for line in output.getvalue().splitlines() if line.strip()]}
    report.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'report':str(report),'offline':True,'simulated_logins':6,'simulated_successes':1,
                      'metadata_attempts':7,'history_rows':len(history),'stop_reason':run['stop_reason'],
                      'real_network_requests':0,'disposable_db_removed':result['disposable_db_removed']}))
    return 0


if __name__=='__main__':raise SystemExit(main())
