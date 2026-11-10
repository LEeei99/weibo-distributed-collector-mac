"""Export deferred login, other access gates, and transient errors offline."""
from __future__ import annotations
import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from weibo_worker_gate import annotate_gate

LOGIN_STATUSES = {'login_required', 'session_required'}
ACCESS_STATUSES = {'visitor_gate', 'browser_session_required', 'captcha_required', 'rate_limited'}
TRANSIENT_STATUSES = {'timeout', 'network_error', 'http_error', 'server_error', 'parse_error', 'unknown', 'unexpected_response'}
FIELDS = ['queue_id','post_url','status','error','gate_scope','gate_evidence','access_disposition','attempts','updated_at','source_workbook','source_sheet','source_excel_row','source_region','export_post_url']

def safe_cell(value):
    text='' if value is None else str(value)
    return "'"+text if text.lstrip().startswith(('=','+','-','@','\t','\r')) else text

def export_access_lists(out):
    out=Path(out).resolve()
    if out.drive.upper()=='G:':
        raise ValueError('Use a local output directory.')
    checkpoint=out/'state.sqlite'
    if not checkpoint.is_file():
        raise ValueError('No prepared checkpoint exists. Prepare the input queue first.')
    db=sqlite3.connect(checkpoint.as_uri()+'?mode=ro',uri=True)
    db.row_factory=sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    result={'generated_at':datetime.now(timezone.utc).isoformat(),'checkpoint':str(checkpoint),'lists':{}}
    try:
        db.execute('BEGIN')
        result['post_statuses']=dict(db.execute('SELECT status,COUNT(*) FROM posts GROUP BY status').fetchall())
        result['access_dispositions']={'post_restricted':0,'post_missing':0,'unavailable_unknown':0}
        for filename,statuses,required in [('login_required_posts.csv',LOGIN_STATUSES,None),('access_review_posts.csv',ACCESS_STATUSES,None),('transient_failures.csv',TRANSIENT_STATUSES,None),('unavailable_posts.csv',{'unavailable'},None),('permission_review_posts.csv',{'unavailable'},'post_restricted')]:
            placeholders=','.join('?' for _ in statuses)
            values=sorted(statuses)
            unique_ids=set()
            rows=db.execute(f"""SELECT p.post_id,p.source_url,p.status,p.last_error,p.attempts,p.updated_at,p.result_json,
                s.workbook,s.sheet,s.excel_row,s.row_json FROM posts p JOIN sources s ON s.post_id=p.post_id
                WHERE p.status IN ({placeholders}) ORDER BY p.ordinal,s.source_id""",values)
            temporary=out/(filename+'.tmp')
            count=0
            try:
                with temporary.open('w',encoding='utf-8-sig',newline='') as stream:
                    writer=csv.DictWriter(stream,fieldnames=FIELDS)
                    writer.writeheader()
                    for r in rows:
                        record=json.loads(r['result_json'])
                        evidence=annotate_gate({**record,'status':r['status'],'error':r['last_error'] or record.get('error')})
                        disposition=evidence.get('access_disposition','')
                        if required is not None and disposition!=required:
                            continue
                        if filename=='unavailable_posts.csv' and r['post_id'] not in unique_ids:
                            result['access_dispositions'][disposition]+=1
                        unique_ids.add(r['post_id'])
                        source=json.loads(r['row_json'])
                        row={'queue_id':r['post_id'],'post_url':r['source_url'],'status':r['status'],'error':r['last_error'],
                             'gate_scope':evidence.get('gate_scope',''),'gate_evidence':evidence.get('gate_evidence',''),
                             'access_disposition':disposition,
                             'attempts':r['attempts'],'updated_at':r['updated_at'],'source_workbook':r['workbook'],
                             'source_sheet':r['sheet'],'source_excel_row':r['excel_row'],'source_region':source.get('_region',''),
                             'export_post_url':source.get('原文/评论链接','')}
                        writer.writerow({k:safe_cell(v) for k,v in row.items()});count+=1
                temporary.replace(out/filename)
            finally:
                if temporary.exists(): temporary.unlink()
            result['lists'][filename]={'unique_posts':len(unique_ids),'source_rows':count,'statuses':values,'access_disposition_filter':required}
    finally:
        db.close()
    target=out/'access_review_summary.json'
    target.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    return result

def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',type=Path,required=True)
    args=parser.parse_args(argv)
    result=export_access_lists(args.out)
    print(json.dumps(result,ensure_ascii=True))
    return 0

if __name__=='__main__': raise SystemExit(main())
