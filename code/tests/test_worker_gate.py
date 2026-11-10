"""Offline worker policy tests. All queues and responses are synthetic."""
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from collect import run_batch
from weibo_queue import Queue, canonical_identity
from weibo_browser import BrowserBackend, _classify_page_access
from weibo_worker_gate import annotate_gate, health_response
from weibo_supervisor import SupervisedBrowserBackend, _worker


class SequenceBackend:
    def __init__(self,statuses,health=None):
        self.statuses=list(statuses);self.calls=[];self.probes=0
        self.health=health or {'status':'health_unknown','requests':1,'error':'fixture_unknown'}
    def fetch_post(self,url):
        index=len(self.calls);self.calls.append(url)
        value=self.statuses[index]
        result={'status':value} if isinstance(value,str) else dict(value)
        return {'source_url':url,'post_id':canonical_identity(url)[0],'media':[],'locations':[],'error':None if result['status']=='success' else 'fixture '+result['status'],**result}
    def probe_public_health(self):
        self.probes+=1;return dict(self.health)


class WorkerPolicyTests(unittest.TestCase):
    def execute(self,statuses,health=None,no_interface=False,**options):
        with tempfile.TemporaryDirectory() as directory:
            out=Path(directory);q=Queue(out/'state.sqlite');backend=SequenceBackend(statuses,health)
            if no_interface:backend.probe_public_health=None
            try:
                for index in range(len(statuses)+1):
                    q.add_source({'_source_filepath':'fixture.xlsx','_sheet':'fixture','_excel_row':index+2,
                                  'source_url':'https://weibo.com/detail/'+str(1234567890100+index)})
                q.db.commit();stream=io.StringIO()
                with redirect_stdout(stream):
                    run=run_batch(q,out,lambda:backend,max_posts=len(statuses),worker_mode=True,delay=0,min_free_gb=0,verify_media=False,**options)
                rows=[dict(x) for x in q.db.execute('SELECT status,attempts,last_error,result_json FROM posts ORDER BY ordinal')]
                history=[dict(x) for x in q.db.execute('SELECT status,error FROM request_history ORDER BY attempt_id')]
                return run,rows,history,backend,stream.getvalue()
            finally:q.close()

    def test_six_explicit_post_logins_then_success_never_count_stop(self):
        run,rows,history,backend,_=self.execute([{'status':'login_required','gate_scope':'post'}]*6+['success'])
        self.assertIsNone(run['stop_reason']);self.assertEqual(run['metadata_requests'],7)
        self.assertEqual(run['login_skipped'],6);self.assertEqual(run['health_probe_count'],0)
        self.assertEqual(len(history),7);self.assertTrue(all(x['attempts']==1 for x in rows[:7]))
        self.assertEqual(rows[7]['attempts'],0);self.assertEqual(backend.probes,0)

    def test_many_unknown_logins_and_unknown_health_continue_with_one_probe(self):
        run,rows,history,backend,log=self.execute(['session_required']*25+['success'])
        self.assertIsNone(run['stop_reason']);self.assertEqual(run['metadata_requests'],26)
        self.assertEqual(run['health_probe_count'],1);self.assertEqual(backend.probes,1)
        self.assertEqual(run['health_requests'],1);self.assertEqual(run['request_calls_total'],27)
        self.assertEqual(len(history),26);self.assertTrue(all(x['attempts']==1 for x in rows[:26]))
        self.assertEqual(log.count('"event": "worker_health_probe"'),1)
        self.assertEqual(json.loads(rows[0]['result_json'])['gate_scope'],'unknown')

    def test_missing_health_control_or_generic_landing_login_does_not_pause(self):
        for no_interface,health in [(True,None),(False,{'status':'session_required','requests':1,'error':'generic_login'})]:
            with self.subTest(no_interface=no_interface):
                run,rows,history,_,_=self.execute(['login_required']*6+['success'],health,no_interface)
                self.assertIsNone(run['stop_reason']);self.assertEqual(len(history),7)
                self.assertEqual(rows[6]['status'],'success')
                self.assertEqual(run['health_requests'],0 if no_interface else 1)

    def test_platform_gates_pause_immediately_after_saving_current_post(self):
        for gate in ('captcha_required','rate_limited','visitor_gate','browser_session_required'):
            with self.subTest(gate=gate):
                run,rows,history,backend,_=self.execute([gate,'success'])
                self.assertEqual(run['stop_reason'],gate);self.assertEqual(run['metadata_requests'],1)
                self.assertEqual(run['stop_scope'],'session_or_environment')
                self.assertEqual(rows[0]['status'],gate);self.assertEqual(rows[1]['attempts'],0)
                self.assertEqual(len(history),1);self.assertEqual(backend.probes,0)

    def test_probe_gate_pauses_without_polluting_post_attempts_or_history(self):
        run,rows,history,_,_=self.execute(['session_required']*6+['success'],{'status':'visitor_gate','requests':1,'error':'fixture visitor'})
        self.assertEqual(run['stop_reason'],'visitor_gate');self.assertEqual(run['metadata_requests'],5)
        self.assertEqual(len(history),5);self.assertTrue(all(x['status']=='session_required' for x in history))
        self.assertEqual(rows[5]['status'],'pending');self.assertEqual(rows[5]['attempts'],0)
        self.assertEqual(run['health_requests'],1);self.assertEqual(run['request_calls_total'],6)

    def test_unavailable_resets_unknown_streak_and_network_breaker_remains(self):
        run,rows,history,backend,_=self.execute(['session_required']*4+[{'status':'unavailable','error':'因博主设置，内容暂不可见'}]+['session_required']*4+['success'])
        self.assertIsNone(run['stop_reason']);self.assertEqual(backend.probes,0)
        self.assertEqual(rows[4]['last_error'],'因博主设置，内容暂不可见')
        run,_,history,_,_=self.execute(['timeout','network_error','timeout','success'])
        self.assertEqual(run['stop_reason'],'repeated_network_failure');self.assertEqual(len(history),3)


class WorkerBrowserTests(unittest.TestCase):
    def response(self,payload=None,status=200,text='',url='https://weibo.com/',headers=None):
        result=SimpleNamespace(status=status,url=url,headers=headers or {},disposed=False)
        result.json=lambda:payload
        result.text=lambda:text
        result.dispose=lambda:setattr(result,'disposed',True)
        return result
    def backend(self,response,worker=True):
        backend=BrowserBackend(timeout=5,worker_mode=worker);backend._public_session_ready=True
        calls=[]
        def get(*args,**kwargs):calls.append((args,kwargs));return response
        backend.context=SimpleNamespace(request=SimpleNamespace(get=get))
        return backend,calls
    def quick(self,backend):
        return backend._fetch_from_public_endpoint('https://weibo.com/detail/1234567890100','1234567890100',{'1234567890100'},lambda p,u:p,5)

    def test_generic_endpoint_login_is_unknown_and_clears_fast_path(self):
        response=self.response({'msg':'请先登录'},status=401)
        backend,calls=self.backend(response);result=self.quick(backend)
        self.assertEqual(result['status'],'session_required');self.assertEqual(result['gate_scope'],'unknown')
        self.assertFalse(backend._public_session_ready);self.assertEqual(len(calls),1)
        self.assertEqual(result['request_source'],'public_status_endpoint');self.assertTrue(response.disposed)

    def test_endpoint_platform_gate_is_not_hidden_by_http401_or_fallback(self):
        for payload,status,expected in [({'msg':'请完成安全验证'},401,'captcha_required'),({'msg':'请求过于频繁'},401,'rate_limited')]:
            with self.subTest(expected=expected):
                backend,_=self.backend(self.response(payload,status));result=self.quick(backend)
                self.assertEqual(result['status'],expected);self.assertEqual(result['gate_scope'],'session_or_environment')
        legacy,_=self.backend(self.response({'ok':-100}),worker=False)
        self.assertIsNone(self.quick(legacy))
        worker,_=self.backend(self.response({'ok':-100}),worker=True)
        self.assertIsNone(self.quick(worker))
        self.assertEqual(worker._pending_browser_gate[0],'browser_session_required')

    def test_worker_page_visitor_gate_and_ordinary_login_panel(self):
        for snapshot in ({'url':'https://visitor.passport.weibo.com/visitor/visitor'}, {'url':'https://weibo.com/','title':'Sina Visitor System'}):
            self.assertEqual(_classify_page_access(snapshot,worker_mode=True)[0],'visitor_gate')
            self.assertIsNone(_classify_page_access(snapshot))
        login=_classify_page_access({'panels':[{'kind':'login','controls':True,'text':'登录后查看'}]},worker_mode=True)
        self.assertEqual(annotate_gate({'status':login[0]})['gate_scope'],'unknown')

    def test_health_response_only_explicit_dedicated_gates_pause(self):
        self.assertEqual(health_response(429)['status'],'rate_limited')
        self.assertEqual(health_response(200,text='<title>Sina Visitor System</title>')['status'],'health_unknown')
        self.assertEqual(health_response(200,text='{"ok":-100}')['status'],'health_unknown')
        self.assertEqual(health_response(200,text='{"msg":"请先登录"}')['status'],'session_required')
        self.assertEqual(health_response(200,text='<title>登录</title>')['gate_scope'],'unknown')
        self.assertEqual(health_response(200,text='<title>微博</title><p>作者讨论验证码、频繁、请先登录</p>')['status'],'health_reachable')

    def test_health_get_has_one_request_no_redirect_or_login_handling(self):
        response=self.response(status=302,headers={'location':'https://visitor.passport.weibo.com/visitor/visitor'})
        backend,calls=self.backend(response);result=backend.probe_public_health()
        self.assertEqual(result['status'],'health_unknown');self.assertTrue(response.disposed)
        self.assertEqual(len(calls),1);self.assertEqual(calls[0][0],('https://weibo.com/',))
        self.assertEqual(calls[0][1]['max_redirects'],0)
        self.assertLessEqual(calls[0][1]['timeout'],8000)

    def test_supervisor_health_rpc_and_worker_mode_forwarding(self):
        supervisor=SupervisedBrowserBackend(worker_mode=True);sent=[]
        supervisor._start=lambda:None
        supervisor._connection=SimpleNamespace(send=sent.append,poll=lambda _:True,recv=lambda:{'kind':'result','result':{'status':'health_unknown','requests':1}})
        self.assertEqual(supervisor.probe_public_health()['status'],'health_unknown')
        self.assertEqual(sent,[{'kind':'health','url':'https://weibo.com/'}])
        self.assertTrue(supervisor.worker_mode)
        options=[];messages=[];jobs=iter([{'kind':'health'},{'kind':'fetch','url':'fixture'},{'kind':'close'}])
        class FakeBrowser:
            context=None
            def __init__(self,**kwargs):options.append(kwargs)
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def fetch_post(self,url):return {'status':'success'}
            def probe_public_health(self):return {'status':'health_unknown','requests':1}
        conn=SimpleNamespace(send=messages.append,recv=lambda:next(jobs),close=lambda:None)
        with tempfile.TemporaryDirectory() as directory,patch('weibo_browser.BrowserBackend',FakeBrowser),patch('weibo_supervisor.faulthandler.dump_traceback_later'),patch('weibo_supervisor.faulthandler.cancel_dump_traceback_later'):
            _worker(conn,'msedge',directory,10,worker_mode=True)
        self.assertTrue(options[0]['worker_mode']);self.assertEqual(messages[1]['result']['status'],'health_unknown')
        self.assertEqual(messages[2]['result']['status'],'success')


if __name__=='__main__':unittest.main()
