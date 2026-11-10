"""Natural visitor transitions, final gate priority and access dispositions."""
from contextlib import closing
import csv
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from weibo_browser import BrowserBackend
from weibo_queue import Queue
from weibo_worker_gate import annotate_gate
from weibo_access_export import export_access_lists
from test_browser_locations_legacy import FakePage, FakeContext, FakeResponse, POST_ID, POST_URL

VISITOR='https://visitor.passport.weibo.com/visitor/visitor'


class VisitorTimingTests(unittest.TestCase):
    def fetch(self,page):
        class BrowserError(Exception):pass
        api=types.ModuleType('playwright.sync_api');api.Error=BrowserError;api.TimeoutError=type('TimeoutError',(BrowserError,),{})
        backend=BrowserBackend(timeout=5,worker_mode=True);backend.context=FakeContext(page)
        tick=iter(range(10000))
        with patch.dict(sys.modules,{'playwright':types.ModuleType('playwright'),'playwright.sync_api':api}),patch('weibo_browser.time.monotonic',side_effect=lambda:next(tick)*.05):
            result=backend.fetch_post(POST_URL)
        self.assertTrue(page.closed)
        return result

    def test_transient_official_visitor_naturally_reaches_target_in_one_navigation(self):
        class TransitionPage(FakePage):
            gotos=0
            def goto(self,*args,**kwargs):self.gotos+=1;return super().goto(*args,**kwargs)
            def wait_for_timeout(self,value):
                self.url=POST_URL;self.snapshot={'url':POST_URL,'title':'微博','panels':[]}
                return super().wait_for_timeout(value)
        response=FakeResponse({'idstr':POST_ID,'text_raw':'target after natural redirect'})
        page=TransitionPage(navigation_url=VISITOR,snapshot={'url':VISITOR,'title':'Sina Visitor System','panels':[]},responses_after_wait=[response])
        result=self.fetch(page)
        self.assertEqual(result['status'],'success');self.assertEqual(result['post_id'],POST_ID)
        self.assertEqual(page.gotos,1)

    def test_persistent_visitor_reaches_existing_deadline_and_discards_stale_dom(self):
        class PersistentPage(FakePage):
            waits=0
            def wait_for_timeout(self,value):self.waits+=1;return super().wait_for_timeout(value)
        page=PersistentPage(navigation_url=VISITOR,snapshot={'url':VISITOR,'title':'Sina Visitor System','panels':[]},dom={'id':POST_ID,'text':'stale accessible post'})
        result=self.fetch(page)
        self.assertEqual(result['status'],'visitor_gate');self.assertIsNone(result['post'])
        self.assertGreater(page.waits,0)

    def test_explicit_challenge_rate_and_block_panel_win_immediately(self):
        for panel,status in [({'kind':'challenge','controls':True,'text':'请完成安全验证'},'captcha_required'),({'kind':'error','text':'访问频次过高'},'rate_limited'),({'kind':'error','text':'访问异常，请稍后重试'},'visitor_gate')]:
            class ChallengePage(FakePage):
                waits=0
                def wait_for_timeout(self,value):self.waits+=1;return super().wait_for_timeout(value)
            page=ChallengePage(navigation_url=VISITOR,snapshot={'url':VISITOR,'title':'Sina Visitor System','panels':[panel]},dom={'id':POST_ID,'text':'stale'})
            with self.subTest(status=status):
                result=self.fetch(page);self.assertEqual(result['status'],status);self.assertEqual(page.waits,0)
                self.assertIsNone(result['post'])


class AccessDispositionTests(unittest.TestCase):
    def test_classified_errors_separate_permission_missing_and_generic(self):
        cases=[('暂无查看权限','post_restricted'),('因博主设置，内容暂不可见','post_restricted'),('该微博不存在','post_missing'),('此微博已被作者删除','post_missing'),('该内容暂时无法查看','unavailable_unknown'),('HTTP 404','unavailable_unknown')]
        for error,expected in cases:
            with self.subTest(error=error):
                row=annotate_gate({'status':'unavailable','error':error})
                self.assertEqual(row['status'],'unavailable');self.assertEqual(row['access_disposition'],expected)
        post=annotate_gate({'status':'success','post':{'text_raw':'暂无查看权限、该微博不存在、请先登录'},'error':None})
        self.assertNotIn('access_disposition',post);self.assertNotIn('gate_scope',post)

    def test_export_permission_only_and_login_scope_without_source_db_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            out=Path(directory);queue=Queue(out/'state.sqlite')
            try:
                cases=[('unavailable','暂无查看权限'),('unavailable','该微博不存在'),('unavailable','该内容暂时无法查看'),('session_required','请先登录')]
                for index,(status,error) in enumerate(cases):
                    row={'_source_filepath':'package:fixture','_sheet':'jobs','_excel_row':index+1,'source_url':'https://weibo.com/detail/'+str(1234567890200+index)}
                    queue.add_source(row);key=str(1234567890200+index)
                    record=annotate_gate({'source_url':row['source_url'],'post_id':key,'status':status,'error':error,'post':{'text_raw':'引用“该微博不存在、暂无查看权限”'},'media':[],'locations':[]})
                    queue.save_result(key,record,attempted=True)
            finally:queue.close()
            before=(out/'state.sqlite').read_bytes();summary=export_access_lists(out)
            self.assertEqual((out/'state.sqlite').read_bytes(),before)
            self.assertEqual(summary['access_dispositions'],{'post_restricted':1,'post_missing':1,'unavailable_unknown':1})
            with (out/'permission_review_posts.csv').open(encoding='utf-8-sig',newline='') as stream:restricted=list(csv.DictReader(stream))
            self.assertEqual(len(restricted),1);self.assertEqual(restricted[0]['access_disposition'],'post_restricted')
            with (out/'login_required_posts.csv').open(encoding='utf-8-sig',newline='') as stream:login=list(csv.DictReader(stream))
            self.assertEqual(login[0]['gate_scope'],'unknown')


if __name__=='__main__':unittest.main()
