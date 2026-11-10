"""Offline end-to-end queue regressions for login-only streak thresholds."""
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from collect import run_batch
from weibo_queue import Queue, canonical_identity


class SequenceBackend:
    def __init__(self, statuses, errors=None):
        self.statuses = list(statuses)
        self.errors = errors or {}
        self.calls = []

    def fetch_post(self, url):
        index = len(self.calls)
        self.calls.append(url)
        status = self.statuses[index]
        return {
            'source_url': url, 'post_id': canonical_identity(url)[0], 'status': status,
            'media': [], 'locations': [],
            'error': self.errors.get(index, None if status == 'success' else 'fixture ' + status),
        }


class LoginGateThresholdTests(unittest.TestCase):
    def execute(self, statuses, *, errors=None, **options):
        """An extra pending row proves the breaker did not fetch past its limit."""
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            queue = Queue(out / 'state.sqlite')
            backend = SequenceBackend(statuses, errors)
            try:
                for index in range(len(statuses) + 1):
                    queue.add_source({
                        '_source_filepath': 'fixture.xlsx', '_sheet': 'Sheet1',
                        '_excel_row': index + 2, '_region': 'fixture',
                        '原文/评论链接': 'https://m.weibo.cn/status/' + str(1234567890100 + index),
                    })
                queue.db.commit()
                with contextlib.redirect_stdout(io.StringIO()):
                    stats = run_batch(queue, out, lambda: backend, delay=0, min_free_gb=0,
                                      max_posts=len(statuses), verify_media=False, **options)
                posts = [dict(row) for row in queue.db.execute(
                    'SELECT ordinal,status,attempts,last_error,result_json FROM posts ORDER BY ordinal')]
                history = [dict(row) for row in queue.db.execute(
                    'SELECT post_id,status,error FROM request_history ORDER BY attempt_id')]
                return {'run': stats, 'posts': posts, 'history': history,
                        'calls': list(backend.calls), 'queue': queue.stats()}
            finally:
                queue.close()

    def test_four_login_gates_then_success_continue_with_default_five(self):
        result = self.execute(['session_required'] * 4 + ['success'])
        self.assertIsNone(result['run']['stop_reason'])
        self.assertEqual(result['run']['metadata_requests'], 5)
        self.assertEqual(result['run']['login_gate_limit'], 5)
        self.assertEqual(result['run']['consecutive_login_gates'], 0)
        self.assertEqual(result['queue']['post_statuses'], {'pending': 1, 'session_required': 4, 'success': 1})

    def test_fifth_login_gate_is_saved_before_stop_and_sixth_remains_pending(self):
        result = self.execute(['session_required'] * 5 + ['success'])
        self.assertEqual(result['run']['stop_reason'], 'repeated_access_gate')
        self.assertEqual(result['run']['metadata_requests'], 5)
        self.assertEqual(result['run']['consecutive_login_gates'], 5)
        self.assertEqual(len(result['calls']), 5)
        self.assertEqual(result['posts'][4]['status'], 'session_required')
        self.assertEqual(result['posts'][4]['attempts'], 1)
        self.assertEqual(json.loads(result['posts'][4]['result_json'])['status'], 'session_required')
        self.assertEqual(result['history'][-1]['status'], 'session_required')
        self.assertEqual(result['posts'][5]['status'], 'pending')
        self.assertEqual(result['posts'][5]['attempts'], 0)

    def test_login_and_session_statuses_share_one_login_streak(self):
        statuses = ['login_required', 'session_required', 'login_required', 'session_required',
                    'login_required', 'success']
        result = self.execute(statuses)
        self.assertEqual(result['run']['stop_reason'], 'repeated_access_gate')
        self.assertEqual(result['run']['metadata_requests'], 5)
        self.assertEqual(result['run']['consecutive_login_gates'], 5)
        self.assertEqual([row['status'] for row in result['history']], statuses[:5])

    def test_configurable_limit_seven_allows_six_and_stops_at_seventh(self):
        completed = self.execute(['session_required'] * 6 + ['success'], login_gate_limit=7)
        self.assertIsNone(completed['run']['stop_reason'])
        self.assertEqual(completed['run']['metadata_requests'], 7)
        self.assertEqual(completed['run']['login_gate_limit'], 7)
        stopped = self.execute(['session_required'] * 7 + ['success'], login_gate_limit=7)
        self.assertEqual(stopped['run']['stop_reason'], 'repeated_access_gate')
        self.assertEqual(stopped['run']['metadata_requests'], 7)
        self.assertEqual(stopped['run']['consecutive_login_gates'], 7)

    def test_invalid_login_limits_are_rejected(self):
        for limit in (0, -1, True, False):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                self.execute(['success'], login_gate_limit=limit)

    def test_unavailable_resets_login_streak_and_preserves_each_platform_message(self):
        for raw_error in ('该微博不存在', '暂无查看权限', '因博主设置，内容暂不可见'):
            with self.subTest(error=raw_error):
                statuses = ['session_required'] * 4 + ['unavailable'] + ['session_required'] * 4 + ['success']
                result = self.execute(statuses, errors={4: raw_error})
                self.assertIsNone(result['run']['stop_reason'])
                self.assertEqual(result['run']['metadata_requests'], len(statuses))
                self.assertEqual(result['run']['consecutive_login_gates'], 0)
                self.assertEqual(result['posts'][4]['status'], 'unavailable')
                self.assertEqual(result['posts'][4]['last_error'], raw_error)
                self.assertEqual(result['history'][4]['error'], raw_error)
                self.assertEqual(json.loads(result['posts'][4]['result_json'])['error'], raw_error)
                self.assertEqual(result['queue']['source_rows'], len(statuses) + 1)

    def test_other_or_mixed_access_gates_keep_three_request_breaker(self):
        cases = (
            ['visitor_gate'] * 3,
            ['browser_session_required'] * 3,
            ['visitor_gate', 'session_required', 'login_required'],
            ['login_required', 'session_required', 'browser_session_required'],
        )
        for statuses in cases:
            with self.subTest(statuses=statuses):
                result = self.execute(statuses + ['success'])
                self.assertEqual(result['run']['stop_reason'], 'repeated_access_gate')
                self.assertEqual(result['run']['metadata_requests'], 3)
                self.assertEqual([row['status'] for row in result['history']], statuses)
                self.assertEqual(result['posts'][3]['status'], 'pending')

    def test_network_breaker_still_stops_after_three_and_verification_stops_immediately(self):
        for statuses, expected in ((['timeout', 'network_error', 'timeout'], 'repeated_network_failure'),
                                   (['captcha_required'], 'captcha_required'),
                                   (['rate_limited'], 'rate_limited')):
            with self.subTest(statuses=statuses):
                result = self.execute(statuses + ['success'])
                self.assertEqual(result['run']['stop_reason'], expected)
                self.assertEqual(result['run']['metadata_requests'], len(statuses))
                self.assertEqual([row['status'] for row in result['history']], statuses)
                self.assertEqual(result['posts'][len(statuses)]['status'], 'pending')


if __name__ == '__main__':
    unittest.main()
