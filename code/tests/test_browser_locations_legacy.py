"""Offline regression fixtures. No Weibo, browser, or media network calls."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from weibo_backend import WeiboBackend, _classify_payload, classify_access_error, parse_status
from weibo_browser import BrowserBackend, _ARTICLE_DOM, _find_status, _classify_page_access
from weibo_locations import has_coordinates

POST_ID = "5000000000123456"
POST_URL = "https://weibo.com/detail/" + POST_ID
QUOTED = "作者讨论提示：请先登录、登录后查看、安全验证、拖动滑块、该微博不存在、此微博已被删除。"


class FakeResponse:
    def __init__(self, payload=None, status=200, url=None):
        self.payload, self.status = payload, status
        self.url = url or "https://weibo.com/ajax/statuses/show?id=" + POST_ID
        self.disposed = False
        self.json_reads = 0
        self.body_finished = True

    def json(self):
        self.json_reads += 1
        if not self.body_finished:
            raise AssertionError("Response body was read before requestfinished")
        return self.payload

    def dispose(self):
        self.disposed = True


class FakePage:
    def __init__(self, *, responses=(), dom=None, snapshot=None, status=200, finish_requests=True,
                 evaluation_errors=(), responses_after_wait=(), navigation_url=None, goto_error=None, wait_errors=()):
        self.responses, self.dom = responses, dom
        self.snapshot = snapshot or {"url": POST_URL, "title": "微博", "panels": [], "body": QUOTED}
        self.status, self.url, self.closed = status, POST_URL, False
        self.callbacks = {}
        self.finish_requests = finish_requests
        self.header_body_reads = []
        self.default_timeout = None
        self.evaluation_errors = list(evaluation_errors)
        self.responses_after_wait = list(responses_after_wait)
        self.navigation_url = navigation_url
        self.goto_error = goto_error
        self.wait_errors = list(wait_errors)

    def on(self, name, callback):
        self.callbacks[name] = callback

    def remove_listener(self, name, callback):
        self.callbacks.pop(name, None)

    def set_default_timeout(self, value):
        self.default_timeout = value

    def set_default_navigation_timeout(self, value):
        self.navigation_timeout = value

    def goto(self, url, **kwargs):
        self.url = self.navigation_url or url
        if self.goto_error:
            raise sys.modules["playwright.sync_api"].Error(self.goto_error)
        for response in self.responses:
            response.body_finished = False
            self.callbacks["response"](response)
            self.header_body_reads.append(response.json_reads)
            if self.finish_requests:
                response.body_finished = True
                request = types.SimpleNamespace(response=lambda response=response: response)
                self.callbacks["requestfinished"](request)
        return FakeResponse(status=self.status)

    def evaluate(self, script, *args):
        if self.evaluation_errors:
            error = self.evaluation_errors.pop(0)
            if error is not None:
                raise sys.modules["playwright.sync_api"].Error(error)
        return self.dom if script == _ARTICLE_DOM else self.snapshot

    def wait_for_timeout(self, value):
        if self.wait_errors:
            raise sys.modules["playwright.sync_api"].Error(self.wait_errors.pop(0))
        for response in self.responses_after_wait:
            response.body_finished = False
            self.callbacks["response"](response)
            self.header_body_reads.append(response.json_reads)
            response.body_finished = True
            self.callbacks["requestfinished"](types.SimpleNamespace(response=lambda response=response: response))
        self.responses_after_wait = []

    def close(self, **kwargs):
        self.closed = True


class FakeRequest:
    def __init__(self, response):
        self.response = response

    def get(self, *args, **kwargs):
        return self.response


class FakeContext:
    def __init__(self, page=None, response=None):
        self.page = page
        self.request = FakeRequest(response) if response else None

    def new_page(self):
        return self.page


class BrowserAccessTests(unittest.TestCase):
    def fetch(self, page):
        # Import stubs let these fixtures run before installing Playwright.
        class PlaywrightError(Exception):
            pass
        api = types.ModuleType("playwright.sync_api")
        api.Error = PlaywrightError
        api.TimeoutError = type("TimeoutError", (PlaywrightError,), {})
        backend = BrowserBackend(timeout=5)
        backend.context = FakeContext(page)
        clock = iter(range(1000))
        with patch.dict(sys.modules, {"playwright": types.ModuleType("playwright"), "playwright.sync_api": api}), patch("weibo_browser.time.monotonic", side_effect=lambda: next(clock) * 0.1):
            result = backend.fetch_post(POST_URL)
        self.assertTrue(page.closed)
        return result

    def test_json_id_match_wins_over_quoted_gate_words(self):
        requested = {"idstr": POST_ID, "text_raw": QUOTED}
        payload = {"recommendations": [{"idstr": "5111111111111111", "text": "other"}], "data": requested}
        self.assertIs(_find_status(payload, {POST_ID}), requested)
        result = self.fetch(FakePage(responses=[FakeResponse(payload)]))
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["post_id"], POST_ID)
        self.assertEqual(result["acquisition"], "normal_browser_observed_json")

    def test_navigation_context_replacement_recovers_within_original_deadline(self):
        response = FakeResponse({"idstr": POST_ID, "text_raw": "post after visitor redirect"})
        page = FakePage(evaluation_errors=["Page.evaluate: Execution context was destroyed, most likely because of a navigation"],
                        responses_after_wait=[response])
        result = self.fetch(page)
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["post_id"], POST_ID)
        self.assertEqual(response.json_reads, 1)

    def test_context_loss_with_blank_or_passport_intermediate_url_recovers(self):
        for intermediate in ("about:blank", "https://passport.weibo.com/visitor/visitor", "https://passport.weibo.cn/visitor/visitor"):
            with self.subTest(intermediate=intermediate):
                response = FakeResponse({"idstr": POST_ID, "text_raw": "post"})
                page = FakePage(navigation_url=intermediate,
                                evaluation_errors=["Page.evaluate: Execution context was destroyed, most likely because of a navigation"],
                                responses_after_wait=[response])
                self.assertEqual(self.fetch(page)["status"], "success")

    def test_access_snapshot_context_loss_also_recovers(self):
        response = FakeResponse({"idstr": POST_ID, "text_raw": "post"})
        page = FakePage(evaluation_errors=[None, "Page.evaluate: Execution context was destroyed, most likely because of a navigation"],
                        responses_after_wait=[response])
        self.assertEqual(self.fetch(page)["status"], "success")

    def test_login_redirect_is_detected_before_evaluating_replaced_context(self):
        page = FakePage(navigation_url="https://passport.weibo.com/login",
                        evaluation_errors=["Page.evaluate: Execution context was destroyed, most likely because of a navigation"])
        self.assertEqual(self.fetch(page)["status"], "session_required")
        self.assertEqual(len(page.evaluation_errors), 1)

    def test_context_exception_during_initial_navigation_also_recovers(self):
        response = FakeResponse({"idstr": POST_ID, "text_raw": "post"})
        page = FakePage(goto_error="Page.evaluate: Execution context was destroyed, most likely because of a navigation",
                        responses_after_wait=[response])
        self.assertEqual(self.fetch(page)["status"], "success")

    def test_context_exception_while_pumping_events_also_recovers(self):
        response = FakeResponse({"idstr": POST_ID, "text_raw": "post"})
        page = FakePage(wait_errors=["Page.evaluate: Execution context was destroyed, most likely because of a navigation"],
                        responses_after_wait=[response])
        self.assertEqual(self.fetch(page)["status"], "success")

    def test_non_navigation_evaluation_error_remains_fatal(self):
        result = self.fetch(FakePage(evaluation_errors=["Target page, context or browser has been closed"]))
        self.assertEqual(result["status"], "network_error")

    def test_persistent_navigation_context_errors_do_not_extend_deadline(self):
        page = FakePage(evaluation_errors=["Cannot find context with specified id"] * 100)
        result = self.fetch(page)
        self.assertEqual(result["status"], "timeout")
        self.assertIsNone(result["post_id"])

    def test_response_event_does_not_read_unfinished_body(self):
        response = FakeResponse({"idstr": POST_ID, "text_raw": "post"})
        page = FakePage(responses=[response], finish_requests=False)
        result = self.fetch(page)
        self.assertEqual(response.json_reads, 0)
        self.assertEqual(page.header_body_reads, [0])
        self.assertEqual(result["status"], "timeout")
        self.assertLessEqual(page.default_timeout, 2000)
        self.assertEqual(page.callbacks, {})

    def test_completed_matching_response_is_read_once(self):
        response = FakeResponse({"idstr": POST_ID, "text_raw": "post"})
        page = FakePage(responses=[response])
        result = self.fetch(page)
        self.assertEqual(page.header_body_reads, [0])
        self.assertEqual(response.json_reads, 1)
        self.assertEqual(result["status"], "success")

    def test_target_matched_dom_with_quoted_words_is_accessible(self):
        dom = {"text": QUOTED, "images": [{"src": "https://wx1.sinaimg.cn/large/example.jpg"}],
               "videos": [], "locations": [{"label": "香港九龙公园", "url": "https://weibo.com/p/100101example"}]}
        result = self.fetch(FakePage(dom=dom))
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["locations"][0]["label"], "香港九龙公园")
        self.assertFalse(result["locations"][0]["is_checkin"])
        self.assertEqual(result["locations"][0]["role"], "unknown")

    def test_wrong_id_recommendation_never_becomes_requested_post(self):
        result = self.fetch(FakePage(responses=[FakeResponse({"idstr": "5111111111111111", "text_raw": QUOTED})]))
        self.assertEqual(result["status"], "timeout")
        self.assertIsNone(result["post_id"])

    def test_real_structured_restrictions_stop_even_with_dom(self):
        for message, expected in (("访问频次过高", "rate_limited"), ("请完成安全验证", "captcha_required"),
                                  ("请先登录", "session_required"), ("该微博不存在", "unavailable")):
            with self.subTest(message=message):
                result = self.fetch(FakePage(responses=[FakeResponse({"ok": 0, "msg": message})], dom={"text": "post"}))
                self.assertEqual(result["status"], expected)

    def test_real_rate_limit_after_status_still_stops(self):
        responses = [FakeResponse({"idstr": POST_ID, "text_raw": "post"}),
                     FakeResponse({"ok": 0, "msg": "访问频次过高"})]
        self.assertEqual(self.fetch(FakePage(responses=responses))["status"], "rate_limited")

    def test_http_429_stops(self):
        result = self.fetch(FakePage(responses=[FakeResponse(status=429)]))
        self.assertEqual(result["status"], "rate_limited")

    def test_dedicated_challenge_panel_stops(self):
        snapshot = {"url": POST_URL, "title": "微博", "panels": [{"kind": "challenge", "text": "拖动滑块完成安全验证", "controls": True}]}
        result = self.fetch(FakePage(dom={"text": "post"}, snapshot=snapshot))
        self.assertEqual(result["status"], "captcha_required")

    def test_plain_body_and_unrelated_title_are_not_gate_evidence(self):
        self.assertIsNone(_classify_page_access({"url": POST_URL, "title": "讨论安全验证的微博", "body": QUOTED, "panels": []}))
        self.assertEqual(_classify_payload(None, "<article>" + QUOTED + "</article>", POST_URL)["status"], "unexpected_response")

    def test_actual_error_panel_and_login_redirect(self):
        self.assertEqual(_classify_page_access({"panels": [{"kind": "error", "text": "抱歉，此微博已被作者删除"}]})[0], "unavailable")
        self.assertIsNone(_classify_page_access({"panels": [{"kind": "error", "text": "抱歉，此微博已被作者删除"}]}, matched_post=True))
        self.assertEqual(_classify_page_access({"url": "https://passport.weibo.com/login", "panels": []})[0], "session_required")

    def test_own_public_session_endpoint_keeps_true_verification(self):
        response = FakeResponse({"ok": 0, "msg": "人机验证"})
        backend = BrowserBackend()
        backend.context = FakeContext(response=response)
        result = backend._fetch_from_public_endpoint(POST_URL, POST_ID, {POST_ID}, parse_status, 5)
        self.assertEqual(result["status"], "captcha_required")
        self.assertTrue(response.disposed)

    def test_public_http_matched_post_keeps_quoted_text(self):
        backend = WeiboBackend(retries=0, delay=0)
        fixture = {"data": {"idstr": POST_ID, "text_raw": QUOTED}}
        with patch.object(backend, "_request", return_value=(200, {}, json.dumps(fixture), None, 1)):
            result = backend.fetch_post(POST_URL)
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["post"]["text_snippet"], QUOTED)

    def test_structured_errors_and_standalone_titles(self):
        self.assertEqual(classify_access_error({"data": {"ok": 0, "msg": "请求过于频繁"}})[0], "rate_limited")
        self.assertIsNone(classify_access_error({"idstr": POST_ID, "text_raw": QUOTED, "msg": "请先登录"}))
        self.assertEqual(_classify_payload(None, "<title>安全验证</title>", POST_URL)["status"], "captcha_required")
        self.assertEqual(_classify_payload(None, "<title>讨论该微博不存在</title>", POST_URL)["status"], "unexpected_response")


class CoordinateEvidenceTests(unittest.TestCase):
    def test_coordinate_forms_and_zero(self):
        for value in ([23.5, 113.5], (113.5, 23.5), "23.5,113.5", "113.5 23.5", [0, 0],
                      {"lat": "23.5", "lng": "113.5"}, {"type": "Point", "coordinates": [113.5, 23.5]},
                      {"kind": "geo", "value": [23.5, 113.5]},
                      {"geo": {"detail": {"lat": 23.5, "lon": 113.5}}},
                      {"kind": "geo", "coordinates": "23.5,113.5"},
                      {"kind": "poi", "evidence_objects": [{"value": {"longitude": 113.5, "latitude": 23.5}}]}):
            with self.subTest(value=value):
                self.assertTrue(has_coordinates(value))

    def test_labels_and_partial_or_nonfinite_fields_do_not_count(self):
        for value in ({"name": "广州塔", "id": "100101123456"}, {"position": 7}, {"lat": 23.5},
                      "广州23.5 113.5", [float("nan"), 113], [True, False], [], None,
                      {"kind": "poi", "coordinates": {"poi_id": "123456"}}, {"user": {"location": "广州"}}):
            with self.subTest(value=value):
                self.assertFalse(has_coordinates(value))

    def test_source_coordinate_order_and_raw_objects_are_preserved(self):
        geo = {"type": "Point", "coordinates": [23.5, 113.5], "detail": {"poiid": "100101123456", "title": "广州塔", "is_checkin": 1}}
        result = parse_status({"idstr": POST_ID, "text_raw": "post", "geo": geo}, POST_URL)
        location = result["locations"][0]
        self.assertEqual(location["coordinates"]["coordinates"], [23.5, 113.5])
        self.assertIs(location["value"], geo)
        self.assertTrue(location["is_checkin"])
        self.assertEqual(location["label"], "广州塔")
        self.assertTrue(has_coordinates(location))

    def test_direct_geo_pair_or_string_is_retained(self):
        for geo in ([113.5, 23.5], "113.5,23.5"):
            with self.subTest(geo=geo):
                result = parse_status({"idstr": POST_ID, "text_raw": "post", "geo": geo}, POST_URL)
                location = result["locations"][0]
                self.assertEqual(location["value"], geo)
                self.assertEqual(location["coordinates"]["coordinates"], geo)
                self.assertTrue(has_coordinates(location))

    def test_label_only_poi_and_repost_ownership_survive(self):
        root = {"idstr": POST_ID, "text_raw": "post", "place": {"poiid": "100101123456", "name": "广州塔"},
                "user": {"location": "北京"}, "region_name": "广东", "retweeted_status": {
                    "idstr": "5111111111111111", "text_raw": "original", "geo": {"coordinates": [22.3, 114.2]}}}
        result = parse_status(root, POST_URL)
        locations = result["locations"]
        self.assertEqual(len(locations), 2)
        self.assertEqual(locations[0]["label"], "广州塔")
        self.assertFalse(has_coordinates(locations[0]))
        self.assertEqual(locations[0]["source_post_id"], POST_ID)
        self.assertEqual(locations[1]["role"], "repost")
        self.assertEqual(locations[1]["source_post_id"], "5111111111111111")

    def test_numeric_text_link_position_is_not_coordinate(self):
        status = {"idstr": POST_ID, "text_raw": "post", "url_struct": [{"object_type": "place", "object_id": "100101123456", "url_title": "广州塔", "position": 7}]}
        location = parse_status(status, POST_URL)["locations"][0]
        self.assertNotIn("coordinates", location)
        self.assertEqual(location["value"]["position"], 7)


if __name__ == "__main__":
    unittest.main()
