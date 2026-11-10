"""Normal-browser acquisition for public Weibo posts.

This is an optional Playwright backend. It starts a clean non-persistent Edge
session by default, observes the responses that the page itself requests, and
closes the session on exit. An explicitly supplied in-memory storage-state dict
can enable a user-authorized login session. This module never reads an existing
browser profile, writes session state, handles a challenge, or modifies the
website's requests.
"""
from __future__ import annotations

from copy import deepcopy
import http.cookiejar
import re
import time
import urllib.request
from urllib.parse import parse_qs, quote, urlparse, urljoin

from weibo_backend import classify_access_error
from weibo_worker_gate import HEALTH_URL, PLATFORM_GATES, annotate_gate, explicit_page_gate, health_response


_BASE62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def _token_from_url(url: str) -> str:
    parts = [part for part in urlparse(url).path.split("/") if part]
    return parts[-1] if parts else ""


def _mid_from_bid(token: str) -> str:
    if token.isdigit():
        return token
    if not token or not re.fullmatch(r"[A-Za-z0-9]+", token):
        return ""
    groups = []
    while token:
        group, token = token[-4:], token[:-4]
        value = 0
        for char in group:
            value = value * 62 + _BASE62.index(char)
        groups.append(str(value).zfill(7) if token else str(value))
    return "".join(reversed(groups))


def _find_status(payload, target_ids):
    """Select a matching status; never select the first recommendation."""
    if isinstance(payload, dict):
        ids = {str(payload.get(key, "")) for key in ("id", "idstr", "mid", "bid", "mblogid")}
        if ids.intersection(target_ids) and any(key in payload for key in ("text", "text_raw", "pic_infos", "page_info", "user")):
            return payload
        for value in payload.values():
            if isinstance(value, (dict, list)):
                found = _find_status(value, target_ids)
                if found is not None:
                    return found
    elif isinstance(payload, list):
        for value in payload:
            found = _find_status(value, target_ids)
            if found is not None:
                return found
    return None


_ARTICLE_DOM = r"""(target) => {
  const ids = target.ids;
  const links = [...document.querySelectorAll('a[href]')];
  const own = links.find(a => {
    try {
      const u = new URL(a.href, location.href);
      return /(^|\.)weibo\.(com|cn)$/.test(u.hostname)
        && ids.includes(u.pathname.split('/').filter(Boolean).pop());
    } catch { return false; }
  });
  let article = own && own.closest('article, [data-mid], [mid], [class*="Feed_wrap"], [class*="card-wrap"]');
  if (!article) {
    article = [...document.querySelectorAll('[data-mid], [mid]')].find(e =>
      ids.includes(e.getAttribute('data-mid')) || ids.includes(e.getAttribute('mid')));
  }
  if (!article) return null;
  const declaredId = article.getAttribute('data-mid') || article.getAttribute('mid');
  if (declaredId && !ids.includes(declaredId)) return null;
  const content = article.querySelector('[class*="detail_wbtext"], [class*="Feed_detail"], [class*="weibo-text"], [node-type="feed_list_content"]') || article;
  const imgs = [...content.querySelectorAll('img')].map(i => ({src: i.currentSrc || i.src, width: i.naturalWidth, height: i.naturalHeight}))
    .filter(i => i.src && /(^|\/)\w*(large|original|mw\d+|bmiddle|orj\d+|thumb\d*)\//.test(i.src));
  const videos = [...article.querySelectorAll('video')].map(v => ({
    src: v.currentSrc || v.src || v.querySelector('source')?.src || '',
    duration: Number.isFinite(v.duration) ? v.duration : null,
    width: v.videoWidth, height: v.videoHeight,
  })).filter(v => /^https?:/.test(v.src));
  const locations = [...content.querySelectorAll('a[href]')].map(a => ({label: a.innerText.trim(), url: a.href}))
    .filter(a => {
      try {
        const u = new URL(a.url);
        if (/s\.weibo\.(com|cn)$/.test(u.hostname)) return false;
        return /(^|\.)(place\.weibo\.com|map\.weibo\.com|maps\.weibo\.com)$/.test(u.hostname)
          || (/^(www\.)?weibo\.com$/.test(u.hostname) && /\/(p|poi)\//.test(u.pathname)
              && /^(?:100101|100135|100102)/.test(u.pathname.split('/').filter(Boolean)[1] || ''));
      } catch { return false; }
    });
  return {text: content.innerText, images: imgs, videos, locations, matched_post_link: own?.href || null};
}"""


_PAGE_ACCESS_DOM = r"""() => {
  const postContainer = 'article,[data-mid],[mid],[class*="Feed_wrap"],[class*="card-wrap"]';
  const visible = e => !!(e.getClientRects().length) && getComputedStyle(e).visibility !== 'hidden';
  const panels = [];
  const selectors = [
    ['challenge', '[id*="captcha" i],[class*="captcha" i],[id*="geetest" i],[class*="geetest" i],[id*="verify" i],[class*="verify" i]'],
    ['error', '[role="alert"],[data-error],[class*="Error" i],[class*="Empty" i],[class*="Unavailable" i],[node-type="feed_list_error"]'],
    ['login', 'form,[class*="Login" i],[id*="Login" i]'],
  ];
  for (const [kind, selector] of selectors) {
    for (const e of document.querySelectorAll(selector)) {
      if (!visible(e) || e.closest(postContainer)) continue;
      const text = (e.innerText || '').trim();
      if (!text || text.length > 600) continue;
      const controls = !!e.querySelector('input,button,[role="slider"],iframe,[class*="slider" i]');
      panels.push({kind, text, controls});
    }
  }
  return {url: location.href, title: document.title, panels};
}"""


def _classify_page_access(snapshot, *, matched_post=False, worker_mode=False):
    """Recognize dedicated gate UI; ordinary post/body text is not evidence."""
    if not isinstance(snapshot, dict):
        return None
    current = urlparse(str(snapshot.get("url") or ""))
    host = (current.hostname or "").lower()
    known_host = host == "weibo.com" or host.endswith(".weibo.com") or host == "weibo.cn" or host.endswith(".weibo.cn")
    if worker_mode:
        signal=explicit_page_gate(snapshot.get('url'),snapshot.get('title'))
        if signal and signal[0]!='visitor_gate':
            return signal
    else:
        signal=None
    if known_host and re.search(r"/(?:captcha|verify|security)(?:/|$)", current.path, re.I):
        return "captcha_required", "Public page redirected to security verification; no challenge was handled"
    if known_host and re.search(r"/(?:signin|login)(?:/|$)", current.path, re.I):
        return "session_required", "Public page redirected to login"
    for panel in snapshot.get("panels") or []:
        if not isinstance(panel, dict):
            continue
        kind, text = panel.get("kind"), str(panel.get("text") or "").strip()
        if kind in ("error", "challenge") and re.search(r"^(?:抱歉[，, ]*)?(?:访问频次过高|访问过于频繁|请求过于频繁|请求过多|Too many requests)", text, re.I):
            return "rate_limited", "Public page request-limit panel: " + text[:200]
        if kind == "challenge" and panel.get("controls") and re.search(r"请完成验证|安全验证|拖动滑块|人机验证|完成验证码|captcha|verification", text, re.I):
            return "captcha_required", "Public page security-verification panel; no challenge was handled"
        if worker_mode and kind in ('error','challenge') and re.search(r'^(?:抱歉[，, ]*)?(?:访问异常|访问受限|访问被拒绝|暂时禁止访问|系统检测到异常)',text):
            return 'visitor_gate','Public page access-block panel: '+text[:200]
        if not matched_post and kind == "error" and re.search(r"^(?:抱歉[，, ]*)?(?:该微博不存在|此微博已被删除|该内容暂时无法查看|此微博已被作者删除|该微博已被删除)", text):
            return "unavailable", "Public page unavailable-post panel: " + text[:200]
        if not matched_post and kind == "login" and panel.get("controls") and re.search(r"^(?:登录后查看|登录后可查看|请先登录)", text):
            return "session_required", "Public page requires login"
    title = str(snapshot.get("title") or "").strip()
    if re.fullmatch(r"(?:安全验证|人机验证|Security Verification|Captcha)(?:\s*[-–|]\s*(?:微博|Weibo))?", title, re.I):
        return "captcha_required", "Public page security-verification title; no challenge was handled"
    if not matched_post and re.fullmatch(r"(?:微博登录|新浪通行证|用户登录|登录)(?:\s*[-–|]\s*(?:微博|新浪微博|Weibo))?", title, re.I):
        return "session_required", "Public page login title"
    if signal:
        return signal
    return None


def _is_navigation_context_error(error, page_url=None):
    """Recognize document replacement while an already validated post loads.

    page.url is not reliable during context destruction: it may temporarily be
    about:blank or a visitor/passport intermediate URL. This helper only says
    to wait within the existing deadline. It grants no access, new navigation,
    login, or challenge handling; the next iteration checks explicit gates.
    """
    return bool(re.search(
        r"Execution context was destroyed|Cannot find context(?: with specified id)?|"
        r"Execution context is not available in detached frame|Frame (?:was|has been) detached",
        str(error), re.I))


def _browser_error(payload, http_status=None):
    error = classify_access_error(payload, http_status)
    if error and error[0] == "login_required":
        return "session_required", error[1]
    return error


class BrowserBackend:
    """Context manager with fetch_post(url) and a media-download opener.

    Only this browser context's own cookies are made available to streaming
    downloads through ``make_download_opener``. Optional login storage state is
    supplied explicitly in memory; fetch_post never returns it or cookie values,
    and this module never writes either to disk.
    """

    def __init__(self, *, headless=True, timeout=25, channel="msedge", storage_state=None, worker_mode=False):
        if storage_state is not None and not isinstance(storage_state, dict):
            raise ValueError("storage_state must be an in-memory dictionary or None")
        self._storage_state = deepcopy(storage_state) if storage_state is not None else None
        self.headless = headless
        self.timeout = min(max(float(timeout), 5), 60)
        self.channel = channel
        self._playwright = None
        self.browser = None
        self.context = None
        self._user_agent = None
        self._public_session_ready = False
        self.worker_mode = bool(worker_mode)
        self._pending_browser_gate = None

    def __enter__(self):
        from playwright.sync_api import Error, sync_playwright
        self._playwright = sync_playwright().start()
        try:
            self.browser = self._playwright.chromium.launch(channel=self.channel, headless=self.headless,
                                                               timeout=int(self.timeout * 1000))
            context_options = {"accept_downloads": False}
            if self._storage_state is not None:
                context_options["storage_state"] = self._storage_state
            self.context = self.browser.new_context(**context_options)
            self.context.set_default_timeout(min(2000, int(self.timeout * 1000)))
            self.context.set_default_navigation_timeout(int(self.timeout * 1000))
            probe = self.context.new_page()
            try:
                probe_deadline = time.monotonic() + min(2, self.timeout)
                while time.monotonic() < probe_deadline:
                    try:
                        self._user_agent = probe.evaluate("navigator.userAgent")
                        break
                    except Error as exc:
                        if not _is_navigation_context_error(exc):
                            raise
                        remaining = int((probe_deadline - time.monotonic()) * 1000)
                        if remaining > 0:
                            probe.wait_for_timeout(min(100, remaining))
            finally:
                probe.close(run_before_unload=False)
            return self
        except Exception:
            self.close()
            raise

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def close(self):
        if self.context is not None:
            self.context.close()
            self.context = None
        if self.browser is not None:
            self.browser.close()
            self.browser = None
        if self._playwright is not None:
            self._playwright.stop()
            self._playwright = None

    def make_download_opener(self):
        """Build a streaming urllib opener using this session's cookies in RAM."""
        if self.context is None:
            raise RuntimeError("Use BrowserBackend as a context manager")
        jar = http.cookiejar.CookieJar()
        for cookie in self.context.cookies():
            domain = cookie["domain"]
            expires = cookie.get("expires", -1)
            jar.set_cookie(http.cookiejar.Cookie(
                version=0, name=cookie["name"], value=cookie["value"], port=None,
                port_specified=False, domain=domain, domain_specified=domain.startswith("."),
                domain_initial_dot=domain.startswith("."), path=cookie.get("path", "/"),
                path_specified=True, secure=cookie.get("secure", False),
                expires=int(expires) if expires and expires > 0 else None,
                discard=not expires or expires <= 0, comment=None, comment_url=None,
                rest={"HttpOnly": cookie.get("httpOnly", False)}, rfc2109=False,
            ))
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
        opener.addheaders = [("User-Agent", self._user_agent or "Mozilla/5.0"), ("Referer", "https://weibo.com/")]
        return opener

    def fetch_post(self, url: str):
        if self.context is None:
            raise RuntimeError("Use BrowserBackend as a context manager")
        from playwright.sync_api import Error, TimeoutError as PlaywrightTimeoutError
        from weibo_backend import parse_status, post_token

        try:
            token = post_token(url)
        except ValueError as exc:
            return self._failure(url, "unsupported_url", str(exc))
        target_ids = {token, _mid_from_bid(token)} - {""}
        deadline = time.monotonic() + self.timeout
        self._pending_browser_gate=None
        # Once the normal public application establishes its own visitor session,
        # reuse that session for the endpoint already observed on the public page.
        # If it no longer accepts the session, a single normal navigation follows.
        if self._public_session_ready:
            quick = self._fetch_from_public_endpoint(url, token, target_ids, parse_status,
                                                    timeout=min(8, self.timeout))
            if quick is not None:
                return quick
        observed = {"status": None, "error": None, "http_status": None}
        page = self.context.new_page()

        def matches_response(response):
            response_url = urlparse(response.url)
            host = (response_url.hostname or "").lower()
            if host not in {"weibo.com", "www.weibo.com", "m.weibo.cn", "m.weibo.com", "weibo.cn", "www.weibo.cn"}:
                return False
            if "/statuses/show" not in response_url.path:
                return False
            query = parse_qs(response_url.query)
            requested_ids = set(query.get("id", []) + query.get("mid", []))
            return bool(requested_ids.intersection(target_ids))

        def on_response(response):
            # This event arrives when headers are available, before the body
            # finishes. Calling json()/body()/text() here can synchronously
            # block Playwright's event dispatch past the post deadline.
            try:
                if matches_response(response) and response.status in (429, 404, 410, 401) and not (self.worker_mode and response.status==401):
                    observed["error"] = _browser_error({}, response.status)
            except Exception:
                return

        def on_request_finished(request):
            try:
                if time.monotonic() >= deadline:
                    return
                response = request.response()
                if response is None or not matches_response(response):
                    return
                if self.worker_mode and response.status==401:
                    try:
                        payload=response.json()
                        error=_browser_error(payload)
                    except Exception:
                        error=None
                    observed['error']=error if error and error[0] in PLATFORM_GATES else _browser_error({},401)
                    return
                if response.status in (429, 404, 410, 401):
                    observed["error"] = _browser_error({}, response.status)
                    return
                # requestfinished guarantees the body has completed. Only
                # completed matching status responses are ever read as JSON.
                payload = response.json()
                status = _find_status(payload, target_ids)
                if status is not None:
                    observed["status"] = status
                    observed["http_status"] = response.status
                    return
                error = _browser_error(payload, response.status)
                if error and (error[0] != "browser_session_required" or self.worker_mode):
                    observed["error"] = error
            except Exception:
                # Empty or failed completed responses are not post evidence.
                return

        def pump_events(milliseconds):
            remaining = int((deadline - time.monotonic()) * 1000)
            if remaining <= 0:
                return
            try:
                page.wait_for_timeout(min(milliseconds, remaining))
            except Error as exc:
                # Sync Playwright dispatch can surface a context-replacement
                # exception while another API is pumping navigation events.
                if not _is_navigation_context_error(exc):
                    raise

        page.on("response", on_response)
        page.on("requestfinished", on_request_finished)
        navigation_error = None
        dom_ready_at = None
        last_dom = None
        pending_visitor = self._pending_browser_gate
        try:
            page.set_default_timeout(min(2000, max(1, int((deadline - time.monotonic()) * 1000))))
            page.set_default_navigation_timeout(max(1, int((deadline - time.monotonic()) * 1000)))
            try:
                remaining = max(1, int((deadline - time.monotonic()) * 1000))
                response = page.goto(url, wait_until="domcontentloaded", timeout=remaining)
                if response and response.status == 429:
                    return self._failure(url, "rate_limited", "Post page returned HTTP 429")
                if response and response.status in (404, 410):
                    return self._failure(url, "unavailable", "Post page returned HTTP " + str(response.status))
            except PlaywrightTimeoutError:
                navigation_error = "Post navigation timed out"
            except Error as exc:
                if _is_navigation_context_error(exc):
                    navigation_error = "Public navigation temporarily replaced the document context"
                    pump_events(200)
                else:
                    # An observed normal visitor redirect makes an aborted
                    # initial navigation recoverable within this deadline.
                    if "ERR_ABORTED" not in str(exc):
                        raise
                    pump_events(350)
                    current = urlparse(page.url)
                    host = (current.hostname or "").lower()
                    normal_host = host in {"weibo.com", "weibo.cn"} or host.endswith(".weibo.com") or host.endswith(".weibo.cn")
                    if page.url == url or current.scheme not in ("http", "https") or not normal_host:
                        raise
                    navigation_error = "Initial navigation was replaced by a normal public visitor redirect"
            if self.worker_mode:
                initial_gate=explicit_page_gate(page.url)
                if initial_gate and initial_gate[0]=='visitor_gate':
                    pending_visitor=initial_gate
            while time.monotonic() < deadline:
                if observed["error"] is not None:
                    if self.worker_mode and observed['error'][0]=='browser_session_required':
                        pending_visitor=observed['error']
                        observed['error']=None
                        last_dom=None
                        dom_ready_at=None
                    else:
                        return self._failure(url, *observed["error"])
                if observed["status"] is not None:
                    result = parse_status(observed["status"], url)
                    result["acquisition"] = "normal_browser_observed_json"
                    result["request_source"] = "public_page_network_response"
                    result["http_status"] = observed["http_status"]
                    self._public_session_ready = True
                    return result
                redirect_error = _classify_page_access({"url": page.url, "panels": []},worker_mode=self.worker_mode)
                visitor_this_iteration=bool(redirect_error and redirect_error[0]=='visitor_gate' and redirect_error[1].startswith('official_visitor_'))
                if visitor_this_iteration:
                    pending_visitor=redirect_error
                    last_dom=None
                    dom_ready_at=None
                    self._public_session_ready=False
                elif redirect_error:
                    return self._failure(url, *redirect_error)
                try:
                    dom = page.evaluate(_ARTICLE_DOM, {"ids": sorted(target_ids)})
                    if dom is not None and not visitor_this_iteration:
                        last_dom = dom
                    if time.monotonic() >= deadline:
                        break
                    access = page.evaluate(_PAGE_ACCESS_DOM)
                except PlaywrightTimeoutError:
                    # A visitor redirect can temporarily replace the document.
                    continue
                except Error as exc:
                    if not _is_navigation_context_error(exc):
                        raise
                    # The normal anonymous visitor redirect replaces the JS
                    # context. Wait for its new document within this same
                    # deadline; no login/challenge handling or retry extension.
                    navigation_error = "Public navigation temporarily replaced the document context"
                    pump_events(200)
                    continue
                if observed["error"] is not None:
                    if self.worker_mode and observed['error'][0]=='browser_session_required':
                        pending_visitor=observed['error']
                        observed['error']=None
                        last_dom=None
                        dom_ready_at=None
                    else:
                        return self._failure(url, *observed["error"])
                if observed["status"] is not None:
                    continue
                error = _classify_page_access(access, matched_post=dom is not None,worker_mode=self.worker_mode)
                if error and error[0]=='visitor_gate' and error[1].startswith('official_visitor_'):
                    pending_visitor=error
                    visitor_this_iteration=True
                    last_dom=None
                    dom_ready_at=None
                    self._public_session_ready=False
                elif error:
                    return self._failure(url, *error)
                if visitor_this_iteration:
                    # Observe only this navigation within its original deadline.
                    # Never refresh, navigate again, click or solve a visitor UI.
                    pump_events(200)
                    continue
                if pending_visitor is None or pending_visitor[0]!='browser_session_required' or dom is not None:
                    pending_visitor=None
                if dom is not None:
                    # Allow structured metadata and late-loading media to arrive
                    # before the less informative DOM fallback is accepted.
                    if dom_ready_at is None:
                        dom_ready_at = time.monotonic()
                    elif time.monotonic() - dom_ready_at >= 2:
                        return self._parse_dom(dom, url, token)
                pump_events(400)

            if observed["error"] is not None:
                return self._failure(url, *observed["error"])
            if observed["status"] is not None:
                result = parse_status(observed["status"], url)
                result["acquisition"] = "normal_browser_observed_json"
                result["request_source"] = "public_page_network_response"
                result["http_status"] = observed["http_status"]
                self._public_session_ready = True
                return result
            if pending_visitor is not None:
                self._public_session_ready=False
                return self._failure(url,*pending_visitor)
            # Do not start another browser RPC after the acquisition deadline.
            if last_dom is not None:
                return self._parse_dom(last_dom, url, token)
            return self._failure(url, "timeout", navigation_error or "No matching public post was exposed within the browser timeout")
        except Error as exc:
            return self._failure(url, "network_error", str(exc).splitlines()[0][:220])
        finally:
            # Detach callbacks before shutdown so closing an in-flight request
            # cannot initiate a fresh body read during cleanup.
            page.remove_listener("response", on_response)
            page.remove_listener("requestfinished", on_request_finished)
            try:
                page.close(run_before_unload=False)
            except Error:
                pass

    def _fetch_from_public_endpoint(self, url, token, target_ids, parse_status, timeout):
        endpoint = "https://weibo.com/ajax/statuses/show?id=" + quote(token, safe="") + "&locale=zh-CN&isGetLongText=true"
        response = None
        try:
            response = self.context.request.get(endpoint, timeout=int(timeout * 1000),
                headers={"Referer": url, "User-Agent": self._user_agent or "Mozilla/5.0", "Accept": "application/json"})
            if self.worker_mode:
                signal=explicit_page_gate(getattr(response,'url',''))
                if signal:
                    self._public_session_ready=False
                    if signal[0]=='visitor_gate':
                        self._pending_browser_gate=signal
                        return None
                    return self._failure(url,*signal,request_source='public_status_endpoint',http_status=response.status)
                if response.status==401:
                    try:
                        error=_browser_error(response.json())
                    except Exception:
                        error=None
                    self._public_session_ready=False
                    selected=error if error and error[0] in PLATFORM_GATES else _browser_error({},401)
                    if selected[0]=='browser_session_required':
                        self._pending_browser_gate=selected
                        return None
                    return self._failure(url,*selected,request_source='public_status_endpoint',http_status=401)
            if response.status in (429, 404, 410, 401):
                return self._failure(url, *_browser_error({}, response.status),request_source='public_status_endpoint',http_status=response.status)
            payload = response.json()
            status = _find_status(payload, target_ids)
            if status is not None:
                result = parse_status(status, url)
                result["acquisition"] = "public_endpoint_with_own_browser_session"
                result["request_source"] = "public_status_endpoint"
                result["http_status"] = response.status
                return result
            error = _browser_error(payload, response.status)
            if self.worker_mode and error and error[0]=='browser_session_required':
                self._public_session_ready=False
                self._pending_browser_gate=error
                return None
            if error and error[0] != "browser_session_required":
                if self.worker_mode and error[0] in {'login_required','session_required','browser_session_required','visitor_gate'}:
                    # No retry of this post: clear the fast-path assumption so
                    # the next target uses one normal public-page navigation.
                    self._public_session_ready=False
                return self._failure(url,*error,request_source='public_status_endpoint',http_status=response.status)
            self._public_session_ready = False
            return None
        except Exception:
            # An HTML login or challenge page is not parsed or solved.
            self._public_session_ready = False
            return None
        finally:
            if response is not None:
                response.dispose()

    def probe_public_health(self):
        """One same-context official GET, no login/challenge/redirect following."""
        response=None
        try:
            response=self.context.request.get(HEALTH_URL,timeout=int(min(8,self.timeout)*1000),max_redirects=0,
                                              headers={'Accept':'text/html,application/json'})
            destination=urljoin(getattr(response,'url',HEALTH_URL),response.headers.get('location',''))
            # Read only the existing response, never a second request to a
            # redirect destination. Body words in posts are not gate evidence.
            text=response.text()[:65536]
            return health_response(response.status,destination=destination,text=text)
        except Exception as error:
            return {'status':'health_unknown','gate_scope':'unknown','requests':1,
                    'request_source':'official_public_landing','error':'health_probe_'+type(error).__name__}
        finally:
            if response is not None:
                response.dispose()

    @staticmethod
    def _failure(url, status, reason, **extra):
        return annotate_gate({"source_url": url, "status": status, "error": reason,
                "post_id": None, "bid": None, "post": None, "relation": None,
                "media": [], "locations": [], "warnings": [], "acquisition": "normal_browser",**extra})

    @staticmethod
    def _parse_dom(dom, url, token):
        from weibo_backend import parse_status
        result = parse_status({"idstr": _mid_from_bid(token), "mblogid": token,
                               "text_raw": dom.get("text", "")}, url)
        media = []
        seen = set()
        for kind, items in (("image", dom.get("images", [])), ("video", dom.get("videos", []))):
            for item in items:
                source = item["src"]
                if source in seen:
                    continue
                seen.add(source)
                candidate = {"url": source, "field": "matched_post_dom." + kind,
                             **{key: item[key] for key in ("width", "height") if item.get(key)}}
                media.append({"kind": kind, "url": source, "candidates": [candidate],
                              "media_id": source.split("?")[0].rsplit("/", 1)[-1],
                              "source_post_id": None, "source_bid": None, "role": "unknown"})
        result["media"] = media
        result["locations"] = [{"kind": "poi", "field": "matched_post_dom_anchor",
                                "value": {"name": place.get("label"), "url": place.get("url")},
                                "label": place.get("label"), "label_field": "matched_post_dom_anchor.name",
                                "url": place.get("url"), "url_field": "matched_post_dom_anchor.url",
                                "evidence": "matched_public_place_link", "is_checkin": False,
                                "source_post_id": None, "source_bid": None, "role": "unknown"}
                               for place in dom.get("locations", [])]
        result["relation"]["type"] = "unknown"
        result["warnings"].append("DOM fallback identifies the requested post container; repost ownership and relationship are unknown")
        result["acquisition"] = "normal_browser_matched_dom"
        result["request_source"] = "matched_public_post_dom"
        return result
