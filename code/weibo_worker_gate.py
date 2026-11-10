"""Worker gate evidence and one bounded, non-authenticating health response.

Login requirements are observations with unknown scope, not proof that a post
is permanently inaccessible. Never inspect accessible post text for gate words.
"""
from __future__ import annotations

import html
import json
import re
from urllib.parse import urlparse

PLATFORM_GATES = frozenset({'captcha_required','rate_limited','visitor_gate','browser_session_required'})
LOGIN_GATES = frozenset({'login_required','session_required'})
HEALTH_URL = 'https://weibo.com/'


def explicit_page_gate(url='', title='', *, include_visitor=True):
    parsed=urlparse(str(url or ''))
    host=(parsed.hostname or '').lower()
    known=host=='weibo.com' or host.endswith('.weibo.com') or host=='weibo.cn' or host.endswith('.weibo.cn')
    if not known:
        return None
    if re.search(r'/(?:captcha|verify|security)(?:/|$)',parsed.path,re.I):
        return 'captcha_required','official_security_redirect'
    if include_visitor and (host.startswith('visitor.passport.') or re.search(r'^/visitor(?:/|$)',parsed.path,re.I) and host.startswith('passport.')):
        return 'visitor_gate','official_visitor_redirect'
    if include_visitor and str(title or '').strip()=='Sina Visitor System':
        return 'visitor_gate','official_visitor_page'
    return None


def annotate_gate(result):
    result=dict(result)
    status=result.get('status')
    if status in PLATFORM_GATES:
        result.update(gate_scope='session_or_environment',gate_evidence=result.get('gate_evidence') or status)
    elif status in LOGIN_GATES:
        # Only a separately confirmed target-specific response may supply post.
        # The existing generic login classifier never upgrades scope by wording.
        result['gate_scope']='post' if result.get('gate_scope')=='post' else 'unknown'
        result.setdefault('gate_evidence','login_response_scope_not_proven')
    elif status=='unavailable':
        # Only the already-classified error channel is considered. Successful
        # post text, quoted screenshots and captions are never examined here.
        reason=str(result.get('error') or '')
        if re.search(r'暂无查看权限|无(?:权|权限)查看|没有(?:查看|访问)权限|权限(?:不足|限制)|(?:博主|作者).{0,12}设置.{0,16}(?:不可见|无法查看|不可查看)|仅(?:粉丝|好友|自己)(?:可见|可查看)',reason):
            disposition='post_restricted'
        elif re.search(r'(?:该|此|这条)?微博(?:不存在|已(?:被作者|被)?删除)|(?:该|此)(?:帖子|内容)(?:不存在|已(?:被作者|被)?删除)|post (?:not found|does not exist|deleted)',reason,re.I):
            disposition='post_missing'
        else:
            disposition='unavailable_unknown'
        result.update(access_disposition=disposition,access_evidence_source='classified_error_message')
    return result


def health_response(http_status, *, destination=HEALTH_URL, text=''):
    """One official landing-page response; no challenge/login/redirect followed."""
    from weibo_backend import classify_access_error
    result={'request_role':'session_health_probe','request_source':'official_public_landing',
            'requests':1,'http_status':http_status,'status':'health_unknown','gate_scope':'unknown',
            'error':'public_landing_not_post_access_proof'}
    title_match=re.search(r'<title[^>]*>([^<]*)</title\s*>',text[:10000],re.I)
    title=html.unescape(title_match.group(1)).strip() if title_match else ''
    error=None
    signal=explicit_page_gate(destination,title)
    if not signal and http_status==429:
        signal=('rate_limited','official_public_http_429')
    if not signal and re.fullmatch(r'(?:安全验证|人机验证|Security Verification|Captcha)(?:\s*[-–|]\s*(?:微博|Weibo))?',title,re.I):
        signal=('captcha_required','official_public_security_title')
    if not signal and len(text)<=65536:
        try:
            error=classify_access_error(json.loads(text))
        except (ValueError,TypeError):
            error=None
        if error and error[0] in PLATFORM_GATES:
            signal=(error[0],'official_public_error_'+error[0])
    if signal and signal[0] in ('visitor_gate','browser_session_required'):
        # A single landing response can be an ordinary anonymous intermediate.
        # Persistence is only established by the bounded normal-page observer.
        result.update(status='health_unknown',error='official_public_visitor_intermediate_unconfirmed')
        return result
    if signal:
        result.update(status=signal[0],error=signal[1])
        return annotate_gate(result)
    parsed=urlparse(destination)
    if http_status==401 or error and error[0] in LOGIN_GATES or re.search(r'/(?:signin|login)(?:/|$)',parsed.path,re.I) or re.fullmatch(r'(?:微博登录|新浪通行证|用户登录|登录)(?:\s*[-–|]\s*(?:微博|新浪微博|Weibo))?',title,re.I):
        result.update(status='session_required',error='official_public_generic_login_scope_unknown')
    elif http_status==200:
        result.update(status='health_reachable',error='public_landing_reachable_not_post_access_proof')
    return result
