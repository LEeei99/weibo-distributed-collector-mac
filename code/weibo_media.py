"""Bounded media storage and local-only recovery of cached Weibo assets.

An asset keeps its originating post/role and rendition list. Only its ``download``
entry is updated by recovery; no metadata requests or URL guessing happen here.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import http.client
import json
import math
from pathlib import Path, PurePosixPath, PureWindowsPath
import sqlite3
import time
from urllib.parse import urlsplit
import urllib.error
import urllib.request

_MEDIA_DOMAINS = ('sinaimg.cn', 'weibocdn.com', 'weibo.com', 'weibo.cn', 'sina.com.cn')
_SAVED = {'downloaded', 'reused'}
PLACEHOLDER_REVIEW_STATUS = 'placeholder_review_required'
_TERMINAL = {'stream_manifest_only', 'unsupported_url', 'unsupported_media', 'unsupported_redirect',
             PLACEHOLDER_REVIEW_STATUS}
# Identified by actual content, never by a GIF MIME type, filename or dimensions.
_KNOWN_IMAGE_PLACEHOLDERS = {
    '07f6b376a6e724f0fb1753c93a5648fb4975a204a66c651205f54715024253f3':
        {'id': 'weibo_gray_missing_image', 'bytes': 11859},
}
_IMAGE_BRANDS = (b'avif', b'avis', b'heic', b'heix', b'hevc', b'hevx', b'mif1', b'msf1')


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _known_placeholder(kind, sha256):
    if kind != 'image' or not isinstance(sha256, str):
        return None
    return _KNOWN_IMAGE_PLACEHOLDERS.get(sha256.lower())


def mark_placeholder_review(saved, kind):
    """Keep downloaded evidence while separating a known placeholder from content."""
    if not isinstance(saved, dict) or kind != 'image':
        return None
    if saved.get('status') == PLACEHOLDER_REVIEW_STATUS:
        return deepcopy(saved)
    placeholder = _known_placeholder(kind, saved.get('sha256'))
    if placeholder is None:
        return None
    result = deepcopy(saved)
    if saved.get('status'):
        result['prior_status'] = saved['status']
    result.update(status=PLACEHOLDER_REVIEW_STATUS, review_required=True,
                  review_reason='known_image_placeholder', placeholder_id=placeholder['id'],
                  review_marked_at=_utc_now())
    return result


def _saved_placeholder_review(saved, kind, out):
    review = mark_placeholder_review(saved, kind)
    if review is not None or kind != 'image' or saved.get('sha256'):
        return review
    # Legacy records without a hash need a tiny local fingerprint check. Size
    # only selects files to hash; it never decides whether content is a placeholder.
    path = _safe_saved_path(out, saved.get('file'))
    try:
        if path is None or path.stat().st_size not in {p['bytes'] for p in _KNOWN_IMAGE_PLACEHOLDERS.values()}:
            return None
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        return mark_placeholder_review({**saved, 'sha256': digest}, kind)
    except OSError:
        return None


def allowed_media_url(url):
    """Accept public media HTTP(S) endpoints belonging to the existing collector."""
    if not isinstance(url, str) or not url or any(c.isspace() or ord(c) < 32 for c in url):
        return False
    try:
        parsed = urlsplit(url)
        host = (parsed.hostname or '').lower()
        port = parsed.port
        return (parsed.scheme in {'https', 'http'} and not parsed.username and not parsed.password
                and port in (None, 80, 443) and any(host == d or host.endswith('.' + d) for d in _MEDIA_DOMAINS))
    except ValueError:
        return False


def _safe_saved_path(out, relative):
    """Validate a saved POSIX relative path and the actual resolved destination."""
    if not isinstance(relative, str) or not relative or '\\' in relative or ':' in relative or '\x00' in relative:
        return None
    path = PurePosixPath(relative)
    if path.is_absolute() or PureWindowsPath(relative).is_absolute() or '..' in path.parts:
        return None
    if len(path.parts) < 2 or path.parts[0] != 'media':
        return None
    try:
        root = Path(out).resolve()
        resolved = (root / Path(*path.parts)).resolve()
        resolved.relative_to(root)
        return resolved
    except (ValueError, OSError, RuntimeError):
        return None


def _signature_valid(head, kind):
    if kind not in {'image', 'video'}:
        return False
    if kind == 'image' and (head.startswith((b'\xff\xd8\xff', b'\x89PNG\r\n\x1a\n', b'GIF87a', b'GIF89a'))
                           or (head.startswith(b'RIFF') and head[8:12] == b'WEBP')):
        return True
    if kind == 'video' and head.startswith(b'\x1a\x45\xdf\xa3'):
        return True
    # ISO BMFF files have a real, length-prefixed ftyp box, not an arbitrary
    # occurrence of the string in a login/error page.
    if len(head) >= 16 and head[4:8] == b'ftyp':
        box_size = int.from_bytes(head[:4], 'big')
        if box_size < 16:
            return False
        brands = head[8:min(box_size, len(head))]
        is_image = any(brand in brands for brand in _IMAGE_BRANDS)
        return is_image if kind == 'image' else not is_image
    return False


def valid_media(path, kind, expected=None, *, out=None):
    """Check existence/signature, plus recorded length and SHA256 when available.

    With ``out``, ``path`` must be a safe relative saved path under its media
    directory. Legacy records without checksums still receive signature/size
    checks; new downloads always record both.
    """
    try:
        file_path = _safe_saved_path(out, path) if out is not None else Path(path)
        if file_path is None or not file_path.is_file():
            return False
        size = file_path.stat().st_size
        if size <= 0:
            return False
        expected = expected or {}
        if expected.get('bytes') is not None:
            recorded_size = expected['bytes']
            if isinstance(recorded_size, bool) or int(recorded_size) != recorded_size or int(recorded_size) != size:
                return False
        fingerprint_needed = (kind == 'image' and size in
                              {p['bytes'] for p in _KNOWN_IMAGE_PLACEHOLDERS.values()})
        digest = hashlib.sha256() if expected.get('sha256') or fingerprint_needed else None
        with file_path.open('rb') as stream:
            head = stream.read(64)
            if not _signature_valid(head, kind):
                return False
            if digest is not None:
                digest.update(head)
                for block in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(block)
        if digest is None:
            return True
        actual_digest = digest.hexdigest()
        if _known_placeholder(kind, actual_digest) is not None:
            return False
        return not expected.get('sha256') or actual_digest == str(expected['sha256']).lower()
    except (OSError, ValueError, TypeError, OverflowError):
        return False


def _cached_download_valid(asset, out):
    saved = asset.get('download') or {}
    return saved.get('status') in _SAVED and valid_media(saved.get('file'), asset.get('kind'), saved, out=out)


def _repair_attempt_count(asset):
    try:
        return max(0, int(asset.get('repair_attempts', 0)))
    except (ValueError, TypeError, OverflowError):
        return 0


def _validate_attempt_limit(max_attempts):
    if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or max_attempts < 1:
        raise ValueError('max_attempts must be a positive integer')


def _asset_needs_repair(asset, out, max_attempts=3):
    saved = asset.get('download') or {}
    review = _saved_placeholder_review(saved, asset.get('kind'), out)
    if review is not None:
        # Select an old unclassified placeholder exactly once for zero-network
        # classification; persisted review records never enter automatic retries.
        return saved.get('status') != PLACEHOLDER_REVIEW_STATUS
    if _cached_download_valid(asset, out):
        return False
    # The persisted count, compared with today's configured limit, is
    # authoritative. Increasing the allowance can revive an exhausted asset.
    return _repair_attempt_count(asset) < max_attempts and saved.get('status') not in _TERMINAL


def cached_media_needs_repair(record, out, *, max_attempts=3):
    """Detect actionable incomplete/corrupt media without obtaining post metadata.

    Pass the same current retry allowance used by repair_cached_media so an
    explicitly increased limit also selects previously exhausted assets.
    """
    _validate_attempt_limit(max_attempts)
    return any(_asset_needs_repair(asset, out, max_attempts) for asset in record.get('media', [])
               if isinstance(asset, dict))


def repair_cached_media(record, store, referer, metadata_only=False, max_attempts=3):
    """Return a copy retaining every source/owner relationship and existing metadata.

    No backend is called here. Failed cached-URL repairs can later be followed by
    a metadata refresh under the runner's policy. Existing valid downloads are
    never changed to metadata_only when a metadata-only batch is resumed. Failed
    recovery is bounded across runs by each asset's persistent repair_attempts.
    Budget/size deferrals do not consume that retry allowance.
    """
    _validate_attempt_limit(max_attempts)
    repaired = deepcopy(record)
    for asset in repaired.get('media', []):
        if not isinstance(asset, dict):
            continue
        prior = asset.get('download') or {}
        review = _saved_placeholder_review(prior, asset.get('kind'), store.out)
        if review is not None:
            asset['download'] = review
            continue
        if _cached_download_valid(asset, store.out):
            asset['download'] = {**prior, 'status': 'reused'}
            asset.pop('repair_status', None)
        elif metadata_only:
            if not prior:
                asset['download'] = {'status': 'metadata_only', 'bytes': 0}
        elif _asset_needs_repair(asset, store.out, max_attempts):
            asset.pop('repair_status', None)
            if prior.get('status') == 'size_limit':
                try:
                    known_size = int(prior.get('content_length') or 0)
                    previous_cap = int(prior.get('max_file_bytes') or 0)
                except (ValueError, TypeError, OverflowError):
                    known_size, previous_cap = 0, 0
                # A known oversized file cannot fit until the cap is raised;
                # unknown-length responses record the cap they reached, too.
                if known_size > store.max_file or (not known_size and previous_cap >= store.max_file):
                    continue
            attempts = _repair_attempt_count(asset)
            if attempts >= max_attempts:
                asset['repair_status'] = 'retry_exhausted'
                continue
            result = store.download(asset, referer)
            # Limit failed transport/invalid-media retries, not successful
            # recovery or budget deferrals.
            counts_as_attempt = result.get('status') == 'download_failed'
            asset['repair_attempts'] = attempts + int(counts_as_attempt)
            # Carry the last successfully saved file's metadata through repeated
            # failed repairs, rather than nesting an ever-growing retry history.
            prior_saved = prior if prior.get('status') in _SAVED else prior.get('prior_download')
            if prior_saved and result.get('status') not in _SAVED:
                result['prior_download'] = deepcopy(prior_saved)
                result['cache_integrity'] = 'missing_or_corrupt'
            asset['download'] = result
            if result.get('status') in _SAVED:
                asset.pop('repair_status', None)
            elif result.get('status') == 'download_failed' and asset['repair_attempts'] >= max_attempts:
                asset['repair_status'] = 'retry_exhausted'
    return repaired


class _AllowedRedirect(urllib.request.HTTPRedirectHandler):
    handler_order = 499

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not allowed_media_url(newurl):
            raise urllib.error.URLError('unsupported_redirect')
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class MediaStore:
    """Streaming, atomic, checksummed downloads with three rendition attempts.

    ``new_bytes`` counts bytes read during this run, including failed attempts.
    HLS manifests are reported without fetching/downloading playlist segments.
    ``opener`` can be supplied by the browser backend for its in-memory session.
    """
    def __init__(self, out, db, *, max_file_mb=30, max_run_mb=150, timeout=20, wall_timeout=60):
        for value in (max_file_mb, max_run_mb, timeout, wall_timeout):
            if not math.isfinite(float(value)) or float(value) <= 0:
                raise ValueError('Media limits and timeouts must be finite and positive')
        self.out, self.db = Path(out).resolve(), db
        self.max_file = int(float(max_file_mb) * 1024 * 1024)
        self.max_run = int(float(max_run_mb) * 1024 * 1024)
        if min(self.max_file, self.max_run) < 1:
            raise ValueError('Media byte limits must be at least one byte')
        self.timeout, self.wall_timeout = float(timeout), float(wall_timeout)
        self.new_bytes = 0
        self.opener = urllib.request.build_opener(_AllowedRedirect())
        self.opener.addheaders = [('User-Agent', 'Mozilla/5.0')]
        self.out.mkdir(parents=True, exist_ok=True)
        media_directory = _safe_saved_path(self.out, 'media/.directory_check')
        if media_directory is None:
            raise ValueError('Media directory escapes output folder')
        media_directory.parent.mkdir(parents=True, exist_ok=True)
        db.execute('CREATE TABLE IF NOT EXISTS assets(asset_key TEXT PRIMARY KEY, result_json TEXT NOT NULL)')

    @property
    def opener(self):
        return self._opener

    @opener.setter
    def opener(self, value):
        # Keep the allowlist guard when the runner supplies its browser-session
        # opener; it runs before the generic redirect handler and preserves the
        # browser's cookie jar without inspecting/persisting its credentials.
        if hasattr(value, 'add_handler') and not any(isinstance(handler, _AllowedRedirect)
                                                    for handler in getattr(value, 'handlers', [])):
            value.add_handler(_AllowedRedirect())
        self._opener = value

    @staticmethod
    def _asset_key(asset, url):
        # Match the previous collector's stable kind/media-ID cache keys.
        try:
            url_path = urlsplit(url).path
        except ValueError:
            url_path = url
        identity = asset.get('media_id') or url_path
        return hashlib.sha256(f"{asset.get('kind', '')}|{identity}".encode()).hexdigest()

    def _reuse(self, asset, url):
        saved = asset.get('download') or {}
        review = _saved_placeholder_review(saved, asset.get('kind'), self.out)
        if review is not None:
            return review
        if saved.get('status') in _SAVED and valid_media(saved.get('file'), asset.get('kind'), saved, out=self.out):
            return {**saved, 'status': 'reused'}
        row = self.db.execute('SELECT result_json FROM assets WHERE asset_key=?', (self._asset_key(asset, url),)).fetchone()
        if row:
            try:
                previous = json.loads(row[0])
                review = _saved_placeholder_review(previous, asset.get('kind'), self.out)
                if review is not None:
                    if review != previous:
                        self.db.execute('UPDATE assets SET result_json=? WHERE asset_key=?',
                                        (json.dumps(review), self._asset_key(asset, url)))
                        self.db.commit()
                    return review
                if previous.get('status') in _SAVED and valid_media(previous.get('file'), asset.get('kind'), previous, out=self.out):
                    return {**previous, 'status': 'reused'}
            except (ValueError, TypeError):
                pass
        return None

    def download(self, asset, referer):
        raw_urls = [asset.get('url', '')] + [c.get('url', '') for c in asset.get('candidates', []) if isinstance(c, dict)]
        urls = list(dict.fromkeys(u for u in raw_urls if isinstance(u, str)))
        kind = asset.get('kind')
        if kind not in {'image', 'video'}:
            return {'status': 'unsupported_media', 'bytes': 0}
        for url in urls or ['']:
            reused = self._reuse(asset, url)
            if reused is not None:
                return reused
        permitted = [u for u in urls if allowed_media_url(u)]
        progressive = [u for u in permitted if '.m3u8' not in urlsplit(u).path.lower()]
        if not progressive:
            return {'status': 'stream_manifest_only' if permitted else 'unsupported_url', 'bytes': 0}
        attempts, result, selected = [], {'status': 'download_failed', 'bytes': 0}, None
        transferred_before = self.new_bytes
        for selected in progressive[:3]:
            result = self._download_one({**asset, 'url': selected}, referer)
            attempts.append({'url': selected, 'status': result['status'], 'bytes': result.get('transferred_bytes', 0),
                             **({'error': result['error']} if result.get('error') else {})})
            if result['status'] in {'downloaded', 'reused', 'run_budget', PLACEHOLDER_REVIEW_STATUS}:
                break
        return {**result, 'selected_url': selected, 'attempts': attempts,
                'transferred_bytes': self.new_bytes - transferred_before}

    def _download_one(self, asset, referer):
        url, kind = asset.get('url', ''), asset.get('kind', '')
        if self.new_bytes >= self.max_run:
            return {'status': 'run_budget', 'bytes': 0}
        key = self._asset_key(asset, url)
        extension = Path(urlsplit(url).path).suffix.lower()
        allowed_extensions = ({'.jpg', '.jpeg', '.png', '.gif', '.webp', '.avif', '.heic'} if kind == 'image'
                              else {'.mp4', '.webm', '.mov'})
        if extension not in allowed_extensions:
            extension = '.mp4' if kind == 'video' else '.jpg'
        relative = f'media/{kind}_{key[:24]}{extension}'
        target = _safe_saved_path(self.out, relative)
        if target is None:
            return {'status': 'download_failed', 'bytes': 0, 'error': 'unsafe_output_path'}
        temporary = target.with_suffix(target.suffix + '.part')
        if temporary.is_symlink():
            return {'status': 'download_failed', 'bytes': 0, 'error': 'unsafe_partial_path'}
        started, count, digest = time.monotonic(), 0, hashlib.sha256()
        cap = min(self.max_file, self.max_run - self.new_bytes)
        length = None
        result = {'status': 'download_failed', 'bytes': 0}
        try:
            request = urllib.request.Request(url, headers={'Referer': referer, 'Accept': '*/*'})
            with self.opener.open(request, timeout=self.timeout) as response:
                if not allowed_media_url(response.url):
                    return {'status': 'unsupported_redirect', 'bytes': 0}
                content_type = response.headers.get('Content-Type', '').split(';', 1)[0].strip().lower()
                if content_type in {'text/html', 'application/xhtml+xml', 'application/json', 'text/plain'}:
                    raise ValueError('response_is_not_media')
                length_header = response.headers.get('Content-Length')
                if length_header is not None:
                    length = int(length_header)
                    if length < 0:
                        raise ValueError('invalid_content_length')
                    if length > cap:
                        return {'status': 'size_limit' if length > self.max_file else 'run_budget',
                                'bytes': 0, 'content_length': length, 'max_file_bytes': self.max_file}
                with temporary.open('wb') as stream:
                    while True:
                        if time.monotonic() - started > self.wall_timeout:
                            raise TimeoutError('media_wall_timeout')
                        if count >= cap:
                            if length is not None and count == length:
                                break
                            raise ValueError('size_limit' if cap == self.max_file else 'run_budget')
                        try:
                            block = response.read(min(65536, cap - count))
                        except http.client.IncompleteRead as error:
                            # Chunked responses can return partial bytes by
                            # exception rather than through read's return value.
                            block = error.partial
                            count += len(block)
                            self.new_bytes += len(block)
                            digest.update(block)
                            stream.write(block)
                            raise ValueError('incomplete_download') from error
                        if not block:
                            break
                        count += len(block)
                        self.new_bytes += len(block)
                        digest.update(block)
                        stream.write(block)
                if length is not None and count != length:
                    raise ValueError('incomplete_download')
                actual_digest = digest.hexdigest()
                placeholder = _known_placeholder(kind, actual_digest)
                if placeholder is not None:
                    # Preserve any earlier rendition at target. New placeholder
                    # evidence has its own deterministic path and actual GIF suffix.
                    relative = f'media/{kind}_{key[:24]}_review_{actual_digest[:12]}.gif'
                    review_target = _safe_saved_path(self.out, relative)
                    if review_target is None:
                        raise ValueError('unsafe_output_path')
                    if review_target.exists():
                        if hashlib.sha256(review_target.read_bytes()).hexdigest() != actual_digest:
                            raise ValueError('placeholder_evidence_collision')
                    else:
                        temporary.rename(review_target)
                    result = mark_placeholder_review(
                        {'file': relative, 'bytes': count, 'sha256': actual_digest,
                         'retrieved_at': _utc_now(), 'content_type': content_type}, kind)
                else:
                    if not valid_media(temporary, kind):
                        raise ValueError('response_is_not_media')
                    temporary.replace(target)
                    result = {'status': 'downloaded', 'file': relative, 'bytes': count,
                              'sha256': actual_digest, 'retrieved_at': _utc_now(), 'content_type': content_type}
                self.db.execute('INSERT OR REPLACE INTO assets VALUES(?,?)', (key, json.dumps(result)))
                self.db.commit()
        except urllib.error.HTTPError as error:
            result['error'] = f'HTTP {error.code}'
        except Exception as error:
            # Avoid storing arbitrary exception text, which can contain URLs,
            # request headers, or session credentials.
            reason = error.reason if isinstance(error, urllib.error.URLError) else error
            label = str(reason)
            known = {'size_limit', 'run_budget', 'media_wall_timeout', 'incomplete_download',
                     'response_is_not_media', 'invalid_content_length', 'unsupported_redirect',
                     'unsafe_output_path', 'placeholder_evidence_collision'}
            result['error'] = label if label in known else type(error).__name__
            if label in {'size_limit', 'run_budget', 'unsupported_redirect'}:
                result['status'] = label
        finally:
            try:
                if temporary.exists():
                    temporary.unlink()
            except OSError:
                result['partial_cleanup_error'] = True
        if result['status'] != 'downloaded':
            result['bytes'] = count
        if result['status'] == 'size_limit':
            result['max_file_bytes'] = self.max_file
        result['transferred_bytes'] = count
        return result
