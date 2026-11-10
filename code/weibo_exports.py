"""Portable, network-free views connecting downloaded media to export rows."""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
import csv
from html import escape
import json
from pathlib import Path, PureWindowsPath
import sqlite3
from urllib.parse import quote, urlsplit
from weibo_media import PLACEHOLDER_REVIEW_STATUS


def _text(value):
    return '' if value is None else str(value)


def _source_key(row):
    return tuple(_text(row.get(k)) for k in ('_source_filepath', '_sheet', '_excel_row'))


def _source_lookup(out):
    path = out / 'samples.json'
    if not path.is_file():
        return {}
    rows = json.loads(path.read_text(encoding='utf-8'))
    return {_source_key(row): row for row in rows if isinstance(row, dict)}


def source_contexts(record, lookup):
    """Old absolute source paths are identifiers, never files opened here."""
    refs = [ref for ref in (record.get('source_refs') or []) if isinstance(ref, dict)] or [{}]
    return [{**lookup.get(_source_key(ref), {}), **ref} for ref in refs]


def media_file(asset, out):
    """Only link a relative file beneath this output folder."""
    raw = _text((asset.get('download') or {}).get('file')).replace('\\', '/')
    if not raw or Path(raw).is_absolute() or PureWindowsPath(raw).is_absolute() or PureWindowsPath(raw).drive:
        return '', False
    candidate = out / raw
    try:
        candidate.resolve().relative_to(out.resolve())
    except (ValueError, OSError):
        return '', False
    relative = Path(raw).as_posix()
    saved = (asset.get('download') or {}).get('status') in {'downloaded', 'reused'} and candidate.is_file()
    return relative, saved


def _post_link(url):
    raw = _text(url)
    try:
        parsed = urlsplit(raw)
        host = (parsed.hostname or '').lower()
    except ValueError:
        return ''
    if parsed.scheme in {'http', 'https'} and not parsed.username and any(host == d or host.endswith('.' + d) for d in ('weibo.com', 'weibo.cn')):
        return raw
    return ''


def _labels(record, *, checkin=False):
    return list(dict.fromkeys(_text(x.get('label')) for x in (record.get('locations') or [])
                              if x.get('label') and (not checkin or x.get('is_checkin') is True)))


def _checkins(record):
    """A POI may be exposed in several fields of the same source post."""
    result, seen = [], set()
    for location in (record.get('locations') or []):
        if location.get('is_checkin') is not True:
            continue
        poi_id = _text(location.get('poi_id')).removeprefix('1022:')
        identity = (_text(location.get('source_post_id')), poi_id or _text(location.get('label')) or _text(location.get('field')))
        if identity not in seen:
            result.append(location)
            seen.add(identity)
    return result


def _post_rows(records, out, lookup, has_coordinates):
    for record in records:
        media, locations = record.get('media') or [], record.get('locations') or []
        statuses = Counter((m.get('download') or {}).get('status', 'not_attempted') for m in media)
        contexts = source_contexts(record, lookup)
        files = list(dict.fromkeys(path for asset in media for path, saved in [media_file(asset, out)] if saved))
        yield {
            'post_url': record.get('source_url'), 'post_id': record.get('post_id'), 'status': record.get('status'),
            'regions': '|'.join(sorted({_text(x.get('_region')) for x in contexts if x.get('_region')})),
            'source_rows': '|'.join(_text(x.get('_excel_row')) for x in contexts),
            'post_text_snippet': (record.get('post') or {}).get('text_snippet', ''),
            'post_created_at': (record.get('post') or {}).get('created_at', ''),
            'export_text': contexts[0].get('标题/微博内容', ''), 'export_date': contexts[0].get('日期', ''),
            'images_found': sum(m.get('kind') == 'image' for m in media),
            'videos_found': sum(m.get('kind') == 'video' for m in media),
            'media_downloaded': statuses['downloaded'], 'media_reused': statuses['reused'],
            'media_failed_or_skipped': sum(n for s, n in statuses.items() if s not in ('downloaded', 'reused')),
            'media_files': '|'.join(files), 'media_saved_files': len(files),
            'location_entries': len(locations), 'explicit_coordinate_entries': sum(has_coordinates(x) for x in locations),
            'location_labels': '|'.join(_labels(record)),
            'checkin_entries': len(_checkins(record)),
            'checkin_labels': '|'.join(_labels(record, checkin=True)), 'error': record.get('error') or '',
        }


def media_rows(records, out, lookup):
    """One attachment per originating export row; shared files stay shared."""
    for record in records:
        for index, asset in enumerate(record.get('media') or [], 1):
            download = asset.get('download') or {}
            relative, saved = media_file(asset, out)
            for source in source_contexts(record, lookup):
                yield {
                    'post_url': record.get('source_url'), 'post_id': record.get('post_id'), 'post_status': record.get('status'),
                    'attachment_index': index, 'media_kind': asset.get('kind'), 'media_id': asset.get('media_id'),
                    'media_role': asset.get('role'), 'media_source_post_id': asset.get('source_post_id'),
                    'media_source_bid': asset.get('source_bid'), 'file_relative': relative, 'file_saved': int(saved),
                    'download_status': download.get('status', 'not_attempted'), 'bytes': download.get('bytes', 0),
                    'sha256': download.get('sha256', ''), 'media_url': download.get('selected_url') or asset.get('url', ''),
                    'source_workbook': source.get('_source_filepath'), 'source_folder': source.get('_source_folder'),
                    'source_region': source.get('_region'), 'source_sheet': source.get('_sheet'),
                    'source_excel_row': source.get('_excel_row'), 'export_record_id': source.get('序号'),
                    'export_post_url': source.get('原文/评论链接', ''), 'export_date': source.get('日期', ''),
                    'export_text': source.get('标题/微博内容', ''), 'export_comment': source.get('评论内容', ''),
                    'post_text_snippet': (record.get('post') or {}).get('text_snippet', ''),
                    'post_created_at': (record.get('post') or {}).get('created_at', ''),
                    'download_error': download.get('error', ''), 'post_error': record.get('error') or '',
                }


def location_rows(records, lookup):
    """Keep raw field evidence; unique check-in counts are in posts.csv."""
    for record in records:
        for index, location in enumerate(record.get('locations') or [], 1):
            for source in source_contexts(record, lookup):
                yield {
                    'post_url': record.get('source_url'), 'post_id': record.get('post_id'), 'post_status': record.get('status'),
                    'location_index': index, 'location_kind': location.get('kind'), 'location_field': location.get('field'),
                    'is_checkin': int(location.get('is_checkin') is True), 'label': location.get('label', ''),
                    'poi_id': location.get('poi_id', ''), 'location_role': location.get('role'),
                    'location_source_post_id': location.get('source_post_id'), 'location_source_bid': location.get('source_bid'),
                    'coordinates_raw_json': json.dumps(location.get('coordinates'), ensure_ascii=False) if location.get('coordinates') is not None else '',
                    'location_evidence_json': json.dumps(location.get('evidence_objects', location.get('evidence')), ensure_ascii=False) if location.get('evidence_objects', location.get('evidence')) is not None else '',
                    'location_evidence_fields': '|'.join(_text(value) for value in (location.get('evidence_fields') or [])),
                    'location_raw_json': json.dumps(location, ensure_ascii=False),
                    'source_workbook': source.get('_source_filepath'), 'source_sheet': source.get('_sheet'),
                    'source_excel_row': source.get('_excel_row'), 'source_region': source.get('_region'),
                    'export_date': source.get('日期', ''), 'export_text': source.get('标题/微博内容', ''),
                }


def _write_csv(path, rows, fields):
    with path.open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_text(value) if isinstance(value, str) else value for key, value in row.items()})


CSV_FREE_TEXT_FIELDS = {'post_text_snippet', 'export_text', 'export_comment', 'error', 'download_error', 'post_error',
                        'checkin_labels', 'location_labels', 'label', 'source_region', 'source_folder', 'regions'}


def _csv_text(value):
    """Protect spreadsheet-opening text columns while retaining raw JSON text."""
    text = _text(value)
    if text.lstrip(' \t\r\n').startswith(('=', '+', '-', '@')):
        return "'" + text
    return value


POST_FIELDS = ['post_url', 'post_id', 'status', 'regions', 'source_rows', 'post_text_snippet', 'post_created_at',
               'export_text', 'export_date', 'images_found', 'videos_found', 'media_downloaded', 'media_reused',
               'media_failed_or_skipped', 'media_files', 'media_saved_files', 'location_entries',
               'explicit_coordinate_entries', 'location_labels', 'checkin_entries', 'checkin_labels', 'error']
MEDIA_FIELDS = ['post_url', 'post_id', 'post_status', 'attachment_index', 'media_kind', 'media_id', 'media_role',
                'media_source_post_id', 'media_source_bid', 'file_relative', 'file_saved', 'download_status',
                'bytes', 'sha256', 'media_url', 'source_workbook', 'source_folder', 'source_region', 'source_sheet',
                'source_excel_row', 'export_record_id', 'export_post_url', 'export_date', 'export_text',
                'export_comment', 'post_text_snippet', 'post_created_at', 'download_error', 'post_error']
LOCATION_FIELDS = ['post_url', 'post_id', 'post_status', 'location_index', 'location_kind', 'location_field',
                   'is_checkin', 'label', 'poi_id', 'location_role', 'location_source_post_id', 'location_source_bid',
                   'coordinates_raw_json', 'location_evidence_json', 'location_evidence_fields', 'location_raw_json', 'source_workbook',
                   'source_sheet', 'source_excel_row', 'source_region', 'export_date', 'export_text']


def _e(value):
    return escape(_text(value), quote=True)


def _write_html(records, out, lookup):
    with (out / 'index.html').open('w', encoding='utf-8') as stream:
        stream.write('''<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; img-src 'self' data:; media-src 'self'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'">
<title>Weibo collection: posts and media</title><style>
body{font:16px/1.55 system-ui,sans-serif;max-width:1200px;margin:32px auto;padding:0 20px;color:#182433;background:#f5f7fa}
a{color:#1658b3;overflow-wrap:anywhere}h1{font-size:28px}h2{font-size:21px;margin:0}p{margin:8px 0}.muted{color:#526172;font-size:14px}
article{background:white;border:1px solid #dbe2eb;border-radius:12px;margin:24px 0;padding:22px;overflow-wrap:anywhere}
.context{white-space:pre-wrap}.gallery{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:16px;margin-top:18px}
figure{margin:0;padding:12px;background:#f8fafc;border:1px solid #e2e7ee;border-radius:8px}img,video{display:block;max-width:100%;max-height:300px;object-fit:contain;margin:0 auto 10px}figcaption{font-size:14px}
details{margin-top:12px}summary{cursor:pointer;font-weight:600}.source{border-top:1px solid #e0e7ef;padding-top:10px;margin-top:10px}.badge{display:inline-block;background:#e8eef7;padding:2px 9px;border-radius:12px;font-size:13px}
</style><body><h1>Weibo collection: posts and media</h1>
<p>Each card links collected attachments to its Weibo post and original export rows. Media stay in the adjacent media folder; keep this folder together when moving computers.</p>
<p><a href="posts.csv">Post table</a> · <a href="media_index.csv">Attachment-to-export-row table</a> · <a href="location_index.csv">Location evidence table</a> · <a href="results.jsonl">Full records</a></p>
''')
        stream.write(f'<p class="muted">{len(records)} collected post links. Retrieval status and attachment download status are shown separately.</p>')
        for number, record in enumerate(records, 1):
            source_url = _post_link(record.get('source_url'))
            title = f'Post {number}' + (f' · {record["post_id"]}' if record.get('post_id') else '')
            stream.write(f'<article id="post-{number}"><h2>{_e(title)}</h2><p><span class="badge">{_e(record.get("status"))}</span></p>')
            if source_url:
                stream.write(f'<p><a href="{_e(source_url)}" target="_blank" rel="noopener noreferrer">Open source Weibo link</a></p>')
            if record.get('error'):
                stream.write(f'<p class="muted">{_e(record["error"])}</p>')
            post = record.get('post') or {}
            if post.get('text_snippet'):
                stream.write(f'<p class="context">{_e(post["text_snippet"])}</p>')
                stream.write(f'<p class="muted">Post date: {_e(post.get("created_at"))}</p>')
            contexts = source_contexts(record, lookup)
            stream.write(f'<details><summary>Original export context · {len(contexts)} row(s)</summary>')
            for source in contexts:
                stream.write('<div class="source">')
                stream.write(f'<p class="muted">{_e(source.get("_region"))} · {_e(source.get("_source_filepath"))}<br>Sheet {_e(source.get("_sheet"))}, Excel row {_e(source.get("_excel_row"))} · Export date {_e(source.get("日期"))}</p>')
                if source.get('标题/微博内容'):
                    stream.write(f'<p class="context">{_e(source["标题/微博内容"])}</p>')
                if source.get('评论内容'):
                    stream.write(f'<p class="context">Exported comment: {_e(source["评论内容"])}</p>')
                stream.write('</div>')
            stream.write('</details>')
            if record.get('locations'):
                stream.write('<details><summary>Explicit location data</summary>')
                for location in record['locations']:
                    label = location.get('label') or location.get('kind') or location.get('field')
                    tag = ' · explicit check-in tag' if location.get('is_checkin') is True else ''
                    stream.write(f'<p>{_e(label)}{tag} · owner {_e(location.get("source_post_id"))}</p><pre class="context">{_e(json.dumps(location, ensure_ascii=False, indent=2))}</pre>')
                stream.write('</details>')
            stream.write('<div class="gallery">')
            for index, asset in enumerate(record.get('media') or [], 1):
                download = asset.get('download') or {}
                relative, saved = media_file(asset, out)
                stream.write('<figure>')
                if saved:
                    href = quote(relative, safe='/')
                    if asset.get('kind') == 'image':
                        stream.write(f'<a href="{_e(href)}"><img src="{_e(href)}" loading="lazy" alt="Attachment {_e(index)}"></a>')
                    elif asset.get('kind') == 'video':
                        stream.write(f'<video controls preload="none" src="{_e(href)}"></video>')
                    stream.write(f'<p><a href="{_e(href)}">{_e(relative)}</a></p>')
                stream.write(f'<figcaption>Attachment {index} · {_e(asset.get("kind"))}<br>Download: {_e(download.get("status", "not_attempted"))}')
                if download.get('status') == PLACEHOLDER_REVIEW_STATUS:
                    stream.write(' · placeholder file retained; review required')
                elif relative and not saved:
                    stream.write(' · file not present in this folder')
                stream.write(f'<br>Media owner: {_e(asset.get("source_post_id"))} · role {_e(asset.get("role"))}')
                if download.get('error'):
                    stream.write(f'<br>{_e(download["error"])}')
                stream.write('</figcaption></figure>')
            stream.write('</div></article>')
        stream.write('</body></html>')


def write_linked_outputs(records, out, has_coordinates):
    out = Path(out)
    lookup = _source_lookup(out)
    _write_csv(out / 'posts.csv', _post_rows(records, out, lookup, has_coordinates), POST_FIELDS)
    rows = list(media_rows(records, out, lookup))
    _write_csv(out / 'media_index.csv', rows, MEDIA_FIELDS)
    places = list(location_rows(records, lookup))
    _write_csv(out / 'location_index.csv', places, LOCATION_FIELDS)
    _write_html(records, out, lookup)
    return {'media_source_row_links': len(rows), 'saved_media_source_row_links': sum(row['file_saved'] for row in rows),
            'location_source_row_links': len(places), 'unique_checkins': sum(len(_checkins(record)) for record in records),
            'post_table': 'posts.csv', 'media_index': 'media_index.csv', 'location_index': 'location_index.csv', 'offline_browser': 'index.html'}


STREAM_POST_FIELDS = POST_FIELDS + ['source_count', 'source_details_file', 'queue_id',
                                  'latest_attempt_status', 'latest_attempt_error', 'latest_attempt_at', 'retained_success']



def _attempt_context(record):
    attempt = record.get('last_attempt')
    if not isinstance(attempt, dict):
        attempt = {}
    return {'latest_attempt_status': attempt.get('status') or '',
            'latest_attempt_error': attempt.get('error') or '',
            'latest_attempt_at': attempt.get('at') or '',
            'retained_success': int(bool(attempt) and record.get('status') == 'success' and attempt.get('status') != 'success')}


def _guarded_row(writer, row):
    writer.writerow({key: _csv_text(value) if isinstance(value, str) else value for key, value in row.items()})


def _stream_page_header(stream, prefix, title):
    stream.write("""<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; img-src 'self' data:; media-src 'self'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'">
<title>Weibo collection</title><style>
body{font:16px/1.55 system-ui,sans-serif;max-width:1200px;margin:32px auto;padding:0 20px;color:#182433;background:#f5f7fa}
a{color:#1658b3;overflow-wrap:anywhere}h1{font-size:28px}h2{font-size:21px;margin:0}p{margin:8px 0}.muted{color:#526172;font-size:14px}
article{background:white;border:1px solid #dbe2eb;border-radius:12px;margin:24px 0;padding:22px;overflow-wrap:anywhere}
.context{white-space:pre-wrap}.gallery{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:16px;margin-top:18px}
figure{margin:0;padding:12px;background:#f8fafc;border:1px solid #e2e7ee;border-radius:8px}img,video{display:block;max-width:100%;max-height:300px;object-fit:contain;margin:0 auto 10px}figcaption{font-size:14px}
details{margin-top:12px}summary{cursor:pointer;font-weight:600}.source{border-top:1px solid #e0e7ef;padding-top:10px;margin-top:10px}.badge{display:inline-block;background:#e8eef7;padding:2px 9px;border-radius:12px;font-size:13px}
</style><body>""")
    stream.write(f'<h1>{_e(title)}</h1><p>Explicit location evidence and available images/videos linked to each original export row.</p>')
    for filename, label in [('posts.csv', 'Post table'), ('media_index.csv', 'Media mappings'),
                            ('location_index.csv', 'Location evidence'), ('sources.jsonl', 'All original source rows'),
                            ('results.jsonl', 'Full collected records')]:
        stream.write(f'<a href="{_e(prefix + filename)}">{_e(label)}</a> · ')
    stream.write('<p class="muted">Keep the output folder together when moving it. Only saved local media are displayed.</p>')


def _stream_article(stream, record, out, contexts, number, source_count):
    source_url = _post_link(record.get('source_url'))
    title = f'Post {number}' + (f' · {record["post_id"]}' if record.get('post_id') else '')
    attempt = _attempt_context(record)
    status_label = _text(record.get('status'))
    if attempt['retained_success']:
        status_label += ' · retained from an earlier collection'
    stream.write(f'<article id="post-{number}"><h2>{_e(title)}</h2><p><span class="badge">{_e(status_label)}</span></p>')
    if record.get('last_attempt'):
        stream.write(f'<p class="muted">Latest collection attempt: {_e(attempt["latest_attempt_status"])}')
        if attempt['latest_attempt_at']:
            stream.write(f' · {_e(attempt["latest_attempt_at"])}')
        if attempt['latest_attempt_error']:
            stream.write(f'<br>{_e(attempt["latest_attempt_error"])}')
        stream.write('</p>')
    if attempt['retained_success']:
        stream.write('<p class="muted">Previously successful metadata and saved media are retained. The latest attempt did not confirm current post availability.</p>')
    if source_url:
        stream.write(f'<p><a href="{_e(source_url)}" target="_blank" rel="noopener noreferrer">Open source Weibo link</a></p>')
    if record.get('error'):
        stream.write(f'<p class="muted">{_e(record["error"])}</p>')
    post = record.get('post') or {}
    if post.get('text_snippet'):
        stream.write(f'<p class="context">{_e(post["text_snippet"])}</p>')
        stream.write(f'<p class="muted">Post date: {_e(post.get("created_at"))}</p>')
    stream.write(f'<details><summary>Original export context · {source_count} row(s)</summary>')
    if source_count > len(contexts):
        stream.write('<p class="muted">First 20 contexts are displayed. All source rows and their post mappings are retained in sources.jsonl.</p>')
    for source in contexts:
        stream.write('<div class="source">')
        stream.write(f'<p class="muted">{_e(source.get("_region"))} · {_e(source.get("_source_filepath"))}<br>Sheet {_e(source.get("_sheet"))}, Excel row {_e(source.get("_excel_row"))} · Export date {_e(source.get("日期"))}</p>')
        if source.get('标题/微博内容'):
            stream.write(f'<p class="context">{_e(source["标题/微博内容"])}</p>')
        if source.get('评论内容'):
            stream.write(f'<p class="context">Exported comment: {_e(source["评论内容"])}</p>')
        stream.write('</div>')
    stream.write('</details>')
    if record.get('locations'):
        stream.write('<details><summary>Explicit location data</summary>')
        for location in record['locations']:
            label = location.get('label') or location.get('kind') or location.get('field')
            tag = ' · explicit check-in tag' if location.get('is_checkin') is True else ''
            stream.write(f'<p>{_e(label)}{tag} · owner {_e(location.get("source_post_id"))}</p><pre class="context">{_e(json.dumps(location, ensure_ascii=False, indent=2))}</pre>')
        stream.write('</details>')
    stream.write('<div class="gallery">')
    for index, asset in enumerate(record.get('media') or [], 1):
        download = asset.get('download') or {}
        relative, saved = media_file(asset, out)
        stream.write('<figure>')
        if saved:
            href = quote('../' + relative, safe='/')
            if asset.get('kind') == 'image':
                stream.write(f'<a href="{_e(href)}"><img src="{_e(href)}" loading="lazy" alt="Attachment {_e(index)}"></a>')
            elif asset.get('kind') == 'video':
                stream.write(f'<video controls preload="none" src="{_e(href)}"></video>')
            stream.write(f'<p><a href="{_e(href)}">{_e(relative)}</a></p>')
        stream.write(f'<figcaption>Attachment {index} · {_e(asset.get("kind"))}<br>Download: {_e(download.get("status", "not_attempted"))}')
        if download.get('status') == PLACEHOLDER_REVIEW_STATUS:
            stream.write(' · placeholder file retained; review required')
        elif relative and not saved:
            stream.write(' · file not present in this folder')
        stream.write(f'<br>Media owner: {_e(asset.get("source_post_id"))} · role {_e(asset.get("role"))}')
        if download.get('error'):
            stream.write(f'<br>{_e(download["error"])}')
        stream.write('</figcaption></figure>')
    stream.write('</div></article>')


def write_streaming_outputs(records_factory, out, has_coordinates, *, sources_for_record=None,
                            sources_factory=None, page_size=100):
    """Write complete outputs from persisted state with bounded application memory.

    ``records_factory()`` yields collected result dicts, normally with one source
    reference and ``queue_id``. ``sources_for_record(record)`` streams the full
    matching input rows; ``sources_factory()`` streams all input rows, including
    unsupported links, with ``_queue_id``. Source JSONL is the complete mapping;
    posts.csv retains the first source row plus the total count. HTML shows at
    most 20 contexts per post and at most ``page_size`` posts per page.
    Factories must read a stable committed checkpoint during export.
    """
    if not isinstance(page_size, int) or page_size < 1:
        raise ValueError('page_size must be a positive integer.')
    out = Path(out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    pages = out / 'pages'
    pages.mkdir(exist_ok=True)
    counts = {'posts': 0, 'source_rows': 0, 'pages': 0, 'media_source_row_links': 0,
              'saved_media_source_row_links': 0, 'location_source_row_links': 0, 'unique_checkins': 0}
    # No full input manifest or result list is loaded into memory. The fallback
    # writes source rows as each processed post is visited, for older callers.
    from contextlib import ExitStack
    with ExitStack() as stack:
        result_stream = stack.enter_context((out / 'results.jsonl').open('w', encoding='utf-8'))
        source_stream = stack.enter_context((out / 'sources.jsonl').open('w', encoding='utf-8'))
        csv_streams = [(name, fields) for name, fields in [('posts.csv', STREAM_POST_FIELDS),
                      ('media_index.csv', MEDIA_FIELDS), ('location_index.csv', LOCATION_FIELDS)]]
        writers = {}
        for name, fields in csv_streams:
            stream = stack.enter_context((out / name).open('w', encoding='utf-8-sig', newline=''))
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writers[name] = writer
        if sources_factory is not None:
            for source in sources_factory():
                source_stream.write(json.dumps(source, ensure_ascii=False) + '\n')
                counts['source_rows'] += 1
        index = stack.enter_context((out / 'index.html').open('w', encoding='utf-8'))
        _stream_page_header(index, '', 'Weibo collection: posts and media')
        index.write('<p>Browse the collected posts in pages:</p><ol>')
        records = iter(records_factory())
        close_records = getattr(records, 'close', None)
        if close_records:
            stack.callback(close_records)
        sentinel = object()
        current = next(records, sentinel)
        while current is not sentinel:
            counts['pages'] += 1
            page_number = counts['pages']
            page_file = f'page-{page_number:06d}.html'
            first_post = counts['posts'] + 1
            with (pages / page_file).open('w', encoding='utf-8') as page:
                _stream_page_header(page, '../', f'Weibo collection · page {page_number}')
                page.write('<p><a href="../index.html">All pages</a></p>')
                for _ in range(page_size):
                    if current is sentinel:
                        break
                    record = current
                    counts['posts'] += 1
                    result_stream.write(json.dumps(record, ensure_ascii=False) + '\n')
                    contexts, source_count = [], 0
                    sources = sources_for_record(record) if sources_for_record else source_contexts(record, {})
                    iterator = iter(sources)
                    try:
                        for source in iterator:
                            source_count += 1
                            if len(contexts) < 20:
                                contexts.append(source)
                            if sources_factory is None:
                                source_stream.write(json.dumps({**source, '_queue_id': record.get('queue_id', record.get('post_id'))}, ensure_ascii=False) + '\n')
                                counts['source_rows'] += 1
                            linked = {**record, 'source_refs': [source]}
                            for row in media_rows([linked], out, {}):
                                _guarded_row(writers['media_index.csv'], row)
                                counts['media_source_row_links'] += 1
                                counts['saved_media_source_row_links'] += row['file_saved']
                            for row in location_rows([linked], {}):
                                _guarded_row(writers['location_index.csv'], row)
                                counts['location_source_row_links'] += 1
                    finally:
                        close_sources = getattr(iterator, 'close', None)
                        if close_sources:
                            close_sources()
                    # Post summaries intentionally identify the first source.
                    # Unbounded source-row concatenation is replaced by a count.
                    summary_record = {**record, 'source_refs': contexts[:1]}
                    row = next(_post_rows([summary_record], out, {}, has_coordinates))
                    row.update(source_count=source_count, source_details_file='sources.jsonl', queue_id=record.get('queue_id', record.get('post_id', '')))
                    row.update(_attempt_context(record))
                    _guarded_row(writers['posts.csv'], row)
                    counts['unique_checkins'] += len(_checkins(record))
                    _stream_article(page, record, out, contexts, counts['posts'], source_count)
                    current = next(records, sentinel)
                page.write('<nav><a href="../index.html">All pages</a>')
                if page_number > 1:
                    page.write(f' · <a href="page-{page_number - 1:06d}.html">Previous page</a>')
                if current is not sentinel:
                    page.write(f' · <a href="page-{page_number + 1:06d}.html">Next page</a>')
                page.write('</nav></body></html>')
            index.write(f'<li><a href="pages/{page_file}">Page {page_number} · posts {first_post}–{counts["posts"]}</a></li>')
        index.write(f'</ol><p>{counts["posts"]} collected posts; {counts["source_rows"]} original input rows retained.</p></body></html>')
    return {**counts, 'post_table': 'posts.csv', 'media_index': 'media_index.csv',
            'location_index': 'location_index.csv', 'sources': 'sources.jsonl',
            'results': 'results.jsonl', 'offline_browser': 'index.html'}


def main(argv=None):
    parser = argparse.ArgumentParser(description='Regenerate portable post/media views from a committed checkpoint without any network requests.')
    parser.add_argument('--out', type=Path, default=Path('pilot'))
    parser.add_argument('--page-size', type=int, default=100)
    args = parser.parse_args(argv)
    out = args.out.resolve()
    database = out / 'state.sqlite'
    if not database.is_file():
        parser.error(f'No checkpoint found: {database}')
    from collect import has_coordinates
    with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True)) as db:
        # A read transaction prevents concurrent collection from changing rows
        # partway through a single export operation.
        db.execute('BEGIN')
        queue_schema = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='sources'").fetchone()
        if queue_schema:
            def sources_for_record(record):
                for (raw,) in db.execute('SELECT row_json FROM sources WHERE post_id=? ORDER BY source_id', (record['queue_id'],)):
                    yield json.loads(raw)

            def sources_factory():
                for raw, post_id, input_error in db.execute('SELECT row_json,post_id,input_error FROM sources ORDER BY source_id'):
                    source = json.loads(raw)
                    source['_queue_id'] = post_id
                    if input_error:
                        source['_input_error'] = input_error
                    yield source

            def records_factory():
                for post_id, ordinal, source_url, status, raw, latest_attempt in db.execute(
                        'SELECT post_id,ordinal,source_url,status,result_json,last_attempt_json FROM posts WHERE result_json IS NOT NULL ORDER BY ordinal'):
                    record = json.loads(raw)
                    record.update(queue_id=post_id, ordinal=ordinal)
                    if latest_attempt:
                        record['last_attempt'] = json.loads(latest_attempt)
                    record.setdefault('source_url', source_url)
                    record.setdefault('post_id', post_id)
                    record.setdefault('status', status)
                    first = db.execute('SELECT row_json FROM sources WHERE post_id=? ORDER BY source_id LIMIT 1', (post_id,)).fetchone()
                    record['source_refs'] = [json.loads(first[0])] if first else []
                    yield record

            result = write_streaming_outputs(records_factory, out, has_coordinates,
                                             sources_for_record=sources_for_record,
                                             sources_factory=sources_factory, page_size=args.page_size)
        else:
            # Retain the original small-pilot checkpoint format.
            records = [json.loads(row[0]) for row in db.execute('SELECT result_json FROM posts ORDER BY ordinal')]
            result = write_linked_outputs(records, out, has_coordinates)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
