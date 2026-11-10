"""Read-only live Weibo checkpoint monitor; never starts or stops collection."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import time

LOGIN_STATUSES = {'login_required', 'session_required'}
HONG_KONG = timezone(timedelta(hours=8), name='HKT')
DELTA_LIMIT = 1000


def process_identity(pid):
    """Query a Windows process without sending it a signal.

    alive=None means unknown, including insufficient permission. FILETIME is
    the raw 64-bit creation value so a reused PID can be recognized exactly.
    """
    result = {'alive': None, 'created_filetime': None, 'error': None}
    if isinstance(pid, bool) or not isinstance(pid, int) or not 0 < pid <= 0xffffffff:
        return {**result, 'error': 'invalid_pid'}
    if os.name != 'nt':
        return {**result, 'error': 'unsupported_platform'}
    import ctypes
    from ctypes import wintypes
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel.GetExitCodeProcess.restype = wintypes.BOOL
    kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
    kernel.GetProcessTimes.restype = wintypes.BOOL
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    handle = kernel.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
    if not handle:
        code = ctypes.get_last_error()
        return {**result, 'alive': False if code == 87 else None,
                'error': 'process_missing' if code == 87 else 'process_query_error_' + str(code)}
    try:
        exit_code = wintypes.DWORD()
        if not kernel.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            return {**result, 'error': 'exit_code_query_error_' + str(ctypes.get_last_error())}
        result['alive'] = exit_code.value == 259  # STILL_ACTIVE
        created, exited, kernel_time, user_time = [wintypes.FILETIME() for _ in range(4)]
        if kernel.GetProcessTimes(handle, ctypes.byref(created), ctypes.byref(exited),
                                  ctypes.byref(kernel_time), ctypes.byref(user_time)):
            result['created_filetime'] = (created.dwHighDateTime << 32) | created.dwLowDateTime
        else:
            result['error'] = 'creation_time_query_error_' + str(ctypes.get_last_error())
        return result
    finally:
        kernel.CloseHandle(handle)


def _read_json(path):
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
        return (value, None) if isinstance(value, dict) else (None, 'invalid JSON object')
    except FileNotFoundError:
        return None, 'not available'
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None, 'unreadable or invalid JSON'


def _hong_kong_time(value):
    if not isinstance(value, str) or not value:
        return 'Unknown'
    try:
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if stamp.tzinfo is None:
            return 'Unknown (timezone missing)'
        return stamp.astimezone(HONG_KONG).strftime('%Y-%m-%d %H:%M:%S HKT')
    except ValueError:
        return 'Unknown (invalid timestamp)'


def _runtime_state(runtime, error, probe):
    if runtime is None:
        return {'state': 'Unknown', 'state_detail': 'runtime ' + str(error)}
    if runtime.get('active') is False:
        return {'state': 'Stopped', 'state_detail': runtime.get('stop_reason') or 'pipeline marked inactive'}
    if runtime.get('active') is not True:
        return {'state': 'Unknown', 'state_detail': 'runtime active state is unknown'}
    pid = runtime.get('pid')
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return {'state': 'Unknown', 'state_detail': 'active runtime has no valid process ID'}
    try:
        identity = probe(pid)
    except Exception:
        identity = {'alive': None, 'created_filetime': None, 'error': 'process query failed'}
    if not isinstance(identity, dict):
        identity = {'alive': None, 'created_filetime': None, 'error': 'invalid process query result'}
    if identity.get('alive') is False:
        return {'state': 'Stopped', 'state_detail': runtime.get('stop_reason') or 'process exited',
                'process_identity': identity}
    if identity.get('alive') is not True:
        return {'state': 'Unknown', 'state_detail': identity.get('error') or 'process could not be confirmed',
                'process_identity': identity}
    expected = runtime.get('process_created_filetime')
    if expected is None and isinstance(runtime.get('process_identity'), dict):
        expected = runtime['process_identity'].get('created_filetime')
    if expected is not None:
        actual = identity.get('created_filetime')
        if actual is None:
            return {'state': 'Unknown', 'state_detail': 'process creation identity could not be confirmed',
                    'process_identity': identity}
        if str(expected) != str(actual):
            return {'state': 'Stopped', 'state_detail': 'process identity changed; recorded PID was reused',
                    'process_identity': identity}
    return {'state': 'Running', 'state_detail': 'process confirmed', 'process_identity': identity}


def _nonnegative_integer(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _timestamp(value):
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return stamp.astimezone(timezone.utc) if stamp.tzinfo is not None else None
    except ValueError:
        return None


def read_snapshot(out, *, process_probe=None, delta_limit=DELTA_LIMIT):
    """Read bounded recent events and primary-key post joins, not full post rows."""
    out = Path(out).expanduser().resolve()
    summary, summary_error = _read_json(out / 'summary.json')
    runtime, runtime_error = _read_json(out / 'pipeline_runtime.json')
    result = {
        'observed_at_hk': datetime.now(timezone.utc).astimezone(HONG_KONG).strftime('%Y-%m-%d %H:%M:%S HKT'),
        'checkpoint_at_hk': _hong_kong_time((summary or {}).get('generated_at')),
        'last_request_at_hk': 'Unknown', 'checkpoint_error': summary_error,
        'runtime_error': runtime_error, 'runtime': runtime or {}, 'requests_total_live': None,
        'checkpoint_requests': (summary or {}).get('metadata_requests_total'), 'request_delta': None,
        'latest_request': None, 'login_streak': None, 'login_streak_scope': 'latest request history',
        'status_counts': (summary or {}).get('post_statuses'), 'counts_mode': 'Checkpoint',
        'counts_note': 'checkpoint counts; live delta not yet verified', 'database_error': None,
    }
    result.update(_runtime_state(runtime, runtime_error, process_probe or process_identity))
    checkpoint = out / 'state.sqlite'
    if not checkpoint.is_file():
        result['database_error'] = 'checkpoint database is not available'
        return result
    db = None
    try:
        db = sqlite3.connect(checkpoint.as_uri() + '?mode=ro', uri=True, timeout=1)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        total = db.execute('SELECT COUNT(*) FROM request_history').fetchone()[0]
        result['requests_total_live'] = total
        recent_sql = '''SELECT h.attempt_id,h.post_id,h.attempted_at,h.status,h.error,
                        p.ordinal,p.attempts FROM request_history h
                        LEFT JOIN posts p ON p.post_id=h.post_id
                        ORDER BY h.attempt_id DESC LIMIT ?'''
        recent = [dict(row) for row in db.execute(recent_sql, (20,))]
        if recent:
            result['latest_request'] = recent[0]
            result['last_request_at_hk'] = _hong_kong_time(recent[0]['attempted_at'])
        baseline = result['checkpoint_requests']
        counts = result['status_counts']
        valid_counts = (isinstance(counts, dict) and all(isinstance(k, str) and _nonnegative_integer(v)
                                                        for k, v in counts.items()))
        delta = total - baseline if _nonnegative_integer(baseline) else None
        result['request_delta'] = delta
        if not valid_counts:
            result['status_counts'] = None
            result['counts_note'] = 'checkpoint counts are unavailable or invalid'
        elif delta is None or delta < 0:
            result['counts_note'] = 'checkpoint counts only: request baseline is unavailable or ahead of the database'
        elif delta > delta_limit:
            result['counts_note'] = 'checkpoint counts only: live delta exceeds the bounded read limit'
        else:
            events = [dict(row) for row in db.execute(recent_sql, (delta,))] if delta else []
            distinct = len({row['post_id'] for row in events}) == len(events)
            first_pass = distinct and all(row['attempts'] == 1 for row in events)
            if not first_pass:
                result['counts_note'] = 'checkpoint counts only: retries or repeated post IDs were detected'
            elif counts.get('pending', 0) < delta:
                result['counts_note'] = 'checkpoint counts only: delta exceeds checkpoint pending posts'
            else:
                live = dict(counts)
                if delta:
                    live['pending'] = live.get('pending', 0) - delta
                    for row in events:
                        status = row['status'] or 'unknown'
                        live[status] = live.get(status, 0) + 1
                result['status_counts'] = live
                result['counts_mode'] = 'Live (first pass)'
                result['counts_note'] = 'checkpoint counts plus verified distinct first-pass requests'
        boundaries = [stamp for stamp in (_timestamp((runtime or {}).get('started_at')),
                                           _timestamp((summary or {}).get('generated_at'))) if stamp is not None]
        boundary = max(boundaries) if boundaries else None
        streak_rows = [row for row in recent if boundary is None or (
            _timestamp(row['attempted_at']) is not None and _timestamp(row['attempted_at']) >= boundary)]
        result['login_streak_scope'] = 'current batch/run boundary; last 20 requests'
        streak = 0
        for row in streak_rows:
            if row['status'] not in LOGIN_STATUSES:
                break
            streak += 1
        checkpoint_run = (summary or {}).get('run') or {}
        recorded_streak = checkpoint_run.get('consecutive_login_gates') if isinstance(checkpoint_run, dict) else None
        finished_checkpoint = (_timestamp((summary or {}).get('generated_at')) is not None
            and (_timestamp((runtime or {}).get('started_at')) is None
                 or _timestamp(summary['generated_at']) >= _timestamp(runtime['started_at'])))
        if (runtime or {}).get('active') is False and finished_checkpoint and _nonnegative_integer(recorded_streak):
            streak = recorded_streak
            result['login_streak_scope'] = 'stopped checkpoint batch counter'
            result['login_streak_bounded'] = False
        else:
            result['login_streak_bounded'] = streak == 20
        result['login_streak'] = streak
    except (sqlite3.Error, OSError, ValueError, TypeError):
        result['database_error'] = 'database read failed; waiting for the next refresh'
    finally:
        if db is not None:
            db.close()
    return result


def _one_line(value, limit=260):
    return ' '.join(str(value or '').split())[:limit]


def render_board(snapshot):
    runtime = snapshot.get('runtime') or {}
    latest = snapshot.get('latest_request') or {}
    limit = runtime.get('login_gate_limit', 5)
    streak = snapshot.get('login_streak')
    streak_label = 'Unknown' if streak is None else str(streak) + ('+' if snapshot.get('login_streak_bounded') else '')
    delta = snapshot.get('request_delta')
    lines = [
        'Weibo live progress - read only',
        'State: ' + snapshot['state'] + ' | ' + _one_line(snapshot.get('state_detail')),
        'Refresh (HK): ' + snapshot['observed_at_hk'],
        'Last request (HK): ' + snapshot['last_request_at_hk'],
        'Checkpoint (HK): ' + snapshot['checkpoint_at_hk'],
        'Requests total (LIVE): ' + str(snapshot.get('requests_total_live')),
        'Checkpoint requests: ' + str(snapshot.get('checkpoint_requests')) + ' | Delta: ' + str(delta),
        'Current ordinal: ' + str(latest.get('ordinal', 'Unknown')) + ' | Status: ' + str(latest.get('status', 'Unknown')),
        'Login streak: ' + streak_label + '/' + str(limit) + ' | ' + snapshot['login_streak_scope'],
        'Counts: ' + snapshot['counts_mode'],
    ]
    counts = snapshot.get('status_counts')
    if isinstance(counts, dict):
        order = ['pending', 'success', 'unavailable', 'session_required', 'login_required',
                 'timeout', 'network_error', 'browser_session_required', 'visitor_gate',
                 'rate_limited', 'captcha_required']
        keys = [key for key in order if key in counts] + sorted(set(counts) - set(order))
        lines.append('  ' + ' | '.join(key + '=' + str(counts[key]) for key in keys))
    lines.append('  ' + snapshot['counts_note'])
    lines.append('Error: ' + (_one_line(latest.get('error')) or '-'))
    lines.append('Stop reason: ' + (_one_line(runtime.get('stop_reason')) or '-'))
    lines.append('Run ID: ' + _one_line(runtime.get('run_id')) + ' | Delay: ' + str(runtime.get('delay', 'Unknown')) + ' seconds')
    lines.append('Log: ' + (_one_line(runtime.get('log_file'), 450) or 'Unknown'))
    for key, label in [('database_error', 'Database'), ('checkpoint_error', 'Checkpoint file'), ('runtime_error', 'Runtime file')]:
        if snapshot.get(key):
            lines.append(label + ': ' + str(snapshot[key]))
    lines.append('This window stays open after collection stops. Ctrl+C stops only this monitor.')
    return '\n'.join(lines)


def _enable_ansi_console():
    if not sys.stdout.isatty():
        return False
    if os.name != 'nt':
        return True
    try:
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.GetStdHandle.argtypes = [wintypes.DWORD]
        kernel.GetStdHandle.restype = wintypes.HANDLE
        kernel.GetConsoleMode.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        kernel.SetConsoleMode.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        handle = kernel.GetStdHandle(-11 & 0xffffffff)
        mode = wintypes.DWORD()
        return bool(kernel.GetConsoleMode(handle, ctypes.byref(mode))
                    and kernel.SetConsoleMode(handle, mode.value | 0x0004))
    except (OSError, AttributeError):
        return False


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--interval', type=float, default=5)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args(argv)
    if not math.isfinite(args.interval) or args.interval <= 0:
        parser.error('--interval must be a finite positive number')
    clear = False if args.once else _enable_ansi_console()
    try:
        while True:
            if clear:
                print('\033[2J\033[H', end='')
            print(render_board(read_snapshot(args.out)), flush=True)
            if args.once:
                return 0
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print('\nMonitor closed. Collection was not changed.', flush=True)
        return 0


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    raise SystemExit(main())
