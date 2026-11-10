"""Hard wall-time supervision for the collector's own browser worker.

Anonymous sessions remain the default. A named opt-in Windows-protected session
is decrypted in the child process and stays in memory; it is never serialized
into post results, logs, or collection outputs.
"""
from __future__ import annotations
import faulthandler
import http.cookiejar
import multiprocessing
from multiprocessing.connection import wait
import os
import signal
import time
import psutil
from pathlib import Path
import subprocess
import urllib.request

def _worker(connection,channel,work,wall_timeout,backend_factory=None,session_name=None,worker_mode=False,isolate=False):
    backend=None
    try:
        if isolate:
            # A browser cannot start until the parent verifies the session.
            if os.name != 'nt':
                os.setsid()
            connection.send({'kind': 'started', 'pid': os.getpid(),
                             'pgid': os.getpgrp() if os.name != 'nt' else None})
            if connection.recv().get('kind') != 'initialize':
                return
        if backend_factory is None:
            from weibo_browser import BrowserBackend
            options={"channel":channel,'worker_mode':worker_mode}
            if session_name is not None:
                from weibo_auth import load_session
                options["storage_state"]=load_session(session_name)
            backend=BrowserBackend(**options)
        else:
            backend=backend_factory()
        if hasattr(backend,"__enter__"):
            backend.__enter__()
        connection.send({"kind":"ready"})
        while True:
            job=connection.recv()
            if job["kind"]=="close":
                break
            log_path=Path(work)/"browser_timeout_trace.log"
            log_path.parent.mkdir(parents=True,exist_ok=True)
            with log_path.open("a",encoding="utf-8") as log:
                faulthandler.dump_traceback_later(max(1,wall_timeout-5),file=log)
                try:
                    if job['kind']=='health':
                        probe=getattr(backend,'probe_public_health',None)
                        result=probe() if callable(probe) else {'status':'health_unknown','requests':0,'error':'no_same_session_health_interface'}
                    else:
                        result=backend.fetch_post(job["url"])
                    context=getattr(backend,"context",None)
                    cookies=context.cookies() if context is not None else []
                    connection.send({"kind":"result","result":result,"cookies":cookies,
                                     "user_agent":getattr(backend,"_user_agent",None)})
                finally:
                    faulthandler.cancel_dump_traceback_later()
    except (EOFError,BrokenPipeError):
        pass
    except Exception as error:
        try:
            connection.send({"kind":"error","error":type(error).__name__})
        except (BrokenPipeError,EOFError):
            pass
    finally:
        if backend is not None and hasattr(backend,"__exit__"):
            backend.__exit__(None,None,None)
        connection.close()

# Do not reap the leader before group cleanup: it still owns its PID.
_TERMINATE_GRACE = 3.0


def _running(process):
    return (process is not None and process.pid is not None
            and not wait([process.sentinel], timeout=0))


def _wait_uninterrupted(process, seconds):
    deadline = time.monotonic() + seconds
    while True:
        try:
            return bool(wait([process.sentinel],
                             timeout=max(0, deadline - time.monotonic())))
        except KeyboardInterrupt:
            if time.monotonic() >= deadline:
                return not _running(process)


def _pause_uninterrupted(seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            time.sleep(min(.05, max(0, deadline - time.monotonic())))
        except KeyboardInterrupt:
            pass


def _signal_owned_group(process, pgid, sig):
    if not pgid or pgid != process.pid or pgid == os.getpgrp():
        return False
    # The parent validated this group before allowing browser startup.
    # The child is not reaped until cleanup finishes, so its PID cannot be
    # reused. On macOS getpgid can fail for an exited leader even while its
    # descendants still occupy the verified group; signal that group directly.
    popen = getattr(process, '_popen', None)
    if (getattr(process, '_parent_pid', None) != os.getpid()
            or popen is None or popen.returncode is not None):
        return False
    try:
        os.killpg(pgid, sig)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        if not _running(process):
            return False
        raise


class SupervisedBrowserBackend:
    def __init__(self,*,channel="msedge",wall_timeout=45,startup_timeout=30,work=None,backend_factory=None,session_name=None,worker_mode=False):
        if session_name is not None and (not isinstance(session_name,str) or not session_name.strip()):
            raise ValueError("session_name must be a nonempty string or None")
        self.session_name=session_name
        self.channel=channel
        self.wall_timeout=float(wall_timeout)
        self.startup_timeout=float(startup_timeout)
        self.work=Path(work or Path(__file__).resolve().parent.parent/"work")
        self.backend_factory=backend_factory
        self.worker_mode=bool(worker_mode)
        self._process=self._connection=None
        self._pgid=None
        self._root_process=None
        self._descendants={}
        self._descendant_scan_failed=False
        self._cookies=[]
        self._user_agent=None
    def __enter__(self):
        return self
    def __exit__(self,*args):
        self.close()
    def _remember_descendants(self):
        if os.name=='nt':
            return
        roots=([self._root_process] if self._root_process is not None else [])+list(self._descendants.values())
        for owner in roots:
            try:
                if not owner.is_running():
                    continue
                for child in owner.children(recursive=True):
                    identity=(child.pid,child.create_time())
                    self._descendants[identity]=child
            except (psutil.NoSuchProcess,psutil.ZombieProcess):
                pass
            except psutil.AccessDenied:
                self._descendant_scan_failed=True

    def _live_descendants(self):
        result=[]
        for child in self._descendants.values():
            try:
                if child.is_running() and child.status()!=psutil.STATUS_ZOMBIE:
                    result.append(child)
            except (psutil.NoSuchProcess,psutil.ZombieProcess):
                pass
            except psutil.AccessDenied:
                self._descendant_scan_failed=True
                result.append(child)
        return result

    def _clean_detached_descendants(self):
        if os.name=='nt':
            return
        self._remember_descendants()
        for child in self._live_descendants():
            try:
                child.terminate()
            except (psutil.NoSuchProcess,psutil.ZombieProcess):
                pass
            except psutil.AccessDenied:
                self._descendant_scan_failed=True
        deadline=time.monotonic()+_TERMINATE_GRACE
        while self._live_descendants() and time.monotonic()<deadline:
            self._remember_descendants()
            _pause_uninterrupted(min(.05,max(0,deadline-time.monotonic())))
        self._remember_descendants()
        for child in self._live_descendants():
            try:
                child.kill()
            except (psutil.NoSuchProcess,psutil.ZombieProcess):
                pass
            except psutil.AccessDenied:
                self._descendant_scan_failed=True
        deadline=time.monotonic()+_TERMINATE_GRACE
        while self._live_descendants() and time.monotonic()<deadline:
            _pause_uninterrupted(min(.05,max(0,deadline-time.monotonic())))
        if self._live_descendants() or self._descendant_scan_failed:
            raise RuntimeError('browser_descendant_cleanup_incomplete')

    def _poll_with_descendants(self, timeout):
        deadline=time.monotonic()+max(0,timeout)
        while True:
            self._remember_descendants()
            remaining=max(0,deadline-time.monotonic())
            ready=self._connection.poll(min(.1,remaining))
            if ready:
                self._remember_descendants()
                return True
            if time.monotonic()>=deadline:
                return False

    def _start(self):
        if _running(self._process):
            return
        self.close()
        ctx=multiprocessing.get_context("spawn")
        parent,child=ctx.Pipe()
        self._connection=parent
        self._process=ctx.Process(target=_worker,args=(child,self.channel,str(self.work),self.wall_timeout,self.backend_factory,self.session_name,self.worker_mode,True),daemon=True)
        try:
            self._process.start()
        finally:
            child.close()
        if os.name!='nt':
            try:
                self._root_process=psutil.Process(self._process.pid)
                self._root_process.create_time()
            except psutil.Error as error:
                self._terminate()
                raise RuntimeError('browser_process_identity_unavailable') from error
        deadline=time.monotonic()+self.startup_timeout
        if not self._poll_with_descendants(max(0,deadline-time.monotonic())):
            self._terminate()
            raise RuntimeError("browser_startup_timeout")
        message=parent.recv()
        if message.get('kind')!='started' or message.get('pid')!=self._process.pid:
            self._terminate()
            raise RuntimeError("browser_startup_error")
        if os.name!='nt':
            if message.get('pgid')!=self._process.pid or os.getpgid(self._process.pid)!=self._process.pid:
                self._terminate()
                raise RuntimeError("browser_process_group_error")
            self._pgid=self._process.pid
        parent.send({'kind':'initialize'})
        if not self._poll_with_descendants(max(0,deadline-time.monotonic())):
            self._terminate()
            raise RuntimeError("browser_startup_timeout")
        message=parent.recv()
        if message.get("kind")!="ready":
            self._terminate()
            raise RuntimeError("browser_startup_error")
        self._remember_descendants()
    def _terminate(self):
        self._remember_descendants()
        process=self._process
        try:
            if process is not None and process.pid is not None:
                if os.name=='nt':
                    if _running(process):
                        subprocess.run(["taskkill","/PID",str(process.pid),"/T","/F"],
                                       stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)
                elif _signal_owned_group(process,self._pgid,signal.SIGTERM):
                    # Descendants may ignore TERM after their leader exits.
                    _pause_uninterrupted(_TERMINATE_GRACE)
                    _signal_owned_group(process,self._pgid,signal.SIGKILL)
                elif _running(process):
                    # Before initialize the child cannot spawn a browser.
                    process.terminate()
                    _wait_uninterrupted(process,_TERMINATE_GRACE)
                    if _running(process):
                        process.kill()
                self._clean_detached_descendants()
                _wait_uninterrupted(process,_TERMINATE_GRACE)
                if _running(process):
                    raise RuntimeError("browser_process_did_not_exit")
                # Exit is confirmed; join only reaps the owned child.
                while True:
                    try:
                        process.join(timeout=0)
                        break
                    except KeyboardInterrupt:
                        continue
        finally:
            if self._connection is not None:
                self._connection.close()
            self._process=self._connection=None
            self._pgid=None
            self._root_process=None
            self._descendants={}
            self._descendant_scan_failed=False
            self._cookies=[]
    def close(self):
        self._remember_descendants()
        if _running(self._process):
            try:
                self._connection.send({"kind":"close"})
                _wait_uninterrupted(self._process,_TERMINATE_GRACE)
            except (EOFError,BrokenPipeError,OSError):
                pass
        self._terminate()
    def fetch_post(self,url):
        return self._request('fetch',url)
    def probe_public_health(self):
        return self._request('health','https://weibo.com/')
    def _request(self,kind,url):
        try:
            self._start()
            self._connection.send({"kind":kind,"url":url})
            if not self._poll_with_descendants(self.wall_timeout):
                self._terminate()
                return self._failure(url,"timeout","browser_wall_timeout")
            message=self._connection.recv()
            if message.get("kind")!="result":
                self._terminate()
                return self._failure(url,"network_error","browser_worker_error")
            self._remember_descendants()
            self._cookies=message.get("cookies",[])
            self._user_agent=message.get("user_agent")
            return message["result"]
        except KeyboardInterrupt:
            self.close()
            raise
        except (RuntimeError,EOFError,BrokenPipeError,OSError):
            self._terminate()
            return self._failure(url,"network_error","browser_startup_or_worker_error")
    @staticmethod
    def _failure(url,status,error):
        return {"source_url":url,"status":status,"error":error,"post_id":None,"media":[],"locations":[]}
    def make_download_opener(self):
        # Transfer only this worker's explicitly selected session in memory.
        # CookieJar enforces cookie domains/paths; no Cookie header is logged.
        jar=http.cookiejar.CookieJar()
        for c in self._cookies:
            domain=c["domain"];expires=c.get("expires",-1)
            jar.set_cookie(http.cookiejar.Cookie(
                version=0,name=c["name"],value=c["value"],port=None,port_specified=False,
                domain=domain,domain_specified=domain.startswith("."),domain_initial_dot=domain.startswith("."),
                path=c.get("path","/"),path_specified=True,secure=c.get("secure",False),
                expires=int(expires) if expires and expires>0 else None,
                discard=not expires or expires<=0,comment=None,comment_url=None,
                rest={"HttpOnly":c.get("httpOnly",False)},rfc2109=False))
        opener=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
        opener.addheaders=[("User-Agent",self._user_agent or "Mozilla/5.0")]
        return opener

