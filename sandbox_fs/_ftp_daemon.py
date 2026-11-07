"""
``sandbox_fs ftp`` 的后台服务进程。

被前台入口以 ``python3 -m sandbox_fs._ftp_daemon`` 的形式拉起（``start_new_session``），
所以它不属于终端的前台进程组：关掉终端、按 Ctrl-C 都不会影响它。

生命周期
--------
* 启动后把 pid 写到 ``<tmp>/sandbox_fs-ftp-<port>.pid``，并等待一个就绪信号；
* 前台读取就绪信号后才知道端口是否真的起来了；
* 收到 SIGTERM 时关闭 FTP 并删除 pid 文件。
"""


import argparse
import os
import signal
import sys
import time

from .serve import DEFAULT_HOST, DEFAULT_PATH, DEFAULT_PORT, DEFAULT_SANDBOX


def pidfile_path(port, tmpdir=None):
    import tempfile

    base = tmpdir
    if base is None:
        try:
            base = tempfile.gettempdir()
            if not os.access(base, os.W_OK):
                raise OSError
        except (OSError, AttributeError):
            base = os.path.expanduser("~")
    return os.path.join(base, "sandbox_fs-ftp-%d.pid" % int(port))


def read_pidfile(port):
    """返回正在运行的后台 FTP 的 pid（没有则 None）。"""

    path = pidfile_path(port)
    try:
        with open(path) as f:
            pid = int(f.read().strip() or 0)
    except (OSError, ValueError):
        return None
    if pid <= 0:
        return None
    try:
        os.kill(pid, 0)
    except OSError:
        try:
            os.unlink(path)
        except OSError:
            pass
        return None
    return pid


def main(argv=None):
    ap = argparse.ArgumentParser(prog="sandbox_fs._ftp_daemon")
    ap.add_argument("--sandbox", default=DEFAULT_SANDBOX)
    ap.add_argument("--root", default=DEFAULT_PATH)
    ap.add_argument("--host", default=DEFAULT_HOST)
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--user", default=None, help="具名账号（默认匿名）")
    ap.add_argument("--password", default=None)
    ap.add_argument("--no-anonymous", action="store_true",
                    help="关闭匿名登录")
    ap.add_argument("--ready-fd", type=int, default=-1,
                    help="就绪后往这个 fd 写 OK（由前台传入）")
    ap.add_argument("--log", default=None, help="日志文件路径")
    args = ap.parse_args(argv)

    def report(msg, ok=False):
        try:
            if args.ready_fd >= 0:
                os.write(args.ready_fd, (("OK " if ok else "ERR ") + msg + "\n").encode())
                if ok:
                    os.close(args.ready_fd)
        except OSError:
            pass

    logf = None
    if args.log:
        try:
            logf = open(args.log, "a", buffering=1)
        except OSError:
            logf = None

    def say(msg):
        line = "[%s] %s" % (time.strftime("%H:%M:%S"), msg)
        if logf:
            logf.write(line + "\n")
        sys.stderr.write(line + "\n")

    from . import connect
    from .ftp import serve_ftp

    fs = None
    server = None
    try:
        fs = connect(args.sandbox, root=args.root,
                     log=(lambda m: logf.write("[dbg] %s\n" % m)) if logf else None)
        server = serve_ftp(fs, host=args.host, port=args.port,
                           user=args.user, password=args.password,
                           anonymous=not args.no_anonymous,
                           blocking=False, log=None)
        if isinstance(server, tuple):
            server, _thread = server
        pid = os.getpid()
        try:
            with open(pidfile_path(args.port), "w") as f:
                f.write("%d\n" % pid)
        except OSError:
            pass
        say("FTP 已就绪 ftp://%s:%d (pid %d, 根 %s)"
            % (args.host, args.port, pid, fs.root))
        report("%d" % pid, ok=True)
    except Exception as exc:  # noqa: BLE001
        say("FTP 启动失败：%s: %s" % (type(exc).__name__, exc))
        report("%s: %s" % (type(exc).__name__, exc))
        _cleanup(server, fs, args.port)
        return 1

    stopping = {"now": False}

    def on_term(signum, frame):
        stopping["now"] = True

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        try:
            signal.signal(sig, on_term)
        except (ValueError, OSError):
            pass

    try:
        while not stopping["now"]:
            time.sleep(0.5)
    finally:
        say("FTP 服务停止")
        _cleanup(server, fs, args.port)
    return 0


def _cleanup(server, fs, port):
    if server is not None:
        try:
            server.close_all()
        except Exception:  # noqa: BLE001
            pass
    if fs is not None:
        try:
            fs.close()
        except Exception:  # noqa: BLE001
            pass
    try:
        os.unlink(pidfile_path(port))
    except OSError:
        pass


if __name__ == "__main__":
    sys.exit(main())
