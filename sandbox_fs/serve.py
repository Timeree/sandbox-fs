"""
``sandbox_fs ftp``：FTP 跑在**后台**，SSH 交互会话占**前台**。

流程
----
1. 用 ``start_new_session`` 拉起一个后台进程跑 FTP（不占用终端，关终端也不退出）；
2. 等它就绪（读回就绪管道），打印 FTP 信息；
3. 前台把终端交给远程沙箱交互会话（``sandbox ssh``），你可以直接敲命令；
4. SSH 退出（或按 Ctrl-C）时，一并停掉后台 FTP。
"""


import os
import select
import signal
import subprocess
import sys
import termios
import time

DEFAULT_SANDBOX = "sandbox"
#: SSH 初始路径与 FTP 根目录共用同一个常量，保证两边一致
DEFAULT_PATH = "/workspace"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 2121


# ---------------------------------------------------------------------------
# 后台 FTP
# ---------------------------------------------------------------------------
class BackgroundFTP(object):
    """把 FTP 作为独立会话的后台进程启动。"""

    def __init__(self, sandbox=DEFAULT_SANDBOX, workdir=DEFAULT_PATH,
                 host=DEFAULT_HOST, port=DEFAULT_PORT, user=None,
                 password=None, anonymous=True, log=None):
        self.sandbox = sandbox
        self.workdir = workdir
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.anonymous = anonymous
        self.log = log or (lambda *a: None)
        self.proc = None
        self.log_path = None
        self.reused = False

    @staticmethod
    def _existing_pid(port):
        """端口上已有存活的 sandbox_fs FTP 就返回它的 pid。"""

        from ._ftp_daemon import read_pidfile

        pid = read_pidfile(port)
        if not pid:
            return None
        # pid 文件存在但进程监听的不是这个端口时不能复用
        try:
            import socket

            with socket.create_connection(("127.0.0.1", port), timeout=1.5):
                return pid
        except OSError:
            return None

    def start(self, timeout=90):
        """启动并等待就绪；成功返回 pid，失败抛异常。

        若该端口已有一个存活的 sandbox_fs FTP，直接复用它（不重复起进程）。
        """

        from ._ftp_daemon import pidfile_path

        existing = self._existing_pid(self.port)
        if existing:
            self.reused = True
            self.log_path = os.path.join(os.path.dirname(pidfile_path(self.port)),
                                         "sandbox_fs-ftp-%d.log" % self.port)
            self.log("端口 %d 已有 FTP 在运行（pid %s），直接复用" % (self.port, existing))
            return existing

        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        read_fd, write_fd = os.pipe()
        self.log_path = os.path.join(os.path.dirname(pidfile_path(self.port)),
                                     "sandbox_fs-ftp-%d.log" % self.port)
        argv = [sys.executable, "-m", "sandbox_fs._ftp_daemon",
                "--sandbox", self.sandbox, "--root", self.workdir,
                "--host", self.host, "--port", str(self.port),
                "--ready-fd", str(write_fd), "--log", self.log_path]
        if self.user:
            argv += ["--user", self.user, "--password", self.password or ""]
        if not self.anonymous:
            argv += ["--no-anonymous"]
        env = dict(os.environ)
        env["PYTHONPATH"] = here + (os.pathsep + env["PYTHONPATH"]
                                    if env.get("PYTHONPATH") else "")
        try:
            self.proc = subprocess.Popen(
                argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL, pass_fds=(write_fd,),
                start_new_session=True, env=env, cwd=here)
        finally:
            os.close(write_fd)

        deadline = time.time() + timeout
        buf = b""
        while time.time() < deadline:
            r, _, _ = select.select([read_fd], [], [], 0.5)
            if r:
                chunk = os.read(read_fd, 4096)
                if not chunk:
                    break
                buf += chunk
                if b"\n" in buf:
                    break
            if self.proc.poll() is not None:
                break
        os.close(read_fd)

        line = buf.decode("utf-8", "replace").strip()
        if line.startswith("OK "):
            return int(line[3:].strip() or 0)
        detail = line[4:] if line.startswith("ERR ") else ""
        if not detail:
            detail = self.tail_log() or "后台进程未返回就绪信号"
        self.stop()
        raise RuntimeError(detail)

    def tail_log(self, lines=8):
        if not self.log_path or not os.path.exists(self.log_path):
            return ""
        try:
            with open(self.log_path, "r", errors="replace") as f:
                return "\n".join(f.read().strip().splitlines()[-lines:])
        except OSError:
            return ""

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def stop(self, timeout=10):
        if self.proc is None:
            return        # 复用的实例归它的启动者管
        if self.proc.poll() is None:
            try:
                self.proc.terminate()
            except OSError:
                pass
            try:
                self.proc.wait(timeout=timeout)
            except (subprocess.TimeoutExpired, OSError):
                try:
                    self.proc.kill()
                    self.proc.wait(timeout=3)
                except (subprocess.TimeoutExpired, OSError):
                    pass
        self.proc = None


# ---------------------------------------------------------------------------
# 前台 SSH
# ---------------------------------------------------------------------------
class SshForeground(object):
    """把当前终端直接交给 ``sandbox ssh``（真正的交互会话）。"""

    def __init__(self, sandbox=DEFAULT_SANDBOX, workdir=DEFAULT_PATH, log=None):
        self.sandbox = sandbox
        self.workdir = workdir
        self.log = log or (lambda *a: None)
        self.proc = None

    @staticmethod
    def _cli():
        import shutil

        for name in ("sandbox", "vercel-sandbox"):
            path = shutil.which(name)
            if path:
                return [path]
        raise RuntimeError("找不到 sandbox CLI（npm i -g sandbox）")

    def _kill_stale(self):
        """清掉上次残留的 sandbox ssh 会话。"""

        try:
            out = subprocess.run(["pgrep", "-f", "bin/sandbox ssh "],
                                 capture_output=True, timeout=10)
        except (OSError, subprocess.SubprocessError):
            return
        for line in out.stdout.decode().split():
            try:
                pid = int(line)
            except ValueError:
                continue
            if pid != os.getpid():
                try:
                    os.kill(pid, signal.SIGTERM)
                except OSError:
                    pass

    def run(self):
        """前台运行，返回退出码。"""

        self._kill_stale()
        # -w 让沙箱内的 shell 直接落在与 FTP 根目录相同的路径
        argv = self._cli() + ["ssh", self.sandbox, "-w", self.workdir]

        if not sys.stdin.isatty():
            # sandbox ssh 是交互式会话，没有终端时它会立刻退出（还会刷屏）。
            # 这里不启动它，只提示用户：FTP 已在后台，可另开终端连 SSH。
            self.log("当前不是交互终端，跳过 SSH 会话（FTP 已在后台运行）")
            self.log("需要交互 shell 请在终端里直接运行：%s"
                     % " ".join(argv))
            return 0

        # 交互环境：把本终端的 termios 原样交给子进程，退出后恢复
        try:
            saved = termios.tcgetattr(sys.stdin.fileno())
        except (termios.error, OSError):
            saved = None

        env = dict(os.environ)
        env.setdefault("TERM", "xterm-256color")
        self.proc = subprocess.Popen(argv, env=env)
        try:
            return self.proc.wait()
        except KeyboardInterrupt:
            try:
                self.proc.terminate()
            except OSError:
                pass
            try:
                return self.proc.wait(timeout=5)
            except (subprocess.TimeoutExpired, OSError):
                try:
                    self.proc.kill()
                except OSError:
                    pass
                return 130
        finally:
            if saved is not None:
                try:
                    termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, saved)
                except (termios.error, OSError):
                    pass

    def stop(self):
        if self.proc is not None and self.proc.poll() is None:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=5)
            except (subprocess.TimeoutExpired, OSError):
                try:
                    self.proc.kill()
                except OSError:
                    pass


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def run_ftp(sandbox=DEFAULT_SANDBOX, workdir=DEFAULT_PATH, host=DEFAULT_HOST,
            port=DEFAULT_PORT, with_ssh=True, user=None, password=None,
            anonymous=True, log=None):
    """FTP 后台 + SSH 前台。返回进程退出码。"""

    log = log or (lambda m: sys.stderr.write(m + "\n"))

    bg = BackgroundFTP(sandbox=sandbox, workdir=workdir, host=host, port=port,
                       user=user, password=password, anonymous=anonymous,
                       log=log)
    try:
        pid = bg.start()
    except Exception as exc:  # noqa: BLE001
        log("FTP 后台启动失败：%s" % exc)
        return 1

    log("FTP 已在后台运行" + ("（复用现有实例）" if bg.reused else ""))
    log("  地址：ftp://%s:%d   （匿名，无需账号密码，全权限）" % (host, port))
    log("  根目录：%s" % workdir)
    log("  进程：pid %s   日志：%s" % (pid, bg.log_path))
    log("  停止：kill %s" % pid)
    log("")

    if not with_ssh:
        if bg.reused:
            # 复用的实例不归本进程管：直接退出，别误杀它的启动者
            log("（复用实例，Ctrl-C 不会影响它；停止请 kill %s）" % pid)
            return 0
        if not sys.stdin.isatty():
            # 非交互：交给后台进程独立运行，本进程直接退出
            log("后台 FTP 已脱离本终端运行（pid %s）" % pid)
            return 0
        log("按 Ctrl-C 停止 FTP 并退出")
        try:
            while bg.alive():
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
        finally:
            if not bg.alive():
                code = bg.proc.poll() if bg.proc else None
                if code not in (0, None):
                    log("后台 FTP 已意外退出（exit=%s），日志：%s"
                        % (code, bg.log_path))
            bg.stop()
        return 0

    log("正在进入 sandbox ssh %s -w %s（退出后自动停止 FTP）" % (sandbox, workdir))
    log("")
    ssh = SshForeground(sandbox=sandbox, workdir=workdir, log=log)
    code = 0
    try:
        code = ssh.run()
    finally:
        ssh.stop()
        if not bg.reused:
            if bg.alive():
                log("")
                log("SSH 会话结束，正在停止后台 FTP（pid %s）……" % pid)
            bg.stop()
        else:
            log("（复用的 FTP 实例保持运行，停止请 kill %s）" % pid)
    return code
