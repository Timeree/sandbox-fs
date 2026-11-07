"""
传输层：一条 WebSocket 通道 + 沙箱内自举的 worker。

实测得出的通道语义
------------------
* ``sandbox ssh`` 是远程沙箱的 WebSocket 通道，WS 二进制帧与进程 stdin/stdout
  之间是**原始字节透传**；
* 进程的 stdio 是一根 pty，**必须先 ``tty.setraw(0)``** 才能读到数据；
* raw 之后连接存活期间 stdin 可持续接收，stdout 也会持续回到 WS；
* 往 Node 桥接的 stdin 直接写字节不会进 WS，必须走控制帧（``bridge.mjs``）。

启动流程（代价 ≈ 一次 python 冷启动）
------------------------------------
::

    python3 -c <~3KB loader>          # boot.py 内嵌，体积敏感
        └─ 读 first 帧 = "get" 请求    ──► 回 DATA: 完整 worker.py
        └─ exec worker.py，进入服务循环
        └─ 之后所有请求在同一连接上多路复用

worker 源码按需下载，避免把几十 KB 塞进命令行（沙箱解析 argv 很慢）。
"""


import base64
import collections
import errno
import os
import subprocess
import threading
import time

from . import proto
from .errors import AuthError, RemoteError, TransportError

HELLO_TIMEOUT = 90.0


# ---------------------------------------------------------------------------
# 消息
# ---------------------------------------------------------------------------
class Response(object):
    """一次请求的结果。DATA 帧按到达顺序累加到 ``body``。"""

    __slots__ = ("rid", "op", "ok", "data", "body", "errno", "msg", "event")

    def __init__(self, rid, op):
        self.rid = rid
        self.op = op
        self.ok = None
        self.data = {}
        self.body = bytearray()
        self.errno = 0
        self.msg = ""
        self.event = threading.Event()

    def _finish_ok(self, data):
        self.data = data
        self.ok = True
        self.event.set()

    def _finish_err(self, errno_, msg):
        self.errno = errno_
        self.msg = msg
        self.ok = False
        self.event.set()

    def wait(self, timeout=None):
        if not self.event.wait(timeout):
            raise TransportError("request %s (%s) timed out after %ss"
                                 % (self.rid, self.op, timeout))
        if not self.ok:
            path = (self.data.get("path") or self.data.get("src")
                    or self.data.get("target") or "")
            raise RemoteError(self.op, path, self.errno, self.msg)
        return self

    # -- 便捷访问 ---------------------------------------------------------
    def bytes(self):
        return bytes(self.body)

    def text(self, encoding="utf-8", errors="replace"):
        return bytes(self.body).decode(encoding, errors)

    def __repr__(self):
        return "<Response %s %s ok=%s>" % (self.rid, self.op, self.ok)


class ExecResult(object):
    __slots__ = ("code", "output", "timed_out")

    def __init__(self, code, output, timed_out=False):
        self.code = code
        self.output = output
        self.timed_out = timed_out

    @property
    def ok(self):
        return self.code == 0

    def __str__(self):
        return self.output


# ---------------------------------------------------------------------------
# 通道
# ---------------------------------------------------------------------------
class Channel(object):
    """管理桥接子进程，以及在它之上多路复用的所有请求。"""

    def __init__(self, sandbox="sandbox", cwd="/workspace", root=None, node=None,
                 request_timeout=60.0, connect_timeout=HELLO_TIMEOUT, log=None):
        self.sandbox = sandbox
        self.cwd = cwd
        self.root = (root or cwd or "/").rstrip("/") or "/"
        self.node = node
        self.request_timeout = request_timeout
        self.connect_timeout = connect_timeout
        self.log = log or (lambda *a: None)

        self._proc = None
        self._lock = threading.RLock()
        self._wlock = threading.Lock()
        self._pending = {}
        self._seq = 0
        self._closed = False
        self._gen = 0
        self._worker_pid = None
        self._stderr_buf = collections.deque(maxlen=80)
        self._last_exit_auth = False
        self._tool = None
        self._bridge_open = False
        self._t_spawn = 0.0
        self._t_open = 0.0
        self.connect()

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    @property
    def worker_pid(self):
        return self._worker_pid

    @property
    def connected(self):
        return self._proc is not None and self._proc.poll() is None

    @property
    def tool(self):
        if self._tool is None:
            from .tool import ToolClient

            self._tool = ToolClient(sandbox=self.sandbox, cwd=self.cwd,
                                    node=self.node, root=self.root, log=self.log)
        return self._tool

    def connect(self, retries=2):
        """建立通道（并完成 worker 自举）。之后的请求全部复用这条连接。

        沙箱刚创建/刚重启时，第一次握手常常失败（会话还在 warmup），
        所以默认重试一次。
        """

        if self._closed:
            raise TransportError("channel is closed")
        last = None
        for attempt in range(max(1, retries)):
            try:
                return self._connect_once()
            except AuthError:
                raise          # 鉴权失败重试也没意义，立即报给用户
            except TransportError as exc:
                last = exc
                self.log("connect attempt %d failed: %s" % (attempt + 1, exc))
                if attempt + 1 >= retries:
                    break
                time.sleep(2.0 * (attempt + 1))
        raise last

    def _connect_once(self):
        with self._lock:
            if self.connected:
                return self
            self._kill_proc()
            self._fail_all_pending("transport restarting")
        # 建连过程不能占用 self._lock：_dispatch 分发事件时也要拿这把锁，
        # 否则 ready 事件永远进不来（死锁 → 超时）。
        self._start()
        return self

    def close(self):
        self._closed = True
        self._fail_all_pending("channel closed")
        self._kill_proc()

    def __enter__(self):
        if not self.connected:
            self.connect()
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    # ------------------------------------------------------------------
    # 启动
    # ------------------------------------------------------------------
    def _start(self):
        bridge = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bridge.mjs")
        if not os.path.isfile(bridge):
            raise TransportError("bridge.mjs missing: %s" % bridge)
        argv = [self.tool.node, bridge,
                "--sandbox", self.sandbox, "--cwd", self.cwd,
                "--command", "python3", "--arg", "-c", "--arg", self._loader_code()]
        env = dict(os.environ)
        env.setdefault("NODE_NO_WARNINGS", "1")
        env.setdefault("NO_COLOR", "1")

        self._t_spawn = time.time()
        # 串行化自举：两个进程同时启动 worker 会互相覆盖通道
        held = self._acquire_start_lock()
        if held is None:
            raise TransportError("另一个进程正在启动同一沙箱的 worker，"
                                 "等待锁超时（请稍后重试）")
        try:
            proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, bufsize=0, env=env)
        finally:
            if held is not None:
                try:
                    import fcntl

                    fcntl.flock(held.fileno(), fcntl.LOCK_UN)
                except (ImportError, OSError):
                    pass
                held.close()
        with self._lock:
            self._proc = proc
            self._gen += 1
        gen = self._gen
        threading.Thread(target=self._pump, args=(proc, gen),
                         name="sandbox_fs-reader", daemon=True).start()
        threading.Thread(target=self._drain_stderr, args=(proc, gen),
                         name="sandbox_fs-stderr", daemon=True).start()

        # 启动后立刻发：沙箱的输入通道只在进程"有输出"时可用，
        # 等 ready 再发反而会因为空闲被冻结（实测）
        if not self._bootstrap():
            errs = " | ".join(list(self._stderr_buf)[-4:])
            self._kill_proc()
            msg = "cannot start sandbox_fs worker in sandbox" + (": " + errs if errs else "")
            # WS 能开但启动立即 1006，几乎都是 cwd 不存在导致的
            if "websocket closed by peer (1006)" in errs and self.cwd:
                msg += ("\n提示：工作目录 `%s` 可能在沙箱中不存在；"
                        "可用 --root 指定沙箱里真实存在的目录"
                        "（`sandbox ssh` 的默认初始路径可用 `sandbox exec %s pwd` 查询）"
                        % (self.cwd, self.sandbox))
            raise TransportError(msg)

    def _explain_bridge_exit(self):
        """桥接进程在建立 WS 前退出：把真实原因翻译成人话。"""

        lines = [ln for ln in list(self._stderr_buf)[-12:] if ln.strip()]
        detail = lines[-1] if lines else ""
        hint = ""
        joined = " ".join(lines).lower()
        if "sandbox_timeout" in joined or "no such sandbox" in joined:
            hint = "沙箱可能已被回收，请 `sandbox create %s` 或运行 `sandbox ls` 确认" % self.sandbox
        elif "not found" in joined or "404" in joined or "unavailable" in joined:
            hint = ("沙箱 `%s` 不存在（或已删除）。`sandbox ls` 查看现有沙箱，"
                    "用 --sandbox <名字> 指定，或 `sandbox create %s` 创建"
                    % (self.sandbox, self.sandbox))
        elif ("not logged in" in joined or "unauthorized" in joined
              or "401" in joined or "403" in joined or "forbidden" in joined
              or "not authorized" in joined):
            hint = "登录态失效或权限不足，请运行 `sandbox login`（或设置 VERCEL_TOKEN）"
        elif ("starting python3" in joined
              or "closed by peer (1006)" in joined):
            hint = ("沙箱正在启动/恢复中（进程还没起来），"
                    "通常 30–60 秒就绪；稍后重试或 `sandbox ls` 观察状态")
        elif "network" in joined or "timeout" in joined or "socket" in joined:
            hint = "网络问题，稍后重试"
        elif not detail:
            hint = ("桥接没有输出任何错误就退出了，通常是沙箱处于回收边缘"
                    "（`sandbox ls` 看 TIME 是否只剩几十秒）")
        auth = "登录态失效" in hint
        msg = "bridge exited before open"
        if detail:
            msg += "：%s" % detail
        if hint:
            msg += "\n提示：%s" % hint
        self._last_exit_auth = auth
        msg += "\n沙箱=%s 目录=%s" % (self.sandbox, self.cwd)
        return msg

    def _bootstrap(self):
        """等 ready → 取源码 → 送源码 → 握手。

        必须先等 boot 发出 ready（说明它已切到 raw 并进入读帧循环），
        否则早期字节会被 pty 行缓冲吞掉。
        """

        # 必须等桥接把 WS 建好再发：过早写会被缓存后补发给一个"还没起来的进程"
        # （沙箱会丢弃它）。这里等 ws.on('open') 的日志。
        opened_at = time.time()
        while not self._bridge_open:
            proc = self._proc
            if proc is None or proc.poll() is not None:
                msg = self._explain_bridge_exit()
                if self._last_exit_auth:
                    raise AuthError(msg)
                raise TransportError(msg)
            if time.time() - opened_at > 90:
                raise TransportError("等待桥接建立 WebSocket 超时（90s），"
                                     "请检查网络或运行 `sandbox ls` 确认沙箱状态")
            time.sleep(0.05)
        # 记录一下从拉起桥接到建立 WS 的耗时，便于排查"沙箱濒临超时"
        self._t_open = time.time()
        # 再给沙箱进程一点时间进入读帧循环
        time.sleep(2.0)

        rid = self._next_rid()
        resp = Response(rid, "get")
        with self._lock:
            self._pending[rid] = resp
        msg = {"op": "get", "id": rid, "root": self.root,
               "proto": proto.VERSION}
        frame = proto.encode_frame(proto.T_REQ, msg)
        try:
            # 一次写入只发一帧（多次写入会被沙箱合并，可能被 TTY 丢弃）
            self.log("t: spawn->open %.2fs, open->send %.2fs"
                     % (self._t_open - self._t_spawn, time.time() - self._t_open))
            self._write(frame)
            self.log("bootstrap: get sent")
            deadline = time.time() + self.connect_timeout
            last = time.time()
            while not resp.event.wait(1.0):
                if time.time() > deadline:
                    raise TransportError("bootstrap timed out")
                proc = self._proc
                if proc is None or proc.poll() is not None:
                    raise TransportError("bridge exited during bootstrap")
                if time.time() - last > 1.0:
                    last = time.time()
                    self.log("bootstrap: resend")
                    self._write(frame)
            self.log("bootstrap: got want")
            src = self._read_worker_source()
            self._write(proto.encode_frame(
                proto.T_REQ,
                {"op": "put", "id": self._next_rid(), "root": self.root},
                src))
        except (TransportError, RemoteError) as exc:
            with self._lock:
                self._pending.pop(rid, None)
            self.log("bootstrap failed: %s" % exc)
            return False
        return self._hello()

    def _hello(self):
        """正式握手：确认 worker 已加载并拿到它的 pid。"""

        rid = self._next_rid()
        resp = Response(rid, "hello")
        with self._lock:
            self._pending[rid] = resp
        msg = {"op": "hello", "id": rid, "root": self.root,
               "proto": proto.VERSION}
        try:
            self._write(proto.encode_frame(proto.T_REQ, msg))
            if not resp.event.wait(self.connect_timeout):
                raise TransportError("hello timed out")
            if not resp.data.get("ok"):
                raise TransportError("hello rejected: %r" % (resp.data,))
        except (TransportError, RemoteError) as exc:
            with self._lock:
                self._pending.pop(rid, None)
            self.log("hello failed: %s" % exc)
            return False
        pid = resp.data.get("pid")
        if pid:
            self._worker_pid = int(pid)
        self.log("worker ready pid=%s python=%s" % (self._worker_pid,
                                                    resp.data.get("python")))
        return True

    @staticmethod
    def _read_worker_source():
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "worker.py"), "rb") as f:
            return f.read()

    def _loader_code(self):
        """约 3KB 的引导代码：读请求 → 收源码 → 执行 → 进入服务循环。"""

        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "boot.py"), "rb") as f:
            boot_src = base64.b64encode(f.read()).decode("ascii")
        return _LOADER % (self.root, boot_src)

    def _acquire_start_lock(self, timeout=120):
        """进程间锁：同一沙箱的 worker 启动只能有一个在跑。"""

        import hashlib

        try:
            import fcntl
        except ImportError:  # pragma: no cover
            return None
        key = hashlib.sha1(("%s|%s" % (self.sandbox, self.root)).encode()).hexdigest()[:16]
        path = os.path.join(require_tmpdir(), "sandbox_fs-%s.lock" % key)
        try:
            fh = open(path, "w")
        except OSError:
            return None
        deadline = time.time() + timeout
        while True:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return fh
            except OSError:
                if time.time() > deadline:
                    fh.close()
                    return None
                time.sleep(0.1)

    def _kill_proc(self):
        p = self._proc
        self._proc = None
        if p is None:
            return
        try:
            p.stdin.close()
        except (OSError, ValueError):
            pass
        if p.poll() is None:
            try:
                p.terminate()
                p.wait(timeout=3)
            except Exception:  # noqa: BLE001
                try:
                    p.kill()
                    p.wait(timeout=3)
                except Exception:  # noqa: BLE001
                    pass

    # ------------------------------------------------------------------
    # 读写
    # ------------------------------------------------------------------
    def _write(self, blob):
        """送字节进沙箱进程 stdin（走桥接控制帧，见 bridge.mjs）。"""

        with self._wlock:
            proc = self._proc
            if proc is None or proc.poll() is not None:
                raise TransportError("transport closed")
            try:
                proc.stdin.write(_CTRL_IN % len(blob))
                proc.stdin.write(blob)
                proc.stdin.flush()
            except (BrokenPipeError, ValueError, OSError) as exc:
                raise TransportError("transport write failed: %s" % exc)

    def _pump(self, proc, gen):
        decoder = proto.Decoder()
        try:
            while True:
                # 必须逐字节读：稍大一点的读取会让 Node 侧管道进入背压状态，
                # 连带把输入方向也冻住（实测 read(65536) 会 100% 失败）。
                chunk = proc.stdout.read(1)
                if not chunk:
                    break
                # 逐字节喂给解码器，攒够一帧才分发（解码器本身是增量的）
                for hdr, payload in decoder.feed(chunk):
                    self._dispatch(hdr, payload)
        except (ValueError, OSError):
            pass
        finally:
            if gen == self._gen:
                self._fail_all_pending("transport closed (bridge exited)")
                self.log("bridge exited (gen=%d)" % gen)

    def _drain_stderr(self, proc, gen):
        try:
            for line in iter(proc.stderr.readline, b""):
                text = line.decode("utf-8", "replace").rstrip()
                if not text:
                    continue
                if text.startswith("[sandbox_fs-bridge]"):
                    if "websocket open" in text:
                        self._bridge_open = True
                        self._t_open = time.time()
                    # 桥接的输出也是重要的诊断线索（如未登录、沙箱不存在），
                    # 去掉前缀后一并进缓冲。
                    body = text[len("[sandbox_fs-bridge]"):].strip()
                    if body:
                        self._stderr_buf.append(body)
                    self.log(text)
                    continue
                self._stderr_buf.append(text)
                self.log("stderr: %s" % text)
        except (ValueError, OSError):
            pass

    # ------------------------------------------------------------------
    # 请求
    # ------------------------------------------------------------------
    def _next_rid(self):
        with self._lock:
            self._seq += 1
            if self._seq > 0x7FFFFFFF:
                self._seq = 1
            return self._seq

    def request(self, op, payload=b"", **data):
        rid = self._next_rid()
        resp = Response(rid, op)
        data = dict(data)
        data["op"] = op
        data["id"] = rid
        with self._lock:
            self._pending[rid] = resp
        try:
            self._write(proto.encode_frame(proto.T_REQ, data, payload))
        except TransportError:
            with self._lock:
                self._pending.pop(rid, None)
            raise
        return resp

    def call(self, op, timeout=None, **data):
        return self.request(op, **data).wait(timeout or self.request_timeout)

    def wait_rid(self, rid, timeout=None):
        """等待某个 rid 的应答（上传流用）。"""

        with self._lock:
            resp = self._pending.get(rid)
        if resp is None:
            raise TransportError("no pending upload for rid=%s（可能已被取消）" % rid)
        return resp.wait(timeout or self.request_timeout)

    def send_data(self, rid, chunk, eof=False):
        """上传流：同一 rid 的 DATA 帧必须按序发送。"""

        head = {"op": "write_data", "id": rid}
        if eof:
            head["eof"] = True
        for part in _split(chunk):
            self._write(proto.encode_frame(proto.T_DATA, head, part,
                                          proto.F_END if eof else 0))

    def _dispatch(self, hdr, payload):
        rid = hdr.data.get("id")
        t = hdr.type
        if t == proto.T_EVENT:
            self.log("event: %r" % (hdr.data,))
            return
        with self._lock:
            resp = self._pending.get(rid)
        if resp is None:
            return
        if t == proto.T_DATA:
            resp.body += payload
            if hdr.end:
                for k, v in hdr.data.items():
                    if k not in ("id", "len"):
                        resp.data[k] = v
                with self._lock:
                    self._pending.pop(rid, None)
                resp._finish_ok(resp.data)
        else:
            data = dict(resp.data)
            data.update({k: v for k, v in hdr.data.items() if k != "len"})
            if not hdr.end:
                resp.data = data
                return
            with self._lock:
                self._pending.pop(rid, None)
            if data.get("ok"):
                resp._finish_ok(data)
            else:
                resp._finish_err(int(data.get("errno") or 0), data.get("msg") or "")

    def _fail_all_pending(self, why):
        with self._lock:
            pending = list(self._pending.values())
            self._pending.clear()
        for r in pending:
            if not r.event.is_set():
                r._finish_err(errno.EIO, why)

    # ------------------------------------------------------------------
    # 便捷调用
    # ------------------------------------------------------------------
    def exec(self, cmd, cwd=None, env=None, timeout=None):
        resp = self.request("exec", cmd=cmd, cwd=cwd, env=env or {}, timeout=timeout)
        resp.wait((timeout or self.request_timeout) + 15)
        return ExecResult(resp.data.get("code", -1), resp.text(),
                          bool(resp.data.get("timed_out")))

    def ping(self):
        return self.call("ping", root=self.root).data


def require_tmpdir():
    """可写的临时目录（避免依赖 /tmp 权限）。"""

    import tempfile

    try:
        d = tempfile.gettempdir()
        if os.access(d, os.W_OK):
            return d
    except (OSError, AttributeError):
        pass
    d = os.path.expanduser("~/.cache")
    try:
        os.makedirs(d, exist_ok=True)
        return d
    except OSError:
        return os.getcwd()


def _split(chunk):
    """把任意大小的数据切成不超过 MAX_PAYLOAD 的片段。"""

    if not chunk:
        yield b""
        return
    for off in range(0, len(chunk), proto.MAX_PAYLOAD):
        yield chunk[off:off + proto.MAX_PAYLOAD]


_LOADER = (
    "import base64,sys,types\n"
    "sys.argv=[sys.argv[0],%r]\n"
    "m=types.ModuleType('sandbox_fs_boot')\n"
    "m.__dict__['__file__']='<sandbox_fs-boot>'\n"
    "exec(compile(base64.b64decode(%r),'boot.py','exec'),m.__dict__)\n"
    "sys.exit(m.main())\n"
)

#: 桥接控制帧：NUL + JSON + 换行，随后是等长裸字节
_CTRL_IN = b"\x00" + b'{"t":"in","n":%d}\n'
