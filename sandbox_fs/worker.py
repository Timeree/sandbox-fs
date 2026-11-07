# -*- coding: utf-8 -*-
"""
沙箱内 worker：sandbox_fs-proto/1 的服务端。

一条连接 = 一个 python 进程。连接内所有请求按 ``id`` 多路复用、并发执行；
同一个 ``id`` 的 DATA 帧用 END 标志界定结束，不同请求的帧可以交错。

传输媒介无关：只要有 ``next()`` 能按序给出帧、``write()`` 能写出字节即可。
沙箱里由 :mod:`sandbox_fs.boot` 用 raw pty 提供，本地测试可以用 socket 或内存队列。
"""

from __future__ import absolute_import, unicode_literals

import base64
import errno
import hashlib
import json
import os
import shutil
import signal
import socket
import stat as statmod
import struct
import sys
import threading
import time
import traceback

MAGIC = b"SBOX"
PROTO = "sandbox_fs-proto/1"

HDR = struct.Struct(">4sBBI")
T_REQ, T_RES, T_DATA, T_EVENT = 0x01, 0x02, 0x03, 0x04
F_END = 0x01

CHUNK = 96 * 1024  # 下行单片上限（实测）；再大一帧就会卡

#: 可能跑很久、不能阻塞请求循环的操作
ASYNC_OPS = frozenset(("exec", "copy_tree", "rmtree"))


# ---------------------------------------------------------------------------
# 编码
# ---------------------------------------------------------------------------
def write_all(fd, blob, timeout=30.0):
    """把 blob 全部写出。

    pty 的输出缓冲很小，对端不读时会阻塞；这里设上限并分片，
    避免一个卡住的连接把服务线程永久冻住。
    """

    import select

    view = memoryview(blob)
    deadline = time.time() + timeout
    while view:
        try:
            n = os.write(fd, view)
            view = view[n:]
            continue
        except BlockingIOError:
            pass
        except InterruptedError:
            continue
        left = deadline - time.time()
        if left <= 0:
            raise OSError(11, "pty write timeout")
        try:
            select.select([], [fd], [], min(left, 0.5))
        except (OSError, ValueError):
            pass


def encode(type_, data, payload=b"", flags=F_END):
    data = dict(data)
    data["len"] = len(payload)
    head = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return HDR.pack(MAGIC, type_, flags, len(head)) + head + payload


def _stype(st):
    m = st.st_mode
    if statmod.S_ISDIR(m):
        return "dir"
    if statmod.S_ISLNK(m):
        return "link"
    if statmod.S_ISREG(m):
        return "file"
    if statmod.S_ISCHR(m) or statmod.S_ISBLK(m):
        return "device"
    if statmod.S_ISFIFO(m):
        return "fifo"
    if statmod.S_ISSOCK(m):
        return "socket"
    return "file"


def _fields(st, path=None):
    d = {
        "type": _stype(st),
        "size": st.st_size,
        "mode": st.st_mode,
        "uid": st.st_uid,
        "gid": st.st_gid,
        "mtime": int(st.st_mtime),
        "atime": int(st.st_atime),
        "ino": getattr(st, "st_ino", 0),
        "nlink": getattr(st, "st_nlink", 1),
    }
    if path:
        d["path"] = path
        d["name"] = os.path.basename(path.rstrip("/")) or "/"
    return d


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(1 << 20)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# 操作（同步的用 return 返回结果，流式的自己写 DATA 帧）
# ---------------------------------------------------------------------------
def op_ping(conn, rid, req):
    st = os.statvfs(req.get("root") or "/")
    return {
        "pid": os.getpid(),
        "host": socket.gethostname(),
        "free": st.f_bavail * st.f_frsize,
        "total": st.f_blocks * st.f_frsize,
        "proto": PROTO,
        "python": sys.version.split()[0],
    }


def op_stat(conn, rid, req):
    path = req["path"]
    st = os.stat(path) if req.get("follow") else os.lstat(path)
    d = _fields(st, path)
    if d["type"] == "link":
        try:
            d["target"] = os.readlink(path)
        except OSError:
            d["target"] = ""
    return d


def op_list(conn, rid, req):
    path = req["path"]
    out = []
    with os.scandir(path) as it:
        for ent in it:
            try:
                st = ent.stat(follow_symlinks=False)
            except OSError:
                continue
            d = _fields(st)
            d["name"] = ent.name
            if d["type"] == "link":
                try:
                    d["target"] = os.readlink(ent.path)
                except OSError:
                    d["target"] = ""
            out.append(d)
    return {"entries": out, "path": path}


def op_exists(conn, rid, req):
    """路径是否存在（含类型）。

    与 ``os.path.exists`` 语义一致：任何 OSError（ENOENT / EACCES / ENOTDIR
    …）都算"不可用"，映射成 None；否则返回类型字符串。
    """

    kinds = {}
    for p in req["paths"]:
        try:
            kinds[p] = _stype(os.lstat(p))
        except OSError:
            kinds[p] = None
    return {"kinds": kinds}


def op_read(conn, rid, req):
    """流式读：头里带 size/mtime/ino，后面跟 DATA 帧。"""

    path = req["path"]
    offset = int(req.get("offset") or 0)
    length = req.get("length")
    length = int(length) if length is not None else None
    with open(path, "rb") as f:
        st = os.fstat(f.fileno())
        if offset:
            f.seek(offset)
        data = f.read(length) if length is not None else f.read()
    conn.stream(rid, data, path=path, size=st.st_size, mtime=int(st.st_mtime),
                ino=st.st_ino, offset=offset)


def op_upload_begin(conn, rid, req):
    """第一阶段：打开目标文件并登记 fd，后续 DATA 帧按 rid 续写。"""

    path = req["path"]
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    mode = int(req.get("mode") or 0o666)
    tmp = "%s.sandbox_fs-tmp-%d-%d" % (path, os.getpid(), rid)
    fd = os.open(tmp, flags, mode)
    with UPLOAD_LOCK:
        UPLOADS[rid] = (fd, path, tmp, req.get("mode"))
    return {"ready": 1, "path": path}


def op_write_b64(conn, rid, req):
    """小文件一次性写入（原子替换）。"""

    path = req["path"]
    data = base64.b64decode(req["data"])
    mode = req.get("mode")
    tmp = "%s.sandbox_fs-tmp-%d" % (path, os.getpid())
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        if mode is not None:
            os.chmod(tmp, int(mode))
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    st = os.stat(path)
    return {"size": st.st_size, "mtime": int(st.st_mtime), "path": path}


def op_unlink(conn, rid, req):
    for p in req["paths"]:
        try:
            os.unlink(p)
        except OSError as exc:
            if exc.errno in (errno.EISDIR, errno.EPERM):
                raise OSError(errno.EISDIR, "is a directory", p)
            raise
    return {}


def op_rmtree(conn, rid, req):
    path = req["path"]
    if os.path.islink(path) or not os.path.isdir(path):
        raise OSError(errno.ENOTDIR, "not a directory", path)
    shutil.rmtree(path)
    return {}


def op_mkdir(conn, rid, req):
    for item in req.get("dirs") or []:
        os.mkdir(item[0], item[1] if len(item) > 1 else 0o777)
    return {}


def op_rmdir(conn, rid, req):
    for p in req["paths"]:
        os.rmdir(p)
    return {}


def op_rename(conn, rid, req):
    os.replace(req["src"], req["dst"])
    return {}


def op_symlink(conn, rid, req):
    os.symlink(req["target"], req["path"])
    return {}


def op_link(conn, rid, req):
    try:
        os.link(req["src"], req["dst"])
    except OSError as exc:
        if exc.errno in (errno.EMLINK, errno.EPERM, errno.EOPNOTSUPP):
            raise OSError(errno.ENOTSUP, "hard links unsupported", req["src"])
        raise
    return {}


def op_chmod(conn, rid, req):
    os.chmod(req["path"], int(req["mode"]))
    return {}


def op_utime(conn, rid, req):
    at = int(req["atime"]) if req.get("atime") is not None else int(time.time())
    mt = int(req["mtime"]) if req.get("mtime") is not None else int(time.time())
    os.utime(req["path"], (at, mt))
    return {}


def op_truncate(conn, rid, req):
    with open(req["path"], "r+b") as f:
        f.truncate(int(req["size"]))
    return {}


def op_sha(conn, rid, req):
    return {"sha": sha256_file(req["path"]), "path": req["path"]}


def op_copy_tree(conn, rid, req):
    src, dst = req["src"], req["dst"]
    if os.path.isdir(src) and not os.path.islink(src):
        _copy_into(src, dst)
    else:
        target = dst if not os.path.isdir(dst) else os.path.join(dst, os.path.basename(src))
        shutil.copy2(src, target)
    return {"path": dst}


def _copy_into(src, dst):
    if not os.path.isdir(dst):
        shutil.copytree(src, dst, symlinks=True)
        return
    for ent in os.scandir(src):
        target = os.path.join(dst, ent.name)
        if ent.is_dir(follow_symlinks=False):
            if not os.path.isdir(target):
                shutil.copytree(ent.path, target, symlinks=True)
            else:
                _copy_into(ent.path, target)
        else:
            shutil.copy2(ent.path, target, follow_symlinks=False)


def op_exec(conn, rid, req):
    """跑一条命令，输出作为 DATA 流回；末尾带退出码。

    超时由看门狗 ``threading.Timer`` 兜底：``read1`` 在子进程无输出时会一直
    阻塞，单靠读循环里的 deadline 检查永远等不到（静默命令的超时形同虚设）。
    到点 kill 子进程，管道 EOF 会立即唤醒读循环。
    """

    import subprocess

    cmd = req["cmd"]
    timeout = req.get("timeout")
    env = dict(os.environ)
    env.update(req.get("env") or {})
    # start_new_session：超时时才能用 killpg 杀掉整个进程组。
    # shell=True 时 proc.kill() 只杀掉 sh 本身，真正的子进程（这里通常
    # 才占着 stdout 管道）会苟活，管道永远不 EOF，读循环也就永远不会醒。
    proc = subprocess.Popen(cmd, shell=isinstance(cmd, str), cwd=req.get("cwd"),
                            env=env, stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            start_new_session=True)
    timed_out = False
    timer = None
    if timeout:
        def _expire():
            nonlocal timed_out

            timed_out = True
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except OSError:  # noqa: BLE001  进程组恰好自己退出了
                pass

        timer = threading.Timer(float(timeout), _expire)
        timer.daemon = True
        timer.start()
    try:
        while True:
            data = proc.stdout.read1(CHUNK)
            if not data:
                break
            conn.frame(T_DATA, {"id": rid}, data, 0)
        code = proc.wait()
    finally:
        if timer is not None:
            timer.cancel()
        try:
            proc.stdout.close()
        except OSError:
            pass
    return {"code": code, "timed_out": timed_out}


HANDLERS = {
    "ping": op_ping,
    "stat": op_stat,
    "list": op_list,
    "exists": op_exists,
    "read": op_read,
    "upload_begin": op_upload_begin,
    "write_b64": op_write_b64,
    "unlink": op_unlink,
    "rmtree": op_rmtree,
    "mkdir": op_mkdir,
    "rmdir": op_rmdir,
    "rename": op_rename,
    "symlink": op_symlink,
    "link": op_link,
    "chmod": op_chmod,
    "utime": op_utime,
    "truncate": op_truncate,
    "sha": op_sha,
    "copy_tree": op_copy_tree,
    "exec": op_exec,
}

UPLOADS = {}
UPLOAD_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# 会话
# ---------------------------------------------------------------------------
class Daemon(object):
    """持有连接与上传状态，把请求派发到对应操作。"""

    def __init__(self, root):
        self.root = root
        self.conn = None

    def on_attach(self, conn):
        self.conn = conn

    def on_detach(self):
        with UPLOAD_LOCK:
            for rid, (fd, _p, tmp, _m) in list(UPLOADS.items()):
                try:
                    os.close(fd)
                except OSError:
                    pass
                if tmp:  # 中断的上传：清掉半成品临时文件，避免留垃圾
                    try:
                        os.unlink(tmp)
                    except OSError:
                        pass
                UPLOADS.pop(rid, None)

    # -- hello ------------------------------------------------------------
    def on_hello(self, conn, rid, data):
        conn.res(rid, 1, pid=os.getpid(), proto=PROTO,
                 python=sys.version.split()[0])

    # -- 请求派发 ---------------------------------------------------------
    def on_request(self, conn, rid, op, data):
        fn = HANDLERS.get(op)
        if fn is None:
            conn.res(rid, 0, errno=errno.EINVAL, msg="unknown op %r" % (op,))
            return
        if op in ASYNC_OPS:
            t = threading.Thread(target=self._run, args=(fn, conn, rid, data),
                                 name="sandbox_fs-%s" % op)
            t.daemon = True
            t.start()
        else:
            self._run(fn, conn, rid, data)

    def _run(self, fn, conn, rid, data):
        try:
            rv = fn(conn, rid, data)
            if rv is not None:
                conn.res(rid, 1, **rv)
        except OSError as exc:
            conn.res(rid, 0, errno=exc.errno or errno.EIO, msg=str(exc))
        except Exception:  # noqa: BLE001
            conn.res(rid, 0, errno=errno.EIO, msg=traceback.format_exc(limit=4))

    # -- 上传 -------------------------------------------------------------
    def on_data(self, conn, rid, data, payload):
        with UPLOAD_LOCK:
            entry = UPLOADS.get(rid)
        if entry is None:
            conn.res(rid, 0, errno=errno.EBADF,
                     msg="upload not started (rid=%s)" % rid)
            return
        fd, path, tmp, mode = entry
        try:
            view = memoryview(payload)
            while view:
                n = os.write(fd, view)
                view = view[n:]
            if data.get("eof"):
                if mode is not None:
                    try:
                        os.fchmod(fd, int(mode))
                    except OSError:
                        pass
                os.close(fd)
                if tmp:
                    os.replace(tmp, path)
                with UPLOAD_LOCK:
                    UPLOADS.pop(rid, None)
                st = os.stat(path)
                conn.res(rid, 1, written=1, path=path,
                         size=st.st_size, mtime=int(st.st_mtime))
        except OSError as exc:
            with UPLOAD_LOCK:
                UPLOADS.pop(rid, None)
            try:
                os.close(fd)
            except OSError:
                pass
            if tmp:  # 半成品临时文件同样清掉，与 on_detach 的清理一致
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
            conn.res(rid, 0, errno=exc.errno or errno.EIO, msg=str(exc))


# ---------------------------------------------------------------------------
# 连接
# ---------------------------------------------------------------------------
class Conn(object):
    """把任意可读的媒介包装成"连接"：帧写出 + 请求循环。"""

    def __init__(self, rd, wr, daemon):
        self.rd = rd          # 可调用对象：返回 (type, flags, data, payload) 或 None
        self.wr = wr          # 可调用对象：写出字节
        self.daemon = daemon
        self.lock = threading.Lock()

    # -- 写 ---------------------------------------------------------------
    def write(self, blob):
        with self.lock:
            self.wr(blob)

    def frame(self, type_, data, payload=b"", flags=F_END):
        self.write(encode(type_, data, payload, flags))

    def res(self, rid, ok=1, **data):
        data["id"] = rid
        data["ok"] = 1 if ok else 0
        self.frame(T_RES, data)

    def stream(self, rid, payload, **data):
        """流式结果：按 ``CHUNK``（96KB）切片发送。

        下行方向单帧超过约 96KB 就会卡死（实测），切片是稳定传输的必要条件。
        """

        total = len(payload)
        off = 0
        first = True
        while True:
            part = payload[off:off + CHUNK]
            off += len(part)
            head = dict(data) if first else {}
            head["id"] = rid
            first = False
            if not part or off >= total:
                self.frame(T_DATA, head, part, F_END)
                break
            self.frame(T_DATA, head, part, 0)

    # -- 读 ---------------------------------------------------------------
    def serve(self):
        d = self.daemon
        d.on_attach(self)
        try:
            while True:
                fr = self.rd()
                if fr is None:
                    break
                type_, _flags, data, payload = fr
                op = data.get("op")
                rid = data.get("id")
                if op in ("detach", "shutdown"):
                    self.res(rid, 1, pid=os.getpid())
                    break
                if type_ == T_DATA:
                    d.on_data(self, rid, data, payload)
                elif op == "hello":
                    d.on_hello(self, rid, data)
                elif op:
                    d.on_request(self, rid, op, data)
                else:
                    self.res(rid, 0, errno=errno.EINVAL, msg="missing op")
        finally:
            d.on_detach()


class Feeder(object):
    """主线程读 pty，把帧投递给服务线程（避免两处同时读同一个 fd）。"""

    def __init__(self):
        import queue

        self.q = queue.Queue()

    def rd(self):
        return self.q.get()

def run_fed(root, fd_in=0, fd_out=1, buf=None):
    """主线程读字节 → 队列；服务线程从队列取帧并处理。

    ！！！必须与 :func:`sandbox_fs.boot.main` 配合：boot 只负责 pump，
    解析与应答全部发生在服务线程。
    """

    boot = sys.modules.get("sandbox_fs_boot")
    feeder = Feeder()

    def wr(blob):
        write_all(fd_out, blob)

    daemon = Daemon(root)
    conn = Conn(feeder.rd, wr, daemon)
    threading.Thread(target=conn.serve, name="sandbox_fs-serve", daemon=True).start()

    buf = buf if buf is not None else bytearray()
    while True:
        fr = boot.read_frame(fd_in, buf)
        if fr is None:
            break
        feeder.q.put(fr)
    return 0
