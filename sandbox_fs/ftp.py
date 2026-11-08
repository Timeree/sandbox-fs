"""
FTP 适配层：把沙箱文件系统挂到 pyftpdlib 上。

用法::

    from sandbox_fs import connect
    from sandbox_fs.ftp import serve_ftp

    fs = connect("sandbox")                     # 先连沙箱
    serve_ftp(fs, host="127.0.0.1", port=2121)   # 然后对外提供 FTP

客户端（FileZilla / WinSCP / lftp / curl）连 ``ftp://127.0.0.1:2121``
看到的就是沙箱的工作目录（默认 ``/workspace``）。

设计要点
--------
* 只实现 pyftpdlib 需要的 ``AbstractedFS`` 接口，传输走 :class:`sandbox_fs.fs.FileSystem`；
* 每次调用 ``open()`` 返回的文件对象会**在关闭时一次性写入**（沙箱侧没有真正的
  流式追加语义，而且通道对多帧流很敏感），所以大文件上传会占用内存；
* MLSD/LIST 的格式由沙箱返回的 stat 直接生成，保持 ``ls -l`` 风格。
"""


import errno
import os
import posixpath
import stat as statmod
import threading
import time

from .errors import RemoteError
from .fs import MAX_PAYLOAD

try:  # pyftpdlib 是可选的：没装也能用 Python API
    from pyftpdlib.authorizers import DummyAuthorizer
    from pyftpdlib.filesystems import AbstractedFS
    from pyftpdlib.handlers import FTPHandler
    from pyftpdlib.servers import FTPServer
except ImportError:  # pragma: no cover
    AbstractedFS = object
    DummyAuthorizer = None
    FTPHandler = None
    FTPServer = None


def _raise_as_fs_error(func):
    """把 sandbox_fs.RemoteError 转成 pyftpdlib 认识的 FilesystemError。"""

    from functools import wraps

    try:
        from pyftpdlib.filesystems import FilesystemError
    except ImportError:  # pragma: no cover
        FilesystemError = Exception

    @wraps(func)
    def wrapper(self, *args, **kw):
        try:
            return func(self, *args, **kw)
        except RemoteError as exc:
            raise FilesystemError("%s (errno %s)" % (
                exc.message.strip().splitlines()[0] if exc.message else exc.op,
                exc.errno))

    return wrapper


class SandboxAbstractedFS(AbstractedFS if AbstractedFS is not object else object):
    """pyftpdlib 的虚拟文件系统实现，后端是沙箱。"""

    def __init__(self, root, cmd_channel):
        # self.fs 是 sandbox_fs.FileSystem；root 是沙箱根目录。
        # 注意：只做一次取值，别用 getattr 链把对象"取丢"。
        fs = getattr(cmd_channel, "sandbox_fs", None)
        if fs is None:
            authorizer = getattr(cmd_channel, "authorizer", None)
            fs = getattr(authorizer, "sandbox_fs", None)
        self.fs = fs
        # pyftpdlib 自己会做 root 拼接；我们的 ftp2fs/fs2ftp 已经处理了
        # 沙箱根，所以这里给它一个"虚拟根"，避免双重前缀。
        try:
            (AbstractedFS if AbstractedFS is not object else _DummyFS).__init__(
                self, "/", cmd_channel)
        except TypeError:
            pass
        # 基类可能改写，重新固定
        self._root = "/"
        self._cwd = "/"
        self.fs = fs

    # -- 路径映射 ---------------------------------------------------------
    def ftp2fs(self, ftppath):
        """FTP 视角路径（相对 cwd）→ 沙箱绝对路径。

        注意：pyftpdlib 调用本站时通常已经传进来绝对路径，而它的"绝对"是
        FTP 视角的绝对（``/foo`` → 沙箱 ``<root>/foo``）。
        """

        p = ftppath or "/"
        # 已经是沙箱绝对路径就不再转换（方法可能被重复调用）
        if self.fs is not None and (p == self.fs.root
                                    or p.startswith(self.fs.root + "/")):
            return posixpath.normpath(p)
        if not p.startswith("/"):
            p = posixpath.join(self._cwd or "/", p)
        p = posixpath.normpath(p)
        if self.fs is None:
            return p
        if p == "/" or p == ".":
            return self.fs.root
        return self.fs.root + p

    def fs2ftp(self, fspath):
        """沙箱绝对路径 → FTP 视角路径。"""

        if self.fs is None:
            return fspath
        root = self.fs.root
        if fspath == root:
            return "/"
        if fspath.startswith(root + "/"):
            return fspath[len(root):]
        return "/"

    def validpath(self, path):
        return path.startswith(self.fs.root) if self.fs else True

    def realpath(self, path):
        return path

    def lexists(self, path):
        try:
            self.fs.lstat(path)
            return True
        except RemoteError as exc:
            return exc.errno not in (errno.ENOENT, errno.ENOTDIR)

    @_raise_as_fs_error
    def stat(self, path):
        return self.fs.stat(path, follow=True).stat_result()

    @_raise_as_fs_error
    def lstat(self, path):
        return self.fs.lstat(path).stat_result()

    def isdir(self, path):
        try:
            return self.fs.lstat(path).is_dir
        except RemoteError as exc:
            if exc.errno in (errno.ENOENT, errno.ENOTDIR):
                return False
            raise

    def isfile(self, path):
        try:
            return self.fs.lstat(path).is_file
        except RemoteError as exc:
            if exc.errno in (errno.ENOENT, errno.ENOTDIR):
                return False
            raise

    def islink(self, path):
        try:
            return self.fs.lstat(path).is_link
        except RemoteError:
            return False

    @_raise_as_fs_error
    def getsize(self, path):
        return self.fs.getsize(path)

    @_raise_as_fs_error
    def getmtime(self, path):
        return self.fs.getmtime(path)

    def chdir(self, path):
        self._cwd = self.fs2ftp(path)
        return path

    @_raise_as_fs_error
    def mkdir(self, path):
        self.fs.mkdir(path)

    @_raise_as_fs_error
    def rmdir(self, path):
        self.fs.rmdir(path)

    @_raise_as_fs_error
    def remove(self, path):
        self.fs.remove(path)

    @_raise_as_fs_error
    def rename(self, src, dst):
        self.fs.rename(src, dst)

    @_raise_as_fs_error
    def chmod(self, path, mode):
        self.fs.chmod(path, mode)

    @_raise_as_fs_error
    def listdir(self, path):
        return [e.name for e in self.fs.listdir(path)]

    @_raise_as_fs_error
    def listdirinfo(self, path):
        return [(e.name, e.stat_result()) for e in self.fs.listdir(path)]

    @_raise_as_fs_error
    def open(self, filename, mode):
        return SandboxFile(self.fs, filename, mode)

    # -- 列表格式 ---------------------------------------------------------
    MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

    def format_list(self, basedir, listing, ignore_err=True):
        now = time.time()
        for name in listing:
            try:
                e = self.fs.lstat(posixpath.join(basedir, name))
            except RemoteError:
                if ignore_err:
                    continue
                raise
            yield (self._ls_line(e, name, now) + "\r\n").encode("utf-8")

    def _ls_line(self, e, name, now):
        m = e.mode
        perms = statmod.filemode(m if m else (0o40755 if e.is_dir else 0o100644))
        mt = time.localtime(e.mtime)
        if now - e.mtime > 180 * 24 * 3600:
            when = "%s %2d  %d" % (self.MONTHS[mt.tm_mon - 1], mt.tm_mday, mt.tm_year)
        else:
            when = "%s %2d %02d:%02d" % (self.MONTHS[mt.tm_mon - 1], mt.tm_mday,
                                         mt.tm_hour, mt.tm_min)
        return "%s %3d %-8s %-8s %8d %s %s" % (perms, max(1, e.nlink),
                                               str(e.uid), str(e.gid),
                                               e.size, when, name)

    def format_mlsx(self, basedir, listing, perms, facts, ignore_err=True):
        for name in listing:
            try:
                e = self.fs.lstat(posixpath.join(basedir, name))
            except RemoteError:
                if ignore_err:
                    continue
                raise
            parts = []
            if "type" in facts:
                parts.append("type=" + ("dir" if e.is_dir else
                                        "OS.unix=symlink" if e.is_link else "file"))
            if "size" in facts and not e.is_dir:
                parts.append("size=%d" % e.size)
            if "modify" in facts:
                parts.append("modify=" + time.strftime("%Y%m%d%H%M%S",
                                                       time.gmtime(e.mtime)))
            if "perm" in facts:
                parts.append("perm=" + (perms or ""))
            if "unix.mode" in facts:
                parts.append("unix.mode=%04o" % (e.mode & 0o7777))
            if "unix.uid" in facts:
                parts.append("unix.uid=%d" % e.uid)
            if "unix.gid" in facts:
                parts.append("unix.gid=%d" % e.gid)
            yield ("%s; %s\r\n" % (";".join(parts), name)).encode("utf-8")


class _DummyFS(object):
    def __init__(self, root, cmd_channel):
        self._cwd = "/"


class SandboxFile(object):
    """pyftpdlib 用的文件对象（惰性读 + 落地写）。

    为什么不用"一次性读进内存"
    --------------------------
    FTP 经常搬几十上百 MB 的文件，整段读进内存会直接把进程撑爆。
    这里改成：

    * **读**：按需从沙箱分片拉取（每片 ≤ ``READ_SLICE``），只保留一小块缓存；
    * **写**：先写到本地临时文件，``close()`` 时再上传——
      小文件走会话内联，大文件走 ``sandbox cp``，内存占用与文件大小无关。
    """

    #: 读缓存片大小（与 FileSystem 的下行单片上限一致）
    BUF = 96 * 1024

    def __init__(self, fs, path, mode):
        self.fs = fs
        self.path = path
        self.name = path          # pyftpdlib 在关闭时会用到
        self.mode = mode
        self.closed = False
        self.pos = 0

        self._size = 0
        self._rbuf = b""
        self._rbuf_off = 0
        self._tmp = None
        self._wname = None

        reading = ("r" in mode) and ("w" not in mode) and ("a" not in mode) \
            and ("x" not in mode)
        writing = ("w" in mode) or ("a" in mode) or ("x" in mode) or ("+" in mode)

        if reading or "+" in mode:
            st = fs.stat(path, follow=True)   # 不存在时抛 RemoteError → 550
            self._size = st.size
            if st.is_dir:
                raise OSError(errno.EISDIR, "is a directory", path)

        if writing:
            import tempfile

            self._tmp = tempfile.NamedTemporaryFile(prefix="sandbox_fs-ftp-",
                                                    delete=False)
            self._wname = self._tmp.name
            if "a" in mode and self._size:
                # 追加：先把原内容搬到临时文件（分段，不整段入内存）
                self.fs.read_into(path, self._tmp.write)

    # -- 读 ---------------------------------------------------------------
    def read(self, size=-1):
        if size is None or size < 0:
            size = max(0, self._size - self.pos)
        if size <= 0:
            return b""

        end = self.pos + size
        if self._size:
            end = min(end, self._size)
        if end <= self.pos:
            return b""

        cached_start = self._rbuf_off
        cached_end = self._rbuf_off + len(self._rbuf)
        if not (cached_start <= self.pos and end <= cached_end):
            want = max(size, self.BUF)
            if self._size:
                want = min(want, self._size - self.pos)
            if want <= 0:
                return b""
            self._rbuf = self.fs.read(self.path, offset=self.pos, length=want)
            self._rbuf_off = self.pos
            cached_start = self.pos
            if not self._rbuf:
                self._size = self.pos
                return b""

        i = self.pos - cached_start
        data = self._rbuf[i:i + size]
        self.pos += len(data)
        return data

    def readline(self, size=-1):
        limit = None if (size is None or size < 0) else size
        out = bytearray()
        while limit is None or len(out) < limit:
            chunk = self.read(8192)
            if not chunk:
                break
            nl = chunk.find(b"\n")
            if nl >= 0:
                keep = nl + 1
                out += chunk[:keep]
                back = len(chunk) - keep
                if back:
                    self.seek(self.pos - back)
                break
            out += chunk
        data = bytes(out)
        if limit is not None:
            data = data[:limit]
        return data

    def seek(self, offset, whence=0):
        if whence == 0:
            pos = offset
        elif whence == 1:
            pos = self.pos + offset
        else:
            pos = (self._size or 0) + offset
        self.pos = max(0, int(pos))
        return self.pos

    def tell(self):
        return self.pos

    # -- 写 ---------------------------------------------------------------
    def write(self, data):
        if self._tmp is None:
            raise OSError(errno.EBADF, "file not open for writing", self.path)
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._tmp.write(data)
        return len(data)

    def flush(self):
        if self._tmp is not None:
            try:
                self._tmp.flush()
            except (OSError, ValueError):
                pass

    def close(self):
        if self.closed:
            return
        self.closed = True
        if self._tmp is None:
            return
        try:
            self._tmp.close()
        except (OSError, ValueError):
            pass
        try:
            size = os.path.getsize(self._wname)
            if size <= MAX_PAYLOAD:
                with open(self._wname, "rb") as f:
                    self.fs.write(self.path, f.read())
            else:
                # 大文件交给 sandbox cp，内存占用与文件大小无关
                self.fs.tool.upload_many([(self._wname, self.path)])
        finally:
            try:
                os.unlink(self._wname)
            except OSError:
                pass

    def fileno(self):
        raise OSError(errno.EBADF, "sandbox-backed file has no fd")

    # 允许被 with 使用
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# ---------------------------------------------------------------------------
# 处理器 & 服务
# ---------------------------------------------------------------------------
def make_handler(fs, user=None, password=None, anonymous=True):
    """构造一个绑定了沙箱文件系统的 FTPHandler 子类。"""

    if FTPHandler is None:
        raise RuntimeError("pyftpdlib 未安装：pip install pyftpdlib")

    class Handler(FTPHandler):
        sandbox_fs = fs
        abstracted_fs = SandboxAbstractedFS

        passive_ports = range(50000, 50100)
        encoding = "utf-8"
        use_gmt_times = True
        # FileZilla/WinSCP 会在 ASCII 模式下先 SIZE/MLST 探测，允许它更省事
        permit_foreign_addresses = False

        def __init__(self, conn, server, ioloop=None):
            self.sandbox_fs = fs
            FTPHandler.__init__(self, conn, server, ioloop=ioloop)

        def ftp_SIZE(self, path):
            """SIZE 在 ASCII 模式下也放行（FileZilla / WinSCP 会先探测）。"""

            try:
                spath = self.fs.ftp2fs(path)
                entry = self.sandbox_fs.stat(spath, follow=True)
            except RemoteError as exc:
                self.respond("550 %s" % ("No such file."
                                         if exc.errno == errno.ENOENT
                                         else str(exc)))
                return
            except Exception as exc:  # noqa: BLE001
                self.respond("550 %s" % exc)
                return
            if entry.is_dir:
                self.respond("550 not a regular file.")
                return
            self.respond("213 %d" % entry.size)

        def on_login(self, username):
            if isinstance(self.fs, SandboxAbstractedFS):
                self.fs.fs = self.sandbox_fs
                self.fs._cwd = "/"

    if DummyAuthorizer is None:
        raise RuntimeError("pyftpdlib 未安装：pip install pyftpdlib")

    # DummyAuthorizer.add_user()/add_anonymous() 会用 os.path.isdir()、
    # os.path.realpath() 校验"家目录"，而沙箱路径在本机并不存在。
    # 所以这里给它一个本地占位目录，真正的路径解析全部由
    # SandboxAbstractedFS.ftp2fs() 负责（它把 FTP 视角映射到沙箱）。
    placeholder = _local_placeholder()

    if not anonymous and not user:
        # 没人能登录的"就绪"服务比启动失败更糟：前台会打印就绪，用户却
        # 一直连不上。这里在绑定端口之前就明确拒绝。
        raise RuntimeError(
            "已关闭匿名访问（--no-anonymous），但未通过 --user 指定账号；"
            "拒绝启动一个没有任何可登录账号的 FTP 服务")

    authorizer = DummyAuthorizer()
    perm = "elradfmwMT"   # 全权限：读、写、删除、改名、建目录……
    if anonymous:
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            # 匿名账号即"无账号密码"，直接给满权限
            authorizer.add_anonymous(placeholder, perm=perm)
    if user:
        authorizer.add_user(user, password or "", placeholder, perm=perm)
    Handler.authorizer = authorizer
    return Handler


def _local_placeholder():
    """一个本地真实存在、只用于通过 pyftpdlib 家目录校验的目录。"""

    try:
        path = os.path.expanduser("~/.sandbox_fs-ftp-root")
        os.makedirs(path, exist_ok=True)
        return path
    except OSError:
        import tempfile

        return tempfile.mkdtemp(prefix="sandbox_fs-ftp-")


def serve_ftp(fs, host="0.0.0.0", port=2121, user=None, password=None,
              anonymous=True, blocking=True, log=None):
    """启动 FTP 服务。

    ``blocking=True`` 时阻塞（CLI 用）；否则返回 ``(server, thread)``。
    默认无账号密码（匿名 + 全权限），监听所有网卡。
    """

    handler = make_handler(fs, user=user, password=password,
                           anonymous=anonymous)
    try:
        server = FTPServer((host, port), handler)
    except OSError as exc:
        if exc.errno in (errno.EADDRINUSE, errno.EACCES):
            raise RuntimeError(
                "无法监听 %s:%d（%s）。该端口可能已有 sandbox_fs FTP 在运行"
                % (host, port, exc.strerror or exc))
        raise
    if log:
        log("FTP 就绪：ftp://%s:%d  (根=%s)" % (host, port, fs.root))
    if blocking:
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.close_all()
        return server
    t = threading.Thread(target=server.serve_forever, name="sandbox_fs-ftp",
                         daemon=True)
    t.start()
    return server, t
