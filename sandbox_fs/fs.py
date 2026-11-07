# -*- coding: utf-8 -*-
"""
文件系统层：把帧协议翻译成 POSIX 风格的文件操作。

路径规则
--------
对外路径一律是**沙箱内的绝对路径**（例如 ``/workspace/a.txt``）。
``root`` 只是默认工作目录/越界检查边界，不做路径重映射——这样 FTP 客户端
看到的就是真实的沙箱路径，``sandbox cp`` 之类的工具也能直接对照。

传输分档
--------
* 小文件（``inline_limit``，默认 64KB）走会话内联，一次往返；
* 大文件用 ``upload_begin`` + DATA 流，不占内存；
* 超出单帧上限的写入自动转 ``sandbox cp``（大文件批量最稳）。
"""

from __future__ import absolute_import, unicode_literals

import base64
import errno
import os
import posixpath
import stat as statmod
import tempfile

from .errors import RemoteError
from .transport import Channel

DEFAULT_INLINE_LIMIT = 64 * 1024
CHUNK = 128 * 1024
MAX_PAYLOAD = 256 * 1024   # 上行单帧上限（实测可行）
READ_SLICE = 96 * 1024     # 下行单片上限（实测可行）


class Entry(object):
    """目录项 / stat 结果的轻量包装。"""

    __slots__ = ("name", "path", "type", "size", "mode", "uid", "gid",
                 "mtime", "atime", "ino", "nlink", "target")

    def __init__(self, data, parent=None):
        self.name = data.get("name") or ""
        self.path = data.get("path") or (posixpath.join(parent or "", self.name))
        self.type = data.get("type") or "file"
        self.size = int(data.get("size") or 0)
        self.mode = int(data.get("mode") or 0)
        self.uid = int(data.get("uid") or 0)
        self.gid = int(data.get("gid") or 0)
        self.mtime = int(data.get("mtime") or 0)
        self.atime = int(data.get("atime") or 0)
        self.ino = int(data.get("ino") or 0)
        self.nlink = int(data.get("nlink") or 1)
        self.target = data.get("target") or ""

    @property
    def is_dir(self):
        return self.type == "dir"

    @property
    def is_link(self):
        return self.type == "link"

    @property
    def is_file(self):
        return self.type == "file"

    def stat_result(self):
        """转成 ``os.stat_result``，方便 pyftpdlib 之类的库直接用。"""

        kind = {"dir": statmod.S_IFDIR, "file": statmod.S_IFREG,
                "link": statmod.S_IFLNK}.get(self.type, statmod.S_IFREG)
        perm = self.mode & 0o7777 or (0o755 if self.is_dir else 0o644)
        mode = kind | perm
        return os.stat_result((mode, self.ino, 0, self.nlink, self.uid,
                               self.gid, self.size, self.atime, self.mtime,
                               self.mtime))

    def __repr__(self):
        return "<Entry %s %s %d>" % (self.type, self.name, self.size)


class FileSystem(object):
    """沙箱文件系统（线程安全，可被多线程共享）。"""

    def __init__(self, sandbox="sandbox", root="auto", cwd=None,
                 inline_limit=DEFAULT_INLINE_LIMIT, channel=None, log=None,
                 **channel_kw):
        self.log = log or (lambda *a: None)
        # auto: 连接前探测沙箱真实初始路径（保证 cwd 真实存在，否则握手失败）
        if root == "auto":
            from .tool import ToolClient

            try:
                tool = ToolClient(sandbox=sandbox, cwd="/", root="/")
                probed = tool.exec_command("pwd")
                if probed.startswith("/"):
                    root = probed
                    self.log("root=auto -> %s" % root)
            except Exception as exc:  # noqa: BLE001
                self.log("root 探测失败：%s" % exc)
                root = "/workspace"
        self.root = (root or "/").rstrip("/") or "/"
        self.cwd = cwd or self.root
        self.inline_limit = inline_limit if inline_limit is not None else DEFAULT_INLINE_LIMIT
        if channel is not None:
            self.chan = channel
        else:
            self.chan = Channel(sandbox=sandbox, cwd=self.cwd, root=self.root,
                                log=self.log, **channel_kw)

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def connect(self):
        self.chan.connect()
        return self

    def close(self):
        self.chan.close()

    def __enter__(self):
        return self.connect()

    def __exit__(self, *exc):
        self.close()
        return False

    @property
    def tool(self):
        return self.chan.tool

    # ------------------------------------------------------------------
    # 路径
    # ------------------------------------------------------------------
    def abspath(self, path, base=None):
        """把相对路径解析成沙箱绝对路径。"""

        if path is None or path == "":
            return base or self.cwd
        base = base or self.cwd
        if path == "~":
            return "/root"
        if path.startswith("~/"):
            return "/root/" + path[2:]
        if not path.startswith("/"):
            path = posixpath.join(base, path)
        return posixpath.normpath(path)

    def _check(self, spath, allow_root=False):
        """越界检查：默认只允许 root 及其子树（``allow_root=False`` 时禁止根本身）。

        例外：客户端常把 "/" 当作"FTP 根"，这里把它映射成挂载根本身。
        """

        if not spath.startswith("/"):
            raise OSError(errno.EINVAL, "not an absolute path", spath)
        if posixpath.normpath(spath) == "/":
            # "/" 表示挂载点（FTP 客户端的根）
            return self.root
        norm = posixpath.normpath(spath)
        if self.root == "/":
            # 根挂载为 "/" 时不做越界限制（整个沙箱可见）
            if norm == "/" and not allow_root:
                raise OSError(errno.EACCES, "operation on root not allowed", norm)
            return norm
        if not (norm == self.root or norm.startswith(self.root + "/")):
            raise OSError(errno.EACCES, "outside of root %s" % self.root, norm)
        if norm == self.root and not allow_root:
            raise OSError(errno.EACCES, "operation on root not allowed", norm)
        return norm

    # ------------------------------------------------------------------
    # 元数据
    # ------------------------------------------------------------------
    def stat(self, path, follow=False):
        spath = self._check(self.abspath(path), allow_root=True)
        return Entry(self.chan.call("stat", path=spath, follow=bool(follow)).data)

    def lstat(self, path):
        return self.stat(path, follow=False)

    def exists(self, path):
        return self.exists_many([path])[0]

    def exists_many(self, paths):
        abspaths = [self._check(self.abspath(p), allow_root=True) for p in paths]
        resp = self.chan.call("exists", paths=abspaths)
        kinds = resp.data.get("kinds") or {}
        return [kinds.get(p) for p in abspaths]

    def isdir(self, path):
        try:
            return self.lstat(path).is_dir
        except RemoteError as exc:
            if exc.errno in (errno.ENOENT, errno.ENOTDIR):
                return False
            raise

    def isfile(self, path):
        try:
            return self.lstat(path).is_file
        except RemoteError as exc:
            if exc.errno in (errno.ENOENT, errno.ENOTDIR):
                return False
            raise

    def getsize(self, path):
        return self.lstat(path).size

    def getmtime(self, path):
        return self.lstat(path).mtime

    def listdir(self, path="."):
        spath = self._check(self.abspath(path), allow_root=True)
        resp = self.chan.call("list", path=spath)
        return [Entry(e, parent=spath) for e in resp.data.get("entries") or []]

    def names(self, path="."):
        return [e.name for e in self.listdir(path)]

    def walk(self, path="."):
        """递归遍历，产出 ``(dirpath, dirs, files)``，路径都是沙箱绝对路径。"""

        spath = self._check(self.abspath(path), allow_root=True)
        stack = [spath]
        while stack:
            cur = stack.pop()
            dirs, files = [], []
            for ent in self.listdir(cur):
                (dirs if ent.is_dir else files).append(ent.name)
            yield cur, dirs, files
            for d in dirs:
                stack.append(posixpath.join(cur, d))

    # ------------------------------------------------------------------
    # 读
    # ------------------------------------------------------------------
    def read(self, path, offset=0, length=None):
        """读文件内容。

        下行方向单帧只能承载约 96KB，所以这里按片拉取、累加返回——
        对调用方是"一次读完"的语义。
        """

        spath = self._check(self.abspath(path))
        out = bytearray()
        pos = int(offset or 0)
        remaining = None if length is None else int(length)
        while remaining is None or remaining > 0:
            want = READ_SLICE if remaining is None else min(READ_SLICE, remaining)
            resp = self.chan.request("read", path=spath, offset=pos, length=want)
            resp.wait(self.chan.request_timeout + 30)
            chunk = resp.bytes()
            if not chunk:
                break
            out += chunk
            pos += len(chunk)
            if remaining is not None:
                remaining -= len(chunk)
            if len(chunk) < want:
                break
        return bytes(out)

    def read_into(self, path, write_cb, offset=0):
        """流式读：每块调用 ``write_cb(bytes)``，返回总字节数。"""

        spath = self._check(self.abspath(path))
        pos = int(offset or 0)
        total = 0
        while True:
            resp = self.chan.request("read", path=spath, offset=pos,
                                     length=READ_SLICE)
            resp.wait(self.chan.request_timeout + 30)
            chunk = resp.bytes()
            if not chunk:
                break
            write_cb(chunk)
            total += len(chunk)
            pos += len(chunk)
            if len(chunk) < READ_SLICE:
                break
        return total

    def sha256(self, path):
        spath = self._check(self.abspath(path))
        return self.chan.call("sha", path=spath).data.get("sha")

    # ------------------------------------------------------------------
    # 写
    # ------------------------------------------------------------------
    def write(self, path, data, mode=None):
        """写文件；小文件内联，大文件走分块上传流。"""

        if isinstance(data, str):
            data = data.encode("utf-8")
        spath = self._check(self.abspath(path))
        if len(data) <= self.inline_limit:
            resp = self.chan.call(
                "write_b64", path=spath,
                data=base64.b64encode(data).decode("ascii"),
                mode=mode)
            return resp.data.get("size", len(data))
        return self.write_stream(spath, [data], mode=mode)

    def write_stream(self, path, chunks, mode=None):
        """分块写入（内存占用与总大小无关）。

        实测：同一连接上连续多个 DATA 帧会导致 worker 侧输出通道卡死，
        所以 ≤ ``MAX_PAYLOAD`` 的内容用**单帧**发送；更大的走
        ``sandbox cp``（先落本地临时文件，不把整个文件读进内存）。
        """

        spath = self._check(self.abspath(path))
        buf = bytearray()
        tmp = None
        total = 0
        try:
            for chunk in chunks:
                if not chunk:
                    continue
                if tmp is not None:
                    tmp.write(chunk)
                    total += len(chunk)
                    continue
                buf += chunk
                total += len(chunk)
                if len(buf) > MAX_PAYLOAD:
                    # 超过单帧上限：转存本地临时文件，后续走 cp
                    tmp = tempfile.NamedTemporaryFile(prefix="sandbox_fs-up-",
                                                      delete=False)
                    tmp.write(buf)
                    buf = bytearray()

            if tmp is not None:
                tmp.flush()
                tmp.close()
                if mode is not None:
                    try:
                        os.chmod(tmp.name, mode)
                    except OSError:
                        pass
                self.tool.upload_many([(tmp.name, spath)])
                if mode is not None:
                    try:
                        self.chmod(spath, mode)
                    except RemoteError:
                        pass
                return total

            data = bytes(buf)
            if not data and self.exists(spath):
                self.truncate(spath, 0)
                return 0
            begin = self.chan.request("upload_begin", path=spath, mode=mode)
            begin.wait(self.chan.request_timeout)
            self.chan.send_data(begin.rid, data, eof=True)
            resp = self.chan.wait_rid(begin.rid, self.chan.request_timeout + 120)
            return resp.data.get("size", len(data))
        finally:
            if tmp is not None:
                try:
                    os.unlink(tmp.name)
                except OSError:
                    pass

    def write_from_local(self, path, local_path):
        """把本地文件写进沙箱；默认按大小自动选内联/流/CLI。"""

        size = os.path.getsize(local_path)
        if size > 4 * 1024 * 1024:
            self.tool.upload_many([(local_path, self._check(self.abspath(path)))])
            return size
        if size <= self.inline_limit:
            with open(local_path, "rb") as f:
                return self.write(path, f.read())
        with open(local_path, "rb") as f:
            return self.write_stream(path, iter(lambda: f.read(CHUNK), b""))

    def append(self, path, data):
        """追加：分块读旧内容 + 新内容，整体重写（不整段入内存）。"""

        if isinstance(data, str):
            data = data.encode("utf-8")
        spath = self._check(self.abspath(path))

        def gen():
            pos = 0
            while True:
                chunk = self.read(spath, offset=pos, length=READ_SLICE)
                if not chunk:
                    break
                pos += len(chunk)
                yield chunk
            yield data

        return self.write_stream(spath, gen())

    def truncate(self, path, size=0):
        spath = self._check(self.abspath(path))
        self.chan.call("truncate", path=spath, size=int(size))
        return size

    # ------------------------------------------------------------------
    # 目录
    # ------------------------------------------------------------------
    def mkdir(self, path, mode=0o777):
        spath = self._check(self.abspath(path))
        self.chan.call("mkdir", dirs=[[spath, mode]])
        return spath

    def makedirs(self, path, mode=0o777):
        """等价于 ``mkdir -p``：逐级创建。"""

        spath = self._check(self.abspath(path))
        missing = []
        cur = spath
        while cur.startswith(self.root) and cur != self.root:
            if self.isdir(cur):
                break
            missing.append(cur)
            cur = posixpath.dirname(cur)
        for d in reversed(missing):
            try:
                self.mkdir(d, mode)
            except RemoteError as exc:
                if exc.errno != errno.EEXIST:
                    raise
        return spath

    def rmdir(self, path):
        spath = self._check(self.abspath(path))
        self.chan.call("rmdir", paths=[spath])

    def remove(self, path):
        spath = self._check(self.abspath(path))
        self.chan.call("unlink", paths=[spath])

    def unlink(self, path):
        self.remove(path)

    def rmtree(self, path):
        spath = self._check(self.abspath(path))
        self.chan.call("rmtree", path=spath, timeout=600)

    def rename(self, src, dst):
        sspath = self._check(self.abspath(src))
        dspath = self._check(self.abspath(dst))
        self.chan.call("rename", src=sspath, dst=dspath)
        return dspath

    move = rename

    def copy(self, src, dst):
        """沙箱内复制（服务端完成，不经过本机）。"""

        sspath = self._check(self.abspath(src))
        dspath = self._check(self.abspath(dst))
        self.chan.call("copy_tree", src=sspath, dst=dspath, timeout=900)
        return dspath

    def symlink(self, target, linkpath):
        lpath = self._check(self.abspath(linkpath))
        self.chan.call("symlink", target=target, path=lpath)
        return lpath

    def link(self, src, dst):
        sspath = self._check(self.abspath(src))
        dspath = self._check(self.abspath(dst))
        self.chan.call("link", src=sspath, dst=dspath)
        return dspath

    def chmod(self, path, mode):
        spath = self._check(self.abspath(path))
        self.chan.call("chmod", path=spath, mode=int(mode))

    def utime(self, path, atime=None, mtime=None):
        spath = self._check(self.abspath(path))
        self.chan.call("utime", path=spath, atime=atime, mtime=mtime)

    # ------------------------------------------------------------------
    # 本地 ← 沙箱
    # ------------------------------------------------------------------
    def download_to(self, path, local_path):
        spath = self._check(self.abspath(path))
        size = self.getsize(spath)
        if size > 4 * 1024 * 1024:
            self.tool.download_many([(spath, local_path)])
            return size
        with open(local_path, "wb") as f:
            return self.read_into(spath, f.write)

    # ------------------------------------------------------------------
    # 命令
    # ------------------------------------------------------------------
    def exec(self, cmd, cwd=None, env=None, timeout=None):
        """在沙箱里跑一条命令（内置 pty 之外的第二选择）。"""

        return self.chan.exec(cmd, cwd=self._check(self.abspath(cwd), True) if cwd else self.cwd,
                              env=env, timeout=timeout)

    def ping(self):
        return self.chan.call("ping", root=self.root).data

    # ------------------------------------------------------------------
    # 便利：本地临时文件配合 CLI
    # ------------------------------------------------------------------
    def push(self, local_path, remote_path):
        """本地文件或目录 → 沙箱（目录走 sandbox cp）。"""

        rpath = self._check(self.abspath(remote_path), allow_root=True)
        if os.path.isdir(local_path):
            self.tool.upload_many([(local_path, rpath)])
            return rpath
        return self.write_from_local(rpath, local_path)

    def pull(self, remote_path, local_path):
        """沙箱文件或目录 → 本地。"""

        spath = self._check(self.abspath(remote_path), allow_root=True)
        if self.isdir(spath):
            self.tool.download_many([(spath, local_path)])
            return local_path
        return self.download_to(spath, local_path)
