# -*- coding: utf-8 -*-
"""
sandbox CLI 封装：沙箱生命周期、登录状态、批量文件拷贝。

只在这些"非高频"路径上才用 CLI：

* 判断/创建沙箱（``sandbox ls`` / ``sandbox create``）
* 大文件传输（``sandbox cp``，比 WS 通道更适合一次性大批量）
* 清理测试或注入的临时文件

高频的小操作一律走 :class:`sandbox_fs.transport.Channel`（常驻 worker）。
"""

from __future__ import absolute_import, unicode_literals

import errno
import os
import re
import shutil
import subprocess
import threading

from .errors import RemoteError, SandboxError, TransportError

CLI_NAMES = ("sandbox", "vercel-sandbox")


class ToolClient(object):
    """对 ``sandbox`` CLI 的薄封装（同步、带超时）。"""

    def __init__(self, sandbox="sandbox", cwd="/workspace", node=None,
                 root=None, log=None, cli=None, timeout=180):
        self.sandbox = sandbox
        self.cwd = cwd
        self.root = (root or cwd or "/").rstrip("/") or "/"
        self.node = node or self._find_node()
        self.log = log or (lambda *a: None)
        self.timeout = timeout
        self._cli = cli
        self._cli_lock = threading.Lock()

    # ------------------------------------------------------------------
    # 可执行文件定位
    # ------------------------------------------------------------------
    @staticmethod
    def _find_node():
        node = shutil.which("node")
        if node:
            return node
        home = os.path.expanduser("~")
        for base in (os.path.join(home, ".local/share/fnm/node-versions"),
                     os.path.join(home, ".nvm/versions/node"),
                     "/usr/local/share/nvm/versions/node"):
            if not os.path.isdir(base):
                continue
            for ver in sorted(os.listdir(base), reverse=True):
                for cand in (os.path.join(base, ver, "installation/bin/node"),
                             os.path.join(base, ver, "bin/node")):
                    if os.path.isfile(cand):
                        return cand
        raise TransportError("node not found (sandbox_fs 需要 Node 访问远程沙箱通道)")

    @property
    def cli(self):
        """返回可执行的 CLI 入口：优先 PATH 里的 sandbox，其次 node + mjs。"""

        with self._cli_lock:
            if self._cli:
                return self._cli
            for name in CLI_NAMES:
                path = shutil.which(name)
                if path:
                    self._cli = [path]
                    return self._cli
            # 退回到 node 直接跑入口脚本
            for cand in self._cli_candidates():
                self._cli = [self.node, cand]
                return self._cli
            raise SandboxError("sandbox CLI not found (npm i -g sandbox)")

    def _cli_candidates(self):
        roots = [
            os.environ.get("SANDBOXFS_CLI"),
        ]
        try:
            out = self._run_node_resolve()
            if out:
                roots.append(out)
        except (OSError, subprocess.SubprocessError):
            pass
        home = os.path.expanduser("~")
        for base in (os.path.join(home, ".local/share/fnm/node-versions"),
                     "/usr/local/lib/node_modules"):
            if os.path.isdir(base):
                for sub in sorted(os.listdir(base), reverse=True):
                    for p in ("installation/lib/node_modules/sandbox/bin/sandbox.mjs",
                              "lib/node_modules/sandbox/bin/sandbox.mjs"):
                        cand = os.path.join(base, sub, p)
                        if os.path.isfile(cand):
                            roots.append(cand)
        return [p for p in roots if p and os.path.isfile(p)]

    def _run_node_resolve(self):
        js = ("const{createRequire}=require('node:module');"
              "const r=createRequire(process.argv[2]+'/');"
              "try{process.stdout.write(r.resolve('sandbox/bin/sandbox.mjs'))}catch(e){}")
        proc = subprocess.run([self.node, "-e", js, os.path.dirname(self.node)],
                              capture_output=True, timeout=30)
        return proc.stdout.decode().strip() or None

    # ------------------------------------------------------------------
    # 执行
    # ------------------------------------------------------------------
    def run(self, *args, **kw):
        timeout = kw.pop("timeout", self.timeout)
        check = kw.pop("check", False)
        argv = list(self.cli) + [str(a) for a in args]
        self.log("cli: %s" % " ".join(argv))
        env = dict(os.environ)
        env.setdefault("NO_COLOR", "1")
        env.setdefault("NODE_NO_WARNINGS", "1")
        proc = subprocess.run(argv, capture_output=True, timeout=timeout, env=env, **kw)
        out = proc.stdout.decode("utf-8", "replace")
        err = proc.stderr.decode("utf-8", "replace")
        result = CliResult(proc.returncode, out, err, argv)
        if check and proc.returncode != 0:
            raise SandboxError(result.brief())
        return result

    # ------------------------------------------------------------------
    # 沙箱状态
    # ------------------------------------------------------------------
    def exec_command(self, command, timeout=90):
        """在沙箱里跑一条短命令（走 `sandbox exec`，非交互、无 TTY 也能用）。

        返回 stdout（已去除 CLI 打印的 ``$ cmd`` 回显行）。
        """

        r = self.run("exec", self.sandbox, command, timeout=timeout)
        if r.code != 0:
            raise SandboxError("sandbox exec failed: %s" % r.brief())
        out = r.out.strip()
        # CLI 会先回显 "$ <command>"，从结果里去掉首行
        lines = out.splitlines()
        if lines and lines[0].lstrip("$ ").strip() == command.strip():
            lines = lines[1:]
        return "\n".join(lines).strip()

    # ------------------------------------------------------------------
    # 沙箱状态
    # ------------------------------------------------------------------
    def list_sandboxes(self, timeout=90):
        """返回 ``{name: status}``。"""

        r = self.run("ls", timeout=timeout)
        table = {}
        for line in r.out.splitlines():
            m = re.match(r"^\s*([A-Za-z0-9_.-]+)\s+(running|stopped|creating|error)\b", line)
            if m:
                table[m.group(1)] = m.group(2)
        return table

    def ensure_sandbox(self, timeout=600):
        """确保沙箱可用。

        ``sandbox ls`` 不显示 timeout 已归零的沙箱，但 ``sandbox exec``
        会自动 resume 它 —— 所以顺序是：唤醒尝试 → 真不存在才 create
        （直接 create 同名会被 400 拒绝）。
        """

        if self.list_sandboxes().get(self.sandbox) == "running":
            return "running"
        wake = self.run("exec", self.sandbox, "true", timeout=timeout)
        if wake.code == 0:
            return "running"
        r = self.run("create", self.sandbox, timeout=timeout)
        if r.code != 0 and "already exists" not in (r.out + r.err).lower():
            raise SandboxError("create sandbox failed: %s" % r.brief())
        return "running"

    # ------------------------------------------------------------------
    # 文件传输（大文件通道）
    # ------------------------------------------------------------------
    def remote(self, path):
        return "%s:%s" % (self.sandbox, path)

    def upload_many(self, pairs, timeout=None):
        """pairs: [(本地路径, 沙箱绝对路径)]。

        ``sandbox cp`` 一次只接受一对 src/dst，所以这里逐对调用。
        """

        if not pairs:
            return
        per = timeout or max(120, 60 + 10 * len(pairs))
        for local, remote in pairs:
            r = self.run("cp", local, self.remote(remote), timeout=per)
            if r.code != 0:
                raise RemoteError("cp", remote, errno.EIO, r.brief())

    def download_many(self, pairs, timeout=None):
        if not pairs:
            return
        per = timeout or max(120, 60 + 10 * len(pairs))
        for remote, local in pairs:
            r = self.run("cp", self.remote(remote), local, timeout=per)
            if r.code != 0:
                raise RemoteError("cp", remote, errno.EIO, r.brief())


class CliResult(object):
    __slots__ = ("code", "out", "err", "argv")

    def __init__(self, code, out, err, argv):
        self.code = code
        self.out = out
        self.err = err
        self.argv = argv

    @property
    def ok(self):
        return self.code == 0

    def brief(self, lines=6):
        body = (self.err or self.out).strip().splitlines()
        return "\n".join(body[-lines:]) or ("exit %d" % self.code)

    def __repr__(self):
        return "<CliResult code=%s argv=%r>" % (self.code, self.argv)
