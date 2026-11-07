#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sandbox_fs 命令行入口。

    python -m sandbox_fs                           # 无参数：FTP 后台 + SSH 前台
    python -m sandbox_fs ftp                      # 只启动 FTP（0.0.0.0:2121，匿名全权限）
    python -m sandbox_fs ls /                     # 列沙箱目录
    python -m sandbox_fs cat notes.txt            # 看文件（可 -o 输出到本地）
    python -m sandbox_fs put ./local.txt /path/   # 上传
    python -m sandbox_fs get /path/file ./local   # 下载
    python -m sandbox_fs run "ls -la"             # 在沙箱里跑命令
    python -m sandbox_fs stat /path               # 查看元信息
"""

from __future__ import absolute_import, unicode_literals

import argparse
import sys

from . import __version__, connect
from .errors import SandboxFsError


def _build_parser():
    ap = argparse.ArgumentParser(
        prog="sandbox_fs", description="Remote Sandbox 文件管理（FTP + 命令行）")
    ap.add_argument("--sandbox", default="sandbox", help="沙箱名（默认 sandbox）")
    ap.add_argument("--root", default="auto",
                    help="挂载根目录；auto=连接前自动探测沙箱真实初始路径")
    ap.add_argument("--host", default=None, help="FTP 监听地址（默认 0.0.0.0）")
    ap.add_argument("--port", type=int, default=None, help="FTP 监听端口（默认 2121）")
    ap.add_argument("-v", "--verbose", action="store_true", help="打印调试日志")
    ap.add_argument("--version", action="version", version="sandbox_fs " + __version__)

    sub = ap.add_subparsers(dest="cmd")

    p = sub.add_parser("ftp", help="只启动 FTP（无参数运行为：FTP 后台 + SSH 前台）")
    p.add_argument("--host", default=argparse.SUPPRESS)
    p.add_argument("--port", type=int, default=argparse.SUPPRESS)
    p.add_argument("--user", default=None, help="具名账号（不给则匿名，无密码）")
    p.add_argument("--password", default=None)
    p.add_argument("--no-anonymous", action="store_true", help="关闭匿名登录")
    p.add_argument("--foreground", action="store_true",
                   help="FTP 阻塞运行在当前终端（默认转后台，便于脚本/服务使用）")

    p = sub.add_parser("ls", help="列目录")
    p.add_argument("path", nargs="?", default=".")
    p.add_argument("-l", "--long", action="store_true")

    p = sub.add_parser("cat", help="输出文件内容")
    p.add_argument("path")
    p.add_argument("-o", "--out", default=None, help="写到本地文件")

    p = sub.add_parser("put", help="上传（本地 → 沙箱）")
    p.add_argument("local")
    p.add_argument("remote")

    p = sub.add_parser("get", help="下载（沙箱 → 本地）")
    p.add_argument("remote")
    p.add_argument("local")

    p = sub.add_parser("rm", help="删除文件或目录")
    p.add_argument("path")
    p.add_argument("-r", "--recursive", action="store_true")

    p = sub.add_parser("mkdir", help="创建目录（自动建父级）")
    p.add_argument("path")

    p = sub.add_parser("mv", help="移动/改名")
    p.add_argument("src")
    p.add_argument("dst")

    p = sub.add_parser("stat", help="查看元信息")
    p.add_argument("path")

    p = sub.add_parser("run", help="在沙箱里执行命令")
    p.add_argument("command")

    p = sub.add_parser("ping", help="显示连接信息")

    p = sub.add_parser("doctor", help="诊断：检查 node / CLI / 沙箱 / 连接")
    return ap


def _make_logger(verbose):
    if not verbose:
        return lambda *a: None

    def log(msg):
        sys.stderr.write("[sandbox_fs] %s\n" % (msg,))

    return log


def _resolve_root(args, log):
    """``--root auto``：用 `sandbox exec <name> pwd` 探测沙箱真实初始路径。

    探测失败不影响后续流程（连接失败时的提示里会给出排查方向）。
    """

    if args.root != "auto":
        return args.root
    from .tool import ToolClient

    try:
        tool = ToolClient(sandbox=args.sandbox, cwd="/", root="/")
        root = tool.exec_command("pwd")
        if root.startswith("/"):
            args.root = root
            return root
    except Exception as exc:  # noqa: BLE001
        log("root 探测失败：%s" % exc)
    # 探测失败：保持 auto，交给 connect()/FileSystem 再试一次
    return args.root


def main(argv=None):
    ap = _build_parser()
    args = ap.parse_args(argv)
    log = _make_logger(args.verbose)

    # host/port 可能在根级（无参模式）或 ftp 子命令传入，统一回填默认
    args.host = args.host or "0.0.0.0"
    args.port = args.port or 2121

    bare = args.cmd is None

    _resolve_root(args, log)

    if bare:
        # 无参数：FTP 后台（匿名，无账号密码）+ 前台进入沙箱交互会话
        from .serve import run_ftp

        return run_ftp(args.sandbox, args.root, args.host, args.port,
                       with_ssh=True,
                       log=lambda m: sys.stderr.write(m + "\n"))

    # 显式 `ftp` 子命令：只启动 FTP，接收 FTP 参数
    if args.cmd == "ftp":
        from .serve import run_ftp

        if getattr(args, "foreground", False):
            # 前台模式需要在本地先连上沙箱
            try:
                fs = connect(args.sandbox, root=args.root, log=log)
            except SandboxFsError as exc:
                sys.stderr.write("连接失败：%s\n" % exc)
                return 2
            try:
                from .ftp import serve_ftp

                serve_ftp(fs, host=args.host, port=args.port,
                          anonymous=not args.no_anonymous,
                          blocking=True,
                          log=lambda m: sys.stderr.write(m + "\n"))
                return 0
            finally:
                try:
                    fs.close()
                except Exception:  # noqa: BLE001
                    pass

        return run_ftp(args.sandbox, args.root, args.host, args.port,
                       with_ssh=False, user=args.user, password=args.password,
                       anonymous=not args.no_anonymous,
                       log=lambda m: sys.stderr.write(m + "\n"))

    try:
        fs = connect(args.sandbox, root=args.root, log=log)
    except SandboxFsError as exc:
        sys.stderr.write("连接失败：%s\n" % exc)
        return 2

    try:
        return _dispatch(fs, args, log)
    except SandboxFsError as exc:
        sys.stderr.write("错误：%s\n" % exc)
        return 1
    finally:
        try:
            fs.close()
        except Exception:  # noqa: BLE001
            pass


def _dispatch(fs, args, log):
    cmd = args.cmd

    if cmd == "doctor":
        return _doctor(fs, args)

    if cmd == "ping":
        info = fs.ping()
        print("沙箱: %s" % fs.chan.sandbox)
        print("根目录: %s" % fs.root)
        print("worker pid: %s" % info.get("pid"))
        print("主机: %s" % info.get("host"))
        print("python: %s" % info.get("python"))
        print("可用空间: %.1f GB / %.1f GB"
              % (info.get("free", 0) / 1e9, info.get("total", 0) / 1e9))
        return 0

    if cmd == "ls":
        entries = sorted(fs.listdir(args.path), key=lambda e: (not e.is_dir, e.name))
        if not args.long:
            for e in entries:
                mark = "/" if e.is_dir else ("@" if e.is_link else "")
                print(e.name + mark)
            return 0
        for e in entries:
            print("%-10s %10d %s %s"
                  % (e.type, e.size, _fmt_time(e.mtime), e.name
                     + ("/" if e.is_dir else (" -> " + e.target if e.is_link else ""))))
        return 0

    if cmd == "cat":
        data = fs.read(args.path)
        if args.out:
            with open(args.out, "wb") as f:
                f.write(data)
            sys.stderr.write("已写入 %s（%d 字节）\n" % (args.out, len(data)))
            return 0
        buf = getattr(sys.stdout, "buffer", None)
        if buf is not None:
            buf.write(data)
            buf.flush()
        else:
            sys.stdout.write(data.decode("utf-8", "replace"))
            sys.stdout.flush()
        return 0

    if cmd == "put":
        result = fs.push(args.local, args.remote)
        if isinstance(result, int):
            print("已上传 %s -> %s（%d 字节）"
                  % (args.local, args.remote, result))
        else:
            print("已上传 %s -> %s" % (args.local, result))
        return 0

    if cmd == "get":
        fs.pull(args.remote, args.local)
        print("已下载 %s -> %s" % (args.remote, args.local))
        return 0

    if cmd == "rm":
        if fs.isdir(args.path):
            if not args.recursive:
                sys.stderr.write("%s 是目录，需要 -r\n" % args.path)
                return 1
            fs.rmtree(args.path)
        else:
            fs.remove(args.path)
        return 0

    if cmd == "mkdir":
        fs.makedirs(args.path)
        print("已创建 %s" % fs.abspath(args.path))
        return 0

    if cmd == "mv":
        print(fs.rename(args.src, args.dst))
        return 0

    if cmd == "stat":
        e = fs.stat(args.path)
        print("路径: %s" % e.path)
        print("类型: %s" % e.type)
        print("大小: %d" % e.size)
        print("权限: %04o" % (e.mode & 0o7777))
        print("uid/gid: %d/%d" % (e.uid, e.gid))
        print("修改: %s" % _fmt_time(e.mtime))
        if e.is_link:
            print("指向: %s" % e.target)
        return 0

    if cmd == "run":
        result = fs.exec(args.command)
        sys.stdout.write(result.output)
        return result.code

    ap = _build_parser()
    ap.print_help()
    return 2


def _doctor(fs, args):
    """逐项检查环境，输出可操作的结论。"""

    import shutil
    import subprocess

    ok = True

    def check(label, fn):
        nonlocal ok
        try:
            value = fn()
            print("[OK]   %-16s %s" % (label, value))
            return value
        except Exception as exc:  # noqa: BLE001
            ok = False
            print("[FAIL] %-16s %s" % (label, exc))
            return None

    node = check("node", lambda: shutil.which("node") or _raise("未找到 node"))
    cli = check("sandbox CLI", lambda: shutil.which("sandbox")
                or shutil.which("vercel-sandbox") or _raise("未找到 sandbox 命令"))

    def ls():
        out = subprocess.run([cli, "ls"], capture_output=True, timeout=120)
        text = out.stdout.decode("utf-8", "replace")
        for line in text.splitlines():
            parts = line.split()
            if parts and parts[0] == args.sandbox:
                state = " ".join(parts[1:8])
                return "%s -> %s" % (args.sandbox, state)
        raise RuntimeError("沙箱列表里没有 %s" % args.sandbox)

    check("沙箱状态", ls)
    check("连接与握手", lambda: "worker pid=%s, 往返 %.0fms"
          % (fs.chan.worker_pid, _roundtrip(fs)))
    check("FTP 依赖", lambda: __import__("pyftpdlib").__ver__)

    print()
    if ok:
        print("一切正常，可以运行：python3 -m sandbox_fs ftp")
    else:
        print("存在问题，请按上面的 FAIL 项处理（常见原因见 README「注意事项」）")
        if node is None:
            print("  * 未找到 node：请安装 Node.js（sandbox_fs 用它与远程沙箱通道通信）")
        if cli is None:
            print("  * 未找到 sandbox：npm i -g sandbox")
    return 0 if ok else 1


def _raise(msg):
    raise RuntimeError(msg)


def _roundtrip(fs):
    import time as _t

    t0 = _t.time()
    fs.ping()
    return (_t.time() - t0) * 1000


def _fmt_time(ts):
    import time

    try:
        return time.strftime("%Y-%m-%d %H:%M", time.localtime(ts))
    except (ValueError, OSError):
        return "-"


if __name__ == "__main__":
    sys.exit(main())
