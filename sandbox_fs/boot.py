# -*- coding: utf-8 -*-
"""
沙箱内引导（体积敏感，只保留必要逻辑）。

被宿主以 ``python3 -c "<loader>"`` 拉起后：

1. ``tty.setraw(0)`` —— 沙箱的 stdio 是 pty，不设 raw 就读不到任何字节
   （raw 之前收到的字节会被 TTY 行缓冲吞掉）；
2. 发一帧 ``ready`` 告诉宿主``我进入读帧循环了``；
3. 读第一条 REQ：``get`` → 回 ``want: put``；宿主随后送 ``put``（worker 源码）——
   这两步谁先到都可以，循环里按需处理；
4. ``exec`` 这份源码，交给 worker 的请求循环（之后一直复用这条连接）。

注意：沙箱的输入通道依赖"进程持续有输出"，所以这里在等待期间保持 1 秒一次
的心跳；worker 接棒后由它自己的应答提供输出。
"""

from __future__ import absolute_import, unicode_literals

import json
import os
import select
import struct
import sys
import threading
import time
import types

MAGIC = b"SBOX"
HDR = struct.Struct(">4sBBI")
T_REQ, T_RES, T_DATA, T_EVENT = 0x01, 0x02, 0x03, 0x04
F_END = 0x01

READY_TAG = "sandbox_fs-ready"
HEARTBEAT = 0.15
MAX_HEADER = 1 << 20
MAX_PAYLOAD = 1 << 20


# ---------------------------------------------------------------------------
# 底层 IO
# ---------------------------------------------------------------------------
def make_raw(fd=0):
    try:
        import fcntl

        # 清掉 O_NONBLOCK：read 必须是阻塞语义，否则会"读到 0 字节就返回"
        try:
            flags = fcntl.fcntl(fd, fcntl.F_GETFL)
            fcntl.fcntl(fd, fcntl.F_SETFL, flags & ~os.O_NONBLOCK)
        except OSError:
            pass
    except ImportError:
        pass
    try:
        import termios
        import tty

        tty.setraw(fd, termios.TCSANOW)
        # 关闭回显（echo 会让输入字节被 pty 再写回来，大流量时直接挤爆缓冲）
        for target in (fd, 0, 1, 2):
            try:
                attrs = termios.tcgetattr(target)
                attrs[3] &= ~(termios.ECHO | termios.ECHONL | termios.ICANON)
                attrs[1] &= ~termios.OPOST
                attrs[0] &= ~(termios.ICRNL | termios.IXON | termios.IXOFF)
                attrs[2] &= ~(termios.CSIZE | termios.CS8) & 0xFFFFFFFF
                attrs[2] |= termios.CS8
                termios.tcsetattr(target, termios.TCSANOW, attrs)
            except (OSError, ValueError):
                pass
    except (ImportError, OSError, ValueError):
        pass


def write_all(fd, blob):
    view = memoryview(blob)
    while view:
        try:
            n = os.write(fd, view)
        except BlockingIOError:
            try:
                select.select([], [fd], [], 1.0)
            except (OSError, ValueError):
                continue
            continue
        view = view[n:]


def read_frame(fd, buf):
    """从 fd 读一帧；返回 (type, flags, data, payload)，EOF 返回 None。

    坏帧（JSON 解析失败、len 越界）视为脏数据跳过并继续对齐，
    不会中断读循环——只有 EOF（通道关闭）才返回 None。
    """

    def need(n):
        nonlocal buf
        while len(buf) < n:
            # 注意：必须是阻塞读（make_raw 已清 O_NONBLOCK）。阻塞语义下
            # os.read 返回 b"" 只可能是 EOF——直接结束，否则会陷入
            # "select 永远立即可读" 的忙循环（pty 上 EOF 走 EIO 分支，
            # 管道/套接字上返回 b""）。
            try:
                chunk = os.read(fd, 65536)
            except InterruptedError:
                continue
            except OSError:
                return None
            if chunk:
                buf += chunk
                continue
            return None
        out = bytes(buf[:n])
        del buf[:n]
        return out

    # 对齐帧头：脏数据只丢 1 个字节并保留其余，用循环而不是递归，
    # 否则一次读到几十 KB 垃圾数据就会递归爆栈。
    while True:
        head = need(HDR.size)
        if head is None:
            return None
        magic, type_, flags, hlen = HDR.unpack(head)
        if magic != MAGIC or not 0 <= hlen <= MAX_HEADER:
            buf[:0] = head[1:]
            continue
        raw = need(hlen)
        if raw is None:
            return None
        try:
            data = json.loads(raw.decode("utf-8"))
        except ValueError:
            continue          # 坏头：跳过整帧继续对齐
        plen = int(data.get("len", 0) or 0)
        if plen < 0 or plen > MAX_PAYLOAD:
            continue          # 载荷长度非法：跳过整帧
        payload = b""
        if plen:
            payload = need(plen)
            if payload is None:
                return None
        return type_, flags, data, payload


def frame_bytes(type_, data, payload=b"", flags=F_END):
    data = dict(data)
    data["len"] = len(payload)
    head = json.dumps(data, separators=(",", ":")).encode("utf-8")
    return HDR.pack(MAGIC, type_, flags, len(head)) + head + payload


def write_frame(fd, type_, data, payload=b"", flags=F_END):
    write_all(fd, frame_bytes(type_, data, payload, flags))


# ---------------------------------------------------------------------------
# 引导
# ---------------------------------------------------------------------------
class _Beacon(object):
    """等待期间定期发心跳，保持沙箱输入通道活跃。"""

    def __init__(self, fd):
        self.fd = fd
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, name="sandbox_fs-beacon",
                                       daemon=True)
        self.thread.start()

    def _run(self):
        while not self.stop.wait(HEARTBEAT):
            try:
                write_frame(self.fd, T_EVENT, {"ev": "sandbox_fs-beat",
                                               "pid": os.getpid()})
            except OSError:
                return

    def close(self):
        self.stop.set()


def _self_module():
    """返回当前模块对象（被 exec 创建时可能不在 sys.modules 里）。"""

    existing = sys.modules.get("sandbox_fs_boot")
    if existing is not None and getattr(existing, "read_frame", None):
        return existing
    mod = types.ModuleType("sandbox_fs_boot")
    mod.__dict__.update(globals())
    return mod


def main(root=None):
    root = root or (sys.argv[1] if len(sys.argv) > 1 else os.getcwd())
    make_raw(0)

    buf = bytearray()
    beacon = _Beacon(1)
    try:
        write_frame(1, T_EVENT, {"ev": READY_TAG, "pid": os.getpid(),
                                 "root": root, "tty": os.isatty(0)})
        wanted = False
        payload = None
        deadline = time.time() + 300
        while time.time() < deadline and payload is None:
            fr = read_frame(0, buf)
            if fr is None:
                break
            _t, _f, data, body = fr
            op = data.get("op")
            if op == "get":
                wanted = True
                write_frame(1, T_RES, {"id": data.get("id"), "ok": 1,
                                       "want": "put"})
            elif op == "put":
                payload = body
            elif op == "hello" and wanted:
                # 宿主可能先把 hello 排到前面：先应答，再继续等 put
                write_frame(1, T_RES, {"id": data.get("id"), "ok": 0,
                                       "errno": 11, "msg": "still booting"})
    finally:
        beacon.close()

    if payload is None:
        return 3
    mod = types.ModuleType("sandbox_fs_worker")
    mod.__file__ = "<sandbox_fs-worker>"
    sys.modules["sandbox_fs_worker"] = mod
    # worker 会通过 sys.modules["sandbox_fs_boot"] 反查 read_frame
    sys.modules["sandbox_fs_boot"] = _self_module()
    try:
        exec(compile(payload, "worker.py", "exec"), mod.__dict__)
    except BaseException:
        import traceback

        traceback.print_exc()
        write_frame(1, T_RES, {"id": 0, "ok": 0, "errno": 5,
                               "msg": traceback.format_exc(limit=6)})
        return 4
    return mod.run_fed(root, 0, 1, buf)
