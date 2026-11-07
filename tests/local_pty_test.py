#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地快速测试台：用真实 pty 模拟沙箱的 stdio，验证 worker 状态机。

    python3 tests/local_pty_test.py            # 全部用例
    python3 tests/local_pty_test.py -k upload  # 只跑来名字匹配的用例

它的价值：沙箱端一轮要 ~10 秒，本地一轮只要几十毫秒。
任何协议/worker 改动都应先在这里跑绿。
"""

from __future__ import absolute_import, unicode_literals

import os
import pty
import select
import struct
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)

from sandbox_fs import proto  # noqa: E402

MAGIC = b"SBOX"
HDR = struct.Struct(">4sBBI")


class PtySandbox(object):
    """起一个 python 进程并把它的 stdio 接到 pty master 上。"""

    def __init__(self, script_args=None, cwd=None):
        self.master, slave = pty.openpty()
        env = dict(os.environ, PYTHONPATH=HERE, PYTHONUNBUFFERED="1")
        args = [sys.executable, "-c", self._loader()] + list(script_args or [])
        self.proc = subprocess.Popen(args, stdin=slave, stdout=slave, stderr=slave,
                                     env=env, cwd=cwd or HERE, preexec_fn=os.setsid)
        os.close(slave)
        self.buf = b""
        self.noise = b""  # 帧之外的内容（调试输出 / traceback）

    @staticmethod
    def _loader():
        # 等价于沙箱里的 python3 -c <loader>
        return (
            "import sys,types,base64\n"
            "sys.path.insert(0, %r)\n"
            "from sandbox_fs import boot\n"
            "sys.exit(boot.main())\n"
        ) % HERE

    def write(self, data):
        view = memoryview(data)
        while view:
            n = os.write(self.master, view)
            view = view[n:]

    def read_frame(self, timeout=10.0):
        deadline = time.time() + timeout
        while True:
            frame = self._parse()
            if frame is not None:
                return frame
            if self.buf and not MAGIC.startswith(bytes(self.buf[:4])):
                # 沙箱侧调试/异常输出会直接吐在 pty 上，跳过不属于帧的字节
                idx = self.buf.find(MAGIC)
                self.noise += bytes(self.buf if idx < 0 else self.buf[:idx])
                del self.buf[:len(self.buf) if idx < 0 else idx]
            left = deadline - time.time()
            if left <= 0:
                raise TimeoutError("no frame (buffered %d bytes) noise=%r"
                                   % (len(self.buf), self.noise[-300:]))
            r, _, _ = select.select([self.master], [], [], min(left, 0.5))
            if r:
                try:
                    self.buf += os.read(self.master, 1 << 20)
                except OSError:
                    raise TimeoutError("pty closed")

    def _parse(self):
        if self.buf and not MAGIC.startswith(bytes(self.buf[:4])):
            idx = self.buf.find(MAGIC)
            self.noise += bytes(self.buf if idx < 0 else self.buf[:idx])
            del self.buf[:len(self.buf) if idx < 0 else idx]
        if len(self.buf) < HDR.size:
            return None
        magic, type_, flags, hlen = HDR.unpack_from(self.buf, 0)
        if magic != MAGIC:
            raise ValueError("bad magic: %r" % self.buf[:16])
        if len(self.buf) < HDR.size + hlen:
            return None
        import json

        data = json.loads(self.buf[HDR.size:HDR.size + hlen].decode("utf-8"))
        total = HDR.size + hlen + int(data.get("len", 0) or 0)
        if len(self.buf) < total:
            return None
        payload = self.buf[HDR.size + hlen:total]
        self.buf = self.buf[total:]
        return proto.Header(type_, flags, data), payload

    def send(self, op, payload=b"", **data):
        self.write(proto.encode_frame(proto.T_REQ, dict(data, op=op), payload))

    def close(self):
        try:
            self.proc.kill()
        except OSError:
            pass
        try:
            os.close(self.master)
        except OSError:
            pass


class Client(object):
    """在 pty 之上跑一遍 get/put/hello，然后发请求。"""

    def __init__(self, root):
        self.root = root
        self.sb = PtySandbox()
        self.seq = 0
        self.pending = {}

    def boot(self):
        sb = self.sb
        # 先等 ready（raw 之前的字节会被 pty 吞掉）
        while True:
            hdr, _ = sb.read_frame()
            if hdr.type == proto.T_EVENT and hdr.data.get("ev") == "sandbox_fs-ready":
                break
        self.seq += 1
        sb.send("get", id=self.seq, root=self.root)
        while True:
            hdr, _payload = sb.read_frame()
            if hdr.data.get("id") == self.seq and hdr.end:
                assert hdr.data.get("ok"), hdr.data
                break
        with open(os.path.join(HERE, "sandbox_fs", "worker.py"), "rb") as f:
            src = f.read()
        self.seq += 1
        frame = proto.encode_frame(
            proto.T_REQ,
            {"op": "put", "id": self.seq, "root": self.root},
            src)
        sb.write(frame)
        # put 没有单独应答：下一帧 hello 即代表 worker 已就位
        return self.call("hello", proto=proto.VERSION)

    def call(self, op, timeout=15.0, payload=b"", **data):
        self.seq += 1
        rid = self.seq
        self.sb.send(op, payload=payload, id=rid, **data)
        body = bytearray()
        while True:
            hdr, chunk = self.sb.read_frame(timeout)
            if hdr.data.get("id") != rid:
                continue  # 别的请求的帧（交错）
            body += chunk
            if hdr.end:
                return hdr.data, bytes(body)

    def close(self):
        self.sb.close()


# ---------------------------------------------------------------------------
# 用例
# ---------------------------------------------------------------------------
CASES = []


def case(fn):
    CASES.append(fn)
    return fn


@case
def test_ping(c):
    data, _ = c.call("ping", root=c.root)
    assert data["ok"], data
    assert data["proto"] == proto.VERSION


@case
def test_write_read_roundtrip(c):
    path = os.path.join(c.root, "hello.txt")
    data, _ = c.call("write_b64", path=path, data="5L2g5aW9", mode=0o644)
    assert data["ok"], data
    data, body = c.call("read", path=path)
    # 流式响应：状态在最后一个 DATA 帧上，数据在 body 里
    assert body == "你好".encode("utf-8"), body


@case
def test_binary_upload(c):
    path = os.path.join(c.root, "blob.bin")
    payload = bytes(range(256)) * 1200  # 307200 字节，跨多帧
    data, _ = c.call("upload_begin", path=path, mode=0o600)
    assert data["ok"], data
    rid = data["id"]
    off = 0
    while off < len(payload):
        part = payload[off:off + proto.MAX_PAYLOAD]
        off += len(part)
        eof = off >= len(payload)
        c.sb.write(proto.encode_frame(proto.T_DATA, {"id": rid, "eof": eof},
                                      part, proto.F_END if eof else 0))
    while True:
        hdr, _ = c.sb.read_frame()
        if hdr.data.get("id") == rid and hdr.end:
            assert hdr.data.get("ok"), hdr.data
            break
    with open(path, "rb") as f:
        assert f.read() == payload


@case
def test_list_and_stat(c):
    d = os.path.join(c.root, "sub")
    os.makedirs(d, exist_ok=True)
    open(os.path.join(d, "a.txt"), "w").write("x")
    data, _ = c.call("mkdir", dirs=[[os.path.join(d, "inner"), 0o755]])
    assert data["ok"], data
    data, _ = c.call("list", path=d)
    assert data["ok"]
    names = sorted(e["name"] for e in data["entries"])
    assert names == ["a.txt", "inner"], names


@case
def test_rename_missing(c):
    data, _ = c.call("rename", src=os.path.join(c.root, "nope"),
                     dst=os.path.join(c.root, "m2"))
    assert not data["ok"] and data["errno"] == 2, data


@case
def test_missing_errno(c):
    data, _ = c.call("stat", path=os.path.join(c.root, "nope"))
    assert not data["ok"] and data["errno"] == 2, data


@case
def test_exec(c):
    data, body = c.call("exec", cmd="echo hi; echo err >&2; exit 3")
    assert data["code"] == 3, data
    assert b"hi" in body and b"err" in body


@case
def test_concurrent(c):
    """并发：一次性把 5 个请求全部写出，再收 5 个应答（帧可交错）。"""

    c.call("write_b64", path=os.path.join(c.root, "hello.txt"), data="aGk=")
    path = os.path.join(c.root, "hello.txt")
    ids = []
    for _ in range(5):
        c.seq += 1
        ids.append(c.seq)
        c.sb.send("sha", id=c.seq, path=path)
    seen = {}
    while len(seen) < len(ids):
        hdr, _ = c.sb.read_frame(10)
        rid = hdr.data.get("id")
        if rid in ids and hdr.end:
            seen[rid] = hdr.data
    assert len(seen) == 5, seen
    assert all(d.get("ok") for d in seen.values()), seen


@case
def test_exec_timeout(c):
    """静默命令也必须被 timeout 兜底（看门狗 kill → 管道 EOF 唤醒读循环）。"""

    rid = c.seq + 1
    c.seq = rid
    c.sb.send("exec", id=rid, cmd="sleep 60", timeout=2)
    t0 = time.time()
    while True:
        hdr, body = c.sb.read_frame(30)
        if hdr.data.get("id") == rid and hdr.end:
            break
        assert time.time() - t0 < 30, "exec 超时未生效"
    elapsed = time.time() - t0
    assert hdr.data.get("timed_out"), hdr.data
    assert elapsed < 20, "exec 应在超时后立即返回，实际 %.1fs" % elapsed


@case
def test_exists_any_error_is_missing(c):
    """exists 语义与 os.path.exists 一致：ENOTDIR/EACCES 等也算不存在。"""

    f = os.path.join(c.root, "f")
    open(f, "w").write("x")
    bad = f + "/sub"  # 文件下面再取路径 -> ENOTDIR（不是 ENOENT）
    data, _ = c.call("exists", paths=[bad, os.path.join(c.root, "nope")])
    kinds = data["kinds"]
    assert kinds[bad] is None, kinds
    assert kinds[os.path.join(c.root, "nope")] is None, kinds
    data, _ = c.call("exists", paths=[f])
    assert data["kinds"][f] == "file", data


@case
def test_decoder_resync(c):
    """宿主端解码器：失步噪声（含伪 SBOX + 随机 hlen）不得掐断连接。"""

    good = proto.encode_frame(proto.T_RES, {"op": "pong", "id": 1})
    noise = b"\x1b[?2004hlogin: root\nSBOX" + os.urandom(6)
    d = proto.Decoder()
    out = d.feed(good + noise + good)
    assert len(out) == 2, "应在失步后恢复，实际解析出 %d 帧" % len(out)
    assert out[0][0].data["id"] == 1 and out[1][0].data["id"] == 1


def main(argv):
    only = None
    if "-k" in argv:
        only = argv[argv.index("-k") + 1]
    failed = 0
    for fn in CASES:
        if only and only not in fn.__name__:
            continue
        root = tempfile.mkdtemp(prefix="sandbox_fs-pty-")
        c = Client(root)
        t0 = time.time()
        try:
            c.boot()
            fn(c)
            print("PASS %-28s %.0fms" % (fn.__name__, (time.time() - t0) * 1000))
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print("FAIL %-28s %r" % (fn.__name__, exc))
        finally:
            c.close()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
