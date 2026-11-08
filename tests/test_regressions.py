#!/usr/bin/env python3
"""审计报告所列缺陷的回归测试（零第三方依赖，不连远程沙箱）。

    python3 tests/test_regressions.py

全部用内存构造，与 ``local_pty_test.py`` 的真实 pty 端到端用例互补。
改协议 / worker / 传输层之前先跑绿这两个文件。
"""

import errno
import json
import os
import socket
import sys
import tempfile
import threading
import unittest

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)

from sandbox_fs import boot, cli, proto, worker  # noqa: E402
from sandbox_fs.errors import RemoteError  # noqa: E402
from sandbox_fs.fs import FileSystem  # noqa: E402
from sandbox_fs.transport import Channel, Response  # noqa: E402


def raw_frame(type_, data, payload=b"", flags=proto.F_END):
    """手工造帧：不覆盖 data['len']，便于注入非法值。"""

    head = json.dumps(data, separators=(",", ":")).encode("utf-8")
    return proto.HDR.pack(proto.MAGIC, type_, flags, len(head)) + head + payload


def fake_channel():
    """绕过 Channel.__init__（它会真的去连沙箱）。"""

    ch = object.__new__(Channel)
    ch._lock = threading.RLock()
    ch._wlock = threading.Lock()
    ch._pending = {}
    ch.log = lambda *a: None
    ch._proc = None
    ch._gen = 0
    ch._worker_pid = None
    return ch


class Sink(object):
    """收集 worker 写出的帧，不落 pty。"""

    def __init__(self):
        self.frames = []

    def __call__(self, blob):
        self.frames.append(blob)

    def decode(self):
        dec = proto.Decoder()
        out = []
        for blob in self.frames:
            out.extend(dec.feed(blob))
        return out


def new_conn(sink, daemon):
    return worker.Conn(lambda: None, sink, daemon)


class ProtocolTests(unittest.TestCase):
    """S2 / S3 / S5 / S6：坏帧不得掐断连接、不得注入伪造请求。"""

    def test_decoder_non_integer_len_no_exception(self):
        d = proto.Decoder()
        out = d.feed(raw_frame(proto.T_RES, {"id": 1, "len": "oops"}))
        self.assertEqual(out, [])
        self.assertEqual(len(d.buf), 0)

    def test_decoder_len_bounded(self):
        d = proto.Decoder()
        d.feed(raw_frame(proto.T_RES, {"id": 2, "len": 2 ** 30}))
        d.feed(b"Z" * 100000)
        self.assertLess(len(d.buf), 4096)

    def test_decoder_negative_len_realigns(self):
        d = proto.Decoder()
        bad = raw_frame(proto.T_RES, {"id": 3, "len": -1}, b"}PAYLOAD")
        good = proto.encode_frame(proto.T_RES, {"id": 4, "ok": 1})
        self.assertEqual(d.feed(bad + good), [])
        out = d.feed(good)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0][0].data["id"], 4)

    def test_decoder_still_resyncs_after_noise(self):
        """坏 JSON 头只丢已确定的帧头，不能吞掉后面的合法帧。"""

        good = proto.encode_frame(proto.T_RES, {"op": "pong", "id": 1})
        noise = b"\x1b[?2004hSBOX" + b"\x00" * 6
        d = proto.Decoder()
        out = d.feed(good + noise + good)
        self.assertEqual(len(out), 2)

    def test_boot_non_integer_len_no_exception(self):
        r, w = os.pipe()
        os.write(w, raw_frame(boot.T_REQ, {"op": "ping", "id": 3, "len": "oops"}))
        os.close(w)
        try:
            self.assertIsNone(boot.read_frame(r, bytearray()))
        finally:
            os.close(r)

    def test_boot_bad_frame_cannot_inject_request(self):
        forged = raw_frame(boot.T_REQ, {"op": "fake", "id": 99, "len": 0})
        f1 = raw_frame(boot.T_REQ, {"op": "x", "id": 1, "len": 2 ** 30}, forged)
        f2 = raw_frame(boot.T_REQ, {"op": "ping", "id": 2, "len": 0})
        r, w = os.pipe()
        os.write(w, f1 + f2)
        os.close(w)
        buf = bytearray()
        ops = []
        try:
            for _ in range(5):
                fr = boot.read_frame(r, buf)
                if fr is None:
                    break
                ops.append(fr[2].get("op"))
        finally:
            os.close(r)
        self.assertNotIn("fake", ops)


class TransportTests(unittest.TestCase):
    """S4 / S7 / S11。"""

    def test_send_data_eof_only_on_last_part(self):
        ch = fake_channel()
        sent = []
        ch._write = lambda b: sent.append(b)
        big = proto.MAX_PAYLOAD + 100
        ch.send_data(7, b"X" * big, eof=True)
        dec = proto.Decoder()
        pieces = []
        for blob in sent:
            pieces.extend(dec.feed(blob))
        self.assertEqual(len(pieces), 2)
        self.assertEqual([bool(h.flags & proto.F_END) for h, _ in pieces],
                         [False, True])
        self.assertEqual([h.data.get("eof") for h, _ in pieces], [None, True])

    def test_send_data_real_worker_no_truncation(self):
        """多片上传经真实 worker 落盘，长度必须完整且不报 EBADF。"""

        ch = fake_channel()
        sent = []
        ch._write = lambda b: sent.append(b)
        big = proto.MAX_PAYLOAD + 100
        ch.send_data(7, b"X" * big, eof=True)

        tmpdir = tempfile.mkdtemp()
        target = os.path.join(tmpdir, "target.bin")
        daemon = worker.Daemon()
        sink = Sink()
        conn = new_conn(sink, daemon)
        worker.op_upload_begin(conn, 7, {"op": "upload_begin", "id": 7,
                                         "path": target, "mode": None})
        dec = proto.Decoder()
        for blob in sent:
            for hdr, pl in dec.feed(blob):
                daemon.on_data(conn, 7, hdr.data, pl)
        self.assertEqual(os.path.getsize(target), big)
        self.assertEqual(dict(worker.UPLOADS), {})
        frames = sink.decode()
        self.assertTrue(frames[-1][0].data.get("ok"))

    def test_stream_metadata_survives_multiframe(self):
        for size in (100, worker.CHUNK + 10):
            sink = Sink()
            conn = new_conn(sink, worker.Daemon())
            conn.stream(99, b"A" * size, path="/f", size=size,
                        mtime=123, ino=456, offset=7)
            ch = fake_channel()
            resp = Response(99, "read")
            ch._pending[99] = resp
            dec = proto.Decoder()
            for blob in sink.frames:
                for hdr, pl in dec.feed(blob):
                    ch._dispatch(hdr, pl)
            self.assertEqual(len(resp.bytes()), size)
            self.assertEqual(resp.data.get("size"), size)
            self.assertEqual(resp.data.get("path"), "/f")
            self.assertEqual(resp.data.get("mtime"), 123)
            self.assertEqual(resp.data.get("ino"), 456)
            self.assertEqual(resp.data.get("offset"), 7)

    def test_error_response_keeps_path(self):
        ch = fake_channel()
        resp = Response(1, "stat")
        ch._pending[1] = resp
        hdr = proto.Header(proto.T_RES, proto.F_END,
                           {"id": 1, "ok": 0, "errno": 2, "msg": "No such file",
                            "path": "/workspace/a.txt", "len": 0})
        ch._dispatch(hdr, b"")
        with self.assertRaises(RemoteError) as ctx:
            resp.wait(0)
        self.assertEqual(ctx.exception.path, "/workspace/a.txt")

    def test_error_response_tolerates_bad_errno(self):
        """非法 errno 不能在 _dispatch 里抛 ValueError 掐断连接。"""

        ch = fake_channel()
        resp = Response(1, "stat")
        ch._pending[1] = resp
        hdr = proto.Header(proto.T_RES, proto.F_END,
                           {"id": 1, "ok": 0, "errno": "oops", "msg": "x",
                            "len": 0})
        ch._dispatch(hdr, b"")
        with self.assertRaises(RemoteError):
            resp.wait(0)


class RootGuardTests(unittest.TestCase):
    """S1：字符串前缀挡不住软链，宿主端与服务端都要拦。"""

    def setUp(self):
        self.base = tempfile.mkdtemp()
        self.root = os.path.join(self.base, "root")
        self.outside = os.path.join(self.base, "outside")
        os.makedirs(self.root)
        os.makedirs(self.outside)
        with open(os.path.join(self.outside, "secret.txt"), "w") as f:
            f.write("TOP-SECRET-OUTSIDE-ROOT")
        os.symlink(self.outside, os.path.join(self.root, "link"))

    def test_host_check_rejects_symlink_escape(self):
        fs = FileSystem(root=self.root, channel=object())
        with self.assertRaises(OSError) as ctx:
            fs._abs("link/secret.txt")
        self.assertEqual(ctx.exception.errno, errno.EACCES)

    def test_worker_rejects_symlink_escape(self):
        """真正的攻击面：服务端 open() 之前必须自己校验 realpath。"""

        daemon = worker.Daemon()
        daemon.root = self.root
        sink = Sink()
        conn = new_conn(sink, daemon)
        daemon.on_request(conn, 5, "stat",
                          {"op": "stat", "id": 5,
                           "path": os.path.join(self.root, "link", "secret.txt")})
        frames = sink.decode()
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0][0].data.get("errno"), errno.EACCES)
        self.assertFalse(frames[0][0].data.get("ok"))

    def test_worker_allows_paths_inside_root(self):
        daemon = worker.Daemon()
        daemon.root = self.root
        sink = Sink()
        conn = new_conn(sink, daemon)
        daemon.on_request(conn, 6, "stat",
                          {"op": "stat", "id": 6,
                           "path": os.path.join(self.root, "ok.txt")})
        frames = sink.decode()
        # 文件不存在应是 ENOENT，而不是被边界校验误拦成 EACCES
        self.assertNotEqual(frames[0][0].data.get("errno"), errno.EACCES)

    def test_in_root_helper(self):
        self.assertFalse(worker.in_root(
            os.path.join(self.root, "link", "secret.txt"), self.root))
        self.assertTrue(worker.in_root(os.path.join(self.root, "a.txt"), self.root))
        self.assertTrue(worker.in_root("/anything", None))
        self.assertTrue(worker.in_root("/anything", "/"))


class ServiceTests(unittest.TestCase):
    """S8 / S9 / S10 / R2。"""

    def test_no_account_rejected(self):
        from sandbox_fs import ftp
        from sandbox_fs.ftp import make_handler

        if ftp.FTPHandler is None:
            self.skipTest("pyftpdlib 未安装")
        with self.assertRaises(RuntimeError) as ctx:
            make_handler(object(), user=None, password=None, anonymous=False)
        self.assertIn("--user", str(ctx.exception))

    def test_anonymous_still_works(self):
        from sandbox_fs import ftp
        from sandbox_fs.ftp import make_handler

        if ftp.FTPHandler is None:
            self.skipTest("pyftpdlib 未安装")
        h = make_handler(object(), user=None, password=None, anonymous=True)
        self.assertTrue(getattr(h.authorizer, "user_table", {}))

    def test_resolve_port_zero_rejected(self):
        with self.assertRaises(ValueError):
            cli.resolve_port(0)
        self.assertEqual(cli.resolve_port(None), cli.DEFAULT_PORT)
        self.assertEqual(cli.resolve_port(2122), 2122)

    def test_pidfile_config_roundtrip(self):
        from sandbox_fs import _ftp_daemon

        port = 65000 + (os.getpid() % 500)
        cfg = {"sandbox": "s", "root": "/r", "host": "127.0.0.1",
               "port": port, "user": "", "anonymous": True}
        _ftp_daemon.write_pidfile(port, os.getpid(), cfg)
        try:
            self.assertEqual(_ftp_daemon.read_pidfile(port), os.getpid())
            self.assertEqual(_ftp_daemon.read_pidinfo(port), cfg)
        finally:
            try:
                os.unlink(_ftp_daemon.pidfile_path(port))
            except OSError:
                pass

    def test_legacy_plain_pidfile_still_readable(self):
        from sandbox_fs import _ftp_daemon

        port = 65000 + (os.getpid() % 500)
        try:
            with open(_ftp_daemon.pidfile_path(port), "w") as f:
                f.write("%d\n" % os.getpid())
            self.assertEqual(_ftp_daemon.read_pidfile(port), os.getpid())
            self.assertIsNone(_ftp_daemon.read_pidinfo(port))
        finally:
            try:
                os.unlink(_ftp_daemon.pidfile_path(port))
            except OSError:
                pass

    def test_existing_pid_rejects_config_conflict(self):
        from sandbox_fs._ftp_daemon import pidfile_path, write_pidfile
        from sandbox_fs.serve import BackgroundFTP

        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        s.listen(32)  # 多次探测连接都不 accept，backlog 太小会误判为端口不可达
        port = s.getsockname()[1]
        old = BackgroundFTP(sandbox="old", workdir="/old", port=port)
        write_pidfile(port, os.getpid(), old._expected_config())
        try:
            new = BackgroundFTP(sandbox="A-BRAND-NEW-SANDBOX",
                                workdir="/a/totally/different/root", port=port,
                                user="alice", anonymous=False)
            self.assertIsNone(new._existing_pid(port))
            self.assertIn("已拒绝复用", new._conflict(port))
            with self.assertRaises(RuntimeError):
                new.start(timeout=1)
            # 完全相同的配置仍然可以复用，不能把复用能力一起砍掉
            same = BackgroundFTP(sandbox="old", workdir="/old", port=port)
            self.assertEqual(same._existing_pid(port), os.getpid())
        finally:
            s.close()
            try:
                os.unlink(pidfile_path(port))
            except OSError:
                pass

    def test_upload_ttl_reaps_stale_state(self):
        tmpdir = tempfile.mkdtemp()
        path = os.path.join(tmpdir, "target.bin")
        daemon = worker.Daemon()
        sink = Sink()
        conn = new_conn(sink, daemon)
        worker.op_upload_begin(conn, 77,
                               {"op": "upload_begin", "id": 77,
                                "path": path, "mode": None})
        self.assertIn(77, worker.UPLOADS)
        self.assertIn(77, worker.UPLOAD_SEEN)
        worker.UPLOAD_SEEN[77] = 0  # 人为置为过期
        worker.reap_uploads()
        self.assertNotIn(77, worker.UPLOADS)
        self.assertNotIn(77, worker.UPLOAD_SEEN)
        self.assertEqual(os.listdir(tmpdir), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
