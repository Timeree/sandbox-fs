"""sandbox_fs 帧协议（sandbox_fs-proto/1）。

设计目标
--------
``sandbox ssh`` 本身不是标准 SSH 服务：它是远程沙箱的 WebSocket + PTY
通道，每次调用都要重新握手，且 payload 是原始字节流。
为了做到"连一次，后面全部复用"，协议分两层：

1. **传输层**：一条 WebSocket 承载一个沙箱内进程的字节流。
   二进制帧 = 原始 stdout/stderr；文本帧只有一个 ``{"type":"exit","code":n}``。
2. **会话层**：在通道里跑一个常驻 python3 worker，双方用下面的定长头帧通信::

       MAGIC(4) = b"SBOX"   TYPE(u8)   FLAGS(u8)    HEADER_LEN(u32 BE)
       HEADER_LEN 字节的 JSON 头（UTF-8）   PAYLOAD（长度由头的 payload 字段给出）

   TYPE: REQ / RES / DATA / EVENT
   FLAGS.END 表示一条消息的最后一块。

worker 的生命周期与通道绑定：通道（及沙箱内进程）关闭时 worker 一并退出，
下一次 ``connect()`` 会重新自举一个新的 worker。worker 的 pid 随 hello/ping
应答返回宿主端，不在沙箱里落盘。
"""


import json
import struct

MAGIC = b"SBOX"
VERSION = "sandbox_fs-proto/1"

HDR = struct.Struct(">4sBBI")  # MAGIC, TYPE, FLAGS, HEADER_LEN(u32)
MAX_HEADER = 1 << 20
#: 单帧载荷上限。沙箱侧 pty 缓冲很小，大帧会挤爆连接（实测 >256KB 必断）。
MAX_PAYLOAD = 256 * 1024

T_REQ = 0x01
T_RES = 0x02
T_DATA = 0x03
T_EVENT = 0x04

F_END = 0x01


class Header(object):
    __slots__ = ("type", "flags", "data")

    def __init__(self, type, flags=0, data=None):
        self.type = type
        self.flags = flags
        self.data = data or {}

    @property
    def end(self):
        return bool(self.flags & F_END)

    def payload_len(self):
        """返回载荷长度；非法值返回 ``-1``（调用方据此丢弃坏帧并重对齐）。

        这里绝不能抛异常：``transport._pump`` 会吞掉 ``ValueError``，于是
        一条 ``{"len": "oops"}`` 的损坏帧就等于掐断整条连接。
        """

        value = self.data.get("len", 0)
        # 只接受协议本身会写出的 int；bool / 字符串 / None 一律视为坏帧，
        # 避免 int("10") 之类把伪造值悄悄解释成合法长度。
        if isinstance(value, bool) or not isinstance(value, int):
            return -1
        if value < 0 or value > MAX_PAYLOAD:
            return -1
        return value


def encode_frame(type, data, payload=b"", flags=0):
    """编码一帧。``data`` 里的 ``len`` 会按 ``payload`` 自动填充。"""

    if len(payload) > MAX_PAYLOAD:
        raise ValueError("payload too large for one frame: %d > %d"
                         % (len(payload), MAX_PAYLOAD))
    data = dict(data)
    data["len"] = len(payload)
    head = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(head) > MAX_HEADER:
        raise ValueError("header too large: %d" % len(head))
    return HDR.pack(MAGIC, type, flags, len(head)) + head + payload


class Decoder(object):
    """增量解码器：喂任意切片，产出 ``(Header, payload)``。

    头与负载可以横跨多个网络块，所以必须缓冲到完整为止。
    """

    def __init__(self):
        self.buf = bytearray()

    def feed(self, chunk):
        self.buf += chunk
        out = []
        while True:
            if len(self.buf) < HDR.size:
                break
            magic, type_, flags, hlen = HDR.unpack_from(self.buf, 0)
            if magic != MAGIC or hlen > MAX_HEADER:
                # 脏数据：跳到下一个候选帧头重新对齐，而不是整个连接作废。
                # 不能 raise：_pump 会吞掉 ValueError，一条损坏帧就等于掐断
                # 整条连接（与 boot.read_frame 的处理保持一致）。
                drop = self.buf.find(MAGIC, 1)
                if drop < 0:
                    # 没有任何候选帧头：整体丢弃，避免逐字节扫描 O(n^2)，
                    # 也避免垃圾字节在缓冲区里无界堆积。
                    del self.buf[:]
                    break
                del self.buf[:drop]
                continue
            if len(self.buf) < HDR.size + hlen:
                break
            raw = bytes(self.buf[HDR.size:HDR.size + hlen])
            try:
                data = json.loads(raw.decode("utf-8"))
            except ValueError:
                # 坏 JSON 头：帧头与头部长度是确定的，只丢这 10+hlen 字节，
                # 后面可能已经到达的合法帧必须保留；也不能 raise。
                del self.buf[:HDR.size + hlen]
                continue
            if not isinstance(data, dict):
                del self.buf[:HDR.size + hlen]
                continue
            hmsg = Header(type_, flags, data)
            need = hmsg.payload_len()
            if need < 0:
                # 载荷长度非法（非整数 / 负数 / 超上限）：帧边界不可信。
                # 连同帧头一起丢弃缓冲区里尚未确认的字节，避免残留数据被
                # 重新对齐成伪造帧；也防止无界长度把缓冲区撑爆。
                del self.buf[:]
                continue
            total = HDR.size + hlen + need
            if len(self.buf) < total:
                break
            payload = bytes(self.buf[HDR.size + hlen:total])
            del self.buf[:total]
            out.append((hmsg, payload))
        return out
