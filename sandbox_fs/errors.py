# -*- coding: utf-8 -*-
"""sandbox_fs 统一异常，以及到 POSIX errno 的映射。"""

from __future__ import absolute_import, unicode_literals


class SandboxFsError(Exception):
    """所有 sandbox_fs 异常的基类。"""


class TransportError(SandboxFsError):
    """通道（WebSocket / 子进程）层的错误。"""


class AuthError(TransportError):
    """无法取得沙箱平台凭据（登录态缺失/失效）。"""


class SandboxError(SandboxFsError):
    """沙箱侧返回了错误。"""


class RemoteError(SandboxFsError):
    """沙箱内操作失败，携带 errno。"""

    def __init__(self, op, path, errno_, message=""):
        self.op = op
        self.path = path
        self.errno = errno_
        self.message = message or ""
        detail = self.message.strip()
        if len(detail) > 300:
            detail = detail[:300] + "..."
        super(RemoteError, self).__init__(
            "%s(%s) failed: errno=%d%s"
            % (op, path, errno_, " (" + detail + ")" if detail else "")
        )
