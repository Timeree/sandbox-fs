# -*- coding: utf-8 -*-
"""
对外入口：``sandbox_fs.connect(...)`` 直接返回可用的 :class:`sandbox_fs.fs.FileSystem`。

    >>> import sandbox_fs
    >>> fs = sandbox_fs.connect("sandbox")            # 连一次
    >>> fs.listdir("/workspace")       # 之后每个操作 ~40ms
    >>> fs.write("notes.txt", "hi")           # 相对当前目录
    >>> fs.close()                            # 或 with 语句
"""

from __future__ import absolute_import, unicode_literals

from .errors import (AuthError, SandboxFsError, RemoteError,
                     SandboxError, TransportError)
from .fs import Entry, FileSystem

__version__ = "1.1.0"
__all__ = [
    "connect", "FileSystem", "Entry", "SandboxFsError", "TransportError",
    "RemoteError", "SandboxError", "AuthError", "__version__",
]


def connect(sandbox="sandbox", root="auto", cwd=None, log=None, **kw):
    """建立连接并返回 :class:`FileSystem`（推荐入口）。"""

    return FileSystem(sandbox=sandbox, root=root, cwd=cwd, log=log, **kw)
