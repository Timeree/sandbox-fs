# -*- coding: utf-8 -*-
"""``python -m sandbox_fs`` 的入口。"""

from __future__ import absolute_import, unicode_literals

import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
