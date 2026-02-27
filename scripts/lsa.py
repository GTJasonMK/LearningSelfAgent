#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""LearningSelfAgent CLI 快捷入口。"""

import os
import sys

# 确保项目根目录在 sys.path 中（与 start.py 模式对齐）
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from backend.src.cli.main import cli

if __name__ == "__main__":
    cli()
