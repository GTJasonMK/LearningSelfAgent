# -*- coding: utf-8 -*-
"""
路径辅助工具（跨平台/WSL 兼容）。

背景：
- Agent 的 plan/action 经常会带有 Windows 盘符路径（如 `E:\\code\\...`）。
- 在 WSL/Linux 下，`os.path.isabs("E:\\\\...")` 会返回 False，导致被误当作“相对路径”拼接到 cwd，
  进而出现：
  - file_write/file_read 等动作落盘到错误位置
  - artifacts 校验误报“文件缺失”

本模块提供一个轻量的“Windows 盘符路径 → WSL /mnt/<drive> 路径”转换：
- 仅在非 Windows（os.name != 'nt'）时生效；
- 输入非盘符绝对路径时保持原样；
- 目标：让同一份 plan 在 Windows 与 WSL 环境都能稳定执行。
"""

from __future__ import annotations

import os
import re
from pathlib import Path

_WIN_DRIVE_ABS_RE = re.compile(r"^(?P<drive>[A-Za-z]):[\\/](?P<rest>.*)$")


def normalize_windows_abs_path_on_posix(path: str) -> str:
    """
    将 Windows 盘符绝对路径（如 E:\\a\\b 或 E:/a/b）转换为 WSL 风格路径（/mnt/e/a/b）。

    说明：
    - 仅在 posix 下转换；Windows 下保持不变；
    - 不尝试处理 UNC 路径（\\\\server\\share\\...），保持原样；
    - 不判断 /mnt/<drive> 是否真实挂载：如果用户给了盘符路径，通常就是 WSL/Windows 场景；
      即便挂载不存在，也比把它当“相对路径”写到奇怪的位置更接近用户意图。
    """
    raw = str(path or "").strip()
    if not raw:
        return raw
    if os.name == "nt":
        return raw
    # UNC / 网络路径：不处理
    if raw.startswith("\\\\") or raw.startswith("//"):
        return raw
    m = _WIN_DRIVE_ABS_RE.match(raw)
    if not m:
        return raw
    drive = str(m.group("drive") or "").lower()
    rest = str(m.group("rest") or "").replace("\\", "/")
    # 输入可能来自 JSON/转义，出现 `\\` -> `//`，这里做一次压缩，避免生成多重分隔符。
    rest = re.sub(r"/+", "/", rest).lstrip("/")
    if rest:
        return f"/mnt/{drive}/{rest}"
    return f"/mnt/{drive}"


def is_path_within_root(path: "str | Path", root: "str | Path") -> bool:
    """
    判断 path 是否位于 root 目录内（含 root 自身）。
    """
    try:
        return Path(path).resolve().is_relative_to(Path(root).resolve())
    except Exception:
        return False
