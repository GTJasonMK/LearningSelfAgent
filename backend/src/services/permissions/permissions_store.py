from __future__ import annotations

import os
from typing import List, Optional, Tuple

from backend.src.common.utils import parse_json_list
from backend.src.constants import (
    DEFAULT_ALLOWED_OPS,
    DEFAULT_ALLOWED_PATHS,
    DEFAULT_DISABLED_ACTIONS,
    DEFAULT_DISABLED_TOOLS,
    OP_EXEC,
    OP_WRITE,
)
from backend.src.repositories.permissions_repo import get_permissions_store


def _norm_path(path: str) -> str:
    # Windows/WSL 下大小写与分隔符差异会导致 startswith 误判：统一做 normcase + realpath。
    return os.path.normcase(os.path.realpath(path or ""))


def get_allowed_ops_and_paths() -> Tuple[List[str], List[str]]:
    try:
        row = get_permissions_store()
    except Exception:
        row = None
    if not row:
        return list(DEFAULT_ALLOWED_OPS), list(DEFAULT_ALLOWED_PATHS)
    allowed_ops = parse_json_list(row["allowed_ops"])
    allowed_paths = parse_json_list(row["allowed_paths"])
    return allowed_ops, allowed_paths


def get_disabled_actions_and_tools() -> Tuple[List[str], List[str]]:
    try:
        row = get_permissions_store()
    except Exception:
        row = None
    if not row:
        return list(DEFAULT_DISABLED_ACTIONS), list(DEFAULT_DISABLED_TOOLS)
    disabled_actions = parse_json_list(row["disabled_actions"])
    disabled_tools = parse_json_list(row["disabled_tools"])
    return disabled_actions, disabled_tools


def is_action_enabled(action_type: Optional[str]) -> bool:
    if not action_type:
        return False
    disabled_actions, _ = get_disabled_actions_and_tools()
    return str(action_type) not in {str(x) for x in disabled_actions}


def is_tool_enabled(tool_name: Optional[str]) -> bool:
    if not tool_name:
        return False
    _, disabled_tools = get_disabled_actions_and_tools()
    return str(tool_name) not in {str(x) for x in disabled_tools}


def has_write_permission() -> bool:
    allowed_ops, _allowed_paths = get_allowed_ops_and_paths()
    return OP_WRITE in allowed_ops


def has_exec_permission(workdir: Optional[str]) -> bool:
    allowed_ops, allowed_paths = get_allowed_ops_and_paths()
    if OP_EXEC not in allowed_ops:
        return False
    if not allowed_paths:
        # 未限制路径：允许执行（仍需 OP_EXEC）
        return True
    if not workdir:
        return False
    resolved = _norm_path(workdir)
    for base_path in allowed_paths:
        try:
            base = _norm_path(str(base_path))
            if not base:
                continue
            # 不能用 startswith：/a 会错误匹配 /ab；用 commonpath 保障“目录包含”语义。
            if os.path.commonpath([resolved, base]) == base:
                return True
        except Exception:
            continue
    return False
