from __future__ import annotations

import os
from typing import Dict, List, Optional, Tuple

from backend.src.common.path_utils import normalize_windows_abs_path_on_posix
from backend.src.common.utils import now_iso, parse_json_list
from backend.src.constants import (
    DEFAULT_ALLOWED_OPS,
    DEFAULT_ALLOWED_PATHS,
    DEFAULT_DISABLED_ACTIONS,
    DEFAULT_DISABLED_TOOLS,
    OP_EXEC,
    OP_WRITE,
)
from backend.src.repositories.permissions_repo import get_permissions_store

_PERMISSION_LIST_COLUMNS = ("allowed_ops", "allowed_paths", "disabled_actions", "disabled_tools")


def _norm_path(path: str) -> str:
    # Windows/WSL 下大小写与分隔符差异会导致 startswith 误判：统一做 normcase + realpath。
    return os.path.normcase(os.path.realpath(path or ""))


def _resolve_target_path(path: Optional[str]) -> str:
    raw = normalize_windows_abs_path_on_posix(str(path or "").strip())
    if not raw:
        return ""
    if not os.path.isabs(raw):
        raw = os.path.abspath(os.path.join(os.getcwd(), raw))
    return _norm_path(raw)


def _normalize_str_set(values: List[str]) -> set[str]:
    return {str(value or "").strip() for value in list(values or []) if str(value or "").strip()}


def _permission_lists_from_row(row) -> Tuple[List[str], List[str], List[str], List[str]]:
    parsed_lists = [parse_json_list(row[column]) for column in _PERMISSION_LIST_COLUMNS]
    return (
        parsed_lists[0],
        parsed_lists[1],
        parsed_lists[2],
        parsed_lists[3],
    )


def _is_name_enabled(matrix: Dict, *, disabled_key: str, name: Optional[str]) -> bool:
    text = str(name or "").strip()
    if not text:
        return False
    disabled = _normalize_str_set(list(matrix.get(disabled_key) or []))
    return text not in disabled


def compile_permission_policy_matrix(
    *,
    allowed_ops: List[str],
    allowed_paths: List[str],
    disabled_actions: List[str],
    disabled_tools: List[str],
) -> Dict:
    allowed_ops_set = _normalize_str_set(list(allowed_ops or []))
    normalized_paths = [str(path or "").strip() for path in list(allowed_paths or []) if str(path or "").strip()]
    normalized_disabled_actions = _normalize_str_set(list(disabled_actions or []))
    normalized_disabled_tools = _normalize_str_set(list(disabled_tools or []))

    return {
        "version": 1,
        "compiled_at": now_iso(),
        "ops": {
            "write": OP_WRITE in allowed_ops_set,
            "execute": OP_EXEC in allowed_ops_set,
        },
        "allowed_paths": normalized_paths,
        "disabled_actions": sorted(normalized_disabled_actions),
        "disabled_tools": sorted(normalized_disabled_tools),
    }


def _load_permissions_lists() -> Tuple[List[str], List[str], List[str], List[str]]:
    try:
        row = get_permissions_store()
    except Exception:
        row = None
    if not row:
        return (
            list(DEFAULT_ALLOWED_OPS),
            list(DEFAULT_ALLOWED_PATHS),
            list(DEFAULT_DISABLED_ACTIONS),
            list(DEFAULT_DISABLED_TOOLS),
        )
    return _permission_lists_from_row(row)


def get_permission_policy_matrix() -> Dict:
    allowed_ops, allowed_paths, disabled_actions, disabled_tools = _load_permissions_lists()
    return compile_permission_policy_matrix(
        allowed_ops=allowed_ops,
        allowed_paths=allowed_paths,
        disabled_actions=disabled_actions,
        disabled_tools=disabled_tools,
    )


def get_allowed_ops_and_paths() -> Tuple[List[str], List[str]]:
    allowed_ops, allowed_paths, _disabled_actions, _disabled_tools = _load_permissions_lists()
    return allowed_ops, allowed_paths


def get_disabled_actions_and_tools() -> Tuple[List[str], List[str]]:
    _allowed_ops, _allowed_paths, disabled_actions, disabled_tools = _load_permissions_lists()
    return disabled_actions, disabled_tools


def is_action_enabled(action_type: Optional[str]) -> bool:
    matrix = get_permission_policy_matrix()
    return _is_name_enabled(matrix, disabled_key="disabled_actions", name=action_type)


def is_tool_enabled(tool_name: Optional[str]) -> bool:
    matrix = get_permission_policy_matrix()
    return _is_name_enabled(matrix, disabled_key="disabled_tools", name=tool_name)


def has_write_permission() -> bool:
    matrix = get_permission_policy_matrix()
    ops = matrix.get("ops") if isinstance(matrix.get("ops"), dict) else {}
    return bool(ops.get("write"))


def is_path_allowed(path: Optional[str]) -> bool:
    matrix = get_permission_policy_matrix()
    allowed_paths = list(matrix.get("allowed_paths") or [])
    if not allowed_paths:
        # 未限制路径：允许访问
        return True

    resolved = _resolve_target_path(path)
    if not resolved:
        return False

    for base_path in allowed_paths:
        try:
            base = _resolve_target_path(str(base_path))
            if not base:
                continue
            # 目录边界判断：避免 /a 误匹配 /ab
            if os.path.commonpath([resolved, base]) == base:
                return True
        except Exception:
            continue
    return False


def has_write_permission_for_path(path: Optional[str]) -> bool:
    return has_write_permission() and is_path_allowed(path)


def has_exec_permission(workdir: Optional[str]) -> bool:
    matrix = get_permission_policy_matrix()
    ops = matrix.get("ops") if isinstance(matrix.get("ops"), dict) else {}
    if not bool(ops.get("execute")):
        return False
    return is_path_allowed(workdir)
