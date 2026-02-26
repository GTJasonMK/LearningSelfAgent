from __future__ import annotations

import os
from typing import Optional

from backend.src.actions.handlers.common_utils import resolve_path_with_workdir
from backend.src.common.task_error_codes import format_task_error
from backend.src.services.permissions.permissions_store import has_write_permission_for_path


def require_action_path(payload: dict, action_name: str) -> str:
    path = payload.get("path") if isinstance(payload, dict) else None
    if not isinstance(path, str) or not path.strip():
        raise ValueError(f"{action_name}.path 不能为空")
    return str(path).strip()


def ensure_write_permission_for_action(path: str, action_name: str) -> Optional[str]:
    if has_write_permission_for_path(path):
        return None
    return format_task_error(
        code="permission_denied",
        message=f"{action_name} 路径不在允许范围内: {path}",
    )


def normalize_encoding(value: object, *, default: str = "utf-8") -> str:
    encoding = value if isinstance(value, str) else ""
    encoding = str(encoding or "").strip()
    return encoding or str(default)


def resolve_action_target_path(path: str) -> str:
    return resolve_path_with_workdir(str(path or ""), os.getcwd())
