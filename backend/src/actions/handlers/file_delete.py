import os
import shutil
from typing import Optional, Tuple

from backend.src.actions.handlers.file_action_common import (
    ensure_write_permission_for_action,
    require_action_path,
    resolve_action_target_path,
)


def execute_file_delete(payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 file_delete：删除文件或目录。
    """
    path = require_action_path(payload, "file_delete")
    permission_error = ensure_write_permission_for_action(path, "file_delete")
    if permission_error:
        return None, permission_error

    target_path = resolve_action_target_path(path)
    if not os.path.exists(target_path):
        return {"path": target_path, "deleted": False, "reason": "not_exists"}, None

    recursive = payload.get("recursive")
    recursive = bool(recursive) if recursive is not None else False

    if os.path.isdir(target_path):
        if not recursive:
            raise ValueError("file_delete.recursive=false 无法删除目录")
        shutil.rmtree(target_path, ignore_errors=True)
        return {"path": target_path, "deleted": True, "type": "dir"}, None

    os.remove(target_path)
    return {"path": target_path, "deleted": True, "type": "file"}, None
