import os
from typing import Optional, Tuple

from backend.src.actions.handlers.file_action_common import (
    ensure_write_permission_for_action,
    require_action_path,
    resolve_action_target_path,
)


def execute_file_list(payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 file_list：列出目录内容。
    """
    path = require_action_path(payload, "file_list")
    permission_error = ensure_write_permission_for_action(path, "file_list")
    if permission_error:
        return None, permission_error

    target_path = resolve_action_target_path(path)
    if not os.path.exists(target_path):
        raise ValueError(f"file_list.path 不存在: {target_path}")
    if not os.path.isdir(target_path):
        raise ValueError(f"file_list.path 不是目录: {target_path}")

    recursive = payload.get("recursive")
    recursive = bool(recursive) if recursive is not None else False

    pattern = payload.get("pattern")
    if isinstance(pattern, str) and pattern.strip():
        pattern = pattern.strip()
    else:
        pattern = None

    max_entries = payload.get("max_entries")
    if max_entries is not None:
        try:
            max_entries = int(max_entries)
        except Exception:
            max_entries = None

    items = []
    if recursive:
        for root, _dirs, files in os.walk(target_path):
            for name in files:
                full = os.path.join(root, name)
                rel = os.path.relpath(full, target_path)
                if pattern and pattern not in rel:
                    continue
                items.append(rel.replace("\\", "/"))
                if isinstance(max_entries, int) and max_entries > 0 and len(items) >= max_entries:
                    break
            if isinstance(max_entries, int) and max_entries > 0 and len(items) >= max_entries:
                break
    else:
        for name in os.listdir(target_path):
            full = os.path.join(target_path, name)
            rel = os.path.relpath(full, target_path)
            if pattern and pattern not in rel:
                continue
            items.append(rel.replace("\\", "/"))
            if isinstance(max_entries, int) and max_entries > 0 and len(items) >= max_entries:
                break

    return {
        "path": target_path,
        "count": len(items),
        "items": items,
        "recursive": bool(recursive),
    }, None
