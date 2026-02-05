import os
from typing import Optional, Tuple

from backend.src.common.path_utils import normalize_windows_abs_path_on_posix


def execute_file_list(payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 file_list：列出目录内容。
    """
    path = payload.get("path")
    if not isinstance(path, str) or not path.strip():
        raise ValueError("file_list.path 不能为空")

    target_path = normalize_windows_abs_path_on_posix(path.strip())
    if not os.path.isabs(target_path):
        target_path = os.path.abspath(os.path.join(os.getcwd(), target_path))
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
