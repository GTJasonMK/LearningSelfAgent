import os
import shutil
from typing import Optional, Tuple

from backend.src.common.path_utils import normalize_windows_abs_path_on_posix


def execute_file_delete(payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 file_delete：删除文件或目录。
    """
    path = payload.get("path")
    if not isinstance(path, str) or not path.strip():
        raise ValueError("file_delete.path 不能为空")

    target_path = normalize_windows_abs_path_on_posix(path.strip())
    if not os.path.isabs(target_path):
        target_path = os.path.abspath(os.path.join(os.getcwd(), target_path))
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
