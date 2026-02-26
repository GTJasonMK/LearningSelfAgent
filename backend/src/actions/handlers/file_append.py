import os
from typing import Optional, Tuple

from backend.src.actions.handlers.file_action_common import (
    ensure_write_permission_for_action,
    normalize_encoding,
    require_action_path,
    resolve_action_target_path,
)
from backend.src.common.errors import AppError
from backend.src.constants import ERROR_CODE_INVALID_REQUEST, HTTP_STATUS_BAD_REQUEST


def _append_text_file(path: str, content: str, encoding: str = "utf-8") -> dict:
    target_path = resolve_action_target_path(path)
    if not target_path:
        raise AppError(
            code=ERROR_CODE_INVALID_REQUEST,
            message="file_append.path 不能为空",
            status_code=HTTP_STATUS_BAD_REQUEST,
        )
    parent = os.path.dirname(target_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(target_path, "a", encoding=encoding, newline="\n") as f:
        f.write(content)
    try:
        size = len(content.encode(encoding, errors="ignore"))
    except Exception:
        size = len(content.encode("utf-8", errors="ignore"))
    return {"path": target_path, "bytes": size}


def execute_file_append(payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 file_append：追加写入文本文件。
    """
    path = require_action_path(payload, "file_append")
    permission_error = ensure_write_permission_for_action(path, "file_append")
    if permission_error:
        return None, permission_error

    content = payload.get("content")
    if content is None:
        content = ""
    if not isinstance(content, str):
        raise ValueError("file_append.content 必须是字符串")

    encoding = normalize_encoding(payload.get("encoding"))

    result = _append_text_file(path=path, content=content, encoding=encoding)
    return result, None
