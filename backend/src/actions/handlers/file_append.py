import os
from typing import Optional, Tuple

from backend.src.common.errors import AppError
from backend.src.common.path_utils import normalize_windows_abs_path_on_posix
from backend.src.constants import ERROR_CODE_INVALID_REQUEST, HTTP_STATUS_BAD_REQUEST


def _append_text_file(path: str, content: str, encoding: str = "utf-8") -> dict:
    target_path = normalize_windows_abs_path_on_posix((path or "").strip())
    if not target_path:
        raise AppError(
            code=ERROR_CODE_INVALID_REQUEST,
            message="file_append.path 不能为空",
            status_code=HTTP_STATUS_BAD_REQUEST,
        )
    if not os.path.isabs(target_path):
        target_path = os.path.abspath(os.path.join(os.getcwd(), target_path))
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
    path = payload.get("path")
    if not isinstance(path, str) or not path.strip():
        raise ValueError("file_append.path 不能为空")

    content = payload.get("content")
    if content is None:
        content = ""
    if not isinstance(content, str):
        raise ValueError("file_append.content 必须是字符串")

    encoding = payload.get("encoding") or "utf-8"
    if not isinstance(encoding, str) or not encoding.strip():
        encoding = "utf-8"

    result = _append_text_file(path=path, content=content, encoding=encoding)
    return result, None
