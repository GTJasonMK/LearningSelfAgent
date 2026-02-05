import os
from typing import Optional, Tuple

from backend.src.common.errors import AppError
from backend.src.common.path_utils import normalize_windows_abs_path_on_posix
from backend.src.constants import ERROR_CODE_INVALID_REQUEST, HTTP_STATUS_BAD_REQUEST


def _read_text_file(path: str, encoding: str, max_bytes: Optional[int]) -> dict:
    target_path = normalize_windows_abs_path_on_posix((path or "").strip())
    if not target_path:
        raise AppError(
            code=ERROR_CODE_INVALID_REQUEST,
            message="file_read.path 不能为空",
            status_code=HTTP_STATUS_BAD_REQUEST,
        )
    if not os.path.isabs(target_path):
        target_path = os.path.abspath(os.path.join(os.getcwd(), target_path))
    if not os.path.exists(target_path):
        raise AppError(
            code=ERROR_CODE_INVALID_REQUEST,
            message=f"file_read.path 不存在: {target_path}",
            status_code=HTTP_STATUS_BAD_REQUEST,
        )

    limit = None
    if isinstance(max_bytes, int) and max_bytes > 0:
        limit = int(max_bytes)

    with open(target_path, "rb") as f:
        raw = f.read(limit) if limit else f.read()
    try:
        text = raw.decode(encoding, errors="ignore")
    except Exception:
        text = raw.decode("utf-8", errors="ignore")

    return {
        "path": target_path,
        "bytes": len(raw),
        "content": text,
    }


def execute_file_read(payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 file_read：读取文本文件内容。
    """
    path = payload.get("path")
    if not isinstance(path, str) or not path.strip():
        raise ValueError("file_read.path 不能为空")

    encoding = payload.get("encoding") or "utf-8"
    if not isinstance(encoding, str) or not encoding.strip():
        encoding = "utf-8"

    max_bytes = payload.get("max_bytes")
    if max_bytes is not None:
        try:
            max_bytes = int(max_bytes)
        except Exception:
            max_bytes = None

    result = _read_text_file(path=path, encoding=encoding, max_bytes=max_bytes)
    return result, None
