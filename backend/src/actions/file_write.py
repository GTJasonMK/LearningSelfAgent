import os

from backend.src.common.errors import AppError
from backend.src.common.path_utils import normalize_windows_abs_path_on_posix
from backend.src.constants import ERROR_CODE_INVALID_REQUEST, HTTP_STATUS_BAD_REQUEST


def write_text_file(path: str, content: str, encoding: str = "utf-8") -> dict:
    """
    写入文本文件并返回写入结果。

    设计要点：
    - 允许相对路径：以当前进程工作目录为基准（Electron / scripts/start.py 会把 cwd 设为项目根目录）
    - 自动创建父目录
    - 统一使用 \\n 换行，避免 Windows 下默认写入 \\r\\n 导致内容不一致
    """
    target_path = normalize_windows_abs_path_on_posix((path or "").strip())
    if not target_path:
        raise AppError(
            code=ERROR_CODE_INVALID_REQUEST,
            message="file_write.path 不能为空",
            status_code=HTTP_STATUS_BAD_REQUEST,
        )
    if not os.path.isabs(target_path):
        target_path = os.path.abspath(os.path.join(os.getcwd(), target_path))
    parent = os.path.dirname(target_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(target_path, "w", encoding=encoding, newline="\n") as f:
        f.write(content)
    try:
        size = len(content.encode(encoding, errors="ignore"))
    except Exception:
        size = len(content.encode("utf-8", errors="ignore"))
    return {"path": target_path, "bytes": size}
