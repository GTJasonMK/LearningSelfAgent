from typing import Optional, Tuple

from backend.src.actions.file_write import write_text_file


def execute_file_write(payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 file_write：写入文本文件。
    """
    path = payload.get("path")
    if not isinstance(path, str) or not path.strip():
        raise ValueError("file_write.path 不能为空")

    content = payload.get("content")
    if content is None:
        content = ""
    if not isinstance(content, str):
        raise ValueError("file_write.content 必须是字符串")

    encoding = payload.get("encoding") or "utf-8"
    if not isinstance(encoding, str) or not encoding.strip():
        encoding = "utf-8"

    result = write_text_file(path=path, content=content, encoding=encoding)
    return result, None
