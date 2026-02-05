from backend.src.constants import STREAM_TAG_RESULT


def format_visible_result(text: str) -> str:
    """
    把“最终可见输出”统一包一层结果标签，便于前端从混合日志中提取最终答案。

    说明：
    - 若内容已以 `STREAM_TAG_RESULT` 开头，则不重复加标签。
    - 若为空则返回空字符串。
    """
    value = str(text or "").strip()
    if not value:
        return ""
    if value.startswith(STREAM_TAG_RESULT):
        return value
    return f"{STREAM_TAG_RESULT}\n{value}"

