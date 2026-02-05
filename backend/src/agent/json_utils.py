from typing import Optional

from backend.src.common.utils import extract_json_object


def _extract_json_object(text: str) -> Optional[dict]:
    """
    从 LLM 输出中尽量提取 JSON 对象。

    设计目标：
    - 允许模型输出前后夹带少量文字（尽量容错）
    - 只返回 dict；失败返回 None
    """
    return extract_json_object(text)


def safe_json_parse(text: str) -> Optional[dict]:
    """
    安全解析 JSON 字符串，容错 LLM 输出中的额外内容。

    参数:
        text: 可能包含 JSON 的文本

    返回:
        解析成功返回 dict，失败返回 None
    """
    return extract_json_object(text)
