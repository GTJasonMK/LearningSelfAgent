import json
from typing import Optional, Tuple

from backend.src.common.utils import extract_json_value
from backend.src.constants import (
    JSON_PARSE_REQUIRE_RECENT_SOURCE_DEFAULT,
    JSON_PARSE_SOURCE_MIN_TEXT_CHARS,
)


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _looks_like_large_structured_text(text: str) -> bool:
    value = _normalize_text(text)
    if not value:
        return False
    if len(value) < int(JSON_PARSE_SOURCE_MIN_TEXT_CHARS or 0):
        return False
    return value.startswith("{") or value.startswith("[")


def _is_bound_to_source(text: str, source: str) -> bool:
    text_value = _normalize_text(text)
    source_value = _normalize_text(source)
    if not text_value or not source_value:
        return False
    if text_value == source_value:
        return True
    if len(text_value) >= 80 and text_value in source_value:
        return True
    if len(source_value) >= 80 and source_value in text_value:
        return True
    return False


def _enforce_recent_source(payload: dict, context: Optional[dict]) -> Optional[str]:
    strict = (context or {}).get("enforce_json_parse_recent_source")
    if strict is None:
        strict = JSON_PARSE_REQUIRE_RECENT_SOURCE_DEFAULT
    if not bool(strict):
        return None

    text = _normalize_text(payload.get("text"))
    if not text:
        return None

    source = _normalize_text((context or {}).get("latest_parse_input_text"))
    if source:
        if _is_bound_to_source(text, source):
            return None
        # 仅对“较大且结构化”的 JSON 强制来源绑定，避免小 JSON（例如 {"ok":true}）误伤导致链路抖动。
        if _looks_like_large_structured_text(text):
            return "json_parse.text 未绑定最近成功步骤输出（请使用最近一步真实输出进行解析）"
        return None

    if _looks_like_large_structured_text(text):
        return "json_parse.text 缺少来源绑定（当前没有可用的最近步骤输出）"

    return None


def execute_json_parse(payload: dict, *, context: Optional[dict] = None) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 json_parse：解析 JSON 字符串并可选提取字段。
    """
    text = payload.get("text")
    if not isinstance(text, str) or not text.strip():
        raise ValueError("json_parse.text 不能为空")

    source_error = _enforce_recent_source(payload, context)
    if source_error:
        raise ValueError(source_error)

    try:
        obj = json.loads(text)
    except Exception as exc:
        extracted = extract_json_value(text)
        if extracted is None:
            return None, f"json_parse 解析失败: {exc}"
        obj = extracted

    pick = payload.get("pick_keys")
    if isinstance(pick, list):
        picked = {}
        for key in pick:
            if key is None:
                continue
            key_text = str(key)
            if isinstance(obj, dict) and key_text in obj:
                picked[key_text] = obj.get(key_text)
        return {"value": picked, "picked": True}, None

    return {"value": obj, "picked": False}, None
