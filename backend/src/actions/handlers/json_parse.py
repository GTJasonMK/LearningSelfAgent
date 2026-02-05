import json
from typing import Optional, Tuple


def execute_json_parse(payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 json_parse：解析 JSON 字符串并可选提取字段。
    """
    text = payload.get("text")
    if not isinstance(text, str) or not text.strip():
        raise ValueError("json_parse.text 不能为空")

    try:
        obj = json.loads(text)
    except Exception as exc:
        return None, f"json_parse 解析失败: {exc}"

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
