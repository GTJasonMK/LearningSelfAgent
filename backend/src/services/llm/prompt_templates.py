from __future__ import annotations

from typing import Optional

from backend.src.constants import PROMPT_TEMPLATE_NAME_MAX_CHARS
from backend.src.storage import get_connection


def normalize_prompt_template_name(
    name: str, max_len: int = PROMPT_TEMPLATE_NAME_MAX_CHARS
) -> str:
    """
    归一化 prompt_templates.name：
    - 去前后空白 + 合并多余空白
    - 截断长度，避免 name 过长导致 UI/日志难读
    """
    value = str(name or "").strip()
    if not value:
        return ""
    # 折叠所有空白（含换行/制表符）为单个空格
    value = " ".join(value.split())
    if len(value) > max_len:
        value = value[:max_len].rstrip()
    return value


def find_prompt_template_id_by_name(name: str) -> Optional[int]:
    """
    按 name 获取最新一条 prompt_template 的 id（同名允许多条，用最新的）。
    """
    normalized = normalize_prompt_template_name(name)
    if not normalized:
        return None
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id FROM prompt_templates WHERE name = ? ORDER BY id DESC LIMIT 1",
            (normalized,),
        ).fetchone()
    if not row:
        return None
    try:
        return int(row["id"])
    except Exception:
        return None


def ensure_llm_call_template(payload: dict, step_title: str) -> None:
    """
    执行器专用：归一化 llm_call.payload 中与 prompt_template 相关的字段。

    背景：
    - 任务执行链路允许 llm_call 指向 prompt_templates.template_id；
    - LLM 规划时可能把 template_id 写成字符串（例如模板名称或数字字符串）。

    策略：
    1) template_id 为数字字符串：转成 int
    2) template_id 为非数字字符串：按 name 尝试解析为“已存在”的模板 id

    约束：
    - 不做“缺失模板自动创建/自动恢复”，避免隐藏错误与引入隐式状态。
    """
    if not isinstance(payload, dict):
        return

    template_id = payload.get("template_id")

    if isinstance(template_id, str):
        normalized = normalize_prompt_template_name(template_id)
        if normalized.isdigit():
            payload["template_id"] = int(normalized)
        elif normalized:
            resolved = find_prompt_template_id_by_name(normalized)
            if resolved is not None:
                payload["template_id"] = resolved
        else:
            payload.pop("template_id", None)
