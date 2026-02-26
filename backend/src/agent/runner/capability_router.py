from __future__ import annotations

from typing import Iterable, List

from backend.src.actions.registry import normalize_action_type


_CAPABILITY_BY_ACTION = {
    "http_request": "source_fetch",
    "tool_call": "source_fetch",
    "json_parse": "data_extract",
    "file_read": "workspace_io",
    "file_list": "workspace_io",
    "file_write": "workspace_io",
    "file_append": "workspace_io",
    "file_delete": "workspace_io",
    "shell_command": "system_exec",
    "llm_call": "reasoning",
    "task_output": "delivery",
    "memory_write": "memory",
    "user_prompt": "interaction",
}

_CAPABILITY_PRIORITY = {
    "system_exec": 100,
    "source_fetch": 90,
    "workspace_io": 80,
    "data_extract": 70,
    "reasoning": 60,
    "delivery": 50,
    "memory": 40,
    "interaction": 30,
}

_EXECUTOR_ROLE_HINT_BY_CAPABILITY = {
    "system_exec": "executor_code",
    "source_fetch": "executor_doc",
    "workspace_io": "executor_code",
    "data_extract": "executor_doc",
    "reasoning": "executor_brain",
    "delivery": "executor_brain",
    "memory": "executor_doc",
    "interaction": "executor_brain",
}


def resolve_step_capability(*, allowed_actions: Iterable[str], step_title: str) -> str:
    """
    根据 allow/action 解析本步能力标签（用于模型路由与提示约束）。
    """
    picked: List[str] = []
    for raw in list(allowed_actions or []):
        action = normalize_action_type(str(raw or "")) or str(raw or "").strip().lower()
        capability = _CAPABILITY_BY_ACTION.get(action)
        if capability:
            picked.append(capability)

    if not picked:
        title = str(step_title or "").strip().lower()
        if "抓取" in title or "fetch" in title or "http" in title:
            return "source_fetch"
        if "写" in title or "生成文件" in title or "file_" in title:
            return "workspace_io"
        if "验证" in title or "校验" in title or "review" in title:
            return "reasoning"
        return "general"

    return sorted(picked, key=lambda name: int(_CAPABILITY_PRIORITY.get(name, 0)), reverse=True)[0]


def resolve_executor_role_by_capability(*, capability: str, fallback_role: str) -> str:
    role = _EXECUTOR_ROLE_HINT_BY_CAPABILITY.get(str(capability or "").strip())
    return str(role or fallback_role or "").strip()


def build_capability_hint(*, capability: str) -> str:
    value = str(capability or "").strip()
    if not value:
        return ""
    return f"能力标签: {value}"

