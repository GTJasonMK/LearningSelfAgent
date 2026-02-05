from typing import Optional, Tuple

from backend.src.constants import ERROR_MESSAGE_PROMPT_RENDER_FAILED
from backend.src.services.memory.memory_items import create_memory_item as _create_memory_item


def execute_memory_write(task_id: int, payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 memory_write：写入 memory_items。
    """
    content = payload.get("content")
    if not isinstance(content, str) or not content.strip():
        raise ValueError("memory_write.content 不能为空")

    payload.setdefault("task_id", task_id)

    result = _create_memory_item(payload)
    item = result.get("item") if isinstance(result, dict) else None
    if not isinstance(item, dict):
        raise ValueError(ERROR_MESSAGE_PROMPT_RENDER_FAILED)
    return item, None
