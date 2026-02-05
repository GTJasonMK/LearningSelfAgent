from typing import Optional, Tuple

from backend.src.common.serializers import task_output_from_row
from backend.src.constants import TASK_OUTPUT_TYPE_TEXT
from backend.src.repositories.task_outputs_repo import create_task_output, get_task_output
from backend.src.repositories.tasks_repo import task_exists


def _create_task_output_record(task_id: int, payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    写入 task_outputs 并返回序列化后的 output。

    说明：
    - 这是 actions 执行链路内部使用的轻量写入函数；
    - API 层的 /tasks/{task_id}/outputs 仍由路由对外提供。
    """
    if not task_exists(task_id=int(task_id)):
        return None, "task_not_found"

    output_id, _ = create_task_output(
        task_id=int(task_id),
        run_id=payload.get("run_id"),
        output_type=str(payload.get("output_type") or ""),
        content=str(payload.get("content") or ""),
    )
    row = get_task_output(output_id=int(output_id))
    return (task_output_from_row(row) if row else None), None


def execute_task_output(
    task_id: int,
    run_id: int,
    payload: dict,
    *,
    context: Optional[dict] = None,
) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 task_output：
    - content 允许为空：会尝试用 context.last_llm_response 自动补齐
    """
    content = payload.get("content")
    if not isinstance(content, str) or not content.strip():
        last_llm = (context or {}).get("last_llm_response")
        if isinstance(last_llm, str) and last_llm.strip():
            payload["content"] = last_llm
        else:
            return None, "task_output.content 不能为空"

    output_type = payload.get("output_type")
    if not isinstance(output_type, str) or not output_type.strip():
        payload["output_type"] = TASK_OUTPUT_TYPE_TEXT

    payload.setdefault("run_id", run_id)
    output, err = _create_task_output_record(task_id, payload)
    if err:
        return None, err
    return output, None
