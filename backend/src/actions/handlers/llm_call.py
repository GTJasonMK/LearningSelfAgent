from typing import Optional, Tuple

from backend.src.constants import ERROR_MESSAGE_LLM_CALL_FAILED, LLM_STATUS_ERROR
from backend.src.services.llm.llm_calls import create_llm_call as _create_llm_call


def execute_llm_call(task_id: int, run_id: int, payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 llm_call：
    - 复用 services 层 create_llm_call（其内部会写 llm_records）
    - create_llm_call 可能抛出业务异常（由 executor 统一捕获并转换为 step_error）
    - record.status=error（HTTP 仍为 200）时应视为失败
    """
    payload.setdefault("task_id", task_id)
    payload.setdefault("run_id", run_id)

    result = _create_llm_call(payload)
    record = result.get("record") if isinstance(result, dict) else None

    # create_llm_call 可能返回 status=error 的 record（HTTP 仍是 200）；任务执行应将其视为失败
    if isinstance(record, dict) and record.get("status") == LLM_STATUS_ERROR:
        error_text = (record.get("error") or "").strip() or ERROR_MESSAGE_LLM_CALL_FAILED
        raise ValueError(error_text)

    if not isinstance(record, dict):
        raise ValueError(ERROR_MESSAGE_LLM_CALL_FAILED)

    return record, None
