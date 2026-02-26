from __future__ import annotations

from backend.src.constants import RUN_STATUS_DONE, RUN_STATUS_FAILED, RUN_STATUS_STOPPED, STREAM_TAG_RESULT

TERMINAL_RESULT_STATUSES = {RUN_STATUS_DONE, RUN_STATUS_FAILED, RUN_STATUS_STOPPED}


def is_terminal_result_status(status: object) -> bool:
    return str(status or "").strip() in TERMINAL_RESULT_STATUSES


def build_missing_visible_result_body(*, run_status: str, task_id: int, run_id: int) -> str:
    status_text = str(run_status or "").strip()
    if status_text == RUN_STATUS_FAILED:
        return f"{STREAM_TAG_RESULT}\n任务执行失败（task={task_id}, run={run_id}）"
    return f"{STREAM_TAG_RESULT}\n任务已结束（task={task_id}, run={run_id}, status={status_text}）"

