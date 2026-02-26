from __future__ import annotations

from typing import Callable


def write_task_result_memory_safe(
    *,
    task_row,
    task_id: int,
    run_id: int,
    write_task_result_memory_if_missing_fn: Callable[..., object],
    safe_write_debug_fn: Callable[..., None],
) -> None:
    """
    写入任务结果短期记忆，失败不阻塞主流程。
    """
    try:
        title = str(task_row["title"] or "").strip() if task_row else ""
        write_task_result_memory_if_missing_fn(task_id=task_id, run_id=run_id, title=title)
    except Exception as exc:
        safe_write_debug_fn(
            int(task_id),
            int(run_id),
            message="memory.auto_task_result_failed",
            data={"error": str(exc)},
            level="warning",
        )
