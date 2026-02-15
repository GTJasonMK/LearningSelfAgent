from __future__ import annotations

import asyncio
import time
from typing import Callable, Optional

from backend.src.common.utils import now_iso
from backend.src.repositories.task_runs_repo import update_task_run


def persist_checkpoint(
    *,
    run_id: int,
    agent_state: dict,
    agent_plan: Optional[dict] = None,
    status: Optional[str] = None,
    clear_finished_at: bool = False,
    retries: int = 3,
    retry_backoff_seconds: float = 0.05,
    task_id: Optional[int] = None,
    safe_write_debug: Optional[Callable[..., None]] = None,
    where: str = "checkpoint",
) -> Optional[str]:
    """
    持久化检查点（带重试）。

    返回：
    - None：成功
    - str：最终失败错误文本
    """
    last_error = ""
    attempts = max(1, int(retries))
    for attempt in range(1, attempts + 1):
        try:
            update_task_run(
                run_id=int(run_id),
                status=str(status).strip() if isinstance(status, str) and str(status).strip() else None,
                agent_plan=agent_plan if isinstance(agent_plan, dict) else None,
                agent_state=agent_state if isinstance(agent_state, dict) else {},
                clear_finished_at=bool(clear_finished_at),
                updated_at=now_iso(),
            )
            return None
        except Exception as exc:
            last_error = str(exc)
            if callable(safe_write_debug):
                try:
                    safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.checkpoint.persist_retry",
                        data={
                            "where": str(where or "checkpoint"),
                            "status": str(status or ""),
                            "clear_finished_at": bool(clear_finished_at),
                            "attempt": attempt,
                            "retries": attempts,
                            "error": str(last_error),
                        },
                        level="warning",
                    )
                except Exception:
                    pass
            if attempt < attempts:
                try:
                    time.sleep(float(retry_backoff_seconds) * float(attempt))
                except Exception:
                    continue
    return last_error or "persist_checkpoint_failed"


async def persist_checkpoint_async(
    *,
    run_id: int,
    agent_state: dict,
    agent_plan: Optional[dict] = None,
    status: Optional[str] = None,
    clear_finished_at: bool = False,
    retries: int = 3,
    retry_backoff_seconds: float = 0.05,
    task_id: Optional[int] = None,
    safe_write_debug: Optional[Callable[..., None]] = None,
    where: str = "checkpoint",
) -> Optional[str]:
    return await asyncio.to_thread(
        persist_checkpoint,
        run_id=run_id,
        agent_state=agent_state,
        agent_plan=agent_plan,
        status=status,
        clear_finished_at=bool(clear_finished_at),
        retries=retries,
        retry_backoff_seconds=retry_backoff_seconds,
        task_id=task_id,
        safe_write_debug=safe_write_debug,
        where=where,
    )
