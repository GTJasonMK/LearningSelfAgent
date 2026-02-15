from __future__ import annotations

import logging
from typing import Optional

from backend.src.services.debug.debug_output import write_task_debug_output

logger = logging.getLogger(__name__)


def safe_write_debug(
    task_id: Optional[int],
    run_id: Optional[int],
    *,
    message: str,
    data: Optional[dict] = None,
    level: str = "debug",
) -> None:
    """
    调试输出公共入口。

    约束：
    - 调试写入失败不影响主链路；
    - 所有入口统一通过该函数写 task_outputs(debug)。
    """
    if task_id is None or run_id is None:
        return
    try:
        write_task_debug_output(
            task_id=int(task_id),
            run_id=int(run_id),
            message=message,
            data=data if isinstance(data, dict) else None,
            level=level,
        )
    except Exception:
        logger.exception("write_task_debug_output failed: %s", message)
