from __future__ import annotations

import logging
from typing import Optional

from backend.src.services.debug.debug_output import write_task_debug_output
from backend.src.services.common.coerce import to_int

logger = logging.getLogger(__name__)


def safe_write_debug(
    task_id: Optional[int],
    run_id: Optional[int],
    *,
    message: str,
    data: Optional[dict] = None,
    level: str = "debug",
) -> None:
    if task_id is None or run_id is None:
        return
    try:
        write_task_debug_output(
            task_id=to_int(task_id),
            run_id=to_int(run_id),
            message=message,
            data=data if isinstance(data, dict) else None,
            level=level,
        )
    except Exception:
        logger.exception("write_task_debug_output failed: %s", message)
