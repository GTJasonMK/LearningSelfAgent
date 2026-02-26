# -*- coding: utf-8 -*-
"""任务后处理工具函数。"""

from typing import Optional

from backend.src.common.utils import json_preview as _json_preview_impl
from backend.src.services.debug.safe_debug import safe_write_debug as _safe_write_debug_impl


def json_preview(value, max_chars: int) -> str:
    return _json_preview_impl(value, max_chars)


def safe_write_debug(
    task_id: int,
    run_id: int,
    *,
    message: str,
    data: Optional[dict] = None,
    level: str = "debug",
) -> None:
    _safe_write_debug_impl(
        task_id=task_id,
        run_id=run_id,
        message=message,
        data=data,
        level=level,
    )
