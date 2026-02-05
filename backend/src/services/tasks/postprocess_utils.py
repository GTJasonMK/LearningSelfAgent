# -*- coding: utf-8 -*-
"""
任务后处理工具函数。

提供调试输出、JSON 压缩等通用功能。
"""

import json
import logging
from typing import Optional

from backend.src.common.utils import truncate_text
from backend.src.services.debug.debug_output import write_task_debug_output

logger = logging.getLogger(__name__)


def json_preview(value, max_chars: int) -> str:
    """
    评估 prompt 的体积控制：把复杂对象压缩为可读片段，避免塞爆上下文。

    Args:
        value: 要转换的值
        max_chars: 最大字符数

    Returns:
        压缩后的字符串
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return truncate_text(value, max_chars)
    try:
        return truncate_text(json.dumps(value, ensure_ascii=False), max_chars)
    except Exception:
        return truncate_text(str(value), max_chars)


def safe_write_debug(
    task_id: int,
    run_id: int,
    *,
    message: str,
    data: Optional[dict] = None,
    level: str = "debug",
) -> None:
    """
    后处理链路的调试输出不应影响主链路：失败时降级为 logger.exception。

    Args:
        task_id: 任务 ID
        run_id: 执行尝试 ID
        message: 调试消息
        data: 附加数据
        level: 日志级别
    """
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
