import logging
import json
import sqlite3
from typing import Any, Optional

from backend.src.common.utils import truncate_text
from backend.src.constants import AGENT_DEBUG_OUTPUT_MAX_CHARS, TASK_OUTPUT_TYPE_DEBUG
from backend.src.repositories.task_outputs_repo import create_task_output

logger = logging.getLogger(__name__)

def write_task_debug_output(
    *,
    task_id: int,
    run_id: int,
    message: str,
    data: Optional[dict[str, Any]] = None,
    level: str = "debug",
) -> None:
    """
    写入一条“调试日志”到 task_outputs（output_type=debug）。

    设计目标：
    - 不引入额外日志系统（直接复用 task_outputs + /records/recent + timeline）
    - 让复杂 bug 可复盘：主链路关键节点（规划/解析/执行/修正/失败）都能留下可读痕迹
    - 控制体积：避免把超长 prompt/输出刷爆数据库与 UI
    """
    head = str(message or "").strip()

    suffix = ""
    if isinstance(data, dict) and data:
        try:
            suffix = json.dumps(data, ensure_ascii=False)
        except Exception:
            suffix = json.dumps({"data": str(data)}, ensure_ascii=False)

    content = head
    if suffix:
        content = f"{head} | {suffix}" if head else suffix

    content = content.strip() or "(empty)"
    if level:
        content = f"[{level}] {content}"

    # debug 输出允许保留换行等格式，因此不做 strip
    content = truncate_text(content, AGENT_DEBUG_OUTPUT_MAX_CHARS, strip=False) or "(empty)"
    try:
        create_task_output(
            task_id=int(task_id),
            run_id=int(run_id),
            output_type=TASK_OUTPUT_TYPE_DEBUG,
            content=content,
        )
    except (sqlite3.Error, OSError) as exc:
        # 调试输出本质是 best-effort：数据库被删除/磁盘异常/权限异常都不应影响主链路，也不应刷屏。
        logger.debug("write_task_debug_output skipped: %s", exc)
        return
    except Exception as exc:
        logger.debug("write_task_debug_output skipped: %s", exc)
        return
