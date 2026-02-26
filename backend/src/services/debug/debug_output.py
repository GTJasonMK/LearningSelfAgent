import logging
import json
import sqlite3
from typing import Any, Optional

from backend.src.common.utils import coerce_int, truncate_text
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
    """
    约定：debug 输出使用 JSON（便于前端结构化渲染与过滤）。

    注意：
    - task_outputs.content 是 TEXT，无法存结构化字段，因此这里统一 JSON 序列化；
    - 为避免 truncate 破坏 JSON 结构：超长时将 data 降级为 data_preview，并标记 truncated=true。
    """
    level_value = str(level or "debug").strip().lower() or "debug"
    msg_value = str(message or "").strip() or "(empty)"
    max_chars = coerce_int(AGENT_DEBUG_OUTPUT_MAX_CHARS, default=1200)
    if max_chars <= 0:
        max_chars = 1200

    payload: dict[str, Any] = {"kind": "debug", "level": level_value, "message": msg_value}
    if isinstance(data, dict) and data:
        payload["data"] = data

    def _dump(obj: dict[str, Any]) -> str:
        try:
            return json.dumps(obj, ensure_ascii=False)
        except Exception:
            # 极端情况：对象不可序列化，降级为字符串
            return json.dumps({"kind": "debug", "level": level_value, "message": msg_value, "error": "json_dump_failed"}, ensure_ascii=False)

    content = _dump(payload)
    if len(content) > max_chars:
        preview = ""
        if isinstance(data, dict) and data:
            try:
                preview = json.dumps(data, ensure_ascii=False)
            except Exception:
                preview = str(data)
        payload = {
            "kind": "debug",
            "level": level_value,
            "message": truncate_text(msg_value, 240, strip=False) or "(empty)",
            "data_preview": truncate_text(preview, 720, strip=False),
            "truncated": True,
        }
        content = _dump(payload)

    if len(content) > max_chars:
        payload = {
            "kind": "debug",
            "level": level_value,
            "message": truncate_text(msg_value, 240, strip=False) or "(empty)",
            "truncated": True,
        }
        content = _dump(payload)

    task_id_value = coerce_int(task_id, default=0)
    run_id_value = coerce_int(run_id, default=0)
    if task_id_value <= 0 or run_id_value <= 0:
        return
    try:
        create_task_output(
            task_id=task_id_value,
            run_id=run_id_value,
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
