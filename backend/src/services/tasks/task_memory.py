# -*- coding: utf-8 -*-
"""
任务记忆后处理模块。

负责将任务执行结果写入记忆系统。
"""

import logging
from typing import List, Optional

from backend.src.constants import (
    DEFAULT_MEMORY_TYPE,
    MEMORY_AUTO_TASK_RESULT_MAX_CHARS,
    MEMORY_TAG_AUTO,
    MEMORY_TAG_TASK_RESULT,
    STREAM_TAG_RESULT,
    TASK_OUTPUT_TYPE_DEBUG,
)
from backend.src.common.utils import extract_json_object
from backend.src.repositories.memory_repo import find_memory_item_id_by_task_and_tag_like
from backend.src.repositories.task_outputs_repo import list_task_outputs_for_run
from backend.src.repositories.task_runs_repo import get_task_run
from backend.src.services.common.coerce import to_int
from backend.src.services.memory.memory_items import create_memory_item as create_memory_item_service

logger = logging.getLogger(__name__)


def write_task_result_memory_if_missing(
    *,
    task_id: int,
    run_id: int,
    title: str,
    output_rows: Optional[List[dict]] = None,
) -> Optional[dict]:
    """
    为一次成功 run 写入"任务结果摘要"到 memory_items（短期记忆）。

    设计目标：
    - 记忆面板不能长期为空（MVP 可用性）
    - 去重：同一个 run 只写一次（通过 tags 中的 run:{run_id} 判断）
    - 不依赖 LLM：直接从 task_outputs 里挑选可用输出并截断

    Args:
        task_id: 任务 ID
        run_id: 执行尝试 ID
        title: 任务标题
        output_rows: 可选的输出行列表，若为 None 则从数据库查询

    Returns:
        创建的记忆项字典，若已存在或创建失败则返回 None
    """
    title_value = str(title or "").strip()

    # 去重检查
    existed_id = find_memory_item_id_by_task_and_tag_like(
        task_id=to_int(task_id),
        tag_like=f"%run:{run_id}%",
    )
    if existed_id:
        return None

    # 获取输出内容
    rows = output_rows
    if rows is None:
        fetched = list_task_outputs_for_run(
            task_id=to_int(task_id),
            run_id=to_int(run_id),
            limit=20,
            order="DESC",
        )
        rows = [dict(row) for row in fetched] if fetched else []

    # 挑选第一条非 user_prompt 的输出
    picked = ""
    for row in rows or []:
        out_type = str((row or {}).get("output_type") or "")
        content = str((row or {}).get("content") or "").strip()
        if not content:
            continue
        if out_type == "user_prompt":
            continue
        if out_type == TASK_OUTPUT_TYPE_DEBUG:
            continue
        picked = content
        break

    # 备选：使用标题
    if not picked:
        picked = title_value

    # 清理内容
    picked = str(picked or "").strip()
    if picked.startswith(STREAM_TAG_RESULT):
        picked = picked[len(STREAM_TAG_RESULT):].strip()

    if not picked:
        return None

    # 截断
    if len(picked) > MEMORY_AUTO_TASK_RESULT_MAX_CHARS:
        picked = picked[:MEMORY_AUTO_TASK_RESULT_MAX_CHARS].rstrip()

    # 构造记忆文本
    memory_text = picked
    if title_value and picked != title_value:
        memory_text = f"任务：{title_value}\n结果：{picked}"

    # 增加 mode 标签（docs/agent 约定：便于后续按 do/think 过滤与溯源）
    mode_tag = "mode:do"
    try:
        run_row = get_task_run(run_id=to_int(run_id))
        state_obj = extract_json_object(run_row["agent_state"] or "") if run_row else None
        mode = str(state_obj.get("mode") or "").strip().lower() if isinstance(state_obj, dict) else ""
        if mode == "think":
            mode_tag = "mode:think"
    except Exception:
        mode_tag = "mode:do"

    # 创建记忆项（统一走 services 层，确保 DB 与 backend/prompt/memory 强一致落盘）
    result = create_memory_item_service(
        {
            "content": memory_text,
            "memory_type": DEFAULT_MEMORY_TYPE,
            "tags": [
                MEMORY_TAG_AUTO,
                MEMORY_TAG_TASK_RESULT,
                f"task:{task_id}",
                f"run:{run_id}",
                mode_tag,
            ],
            "task_id": to_int(task_id),
        }
    )
    item = result.get("item") if isinstance(result, dict) else None
    return item if isinstance(item, dict) else None
