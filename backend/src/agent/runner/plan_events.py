# -*- coding: utf-8 -*-
"""
SSE 计划栏事件（plan / plan_delta）。

背景：
- 旧实现会在每次状态变化时广播全量 plan_items（type=plan），大计划/并行执行下会造成 JSON 洪泛与前端频繁重渲染。
- 新实现提供 plan_delta：仅广播变更的步骤状态（以及可选 brief/title），前端做合并。

约定：
- type="plan"      : 全量快照（用于规划完成、结构变化（plan_patch/replan/反思插入步骤）、resume 兜底、收尾）。
- type="plan_delta": 增量变更（用于 running/done/failed/waiting/skipped 等状态更新）。
"""

from __future__ import annotations

from typing import Iterable, List

from backend.src.services.llm.llm_client import sse_json


def sse_plan(*, task_id: int, run_id: int, plan_items: List[dict]) -> str:
    """全量计划栏快照事件。"""
    return sse_json({"type": "plan", "task_id": int(task_id), "run_id": int(run_id), "items": plan_items})


def sse_plan_delta(*, task_id: int, run_id: int, plan_items: List[dict], indices: Iterable[int]) -> str:
    """
    增量计划栏事件：把 indices 指向的 plan_items 条目提取成 changes。

    注意：
    - 为兼容早期/异常 plan_items（缺少 id），这里会用 step_order（idx+1）兜底生成 id。
    - 前端合并时优先按 id 匹配，找不到再按 step_order 更新。
    """
    unique_indices = []
    seen = set()
    for raw in indices:
        try:
            idx = int(raw)
        except Exception:
            continue
        if idx in seen:
            continue
        seen.add(idx)
        unique_indices.append(idx)

    changes: List[dict] = []
    for idx in unique_indices:
        if idx < 0 or idx >= len(plan_items):
            continue
        item = plan_items[idx] if isinstance(plan_items[idx], dict) else {}

        raw_id = item.get("id") if isinstance(item, dict) else None
        try:
            step_id = int(raw_id)
        except Exception:
            step_id = 0
        if step_id <= 0:
            step_id = int(idx) + 1

        change = {"id": int(step_id), "step_order": int(idx) + 1}
        if isinstance(item, dict):
            if item.get("status") is not None:
                change["status"] = item.get("status")
            if item.get("brief") is not None:
                change["brief"] = item.get("brief")
            if item.get("title") is not None:
                change["title"] = item.get("title")
        changes.append(change)

    return sse_json({"type": "plan_delta", "task_id": int(task_id), "run_id": int(run_id), "changes": changes})
