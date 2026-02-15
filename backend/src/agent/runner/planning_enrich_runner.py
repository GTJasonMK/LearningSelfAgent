# -*- coding: utf-8 -*-
"""
planning enrich 流式执行器（do/think 共用）。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, AsyncGenerator, Awaitable, Callable, Dict

from backend.src.agent.runner.stream_task_events import iter_stream_task_events


@dataclass
class PlanningEnrichRunConfig:
    task_builder: Callable[[Callable[[str], None]], Awaitable[Dict[str, Any]]]
    empty_result_error: str


async def iter_planning_enrich_events(
    *,
    config: PlanningEnrichRunConfig,
 ) -> AsyncGenerator[tuple[str, Any], None]:
    """
    运行 planning enrich 子任务并透传事件：
    - ("msg", sse_chunk)
    - ("done", result_dict)
    """
    async for event_type, event_payload in iter_stream_task_events(task_builder=config.task_builder):
        if event_type == "msg":
            yield ("msg", str(event_payload))
            continue
        yield ("done", event_payload if isinstance(event_payload, dict) else {})


async def run_planning_enrich_with_stream(
    *,
    config: PlanningEnrichRunConfig,
    yield_func: Callable[[str], None],
) -> Dict[str, Any]:
    """
    运行 planning enrich 子任务并将 msg 直接写入回调，返回最终结果。
    """
    enriched: Any = None
    async for event_type, event_payload in iter_planning_enrich_events(config=config):
        if event_type == "msg":
            yield_func(str(event_payload))
            continue
        enriched = event_payload
    if not isinstance(enriched, dict):
        raise RuntimeError(str(config.empty_result_error or "planning enrich 结果为空"))
    return enriched
