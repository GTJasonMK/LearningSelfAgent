from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from backend.src.agent.core.run_context import AgentRunContext
from backend.src.agent.runner.run_bootstrap import bootstrap_new_mode_run
from backend.src.agent.runner.stream_status_event import build_run_status_sse
from backend.src.agent.contracts.stream_events import coerce_session_key
from backend.src.constants import RUN_STATUS_RUNNING
from backend.src.services.llm.llm_client import sse_json


@dataclass
class StartedModeRun:
    task_id: int
    run_id: int
    run_ctx: AgentRunContext
    events: List[str]


async def start_new_mode_run(
    *,
    message: str,
    mode: str,
    model: str,
    parameters: dict,
    max_steps: Optional[int],
    workdir: str,
    stage_where_prefix: str,
    safe_write_debug=None,
    start_debug_message: Optional[str] = None,
    start_debug_data: Optional[Dict] = None,
    start_delta: Optional[str] = None,
    state_overrides: Optional[Dict] = None,
    tools_hint: Optional[str] = None,
    skills_hint: Optional[str] = None,
    solutions_hint: Optional[str] = None,
    memories_hint: Optional[str] = None,
    graph_hint: Optional[str] = None,
) -> StartedModeRun:
    """
    新建 run 的统一入口编排：
    - bootstrap（创建 run + retrieval 阶段持久化）；
    - 输出 run_created / stage 事件；
    - 记录 mode 启动调试信息；
    - 可选输出启动提示 delta。
    """
    boot = await bootstrap_new_mode_run(
        message=message,
        mode=mode,
        model=model,
        parameters=parameters,
        max_steps=max_steps,
        workdir=workdir,
        stage_where_prefix=stage_where_prefix,
        safe_write_debug=safe_write_debug,
        state_overrides=state_overrides,
        tools_hint=tools_hint,
        skills_hint=skills_hint,
        solutions_hint=solutions_hint,
        memories_hint=memories_hint,
        graph_hint=graph_hint,
    )

    events: List[str] = [str(boot.run_created_event)]
    session_key = coerce_session_key(boot.run_ctx.to_agent_state().get("session_key"))
    events.append(
        build_run_status_sse(
            status=RUN_STATUS_RUNNING,
            task_id=int(boot.task_id),
            run_id=int(boot.run_id),
            stage="retrieval",
            session_key=session_key,
        )
    )
    if isinstance(boot.stage_event, str) and boot.stage_event:
        events.append(str(boot.stage_event))

    if callable(safe_write_debug) and start_debug_message:
        safe_write_debug(
            int(boot.task_id),
            int(boot.run_id),
            message=str(start_debug_message),
            data=start_debug_data if isinstance(start_debug_data, dict) else None,
        )

    if isinstance(start_delta, str) and start_delta:
        events.append(sse_json({"delta": str(start_delta)}))

    return StartedModeRun(
        task_id=int(boot.task_id),
        run_id=int(boot.run_id),
        run_ctx=boot.run_ctx,
        events=events,
    )
