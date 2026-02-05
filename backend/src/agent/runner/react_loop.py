"""
ReAct 执行循环（Facade）。

说明：
- 本模块作为“稳定入口”，供 Agent runner（new/resume）调用；
- 关键依赖（create_llm_call / _execute_step_action）在这里保留为模块级符号，便于单测 patch；
- 具体实现下沉到 react_loop_impl，降低耦合并提升可扩展性。
"""

from __future__ import annotations

from typing import Callable, Dict, Generator, List, Optional, Tuple

from backend.src.actions.executor import _execute_step_action
from backend.src.services.llm.llm_calls import create_llm_call

from backend.src.agent.runner.react_loop_impl import ReactLoopResult, run_react_loop_impl

__all__ = [
    "ReactLoopResult",
    "run_react_loop",
]


def run_react_loop(
    *,
    task_id: int,
    run_id: int,
    message: str,
    workdir: str,
    model: str,
    parameters: dict,
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
    plan_artifacts: List[str],
    tools_hint: str,
    skills_hint: str,
    memories_hint: str,
    graph_hint: str,
    agent_state: Dict,
    context: Dict,
    observations: List[str],
    start_step_order: int,
    variables_source: str,
    step_llm_config_resolver: Optional[
        Callable[[int, str, List[str]], Tuple[Optional[str], Optional[dict]]]
    ] = None,
) -> Generator[str, None, ReactLoopResult]:
    return run_react_loop_impl(
        task_id=int(task_id),
        run_id=int(run_id),
        message=message,
        workdir=workdir,
        model=model,
        parameters=parameters,
        plan_titles=plan_titles,
        plan_items=plan_items,
        plan_allows=plan_allows,
        plan_artifacts=plan_artifacts,
        tools_hint=tools_hint,
        skills_hint=skills_hint,
        memories_hint=memories_hint,
        graph_hint=graph_hint,
        agent_state=agent_state,
        context=context,
        observations=observations,
        start_step_order=int(start_step_order),
        variables_source=variables_source,
        llm_call=create_llm_call,
        execute_step_action=_execute_step_action,
        step_llm_config_resolver=step_llm_config_resolver,
    )
