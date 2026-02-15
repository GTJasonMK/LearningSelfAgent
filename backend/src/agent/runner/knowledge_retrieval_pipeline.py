# -*- coding: utf-8 -*-
"""
知识检索管道：
- 图谱检索
- 记忆检索
- 领域筛选
- 技能检索
- 方案匹配
- 汇总工具提示
"""

import asyncio
from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.src.constants import (
    STREAM_TAG_DOMAIN,
    STREAM_TAG_GRAPH,
    STREAM_TAG_MEMORY,
    STREAM_TAG_SKILLS,
)
from backend.src.services.llm.llm_client import sse_json
from backend.src.agent.runner.debug_utils import safe_write_debug


async def retrieve_graph_nodes(
    message: str,
    model: str,
    parameters: dict,
    yield_func: Callable,
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
    *,
    select_graph_nodes_func: Optional[Callable[..., Any]] = None,
    format_graph_for_prompt_func: Optional[Callable[..., str]] = None,
) -> Tuple[List[dict], str]:
    """
    检索相关图谱节点。
    """
    from backend.src.agent.support import _format_graph_for_prompt, _select_relevant_graph_nodes

    yield_func(sse_json({"delta": f"{STREAM_TAG_GRAPH} 检索图谱…\n"}))

    select_func = select_graph_nodes_func or _select_relevant_graph_nodes
    format_func = format_graph_for_prompt_func or _format_graph_for_prompt

    graph_nodes = await asyncio.to_thread(
        select_func,
        message=message,
        model=model,
        parameters=parameters,
    )
    graph_hint = format_func(graph_nodes) if graph_nodes else ""

    yield_func(sse_json({"delta": f"{STREAM_TAG_GRAPH} 已加载：{len(graph_nodes)} 个节点\n"}))

    safe_write_debug(
        task_id,
        run_id,
        message="retrieval.graph",
        data={"count": len(graph_nodes)},
    )

    return graph_nodes, graph_hint


async def retrieve_memories(
    message: str,
    model: str,
    parameters: dict,
    yield_func: Callable,
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
    *,
    select_memories_func: Optional[Callable[..., Any]] = None,
    format_memories_for_prompt_func: Optional[Callable[..., str]] = None,
) -> Tuple[List[dict], str]:
    """
    检索相关记忆。
    """
    from backend.src.agent.support import _format_memories_for_prompt, _select_relevant_memories

    yield_func(sse_json({"delta": f"{STREAM_TAG_MEMORY} 检索记忆…\n"}))

    select_func = select_memories_func or _select_relevant_memories
    format_func = format_memories_for_prompt_func or _format_memories_for_prompt

    memories = await asyncio.to_thread(
        select_func,
        message=message,
        model=model,
        parameters=parameters,
    )
    memories_hint = format_func(memories) if memories else ""

    yield_func(sse_json({"delta": f"{STREAM_TAG_MEMORY} 已加载：{len(memories)} 条\n"}))

    safe_write_debug(
        task_id,
        run_id,
        message="retrieval.memories",
        data={"count": len(memories)},
    )

    return memories, memories_hint


async def retrieve_domains(
    message: str,
    graph_hint: str,
    model: str,
    parameters: dict,
    yield_func: Callable,
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
    *,
    filter_relevant_domains_func: Optional[Callable[..., Any]] = None,
) -> List[str]:
    """
    检索相关领域。
    """
    from backend.src.agent.support import _filter_relevant_domains

    yield_func(sse_json({"delta": f"{STREAM_TAG_DOMAIN} 筛选领域…\n"}))

    domain_ids = await asyncio.to_thread(
        filter_relevant_domains_func or _filter_relevant_domains,
        message=message,
        graph_hint=graph_hint or "",
        model=model,
        parameters=parameters,
    )

    yield_func(sse_json({"delta": f"{STREAM_TAG_DOMAIN} 已匹配：{len(domain_ids)} 个领域\n"}))

    safe_write_debug(
        task_id,
        run_id,
        message="retrieval.domains",
        data={"domain_ids": domain_ids},
    )

    return domain_ids


async def retrieve_skills(
    message: str,
    model: str,
    parameters: dict,
    domain_ids: List[str],
    yield_func: Callable,
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
    *,
    select_skills_func: Optional[Callable[..., Any]] = None,
    format_skills_for_prompt_func: Optional[Callable[..., str]] = None,
) -> Tuple[List[dict], str]:
    """
    检索相关技能。
    """
    from backend.src.agent.support import _format_skills_for_prompt, _select_relevant_skills

    yield_func(sse_json({"delta": f"{STREAM_TAG_SKILLS} 检索技能…\n"}))

    select_func = select_skills_func or _select_relevant_skills
    format_func = format_skills_for_prompt_func or _format_skills_for_prompt

    skills = await asyncio.to_thread(
        select_func,
        message=message,
        model=model,
        parameters=parameters,
        domain_ids=domain_ids,
    )
    skills_hint = format_func(skills) if skills else ""

    yield_func(sse_json({"delta": f"{STREAM_TAG_SKILLS} 已加载：{len(skills)} 个技能\n"}))

    safe_write_debug(
        task_id,
        run_id,
        message="retrieval.skills",
        data={"count": len(skills)},
    )

    return skills, skills_hint


async def retrieve_solutions(
    message: str,
    skills: List[dict],
    domain_ids: List[str],
    model: str,
    parameters: dict,
    yield_func: Callable,
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
    *,
    select_solutions_func: Optional[Callable[..., Any]] = None,
    format_solutions_for_prompt_func: Optional[Callable[..., str]] = None,
) -> Tuple[List[dict], str]:
    """
    匹配相关方案（Solution）。
    """
    from backend.src.agent.support import _format_solutions_for_prompt, _select_relevant_solutions
    from backend.src.constants import STREAM_TAG_SOLUTIONS

    yield_func(sse_json({"delta": f"{STREAM_TAG_SOLUTIONS} 匹配方案…\n"}))

    select_func = select_solutions_func or _select_relevant_solutions
    format_func = format_solutions_for_prompt_func or _format_solutions_for_prompt

    solutions = await asyncio.to_thread(
        select_func,
        message=message,
        skills=skills or [],
        model=model,
        parameters=parameters,
        domain_ids=domain_ids or None,
    )
    solutions_hint = format_func(solutions) if solutions else ""

    yield_func(sse_json({"delta": f"{STREAM_TAG_SOLUTIONS} 已加载：{len(solutions)} 个\n"}))

    safe_write_debug(
        task_id,
        run_id,
        message="retrieval.solutions",
        data={"count": len(solutions)},
    )

    return solutions, solutions_hint


async def retrieve_all_knowledge(
    message: str,
    model: str,
    parameters: dict,
    yield_func: Callable,
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
    *,
    include_memories: bool = False,
    select_graph_nodes_func: Optional[Callable[..., Any]] = None,
    format_graph_for_prompt_func: Optional[Callable[..., str]] = None,
    select_memories_func: Optional[Callable[..., Any]] = None,
    format_memories_for_prompt_func: Optional[Callable[..., str]] = None,
    filter_relevant_domains_func: Optional[Callable[..., Any]] = None,
    select_skills_func: Optional[Callable[..., Any]] = None,
    format_skills_for_prompt_func: Optional[Callable[..., str]] = None,
    select_solutions_func: Optional[Callable[..., Any]] = None,
    format_solutions_for_prompt_func: Optional[Callable[..., str]] = None,
    collect_tools_from_solutions_func: Optional[Callable[..., str]] = None,
) -> Dict[str, Any]:
    """
    统一知识检索：图谱、领域、技能、方案、工具。
    """
    from backend.src.agent.support import _collect_tools_from_solutions

    graph_nodes, graph_hint = await retrieve_graph_nodes(
        message,
        model,
        parameters,
        yield_func,
        task_id,
        run_id,
        select_graph_nodes_func=select_graph_nodes_func,
        format_graph_for_prompt_func=format_graph_for_prompt_func,
    )

    memories: List[dict]
    memories_hint: str
    if bool(include_memories):
        memories, memories_hint = await retrieve_memories(
            message,
            model,
            parameters,
            yield_func,
            task_id,
            run_id,
            select_memories_func=select_memories_func,
            format_memories_for_prompt_func=format_memories_for_prompt_func,
        )
    else:
        memories = []
        memories_hint = "(无)"

    domain_ids = await retrieve_domains(
        message,
        graph_hint,
        model,
        parameters,
        yield_func,
        task_id,
        run_id,
        filter_relevant_domains_func=filter_relevant_domains_func,
    )

    skills, skills_hint = await retrieve_skills(
        message,
        model,
        parameters,
        domain_ids,
        yield_func,
        task_id,
        run_id,
        select_skills_func=select_skills_func,
        format_skills_for_prompt_func=format_skills_for_prompt_func,
    )

    solutions, solutions_hint = await retrieve_solutions(
        message,
        skills,
        domain_ids,
        model,
        parameters,
        yield_func,
        task_id,
        run_id,
        select_solutions_func=select_solutions_func,
        format_solutions_for_prompt_func=format_solutions_for_prompt_func,
    )

    collect_tools_func = collect_tools_from_solutions_func or _collect_tools_from_solutions
    tools_hint = collect_tools_func(solutions or [], limit=8)

    return {
        "graph_nodes": graph_nodes,
        "graph_hint": graph_hint,
        "memories": memories,
        "memories_hint": memories_hint,
        "domain_ids": domain_ids,
        "skills": skills,
        "skills_hint": skills_hint,
        "solutions": solutions,
        "solutions_hint": solutions_hint,
        "tools_hint": tools_hint,
    }
