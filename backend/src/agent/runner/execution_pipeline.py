# -*- coding: utf-8 -*-
"""
Agent 执行管道公共逻辑。

提取 stream_new_run.py 和 stream_think_run.py 的重复代码，
提供统一的：
- 调试输出
- 知识检索
- 状态持久化
- 后处理闭环
- 异常处理
"""

import asyncio
import logging
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Tuple

from backend.src.constants import (
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_STOPPED,
    RUN_STATUS_WAITING,
    STREAM_TAG_DOMAIN,
    STREAM_TAG_FAIL,
    STREAM_TAG_GRAPH,
    STREAM_TAG_MEMORY,
    STREAM_TAG_SKILLS,
    SSE_TYPE_MEMORY_ITEM,
)
from backend.src.services.debug.debug_output import write_task_debug_output
from backend.src.services.llm.llm_client import sse_json
from backend.src.agent.runner.plan_events import sse_plan
from backend.src.services.tasks.task_run_lifecycle import (
    check_missing_artifacts,
    enqueue_postprocess_thread,
    enqueue_review_on_feedback_waiting,
    enqueue_stop_task_run_records,
    finalize_run_and_task_status,
    mark_run_failed,
)
from backend.src.repositories.task_runs_repo import update_task_run
from backend.src.common.utils import now_iso

logger = logging.getLogger(__name__)


async def pump_async_task_messages(
    task: "asyncio.Task[Any]",
    out_q: "asyncio.Queue[str]",
) -> AsyncGenerator[str, None]:
    """
    将“内部异步任务通过 out_q 输出的 SSE 字符串”转发为可 yield 的流。

    典型用法：
    - 内部函数签名：fn(..., yield_func=emit)
    - emit(msg) 只负责把 msg 写入 out_q（同步、非阻塞）
    - 外层 async generator 用本函数把 out_q 的消息逐条 yield 给客户端

    说明：
    - 避免使用 sleep/poll 的忙等方式；
    - 当 task 已完成且 out_q 为空时结束；
    - 不负责处理 task 的异常：由调用方 await task 时抛出。
    """
    while True:
        if task.done() and out_q.empty():
            break

        get_msg_task: "asyncio.Task[str]" = asyncio.create_task(out_q.get())
        done, pending = await asyncio.wait(
            {task, get_msg_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if get_msg_task in done:
            try:
                msg = get_msg_task.result()
            except Exception:
                msg = ""
            if msg:
                yield str(msg)
        else:
            get_msg_task.cancel()

        # task 完成后继续循环 drain 队列（直到 empty）


# ==============================================================================
# 调试输出
# ==============================================================================

def safe_write_debug(
    task_id: Optional[int],
    run_id: Optional[int],
    *,
    message: str,
    data: Optional[dict] = None,
    level: str = "debug",
) -> None:
    """
    调试输出不应影响主链路：失败时降级为 logger.exception。

    Args:
        task_id: 任务 ID
        run_id: 执行尝试 ID
        message: 调试消息
        data: 附加数据
        level: 日志级别
    """
    if task_id is None or run_id is None:
        return
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


# ==============================================================================
# 知识检索
# ==============================================================================

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

    Args:
        message: 用户消息
        model: LLM 模型
        parameters: LLM 参数
        yield_func: SSE yield 函数
        task_id: 任务 ID（用于调试）
        run_id: 执行 ID（用于调试）

    Returns:
        (graph_nodes, graph_hint) 图谱节点列表和格式化提示
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
        task_id, run_id,
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

    Returns:
        (memories, memories_hint) 记忆列表和格式化提示
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
        task_id, run_id,
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

    Returns:
        domain_ids 领域 ID 列表
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
        task_id, run_id,
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

    Returns:
        (skills, skills_hint) 技能列表和格式化提示
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
        task_id, run_id,
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

    docs/agent 对齐：方案匹配应基于已命中的技能，作为 planning 的参考流程。

    Returns:
        (solutions, solutions_hint)
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
        task_id, run_id,
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
    # 可注入的检索/格式化函数（用于单测 patch 与运行时一致性）
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

    说明（docs/agent 对齐）：
    - Memory 默认不参与检索与上下文注入（仅用于后处理沉淀与溯源）；
    - 如需在特定场景启用（例如偏好/配置类任务），可显式传 include_memories=True。

    Returns:
        包含所有检索结果的字典：
        {
            "graph_nodes": List[dict],
            "graph_hint": str,
            "memories": List[dict],
            "memories_hint": str,
            "domain_ids": List[str],
            "skills": List[dict],
            "skills_hint": str,
            "solutions": List[dict],
            "solutions_hint": str,
            "tools_hint": str,
        }
    """
    from backend.src.agent.support import _collect_tools_from_solutions

    # 图谱检索
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

    # 领域筛选
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

    # 技能检索
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

    # 方案匹配
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

    # 工具汇总：方案提到的工具优先（无方案时回退到已注册工具）
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


async def maybe_draft_solution_for_planning(
    message: str,
    model: str,
    parameters: dict,
    *,
    skills: List[dict],
    solutions: List[dict],
    graph_hint: str,
    domain_ids: List[str],
    mode: str,
    yield_func: Callable,
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
    # 可注入的函数（用于单测 patch 与运行时一致性）
    draft_solution_from_skills_func: Optional[Callable[..., Any]] = None,
    create_skill_func: Optional[Callable[..., Any]] = None,
    publish_skill_file_func: Optional[Callable[..., Any]] = None,
    collect_tools_from_solutions_func: Optional[Callable[..., str]] = None,
    tools_limit: int = 8,
    debug_message_prefix: str = "agent.solution_draft",
) -> Dict[str, Any]:
    """
    planning 阶段“方案草稿（Create 流程 A）”兜底（docs/agent 对齐）：
    - 当“有技能但无匹配方案”时，基于技能草拟一个 draft 方案（写入 skills_items.skill_type=solution,status=draft）
    - 将草稿方案注入 planning 用的 solutions_hint，提升规划质量

    注意：
    - 草拟失败不应阻塞 planning：失败时返回原 solutions（通常为空），继续主链路
    - 草稿方案会尝试落盘到 backend/prompt/skills（失败也不阻塞）
    """
    from backend.src.agent.support import _collect_tools_from_solutions, _draft_solution_from_skills
    from backend.src.constants import STREAM_TAG_SOLUTIONS
    from backend.src.repositories.skills_repo import SkillCreateParams, create_skill
    from backend.src.services.skills.skills_publish import publish_skill_file

    draft_solution_func = draft_solution_from_skills_func or _draft_solution_from_skills
    create_func = create_skill_func or create_skill
    publish_func = publish_skill_file_func or publish_skill_file
    collect_tools_func = collect_tools_from_solutions_func or _collect_tools_from_solutions

    solutions_for_prompt = list(solutions or [])
    draft_solution_id: Optional[int] = None

    # 缺少 run_id/task_id 时不创建草稿（避免产生“不可溯源”的孤儿知识）
    if task_id is None or run_id is None:
        return {"solutions_for_prompt": solutions_for_prompt, "draft_solution_id": None}

    if solutions_for_prompt or not skills:
        return {"solutions_for_prompt": solutions_for_prompt, "draft_solution_id": None}

    mode_tag = str(mode or "").strip().lower() or "do"

    try:
        tools_limit_value = int(tools_limit)
    except Exception:
        tools_limit_value = 8
    if tools_limit_value <= 0:
        tools_limit_value = 8

    yield_func(sse_json({"delta": f"{STREAM_TAG_SOLUTIONS} 无匹配方案，草拟草稿方案…\n"}))
    draft_tools_hint = collect_tools_func([], limit=int(tools_limit_value))
    draft_result = await asyncio.to_thread(
        draft_solution_func,
        message=message,
        skills=skills or [],
        tools_hint=draft_tools_hint,
        graph_hint=graph_hint,
        model=model,
        parameters=parameters,
        max_steps=8,
    )

    if not bool(getattr(draft_result, "success", False)):
        yield_func(sse_json({"delta": f"{STREAM_TAG_SOLUTIONS} 草拟失败：{getattr(draft_result, 'error', '')}\n"}))
        safe_write_debug(
            task_id,
            run_id,
            message=f"{debug_message_prefix}.failed",
            data={"error": str(getattr(draft_result, "error", "") or "")},
            level="warning",
        )
        return {"solutions_for_prompt": solutions_for_prompt, "draft_solution_id": None}

    domain_id = str((domain_ids or ["misc"])[0] or "").strip() or "misc"

    skill_ids_for_tags: List[int] = []
    seen = set()
    for s in skills or []:
        if not isinstance(s, dict):
            continue
        raw_id = s.get("id")
        try:
            sid = int(raw_id)
        except Exception:
            continue
        if sid <= 0 or sid in seen:
            continue
        seen.add(sid)
        skill_ids_for_tags.append(sid)
        if len(skill_ids_for_tags) >= 24:
            break

    tags = [
        "solution",
        "draft_solution",
        f"task:{int(task_id)}",
        f"run:{int(run_id)}",
        f"mode:{mode_tag}",
        f"domain:{domain_id}",
    ]
    for sid in skill_ids_for_tags:
        tags.append(f"skill:{int(sid)}")
    for name in (getattr(draft_result, "tool_names", None) or [])[:12]:
        tags.append(f"tool_name:{str(name).strip()}")

    try:
        draft_solution_id = await asyncio.to_thread(
            create_func,
            SkillCreateParams(
                name=str(getattr(draft_result, "name", "") or "").strip() or f"草稿方案#{int(run_id)}",
                description=str(getattr(draft_result, "description", "") or "").strip() or str(message or "").strip(),
                scope=f"solution:draft:run:{int(run_id)}",
                category="solution",
                tags=tags,
                triggers=[],
                aliases=[],
                prerequisites=[],
                inputs=[str(message or "").strip()] if str(message or "").strip() else [],
                outputs=list(getattr(draft_result, "artifacts", None) or []),
                steps=list(getattr(draft_result, "steps", None) or []),
                failure_modes=[],
                validation=[],
                version="0.1.0",
                task_id=int(task_id),
                domain_id=domain_id,
                skill_type="solution",
                status="draft",
                source_task_id=int(task_id),
                source_run_id=int(run_id),
            ),
        )
    except Exception as exc:
        yield_func(sse_json({"delta": f"{STREAM_TAG_SOLUTIONS} 草稿方案落库失败，继续规划\n"}))
        safe_write_debug(
            task_id,
            run_id,
            message=f"{debug_message_prefix}.create_failed",
            data={"error": str(exc)},
            level="warning",
        )
        return {"solutions_for_prompt": solutions_for_prompt, "draft_solution_id": None}

    # 草稿方案也需要落盘（可编辑/可恢复）
    try:
        _source_path, publish_err = await asyncio.to_thread(publish_func, int(draft_solution_id))
        if publish_err:
            safe_write_debug(
                task_id,
                run_id,
                message=f"{debug_message_prefix}.publish_failed",
                data={"draft_solution_id": int(draft_solution_id), "error": str(publish_err)},
                level="warning",
            )
    except Exception as exc:
        safe_write_debug(
            task_id,
            run_id,
            message=f"{debug_message_prefix}.publish_failed",
            data={"draft_solution_id": int(draft_solution_id), "error": str(exc)},
            level="warning",
        )

    solutions_for_prompt = [
        {
            "id": int(draft_solution_id),
            "name": str(getattr(draft_result, "name", "") or "").strip(),
            "description": str(getattr(draft_result, "description", "") or "").strip(),
            "steps": list(getattr(draft_result, "steps", None) or []),
            "domain_id": domain_id,
            "skill_type": "solution",
            "status": "draft",
        }
    ]

    yield_func(
        sse_json(
            {
                "delta": (
                    f"{STREAM_TAG_SOLUTIONS} 已草拟草稿方案："
                    f"#{int(draft_solution_id)} {getattr(draft_result, 'name', '')}\n"
                )
            }
        )
    )
    safe_write_debug(
        task_id,
        run_id,
        message=f"{debug_message_prefix}.created",
        data={
            "draft_solution_id": int(draft_solution_id),
            "name": str(getattr(draft_result, "name", "") or ""),
            "tool_names": list(getattr(draft_result, "tool_names", None) or []),
            "artifacts": list(getattr(draft_result, "artifacts", None) or []),
        },
        level="info",
    )

    return {
        "solutions_for_prompt": solutions_for_prompt,
        "draft_solution_id": int(draft_solution_id),
    }


async def prepare_planning_knowledge_do(
    message: str,
    model: str,
    parameters: dict,
    *,
    graph_nodes: List[dict],
    graph_hint: str,
    domain_ids: List[str],
    skills: List[dict],
    skills_hint: str,
    solutions: List[dict],
    yield_func: Callable,
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
    # 可注入的函数（用于单测 patch 与运行时一致性）
    assess_knowledge_sufficiency_func: Optional[Callable[..., Any]] = None,
    compose_skills_func: Optional[Callable[..., Any]] = None,
    draft_skill_from_message_func: Optional[Callable[..., Any]] = None,
    draft_solution_from_skills_func: Optional[Callable[..., Any]] = None,
    create_skill_func: Optional[Callable[..., Any]] = None,
    publish_skill_file_func: Optional[Callable[..., Any]] = None,
    format_skills_for_prompt_func: Optional[Callable[..., str]] = None,
    format_solutions_for_prompt_func: Optional[Callable[..., str]] = None,
    collect_tools_from_solutions_func: Optional[Callable[..., str]] = None,
    # 可复用参数：用于 think 模式调用同一实现
    mode_tag: str = "do",
    tools_limit: int = 8,
    solution_draft_debug_prefix: str = "agent.solution_draft",
) -> Dict[str, Any]:
    """
    do 模式 planning 前的“知识增强”收敛点（docs/agent 对齐）：
    - 知识充分性判断
    - compose_skills 分支（创建 draft 技能并注入 skills_hint）
    - 无匹配方案时草拟 draft 方案（Solution Create 流程 A）
    - 统一重算 solutions_hint / tools_hint

    注意：
    - 本函数只负责 planning 使用的 hints/草稿兜底，不改变既有检索结果的语义；
    - 草稿知识一律 status=draft，仅用于当前任务的 planning 参考（后续任务检索默认不返回 draft）。

    Returns:
        {
          "skills": List[dict],
          "skills_hint": str,
          "solutions_for_prompt": List[dict],
          "solutions_hint": str,
          "tools_hint": str,
          "draft_solution_id": Optional[int],
          "need_user_prompt": bool,
          "user_prompt_question": str,
          "sufficiency_result": KnowledgeSufficiencyResult,
        }
    """
    from backend.src.agent.support import (
        _assess_knowledge_sufficiency,
        _collect_tools_from_solutions,
        _compose_skills,
        _draft_skill_from_message,
        _format_skills_for_prompt,
        _format_solutions_for_prompt,
    )
    from backend.src.constants import STREAM_TAG_KNOWLEDGE, STREAM_TAG_SOLUTIONS
    from backend.src.repositories.skills_repo import SkillCreateParams, create_skill
    from backend.src.services.skills.skills_publish import publish_skill_file

    assess_func = assess_knowledge_sufficiency_func or _assess_knowledge_sufficiency
    compose_func = compose_skills_func or _compose_skills
    draft_skill_func = draft_skill_from_message_func or _draft_skill_from_message
    create_func = create_skill_func or create_skill
    publish_func = publish_skill_file_func or publish_skill_file
    format_skills_func = format_skills_for_prompt_func or _format_skills_for_prompt
    format_solutions_func = format_solutions_for_prompt_func or _format_solutions_for_prompt
    collect_tools_func = collect_tools_from_solutions_func or _collect_tools_from_solutions

    mode_value = str(mode_tag or "").strip().lower() or "do"
    try:
        tools_limit_value = int(tools_limit)
    except Exception:
        tools_limit_value = 8
    if tools_limit_value <= 0:
        tools_limit_value = 8

    need_user_prompt = False
    user_prompt_question = ""

    # --- 知识充分性判断 ---
    yield_func(sse_json({"delta": f"{STREAM_TAG_KNOWLEDGE} 评估知识充分性…\n"}))
    sufficiency_result = await asyncio.to_thread(
        assess_func,
        message=message,
        skills=skills or [],
        graph_nodes=graph_nodes or [],
        memories=[],
        model=model,
        parameters=parameters,
    )
    safe_write_debug(
        task_id,
        run_id,
        message="agent.knowledge_sufficiency",
        data={
            "sufficient": getattr(sufficiency_result, "sufficient", None),
            "reason": getattr(sufficiency_result, "reason", None),
            "missing_knowledge": getattr(sufficiency_result, "missing_knowledge", None),
            "suggestion": getattr(sufficiency_result, "suggestion", None),
            "skill_count": getattr(sufficiency_result, "skill_count", None),
            "graph_count": getattr(sufficiency_result, "graph_count", None),
            "memory_count": getattr(sufficiency_result, "memory_count", None),
        },
    )

    if bool(getattr(sufficiency_result, "sufficient", True)):
        yield_func(sse_json({"delta": f"{STREAM_TAG_KNOWLEDGE} 知识充分，继续规划\n"}))
    else:
        yield_func(
            sse_json(
                {
                    "delta": (
                        f"{STREAM_TAG_KNOWLEDGE} 知识不足："
                        f"{getattr(sufficiency_result, 'reason', '')}，建议："
                        f"{getattr(sufficiency_result, 'suggestion', '')}\n"
                    )
                }
            )
        )

        suggestion = str(getattr(sufficiency_result, "suggestion", "") or "").strip()

        # 根据建议采取不同动作
        if suggestion == "compose_skills" and skills:
            yield_func(sse_json({"delta": f"{STREAM_TAG_KNOWLEDGE} 尝试组合已有技能…\n"}))
            compose_result = await asyncio.to_thread(
                compose_func,
                message=message,
                skills=skills,
                model=model,
                parameters=parameters,
            )
            if bool(getattr(compose_result, "success", False)):
                draft_skill_id: Optional[int] = None
                if task_id is not None and run_id is not None:
                    # 创建 draft 技能（技能组合产物属于 methodology，而不是 end-to-end 的 solution）
                    try:
                        draft_skill_id = await asyncio.to_thread(
                            create_func,
                            SkillCreateParams(
                                name=str(getattr(compose_result, "name", "") or ""),
                                description=str(getattr(compose_result, "description", "") or ""),
                                steps=list(getattr(compose_result, "steps", []) or []),
                                task_id=int(task_id),
                                domain_id=str(getattr(compose_result, "domain_id", "") or "misc"),
                                skill_type="methodology",
                                status="draft",
                                source_task_id=int(task_id),
                                source_run_id=int(run_id),
                            ),
                        )
                    except Exception as exc:
                        draft_skill_id = None
                        safe_write_debug(
                            task_id,
                            run_id,
                            message="agent.skill_compose.create_failed",
                            data={"error": str(exc)},
                            level="warning",
                        )
                    yield_func(
                        sse_json(
                            {
                                "delta": (
                                    f"{STREAM_TAG_KNOWLEDGE} 已创建组合技能："
                                    f"{getattr(compose_result, 'name', '')}"
                                    + (
                                        f"（草稿 #{int(draft_skill_id)}）\n"
                                        if isinstance(draft_skill_id, int) and int(draft_skill_id) > 0
                                        else "（落库失败，仍可继续规划）\n"
                                    )
                                )
                            }
                        )
                    )

                    # docs/agent 约定：草稿技能同样需要落盘到 backend/prompt/skills（可编辑/可恢复的“灵魂存档”）。
                    if isinstance(draft_skill_id, int) and int(draft_skill_id) > 0:
                        try:
                            source_path, publish_err = await asyncio.to_thread(publish_func, int(draft_skill_id))
                            if publish_err:
                                safe_write_debug(
                                    task_id,
                                    run_id,
                                    message="agent.skill_compose.publish_failed",
                                    data={
                                        "draft_skill_id": int(draft_skill_id),
                                        "error": str(publish_err),
                                    },
                                    level="warning",
                                )
                            else:
                                safe_write_debug(
                                    task_id,
                                    run_id,
                                    message="agent.skill_compose.published",
                                    data={
                                        "draft_skill_id": int(draft_skill_id),
                                        "source_path": source_path,
                                    },
                                    level="info",
                                )
                        except Exception as exc:
                            safe_write_debug(
                                task_id,
                                run_id,
                                message="agent.skill_compose.publish_failed",
                                data={
                                    "draft_skill_id": int(draft_skill_id),
                                    "error": str(exc),
                                },
                                level="warning",
                            )

                    safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.skill_compose.success",
                        data={
                            "draft_skill_id": int(draft_skill_id),
                            "name": str(getattr(compose_result, "name", "") or ""),
                            "source_skill_ids": list(getattr(compose_result, "source_skill_ids", []) or []),
                        },
                    )

                    # 将新创建的 draft 技能添加到技能列表供规划使用
                    if isinstance(draft_skill_id, int) and int(draft_skill_id) > 0:
                        skills.append(
                            {
                                "id": int(draft_skill_id),
                                "name": str(getattr(compose_result, "name", "") or ""),
                                "description": str(getattr(compose_result, "description", "") or ""),
                                "steps": list(getattr(compose_result, "steps", []) or []),
                                "domain_id": str(getattr(compose_result, "domain_id", "") or "misc"),
                                "skill_type": "methodology",
                                "status": "draft",
                            }
                        )
                        skills_hint = format_skills_func(skills)
            else:
                yield_func(
                    sse_json(
                        {
                            "delta": (
                                f"{STREAM_TAG_KNOWLEDGE} 技能组合失败："
                                f"{getattr(compose_result, 'error', '')}，继续使用现有知识\n"
                            )
                        }
                    )
                )
                safe_write_debug(
                    task_id,
                    run_id,
                    message="agent.skill_compose.failed",
                    data={"error": getattr(compose_result, "error", None)},
                )
        elif suggestion == "create_draft_skill":
            yield_func(sse_json({"delta": f"{STREAM_TAG_KNOWLEDGE} 尝试草拟草稿技能…\n"}))
            try:
                domain_id = str((domain_ids or ["misc"])[0] or "").strip() or "misc"
            except Exception:
                domain_id = "misc"

            draft_result = await asyncio.to_thread(
                draft_skill_func,
                message=message,
                skills=skills or [],
                graph_hint=graph_hint,
                domain_id=domain_id,
                model=model,
                parameters=parameters,
            )
            if bool(getattr(draft_result, "success", False)):
                draft_skill_id: Optional[int] = None
                if task_id is not None and run_id is not None:
                    try:
                        draft_skill_id = await asyncio.to_thread(
                            create_func,
                            SkillCreateParams(
                                name=str(getattr(draft_result, "name", "") or ""),
                                description=str(getattr(draft_result, "description", "") or ""),
                                steps=list(getattr(draft_result, "steps", []) or []),
                                task_id=int(task_id),
                                domain_id=str(getattr(draft_result, "domain_id", "") or domain_id),
                                skill_type="methodology",
                                status="draft",
                                source_task_id=int(task_id),
                                source_run_id=int(run_id),
                            ),
                        )
                    except Exception as exc:
                        draft_skill_id = None
                        safe_write_debug(
                            task_id,
                            run_id,
                            message="agent.skill_draft.create_failed",
                            data={"error": str(exc)},
                            level="warning",
                        )

                yield_func(
                    sse_json(
                        {
                            "delta": (
                                f"{STREAM_TAG_KNOWLEDGE} 已草拟技能："
                                f"{getattr(draft_result, 'name', '')}"
                                + (
                                    f"（草稿 #{int(draft_skill_id)}）\n"
                                    if isinstance(draft_skill_id, int) and int(draft_skill_id) > 0
                                    else "（落库失败，仍可继续规划）\n"
                                )
                            )
                        }
                    )
                )

                if isinstance(draft_skill_id, int) and int(draft_skill_id) > 0:
                    # 草稿技能落盘（失败不阻塞）
                    try:
                        _source_path, publish_err = await asyncio.to_thread(publish_func, int(draft_skill_id))
                        if publish_err:
                            safe_write_debug(
                                task_id,
                                run_id,
                                message="agent.skill_draft.publish_failed",
                                data={"draft_skill_id": int(draft_skill_id), "error": str(publish_err)},
                                level="warning",
                            )
                    except Exception as exc:
                        safe_write_debug(
                            task_id,
                            run_id,
                            message="agent.skill_draft.publish_failed",
                            data={"draft_skill_id": int(draft_skill_id), "error": str(exc)},
                            level="warning",
                        )

                    skills.append(
                        {
                            "id": int(draft_skill_id),
                            "name": str(getattr(draft_result, "name", "") or ""),
                            "description": str(getattr(draft_result, "description", "") or ""),
                            "steps": list(getattr(draft_result, "steps", []) or []),
                            "domain_id": str(getattr(draft_result, "domain_id", "") or domain_id),
                            "skill_type": "methodology",
                            "status": "draft",
                        }
                    )
                    skills_hint = format_skills_func(skills)
            else:
                yield_func(
                    sse_json(
                        {
                            "delta": (
                                f"{STREAM_TAG_KNOWLEDGE} 草拟技能失败："
                                f"{getattr(draft_result, 'error', '')}，继续使用现有知识\n"
                            )
                        }
                    )
                )
                safe_write_debug(
                    task_id,
                    run_id,
                    message="agent.skill_draft.failed",
                    data={"error": getattr(draft_result, "error", None)},
                    level="warning",
                )
        elif suggestion == "ask_user":
            need_user_prompt = True
            reason = str(getattr(sufficiency_result, "reason", "") or "").strip()
            missing = str(getattr(sufficiency_result, "missing_knowledge", "") or "").strip()
            if reason:
                user_prompt_question = f"为了继续规划，请补充信息：{reason}"
            elif missing and missing != "none":
                user_prompt_question = f"为了继续规划，请补充与 {missing} 相关的关键信息。"
            else:
                user_prompt_question = "为了继续规划，请补充任务的关键约束（输入/输出/环境/期望产物）。"
            yield_func(sse_json({"delta": f"{STREAM_TAG_KNOWLEDGE} 需要用户补充信息，进入等待…\n"}))

            return {
                "skills": skills,
                "skills_hint": skills_hint or "(无)",
                "solutions_for_prompt": list(solutions or []),
                "solutions_hint": format_solutions_func(list(solutions or [])) or "(无)",
                "tools_hint": collect_tools_func(list(solutions or []), limit=int(tools_limit_value)) or "(无)",
                "draft_solution_id": None,
                "need_user_prompt": True,
                "user_prompt_question": user_prompt_question,
                "sufficiency_result": sufficiency_result,
            }

    draft = await maybe_draft_solution_for_planning(
        message=message,
        model=model,
        parameters=parameters,
        skills=skills,
        solutions=solutions,
        graph_hint=graph_hint,
        domain_ids=domain_ids,
        mode=str(mode_value),
        yield_func=yield_func,
        task_id=task_id,
        run_id=run_id,
        draft_solution_from_skills_func=draft_solution_from_skills_func,
        create_skill_func=create_skill_func,
        publish_skill_file_func=publish_skill_file_func,
        collect_tools_from_solutions_func=collect_tools_from_solutions_func,
        tools_limit=int(tools_limit_value),
        debug_message_prefix=str(solution_draft_debug_prefix or "agent.solution_draft"),
    )
    solutions_for_prompt = list(draft.get("solutions_for_prompt") or solutions or [])
    draft_solution_id = draft.get("draft_solution_id")

    solutions_hint = format_solutions_func(solutions_for_prompt)
    if solutions:
        names = ", ".join(str(s.get("name") or "").strip() for s in solutions if isinstance(s, dict) and s.get("name"))
        if names:
            yield_func(sse_json({"delta": f"{STREAM_TAG_SOLUTIONS} 已加载：{names}\n"}))
        else:
            yield_func(sse_json({"delta": f"{STREAM_TAG_SOLUTIONS} 已加载：{len(solutions)} 个\n"}))
    else:
        # 若草拟成功，则此处不再输出“未命中”，避免用户误解为“完全无方案”。
        if not draft_solution_id:
            yield_func(sse_json({"delta": f"{STREAM_TAG_SOLUTIONS} 未命中\n"}))

    ids = [int(s.get("id")) for s in (solutions or [])[:8] if isinstance(s, dict) and s.get("id") is not None]
    names = [
        str(s.get("name") or "").strip()
        for s in (solutions or [])[:8]
        if isinstance(s, dict) and str(s.get("name") or "").strip()
    ]
    safe_write_debug(
        task_id,
        run_id,
        message="agent.solutions",
        data={
            "count": len(solutions or []),
            "ids": ids,
            "names": names,
            "draft_solution_id": int(draft_solution_id) if draft_solution_id else None,
        },
    )

    # --- 工具汇总：方案提到的工具优先 ---
    tools_hint = collect_tools_func(solutions_for_prompt or [], limit=int(tools_limit_value))

    return {
        "skills": skills,
        "skills_hint": skills_hint or "(无)",
        "solutions_for_prompt": solutions_for_prompt,
        "solutions_hint": solutions_hint or "(无)",
        "tools_hint": tools_hint or "(无)",
        "draft_solution_id": int(draft_solution_id) if draft_solution_id else None,
        "need_user_prompt": bool(need_user_prompt),
        "user_prompt_question": str(user_prompt_question or ""),
        "sufficiency_result": sufficiency_result,
    }


async def prepare_planning_knowledge_think(
    message: str,
    model: str,
    parameters: dict,
    *,
    graph_nodes: List[dict],
    graph_hint: str,
    domain_ids: List[str],
    skills: List[dict],
    skills_hint: str,
    solutions: List[dict],
    yield_func: Callable,
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
    # 可注入的函数（用于单测 patch 与运行时一致性）
    assess_knowledge_sufficiency_func: Optional[Callable[..., Any]] = None,
    compose_skills_func: Optional[Callable[..., Any]] = None,
    draft_skill_from_message_func: Optional[Callable[..., Any]] = None,
    draft_solution_from_skills_func: Optional[Callable[..., Any]] = None,
    create_skill_func: Optional[Callable[..., Any]] = None,
    publish_skill_file_func: Optional[Callable[..., Any]] = None,
    format_skills_for_prompt_func: Optional[Callable[..., str]] = None,
    format_solutions_for_prompt_func: Optional[Callable[..., str]] = None,
    collect_tools_from_solutions_func: Optional[Callable[..., str]] = None,
    tools_limit: int = 12,
) -> Dict[str, Any]:
    """
    think 模式 planning 前的“知识增强”：
    - 与 do 模式复用同一套逻辑（知识充分性判断 / compose_skills / create_draft_skill / ask_user / 草拟 draft solution）
    - 区别：默认 tools_limit 更高（docs/agent：think 更需要更多工具候选）
    """
    return await prepare_planning_knowledge_do(
        message=message,
        model=model,
        parameters=parameters,
        graph_nodes=graph_nodes,
        graph_hint=graph_hint,
        domain_ids=domain_ids,
        skills=skills,
        skills_hint=skills_hint,
        solutions=solutions,
        yield_func=yield_func,
        task_id=task_id,
        run_id=run_id,
        assess_knowledge_sufficiency_func=assess_knowledge_sufficiency_func,
        compose_skills_func=compose_skills_func,
        draft_skill_from_message_func=draft_skill_from_message_func,
        draft_solution_from_skills_func=draft_solution_from_skills_func,
        create_skill_func=create_skill_func,
        publish_skill_file_func=publish_skill_file_func,
        format_skills_for_prompt_func=format_skills_for_prompt_func,
        format_solutions_for_prompt_func=format_solutions_for_prompt_func,
        collect_tools_from_solutions_func=collect_tools_from_solutions_func,
        mode_tag="think",
        tools_limit=int(tools_limit) if isinstance(tools_limit, int) else 12,
        solution_draft_debug_prefix="agent.think.solution_draft",
    )


# ==============================================================================
# 状态持久化
# ==============================================================================

async def persist_agent_state(
    run_id: int,
    agent_plan: dict,
    agent_state: dict,
) -> None:
    """
    持久化 Agent 运行态到数据库。

    Args:
        run_id: 执行尝试 ID
        agent_plan: 计划数据
        agent_state: 状态数据
    """
    updated_at = now_iso()
    await asyncio.to_thread(
        update_task_run,
        run_id=run_id,
        agent_plan=agent_plan,
        agent_state=agent_state,
        updated_at=updated_at,
    )


def build_base_agent_state(
    message: str,
    model: str,
    parameters: dict,
    max_steps: int,
    workdir: str,
    tools_hint: str,
    skills_hint: str,
    memories_hint: str,
    graph_hint: str,
) -> dict:
    """
    构建基础 Agent 状态。

    子类可扩展此状态添加模式特定字段。
    """
    return {
        "message": message,
        "model": model,
        "parameters": parameters,
        "max_steps": max_steps,
        "workdir": workdir,
        "tools_hint": tools_hint,
        "skills_hint": skills_hint,
        "memories_hint": memories_hint,
        "graph_hint": graph_hint,
        "task_feedback_asked": False,
        "last_user_input": None,
        "last_user_prompt": None,
        "context": {"last_llm_response": None},
        "observations": [],
        "step_order": 1,
        "paused": None,
    }


# ==============================================================================
# pending_planning：知识不足 ask_user → waiting
# ==============================================================================

async def enter_pending_planning_waiting(
    *,
    task_id: int,
    run_id: int,
    mode: str,
    message: str,
    workdir: str,
    model: str,
    parameters: dict,
    max_steps: int,
    user_prompt_question: str,
    tools_hint: str,
    skills_hint: str,
    solutions_hint: str,
    memories_hint: str,
    graph_hint: str,
    domain_ids: List[str],
    skills: List[dict],
    solutions: List[dict],
    draft_solution_id: Optional[int] = None,
    think_config: Optional[dict] = None,
    yield_func: Callable = lambda _msg: None,
    safe_write_debug_func: Optional[Callable] = None,
) -> str:
    """
    docs/agent：知识不足需询问用户时，先进入 waiting；resume 后重新检索+规划再继续执行。

    说明：
    - 本函数负责“构造最小 plan + 进入 waiting + 统一后处理闭环”；
    - 不负责 resume 侧的重新检索/重新规划（由 stream_resume_run 处理）。
    """
    from backend.src.agent.runner.react_step_executor import handle_user_prompt_action
    from backend.src.constants import ACTION_TYPE_USER_PROMPT, RUN_STATUS_WAITING

    question = str(user_prompt_question or "").strip()
    if not question:
        return RUN_STATUS_WAITING

    normalized_mode = str(mode or "").strip().lower() or "do"
    if normalized_mode not in {"do", "think"}:
        normalized_mode = "do"

    # plan：单步 user_prompt（等待用户补充）
    plan_titles = [f"user_prompt: {question}"]
    plan_items: List[dict] = [{"id": 1, "brief": "补充信息", "status": "pending"}]
    plan_allows = [[ACTION_TYPE_USER_PROMPT]]
    plan_artifacts: List[str] = []

    yield_func(sse_json({"type": "plan", "task_id": int(task_id), "items": plan_items}))

    # 构造最小 agent_state：等待用户补充后再进入 planning
    agent_state = build_base_agent_state(
        message=message,
        model=model,
        parameters=parameters,
        max_steps=max_steps,
        workdir=workdir,
        tools_hint=tools_hint,
        skills_hint=skills_hint,
        memories_hint=memories_hint,
        graph_hint=graph_hint,
    )
    agent_state["mode"] = normalized_mode
    agent_state["pending_planning"] = True
    agent_state["pending_planning_reason"] = "knowledge_sufficiency"
    agent_state["solutions_hint"] = str(solutions_hint or "(无)")
    agent_state["domain_ids"] = list(domain_ids or [])
    agent_state["skill_ids"] = [
        s.get("id")
        for s in (skills or [])
        if isinstance(s, dict) and isinstance(s.get("id"), int) and int(s.get("id")) > 0
    ]
    agent_state["solution_ids"] = [
        s.get("id")
        for s in (solutions or [])
        if isinstance(s, dict) and isinstance(s.get("id"), int) and int(s.get("id")) > 0
    ]
    if isinstance(draft_solution_id, int) and int(draft_solution_id) > 0:
        agent_state["draft_solution_id"] = int(draft_solution_id)
    if normalized_mode == "think" and isinstance(think_config, dict) and think_config:
        agent_state["think_config"] = think_config

    payload_obj = {"question": question, "kind": "knowledge_sufficiency"}
    prompt_gen = handle_user_prompt_action(
        task_id=int(task_id),
        run_id=int(run_id),
        step_order=1,
        title=str(plan_titles[0] or ""),
        payload_obj=payload_obj,
        plan_items=plan_items,
        plan_titles=plan_titles,
        plan_allows=plan_allows,
        plan_artifacts=plan_artifacts,
        agent_state=agent_state,
        safe_write_debug=safe_write_debug_func or safe_write_debug,
    )

    run_status = RUN_STATUS_WAITING
    try:
        while True:
            msg = next(prompt_gen)
            if msg:
                yield_func(str(msg))
    except StopIteration as stop:
        value = stop.value if isinstance(stop.value, tuple) else None
        run_status = value[0] if value and len(value) >= 1 else RUN_STATUS_WAITING

    # 统一后处理闭环（waiting 不触发 postprocess，仅落库状态/计划栏）
    await run_finalization_sequence(
        task_id=int(task_id),
        run_id=int(run_id),
        run_status=str(run_status),
        agent_state=agent_state,
        plan_items=plan_items,
        plan_artifacts=plan_artifacts,
        message=message,
        workdir=workdir,
        yield_func=yield_func,
    )
    yield_done_event(yield_func)
    return str(run_status)


# ==============================================================================
# pending_planning：resume 后重新检索 + 重新规划
# ==============================================================================

async def resume_pending_planning_after_user_input(
    *,
    task_id: int,
    run_id: int,
    user_input: str,
    message: str,
    workdir: str,
    model: str,
    parameters: dict,
    agent_state: dict,
    paused: dict,
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
    yield_func: Callable[[str], None],
    # 可注入函数（单测 patch / 运行时一致性）
    # retrieval
    select_graph_nodes_func: Optional[Callable[..., Any]] = None,
    format_graph_for_prompt_func: Optional[Callable[..., str]] = None,
    filter_relevant_domains_func: Optional[Callable[..., Any]] = None,
    select_skills_func: Optional[Callable[..., Any]] = None,
    format_skills_for_prompt_func: Optional[Callable[..., str]] = None,
    select_solutions_func: Optional[Callable[..., Any]] = None,
    format_solutions_for_prompt_func: Optional[Callable[..., str]] = None,
    collect_tools_from_solutions_func: Optional[Callable[..., str]] = None,
    # knowledge sufficiency / drafting
    assess_knowledge_sufficiency_func: Optional[Callable[..., Any]] = None,
    compose_skills_func: Optional[Callable[..., Any]] = None,
    draft_skill_from_message_func: Optional[Callable[..., Any]] = None,
    draft_solution_from_skills_func: Optional[Callable[..., Any]] = None,
    create_skill_func: Optional[Callable[..., Any]] = None,
    publish_skill_file_func: Optional[Callable[..., Any]] = None,
    # planning
    run_planning_phase_func: Optional[Callable[..., Any]] = None,
    append_task_feedback_step_func: Optional[Callable[..., Any]] = None,
    run_think_planning_sync_func: Optional[Callable[..., Any]] = None,
    # misc
    safe_write_debug_func: Optional[Callable[..., None]] = None,
) -> Dict[str, Any]:
    """
    pending_planning 的 resume 行为（docs/agent 对齐）：
    - 用户补充信息后，重新检索 + 知识增强 + 重新规划；
    - 若仍不足，插入第 2 个 user_prompt 并进入 waiting；
    - 若充分，生成新 plan 并从 step2 开始执行。

    Returns:
        {
            "outcome": "waiting" | "planned" | "failed",
            "message": str,  # message_for_planning（累积用户补充）
            "plan_titles": List[str],
            "plan_items": List[dict],
            "plan_allows": List[List[str]],
            "plan_artifacts": List[str],
            "agent_state": dict,
            "resume_step_order": int,
        }
    """
    import time

    from backend.src.constants import (
        ACTION_TYPE_USER_PROMPT,
        AGENT_DEFAULT_MAX_STEPS,
        AGENT_PLAN_RESERVED_STEPS,
        AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
        AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
        RUN_STATUS_RUNNING,
        RUN_STATUS_WAITING,
        STREAM_TAG_EXEC,
    )

    safe_debug = safe_write_debug_func or safe_write_debug

    user_input_text = str(user_input or "").strip()
    if not user_input_text:
        yield_func(sse_json({"message": "缺少用户补充信息，无法继续规划"}, event="error"))
        return {
            "outcome": "failed",
            "message": str(message or "").strip(),
            "plan_titles": list(plan_titles or []),
            "plan_items": list(plan_items or []),
            "plan_allows": [list(a) for a in (plan_allows or [])],
            "plan_artifacts": [],
            "agent_state": agent_state,
            "resume_step_order": int(agent_state.get("step_order") or 1),
        }

    # 复用“用户补充”信息进入新一轮检索/规划：保持 audit 可读（不覆盖原始标题）
    message_for_planning = str(message or "").strip() or ""
    message_for_planning = (message_for_planning + "\n\n用户补充：" + user_input_text).strip()

    yield_func(sse_json({"delta": f"{STREAM_TAG_EXEC} 开始重新检索与规划…\n"}))

    # 保留第 1 步 user_prompt（已完成），把新 plan 作为后续步骤插入
    pending_prompt_title = str(plan_titles[0] or "").strip() if plan_titles else ""
    paused_q = str(paused.get("question") or "").strip() if isinstance(paused, dict) else ""
    if not pending_prompt_title and paused_q:
        pending_prompt_title = f"user_prompt: {paused_q}"
    if not pending_prompt_title:
        pending_prompt_title = "user_prompt: （用户补充信息）"

    pending_prompt_allow = (
        plan_allows[0]
        if plan_allows and isinstance(plan_allows[0], list)
        else [ACTION_TYPE_USER_PROMPT]
    )
    pending_prompt_item = (
        plan_items[0]
        if plan_items and isinstance(plan_items[0], dict)
        else {"id": 1, "brief": "补充信息", "status": "done"}
    )
    pending_prompt_item = dict(pending_prompt_item)
    pending_prompt_item["id"] = 1
    pending_prompt_item["status"] = "done"

    # --- 检索：图谱→领域→技能→方案 ---
    knowledge = await retrieve_all_knowledge(
        message=message_for_planning,
        model=model,
        parameters=parameters,
        yield_func=yield_func,
        task_id=int(task_id),
        run_id=int(run_id),
        include_memories=False,
        select_graph_nodes_func=select_graph_nodes_func,
        format_graph_for_prompt_func=format_graph_for_prompt_func,
        filter_relevant_domains_func=filter_relevant_domains_func,
        select_skills_func=select_skills_func,
        format_skills_for_prompt_func=format_skills_for_prompt_func,
        select_solutions_func=select_solutions_func,
        format_solutions_for_prompt_func=format_solutions_for_prompt_func,
        collect_tools_from_solutions_func=collect_tools_from_solutions_func,
    )

    graph_nodes = list(knowledge.get("graph_nodes") or [])
    graph_hint = str(knowledge.get("graph_hint") or "")
    # 文档约定：Memory 不注入上下文
    memories_hint = "(无)"
    domain_ids = list(knowledge.get("domain_ids") or [])
    if not domain_ids:
        domain_ids = ["misc"]
    skills = list(knowledge.get("skills") or [])
    skills_hint = str(knowledge.get("skills_hint") or "") or "(无)"
    solutions = list(knowledge.get("solutions") or [])

    pending_mode = str(agent_state.get("mode") or "").strip().lower() or "do"
    if pending_mode not in {"do", "think"}:
        pending_mode = "do"

    # --- planning 前“知识增强”（do/think 复用同一实现）---
    if pending_mode == "think":
        from backend.src.agent.think import create_think_config_from_dict, get_default_think_config

        tools_limit_value = 12
        try:
            raw_cfg = agent_state.get("think_config") if isinstance(agent_state, dict) else None
            if isinstance(raw_cfg, dict) and raw_cfg:
                think_cfg = create_think_config_from_dict(raw_cfg, base_model=model)
            else:
                think_cfg = get_default_think_config(base_model=model)
            tools_limit_value = int(getattr(think_cfg, "max_tools", 12) or 12)
        except Exception:
            tools_limit_value = 12

        enriched = await prepare_planning_knowledge_think(
            message=message_for_planning,
            model=model,
            parameters=parameters,
            graph_nodes=graph_nodes,
            graph_hint=graph_hint,
            domain_ids=domain_ids,
            skills=skills,
            skills_hint=skills_hint,
            solutions=solutions,
            yield_func=yield_func,
            task_id=int(task_id),
            run_id=int(run_id),
            assess_knowledge_sufficiency_func=assess_knowledge_sufficiency_func,
            compose_skills_func=compose_skills_func,
            draft_skill_from_message_func=draft_skill_from_message_func,
            draft_solution_from_skills_func=draft_solution_from_skills_func,
            create_skill_func=create_skill_func,
            publish_skill_file_func=publish_skill_file_func,
            format_skills_for_prompt_func=format_skills_for_prompt_func,
            format_solutions_for_prompt_func=format_solutions_for_prompt_func,
            collect_tools_from_solutions_func=collect_tools_from_solutions_func,
            tools_limit=int(tools_limit_value),
        )
    else:
        enriched = await prepare_planning_knowledge_do(
            message=message_for_planning,
            model=model,
            parameters=parameters,
            graph_nodes=graph_nodes,
            graph_hint=graph_hint,
            domain_ids=domain_ids,
            skills=skills,
            skills_hint=skills_hint,
            solutions=solutions,
            yield_func=yield_func,
            task_id=int(task_id),
            run_id=int(run_id),
            assess_knowledge_sufficiency_func=assess_knowledge_sufficiency_func,
            compose_skills_func=compose_skills_func,
            draft_skill_from_message_func=draft_skill_from_message_func,
            draft_solution_from_skills_func=draft_solution_from_skills_func,
            create_skill_func=create_skill_func,
            publish_skill_file_func=publish_skill_file_func,
            format_skills_for_prompt_func=format_skills_for_prompt_func,
            format_solutions_for_prompt_func=format_solutions_for_prompt_func,
            collect_tools_from_solutions_func=collect_tools_from_solutions_func,
        )

    need_user_prompt = bool(enriched.get("need_user_prompt"))
    user_prompt_question = str(enriched.get("user_prompt_question") or "").strip()

    if need_user_prompt and user_prompt_question:
        from backend.src.agent.runner.react_step_executor import handle_user_prompt_action

        # 继续询问：把新 user_prompt 作为第 2 步插入
        new_plan_titles = [pending_prompt_title, f"user_prompt: {user_prompt_question}"]
        new_plan_items = [pending_prompt_item, {"id": 2, "brief": "补充信息", "status": "pending"}]
        new_plan_allows = [list(pending_prompt_allow), [ACTION_TYPE_USER_PROMPT]]
        new_plan_artifacts: List[str] = []

        yield_func(sse_json({"type": "plan", "task_id": int(task_id), "items": new_plan_items}))

        agent_state["mode"] = str(pending_mode)
        agent_state["pending_planning"] = True
        agent_state["pending_planning_reason"] = "knowledge_sufficiency"
        # 累积用户补充信息：若“补充后仍不足”继续 ask_user，
        # 下次 resume 仍需要基于已补充内容继续检索/规划（避免丢失上下文）。
        agent_state["message"] = str(message_for_planning or "").strip()
        agent_state["tools_hint"] = str(enriched.get("tools_hint") or "(无)")
        agent_state["skills_hint"] = str(enriched.get("skills_hint") or "(无)")
        agent_state["solutions_hint"] = str(enriched.get("solutions_hint") or "(无)")
        agent_state["graph_hint"] = graph_hint
        agent_state["domain_ids"] = list(domain_ids or [])
        agent_state["step_order"] = 2

        payload_obj = {"question": user_prompt_question, "kind": "knowledge_sufficiency"}
        prompt_gen = handle_user_prompt_action(
            task_id=int(task_id),
            run_id=int(run_id),
            step_order=2,
            title=str(new_plan_titles[1] or ""),
            payload_obj=payload_obj,
            plan_items=new_plan_items,
            plan_titles=new_plan_titles,
            plan_allows=new_plan_allows,
            plan_artifacts=new_plan_artifacts,
            agent_state=agent_state,
            safe_write_debug=safe_debug,
        )
        try:
            while True:
                msg = next(prompt_gen)
                if msg:
                    yield_func(str(msg))
        except StopIteration:
            pass
        yield_done_event(yield_func)
        return {
            "outcome": "waiting",
            "message": str(message_for_planning or "").strip(),
            "plan_titles": list(new_plan_titles),
            "plan_items": list(new_plan_items),
            "plan_allows": [list(a) for a in (new_plan_allows or [])],
            "plan_artifacts": list(new_plan_artifacts),
            "agent_state": agent_state,
            "resume_step_order": 2,
        }

    # knowledge/enrichment 输出
    skills = list(enriched.get("skills") or skills or [])
    skills_hint = str(enriched.get("skills_hint") or skills_hint or "(无)")
    solutions_for_prompt = list(enriched.get("solutions_for_prompt") or solutions or [])
    draft_solution_id = enriched.get("draft_solution_id")
    solutions_hint = str(enriched.get("solutions_hint") or "(无)")
    tools_hint = str(enriched.get("tools_hint") or "(无)")

    # --- 重新规划：do/think 分流 ---
    # 预留步数：确认满意度 + 评估不通过后的自修复/重试；并额外预留 1 步给已执行的 user_prompt
    try:
        reserved = int(AGENT_PLAN_RESERVED_STEPS or 0)
        if reserved < 1:
            reserved = 1
        try:
            max_steps_value = int(agent_state.get("max_steps") or AGENT_DEFAULT_MAX_STEPS)
        except Exception:
            max_steps_value = int(AGENT_DEFAULT_MAX_STEPS or 30)
        planning_max_steps = int(max_steps_value) - reserved - 1 if int(max_steps_value) > 1 else 1
        if planning_max_steps < 1:
            planning_max_steps = 1
    except Exception:
        max_steps_value = int(AGENT_DEFAULT_MAX_STEPS or 30)
        planning_max_steps = 1

    append_feedback = append_task_feedback_step_func
    if append_feedback is None:
        from backend.src.agent.runner.feedback import append_task_feedback_step as _append

        append_feedback = _append

    if pending_mode == "think":
        from backend.src.agent.think import (
            create_think_config_from_dict,
            get_default_think_config,
            run_think_planning_sync,
        )
        from backend.src.agent.think.think_execution import build_executor_assignments_payload
        from backend.src.services.llm.llm_client import call_openai

        # Think 模式：多模型协作规划（与 stream_think_run 对齐）
        raw_cfg = agent_state.get("think_config") if isinstance(agent_state, dict) else None
        if isinstance(raw_cfg, dict) and raw_cfg:
            think_config = create_think_config_from_dict(raw_cfg, base_model=model)
        else:
            think_config = get_default_think_config(base_model=model)

        default_cfg = get_default_think_config(base_model=model)
        if not getattr(think_config, "planners", None):
            think_config.planners = default_cfg.planners
        if not getattr(think_config, "executors", None):
            think_config.executors = default_cfg.executors

        def _llm_call(prompt: str, call_model: str, call_params: dict) -> Tuple[str, Optional[int]]:
            merged_params = {**(parameters or {}), **(call_params or {})}
            text, record_id, err = call_openai(prompt, call_model or model, merged_params)
            if err:
                return "", None
            return text or "", record_id

        progress_messages: List[str] = []

        def _collect_progress(msg: str) -> None:
            progress_messages.append(str(msg))

        plan_started_at = time.monotonic()
        try:
            think_plan_result = await asyncio.to_thread(
                run_think_planning_sync_func or run_think_planning_sync,
                config=think_config,
                message=message_for_planning,
                workdir=workdir,
                graph_hint=graph_hint,
                skills_hint=skills_hint,
                solutions_hint=solutions_hint,
                tools_hint=tools_hint,
                max_steps=int(planning_max_steps),
                llm_call_func=_llm_call,
                yield_progress=_collect_progress,
                planner_hints=None,
            )
        except Exception as exc:
            await asyncio.to_thread(
                mark_run_failed,
                task_id=int(task_id),
                run_id=int(run_id),
                reason=f"think_pending_planning_failed:{exc}",
            )
            enqueue_postprocess_thread(task_id=int(task_id), run_id=int(run_id), run_status=RUN_STATUS_FAILED)
            yield_func(sse_json({"message": "Think 模式规划失败：未生成有效计划"}, event="error"))
            return {
                "outcome": "failed",
                "message": str(message_for_planning or "").strip(),
                "plan_titles": list(plan_titles or []),
                "plan_items": list(plan_items or []),
                "plan_allows": [list(a) for a in (plan_allows or [])],
                "plan_artifacts": [],
                "agent_state": agent_state,
                "resume_step_order": int(agent_state.get("step_order") or 1),
            }

        for msg in progress_messages:
            if msg:
                yield_func(sse_json({"delta": f"{msg}\n"}))

        if not getattr(think_plan_result, "plan_titles", None):
            await asyncio.to_thread(
                mark_run_failed,
                task_id=int(task_id),
                run_id=int(run_id),
                reason="think_pending_planning_failed:think_planning_empty",
            )
            enqueue_postprocess_thread(task_id=int(task_id), run_id=int(run_id), run_status=RUN_STATUS_FAILED)
            yield_func(sse_json({"message": "Think 模式规划失败：未生成有效计划"}, event="error"))
            return {
                "outcome": "failed",
                "message": str(message_for_planning or "").strip(),
                "plan_titles": list(plan_titles or []),
                "plan_items": list(plan_items or []),
                "plan_allows": [list(a) for a in (plan_allows or [])],
                "plan_artifacts": [],
                "agent_state": agent_state,
                "resume_step_order": int(agent_state.get("step_order") or 1),
            }

        duration_ms = int((time.monotonic() - plan_started_at) * 1000)
        safe_debug(
            int(task_id),
            int(run_id),
            message="agent.think.plan_resume_pending.done",
            data={"duration_ms": duration_ms, "steps": len(getattr(think_plan_result, "plan_titles", []) or [])},
            level="info",
        )

        plan_titles_gen = list(think_plan_result.plan_titles or [])
        plan_briefs_gen = list(getattr(think_plan_result, "plan_briefs", []) or [])
        plan_allows_gen = list(think_plan_result.plan_allows or [])
        plan_artifacts_new = list(think_plan_result.plan_artifacts or [])

        plan_items_gen: List[dict] = []
        for i, title in enumerate(plan_titles_gen):
            brief = plan_briefs_gen[i] if i < len(plan_briefs_gen) else ""
            allow = plan_allows_gen[i] if i < len(plan_allows_gen) else []
            plan_items_gen.append({"id": int(i) + 1, "title": title, "brief": brief, "allow": allow, "status": "pending"})

        remaining_max_steps: Optional[int] = None
        try:
            remaining_max_steps = int(max_steps_value) - 1
        except Exception:
            remaining_max_steps = None
        if remaining_max_steps is not None and remaining_max_steps < 1:
            remaining_max_steps = 1

        append_feedback(
            plan_titles=plan_titles_gen,
            plan_items=plan_items_gen,
            plan_allows=plan_allows_gen,
            max_steps=int(remaining_max_steps) if remaining_max_steps is not None else None,
        )

        # 合并：step1 user_prompt(done) + 新 plan
        merged_titles = [pending_prompt_title] + plan_titles_gen
        merged_allows = [list(pending_prompt_allow)] + [list(a) for a in (plan_allows_gen or [])]
        merged_items = [pending_prompt_item]
        for i, it in enumerate(plan_items_gen or []):
            if not isinstance(it, dict):
                merged_items.append({"id": int(i) + 2, "brief": "", "status": "pending"})
                continue
            new_it = dict(it)
            new_it["id"] = int(i) + 2
            new_it["status"] = "pending"
            merged_items.append(new_it)

        yield_func(sse_json({"type": "plan", "task_id": int(task_id), "items": merged_items}))

        # 更新 agent_state：进入可执行态
        agent_state["pending_planning"] = False
        agent_state.pop("pending_planning_reason", None)
        agent_state["message"] = message_for_planning
        agent_state["mode"] = "think"
        agent_state["tools_hint"] = tools_hint
        agent_state["skills_hint"] = skills_hint
        agent_state["solutions_hint"] = solutions_hint
        agent_state["graph_hint"] = graph_hint
        agent_state["domain_ids"] = list(domain_ids or [])
        agent_state["skill_ids"] = [
            s.get("id")
            for s in (skills or [])
            if isinstance(s, dict) and isinstance(s.get("id"), int) and int(s.get("id")) > 0
        ]
        agent_state["solution_ids"] = [
            s.get("id")
            for s in (solutions_for_prompt or [])
            if isinstance(s, dict) and isinstance(s.get("id"), int) and int(s.get("id")) > 0
        ]
        if isinstance(draft_solution_id, int) and int(draft_solution_id) > 0:
            agent_state["draft_solution_id"] = int(draft_solution_id)

        # Think planning 审计字段
        agent_state["winning_planner_id"] = getattr(think_plan_result, "winning_planner_id", None)
        agent_state["alternative_plans"] = getattr(think_plan_result, "alternative_plans", []) or []
        agent_state["vote_records"] = getattr(think_plan_result, "vote_records", []) or []
        agent_state["plan_alternatives"] = agent_state.get("alternative_plans")
        agent_state["plan_votes"] = agent_state.get("vote_records")

        # 用于恢复/审计：可读的分工表（与 stream_think_run 对齐）
        try:
            agent_state["executor_assignments"] = build_executor_assignments_payload(
                plan_titles=merged_titles,
                plan_allows=merged_allows,
            )
        except Exception:
            pass

        resume_step_order = 2
        agent_state["step_order"] = int(resume_step_order)

        try:
            updated_at = now_iso()
            await asyncio.to_thread(
                update_task_run,
                run_id=int(run_id),
                status=RUN_STATUS_RUNNING,
                agent_plan={
                    "titles": merged_titles,
                    "items": merged_items,
                    "allows": merged_allows,
                    "artifacts": plan_artifacts_new,
                },
                agent_state=agent_state,
                updated_at=updated_at,
            )
        except Exception as exc:
            safe_debug(
                int(task_id),
                int(run_id),
                message="agent.pending_planning.persist_failed",
                data={"error": str(exc)},
                level="warning",
            )

        return {
            "outcome": "planned",
            "message": str(message_for_planning or "").strip(),
            "plan_titles": list(merged_titles),
            "plan_items": list(merged_items),
            "plan_allows": [list(a) for a in (merged_allows or [])],
            "plan_artifacts": list(plan_artifacts_new),
            "agent_state": agent_state,
            "resume_step_order": int(resume_step_order),
        }

    # do 模式：单模型规划
    from backend.src.agent.planning_phase import PlanPhaseFailure, run_planning_phase
    from backend.src.agent.runner.stream_pump import pump_sync_generator
    from backend.src.constants import STREAM_TAG_FAIL

    try:
        inner = (run_planning_phase_func or run_planning_phase)(
            task_id=int(task_id),
            run_id=int(run_id),
            message=message_for_planning,
            workdir=workdir,
            model=model,
            parameters=parameters,
            max_steps=int(planning_max_steps),
            tools_hint=tools_hint,
            skills_hint=skills_hint,
            solutions_hint=solutions_hint,
            memories_hint=memories_hint,
            graph_hint=graph_hint,
        )
        plan_result = None
        async for kind, payload in pump_sync_generator(
            inner=inner,
            label="planning_resume_pending",
            poll_interval_seconds=AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
            idle_timeout_seconds=AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
        ):
            if kind == "msg":
                if payload:
                    yield_func(str(payload))
                continue
            if kind == "done":
                plan_result = payload
                break
            if kind == "err":
                if isinstance(payload, BaseException):
                    raise payload
                raise RuntimeError(f"planning_resume_pending 异常:{payload}")

        if plan_result is None:
            raise RuntimeError("planning_resume_pending 返回为空")

    except PlanPhaseFailure as exc:
        await asyncio.to_thread(
            mark_run_failed,
            task_id=int(task_id),
            run_id=int(run_id),
            reason=str(exc.reason),
        )
        enqueue_postprocess_thread(task_id=int(task_id), run_id=int(run_id), run_status=RUN_STATUS_FAILED)
        yield_func(sse_json({"message": exc.public_message}, event="error"))
        return {
            "outcome": "failed",
            "message": str(message_for_planning or "").strip(),
            "plan_titles": list(plan_titles or []),
            "plan_items": list(plan_items or []),
            "plan_allows": [list(a) for a in (plan_allows or [])],
            "plan_artifacts": [],
            "agent_state": agent_state,
            "resume_step_order": int(agent_state.get("step_order") or 1),
        }

    plan_titles_gen = list(plan_result.plan_titles or [])
    plan_allows_gen = list(plan_result.plan_allows or [])
    plan_artifacts_new = list(plan_result.plan_artifacts or [])
    plan_items_gen = list(plan_result.plan_items or [])

    remaining_max_steps: Optional[int] = None
    try:
        remaining_max_steps = int(max_steps_value) - 1
    except Exception:
        remaining_max_steps = None
    if remaining_max_steps is not None and remaining_max_steps < 1:
        remaining_max_steps = 1

    append_feedback(
        plan_titles=plan_titles_gen,
        plan_items=plan_items_gen,
        plan_allows=plan_allows_gen,
        max_steps=int(remaining_max_steps) if remaining_max_steps is not None else None,
    )

    merged_titles = [pending_prompt_title] + plan_titles_gen
    merged_allows = [list(pending_prompt_allow)] + [list(a) for a in (plan_allows_gen or [])]
    merged_items = [pending_prompt_item]
    for i, it in enumerate(plan_items_gen or []):
        if not isinstance(it, dict):
            merged_items.append({"id": int(i) + 2, "brief": "", "status": "pending"})
            continue
        new_it = dict(it)
        new_it["id"] = int(i) + 2
        new_it["status"] = "pending"
        merged_items.append(new_it)

    yield_func(sse_json({"type": "plan", "task_id": int(task_id), "items": merged_items}))

    agent_state["pending_planning"] = False
    agent_state.pop("pending_planning_reason", None)
    agent_state["message"] = message_for_planning
    agent_state["mode"] = "do"
    agent_state["tools_hint"] = tools_hint
    agent_state["skills_hint"] = skills_hint
    agent_state["solutions_hint"] = solutions_hint
    agent_state["graph_hint"] = graph_hint
    agent_state["domain_ids"] = list(domain_ids or [])
    agent_state["skill_ids"] = [
        s.get("id")
        for s in (skills or [])
        if isinstance(s, dict) and isinstance(s.get("id"), int) and int(s.get("id")) > 0
    ]
    agent_state["solution_ids"] = [
        s.get("id")
        for s in (solutions_for_prompt or [])
        if isinstance(s, dict) and isinstance(s.get("id"), int) and int(s.get("id")) > 0
    ]
    if isinstance(draft_solution_id, int) and int(draft_solution_id) > 0:
        agent_state["draft_solution_id"] = int(draft_solution_id)

    resume_step_order = 2
    agent_state["step_order"] = int(resume_step_order)

    try:
        updated_at = now_iso()
        await asyncio.to_thread(
            update_task_run,
            run_id=int(run_id),
            status=RUN_STATUS_RUNNING,
            agent_plan={
                "titles": merged_titles,
                "items": merged_items,
                "allows": merged_allows,
                "artifacts": plan_artifacts_new,
            },
            agent_state=agent_state,
            updated_at=updated_at,
        )
    except Exception as exc:
        safe_debug(
            int(task_id),
            int(run_id),
            message="agent.pending_planning.persist_failed",
            data={"error": str(exc)},
            level="warning",
        )

    return {
        "outcome": "planned",
        "message": str(message_for_planning or "").strip(),
        "plan_titles": list(merged_titles),
        "plan_items": list(merged_items),
        "plan_allows": [list(a) for a in (merged_allows or [])],
        "plan_artifacts": list(plan_artifacts_new),
        "agent_state": agent_state,
        "resume_step_order": int(resume_step_order),
    }


# ==============================================================================
# 后处理闭环
# ==============================================================================

async def check_and_report_missing_artifacts(
    run_status: str,
    plan_artifacts: List[str],
    workdir: str,
    yield_func: Callable,
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
) -> str:
    """
    检查 artifacts 是否缺失，返回更新后的状态。

    仅在任务"成功结束"时检查，避免出现"嘴上完成但没有落盘"。
    """
    if run_status != RUN_STATUS_DONE or not plan_artifacts:
        return run_status

    missing = check_missing_artifacts(artifacts=plan_artifacts, workdir=workdir)
    if missing:
        safe_write_debug(
            task_id, run_id,
            message="agent.artifacts.missing",
            data={"missing": missing},
            level="error",
        )
        yield_func(sse_json({"delta": f"{STREAM_TAG_FAIL} 未生成文件：{', '.join(missing)}\n"}))
        return RUN_STATUS_FAILED

    return run_status


def finalize_plan_items_status(
    plan_items: List[dict],
    run_status: str,
    yield_func: Callable,
    task_id: int,
) -> None:
    """
    计划栏收尾：把 running 状态结算为 done/failed。

    只有在"真正结束(done/failed)"时才结算，waiting 应保留状态。
    """
    if not plan_items or run_status not in {RUN_STATUS_DONE, RUN_STATUS_FAILED}:
        return

    for item in plan_items:
        if item.get("status") == "running":
            item["status"] = "done" if run_status == RUN_STATUS_DONE else "failed"

    yield_func(sse_plan(task_id=int(task_id), plan_items=plan_items))


async def finalize_run_status(
    task_id: int,
    run_id: int,
    run_status: str,
) -> None:
    """
    落库 run/task 状态（waiting 不写 finished_at）。
    """
    await asyncio.to_thread(
        finalize_run_and_task_status,
        task_id=int(task_id),
        run_id=int(run_id),
        run_status=str(run_status),
    )


async def trigger_review_if_waiting(
    task_id: int,
    run_id: int,
    run_status: str,
    agent_state: dict,
) -> None:
    """
    评估触发点：当等待原因是"确认满意度"时触发评估。
    """
    if run_status == RUN_STATUS_WAITING:
        enqueue_review_on_feedback_waiting(
            task_id=int(task_id),
            run_id=int(run_id),
            agent_state=agent_state,
        )


async def write_auto_memory_if_done(
    task_id: int,
    run_id: int,
    run_status: str,
    message: str,
    yield_func: Callable,
) -> Optional[dict]:
    """
    自动记忆：把本次 run 的"最终结果摘要"写入 memory_items。

    通过 SSE 通知前端即时更新。
    """
    if run_status != RUN_STATUS_DONE:
        return None

    try:
        from backend.src.services.tasks.task_postprocess import write_task_result_memory_if_missing

        item = await asyncio.to_thread(
            write_task_result_memory_if_missing,
            task_id=int(task_id),
            run_id=int(run_id),
            title=str(message or "").strip(),
        )
        if isinstance(item, dict) and item.get("id") is not None:
            yield_func(sse_json({
                "type": SSE_TYPE_MEMORY_ITEM,
                "task_id": int(task_id),
                "run_id": int(run_id),
                "item": item,
            }))
            return item
    except Exception as exc:
        safe_write_debug(
            task_id, run_id,
            message="agent.memory.auto_task_result_failed",
            data={"error": str(exc)},
            level="warning",
        )

    return None


def enqueue_postprocess_if_terminal(
    task_id: int,
    run_id: int,
    run_status: str,
) -> None:
    """
    入队后处理线程（评估/技能/图谱）。

    仅在终态（done/failed/stopped）时触发。
    """
    if run_status in {RUN_STATUS_DONE, RUN_STATUS_FAILED, RUN_STATUS_STOPPED}:
        enqueue_postprocess_thread(
            task_id=int(task_id),
            run_id=int(run_id),
            run_status=str(run_status),
        )


async def run_finalization_sequence(
    task_id: int,
    run_id: int,
    run_status: str,
    agent_state: dict,
    plan_items: List[dict],
    plan_artifacts: List[str],
    message: str,
    workdir: str,
    yield_func: Callable,
) -> str:
    """
    统一后处理闭环序列。

    执行顺序：
    1. Artifacts 校验
    2. 计划栏收尾
    3. 状态落库
    4. 评估触发（如果 waiting）
    5. 自动记忆
    6. 后处理入队

    Returns:
        最终的 run_status（可能因 artifacts 缺失而变为 failed）
    """
    # 1. Artifacts 校验
    run_status = await check_and_report_missing_artifacts(
        run_status, plan_artifacts, workdir, yield_func, task_id, run_id
    )

    # 2. 计划栏收尾
    finalize_plan_items_status(plan_items, run_status, yield_func, task_id)

    # 3. 状态落库
    await finalize_run_status(task_id, run_id, run_status)

    # 4. 评估触发
    await trigger_review_if_waiting(task_id, run_id, run_status, agent_state)

    # 5. 自动记忆
    await write_auto_memory_if_done(task_id, run_id, run_status, message, yield_func)

    # 6. 后处理入队
    enqueue_postprocess_if_terminal(task_id, run_id, run_status)

    return run_status


# ==============================================================================
# 异常处理
# ==============================================================================

def handle_stream_cancellation(
    task_id: Optional[int],
    run_id: Optional[int],
    reason: str = "agent_stream_cancelled",
) -> None:
    """
    处理 SSE 流取消（客户端断开）。

    将任务状态收敛为 stopped。
    """
    if run_id is not None:
        enqueue_stop_task_run_records(
            task_id=task_id,
            run_id=int(run_id),
            reason=reason,
        )


async def handle_execution_exception(
    exc: Exception,
    task_id: Optional[int],
    run_id: Optional[int],
    yield_func: Callable,
    mode_prefix: str = "agent",
) -> None:
    """
    处理执行异常。

    将任务状态收敛为 failed，并通过 SSE 通知前端。
    """
    if task_id is not None and run_id is not None:
        await asyncio.to_thread(
            mark_run_failed,
            task_id=int(task_id),
            run_id=int(run_id),
            reason=f"exception:{exc}",
        )
        # 异常失败：也要落库评估记录
        enqueue_postprocess_thread(
            task_id=int(task_id),
            run_id=int(run_id),
            run_status=RUN_STATUS_FAILED,
        )
        safe_write_debug(
            task_id, run_id,
            message=f"{mode_prefix}.exception",
            data={"error": f"{exc}"},
            level="error",
        )

    suffix = f"（task_id={task_id} run_id={run_id}）" if task_id else ""
    try:
        yield_func(sse_json({"message": f"{mode_prefix} 执行失败:{exc}{suffix}"}, event="error"))
    except BaseException:
        pass


def yield_done_event(yield_func: Callable) -> None:
    """
    发送 done 事件，若客户端已断开则忽略。
    """
    try:
        yield_func(sse_json({"type": "done"}, event="done"))
    except BaseException:
        pass


# ==============================================================================
# SSE 响应构建
# ==============================================================================

def create_sse_response(gen, headers: Optional[dict] = None):
    """
    创建 SSE StreamingResponse。
    """
    from fastapi.responses import StreamingResponse

    default_headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }
    if headers:
        default_headers.update(headers)

    return StreamingResponse(gen(), media_type="text/event-stream", headers=default_headers)
