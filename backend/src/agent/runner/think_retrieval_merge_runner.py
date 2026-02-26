from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Callable, Dict, List

from backend.src.agent.runner.planning_enrich_runner import (
    PlanningEnrichRunConfig,
    iter_planning_enrich_events,
)
from backend.src.agent.runner.execution_pipeline import prepare_planning_knowledge_think
from backend.src.constants import (
    STREAM_TAG_DOMAIN,
    STREAM_TAG_FAIL,
    STREAM_TAG_GRAPH,
    STREAM_TAG_SKILLS,
    STREAM_TAG_SOLUTIONS,
    THINK_MERGED_MAX_SOLUTIONS,
)
from backend.src.services.llm.llm_client import sse_json
from backend.src.common.utils import parse_positive_int


def _vote_rank(values_by_planner: List[List]) -> List:
    """
    简单投票排序：
    - 计数：出现于多少个 Planner 的结果中
    - 排序：count desc，其次按首次出现位置稳定排序
    """
    counts: Dict = {}
    first_seen: Dict = {}
    for list_index, items in enumerate(values_by_planner or []):
        seen_local = set()
        for item_index, raw in enumerate(items or []):
            if raw is None or raw in seen_local:
                continue
            seen_local.add(raw)
            counts[raw] = int(counts.get(raw, 0)) + 1
            if raw not in first_seen:
                first_seen[raw] = (list_index, item_index)
    ranked = sorted(
        counts.keys(),
        key=lambda key: (-int(counts.get(key, 0)), first_seen.get(key, (999, 999))),
    )
    return ranked


def _merge_dicts_by_id(items_by_planner: List[List[dict]], *, max_items: int) -> List[dict]:
    id_lists: List[List[int]] = []
    by_id: Dict[int, dict] = {}
    for items in items_by_planner or []:
        ids: List[int] = []
        for item in items or []:
            if not isinstance(item, dict):
                continue
            item_id = parse_positive_int(item.get("id"), default=None)
            if item_id is None:
                continue
            ids.append(int(item_id))
            if int(item_id) not in by_id:
                by_id[int(item_id)] = item
        id_lists.append(ids)

    ranked_ids = _vote_rank(id_lists)
    selected: List[dict] = []
    for item_id in ranked_ids[: max(0, int(max_items))]:
        item = by_id.get(int(item_id))
        if item:
            selected.append(item)
    return selected


def _extract_positive_id_list(items: List[dict], *, limit: int = 0) -> List[int]:
    result: List[int] = []
    max_items = int(limit or 0)
    for item in items or []:
        if not isinstance(item, dict):
            continue
        item_id = parse_positive_int(item.get("id"), default=None)
        if item_id is None:
            continue
        result.append(int(item_id))
        if max_items > 0 and len(result) >= max_items:
            break
    return result


def _extract_positive_id_set(items: List[dict]) -> set[int]:
    return set(_extract_positive_id_list(items, limit=0))


async def _gather_planner_lists(
    *,
    planners: List[object],
    tasks: List[asyncio.Future],
    task_id: int,
    run_id: int,
    safe_write_debug: Callable[..., None],
    failure_message: str,
) -> List[List]:
    """
    执行并行 planner 任务，统一收敛为“与 planners 对齐的列表结果”：
    - 成功：转换为 list；
    - 失败：记录失败详情并返回空列表；
    - CancelledError：立即向上抛出，保持取消语义。
    """
    results = await asyncio.gather(*tasks, return_exceptions=True)
    values_by_planner: List[List] = []
    failures: List[dict] = []
    for index, result in enumerate(results):
        if isinstance(result, asyncio.CancelledError):
            raise result
        if isinstance(result, BaseException):
            failures.append(
                {
                    "planner_id": str(getattr(planners[index], "planner_id", "")),
                    "model": str(getattr(planners[index], "model", "")),
                    "error": str(result),
                }
            )
            values_by_planner.append([])
            continue
        values_by_planner.append(list(result or []))
    if failures:
        safe_write_debug(
            int(task_id),
            int(run_id),
            message=str(failure_message or ""),
            data={"failures": failures},
            level="warning",
        )
    return values_by_planner


@dataclass
class ThinkRetrievalMergeConfig:
    task_id: int
    run_id: int
    message: str
    model: str
    parameters: dict
    think_config: object
    safe_write_debug: Callable[..., None]
    select_relevant_graph_nodes_func: Callable[..., List[dict]]
    format_graph_for_prompt_func: Callable[..., str]
    filter_relevant_domains_func: Callable[..., List[str]]
    select_relevant_skills_func: Callable[..., List[dict]]
    format_skills_for_prompt_func: Callable[..., str]
    select_relevant_solutions_func: Callable[..., List[dict]]
    format_solutions_for_prompt_func: Callable[..., str]
    collect_tools_from_solutions_func: Callable[..., str]
    assess_knowledge_sufficiency_func: Callable[..., object]
    compose_skills_func: Callable[..., List[dict]]
    draft_skill_from_message_func: Callable[..., dict]
    draft_solution_from_skills_func: Callable[..., dict]
    create_skill_func: Callable[..., object]
    publish_skill_file_func: Callable[..., object]


async def iter_think_retrieval_merge_events(
    *,
    config: ThinkRetrievalMergeConfig,
) -> AsyncGenerator[tuple[str, Any], None]:
    """
    Think 多规划者检索 + 合并 + planning 前知识增强。
    """
    task_id = int(config.task_id)
    run_id = int(config.run_id)
    message = str(config.message or "")
    model = str(config.model or "")
    parameters = dict(config.parameters or {})
    think_config = config.think_config
    safe_write_debug = config.safe_write_debug

    planners = list(getattr(think_config, "planners", []) or [])
    if not planners:
        yield ("msg", sse_json({"delta": f"{STREAM_TAG_FAIL} Think 配置缺少 planners，回退为单模型检索\n"}))

    # --- 检索：图谱（多模型并行） ---
    yield ("msg", sse_json({"delta": f"{STREAM_TAG_GRAPH} 检索图谱（多模型）…\n"}))
    if planners:
        graph_tasks = [
            asyncio.to_thread(
                config.select_relevant_graph_nodes_func,
                message=message,
                model=planner.model,
                parameters=parameters,
            )
            for planner in planners
        ]
        graph_by_planner = await _gather_planner_lists(
            planners=planners,
            tasks=graph_tasks,
            task_id=task_id,
            run_id=run_id,
            safe_write_debug=safe_write_debug,
            failure_message="agent.think.retrieval.graph_failed",
        )
        merged_graph_nodes = _merge_dicts_by_id(
            graph_by_planner,
            max_items=int(getattr(think_config, "max_graph_nodes", 10) or 10),
        )
    else:
        graph_by_planner = []
        merged_graph_nodes = await asyncio.to_thread(
            config.select_relevant_graph_nodes_func,
            message=message,
            model=model,
            parameters=parameters,
        )

    graph_hint = config.format_graph_for_prompt_func(merged_graph_nodes)
    if merged_graph_nodes:
        yield ("msg", sse_json({"delta": f"{STREAM_TAG_GRAPH} 已合并：{len(merged_graph_nodes)} 个\n"}))
    else:
        yield ("msg", sse_json({"delta": f"{STREAM_TAG_GRAPH} 未命中\n"}))

    if planners:
        safe_write_debug(
            task_id,
            run_id,
            message="agent.think.retrieval.graph_by_planner",
            data={
                "planners": [
                    {
                        "planner_id": planner.planner_id,
                        "model": planner.model,
                        "node_ids": _extract_positive_id_list(list(graph_by_planner[index] or []), limit=12),
                    }
                    for index, planner in enumerate(planners)
                ],
                "merged_node_ids": _extract_positive_id_list(list(merged_graph_nodes or []), limit=0),
            },
        )

    # 文档约定：Memory 不参与检索与上下文注入（仅作为后处理沉淀原料）。
    memories_hint = "(无)"

    # --- 检索：领域（多模型并行 + 投票） ---
    yield ("msg", sse_json({"delta": f"{STREAM_TAG_DOMAIN} 筛选领域（多模型）…\n"}))
    if planners:
        domain_tasks = [
            asyncio.to_thread(
                config.filter_relevant_domains_func,
                message=message,
                graph_hint=graph_hint,
                model=planner.model,
                parameters=parameters,
            )
            for planner in planners
        ]
        domain_by_planner = await _gather_planner_lists(
            planners=planners,
            tasks=domain_tasks,
            task_id=task_id,
            run_id=run_id,
            safe_write_debug=safe_write_debug,
            failure_message="agent.think.retrieval.domain_failed",
        )
        ranked_domains = [str(value).strip() for value in _vote_rank(domain_by_planner) if str(value).strip()]
        if any(value != "misc" for value in ranked_domains):
            ranked_domains = [value for value in ranked_domains if value != "misc"]
        max_domains = 3
        try:
            from backend.src.constants import AGENT_DOMAIN_PICK_MAX_DOMAINS

            max_domains = int(AGENT_DOMAIN_PICK_MAX_DOMAINS or 3)
        except (ImportError, TypeError, ValueError, AttributeError):
            max_domains = 3
        domain_ids = ranked_domains[:max_domains]
    else:
        domain_by_planner = []
        domain_ids = await asyncio.to_thread(
            config.filter_relevant_domains_func,
            message=message,
            graph_hint=graph_hint,
            model=model,
            parameters=parameters,
        )

    if domain_ids:
        yield ("msg", sse_json({"delta": f"{STREAM_TAG_DOMAIN} 已选择：{', '.join(domain_ids)}\n"}))
    else:
        yield ("msg", sse_json({"delta": f"{STREAM_TAG_DOMAIN} 未命中，使用默认\n"}))
        domain_ids = ["misc"]

    if planners:
        safe_write_debug(
            task_id,
            run_id,
            message="agent.think.retrieval.domain_by_planner",
            data={
                "planners": [
                    {"planner_id": planner.planner_id, "model": planner.model, "domain_ids": list(domain_by_planner[index] or [])}
                    for index, planner in enumerate(planners)
                ],
                "merged_domain_ids": list(domain_ids or []),
            },
        )

    # --- 检索：技能（多模型并行；每个 Planner 独立精选，合并后用于执行） ---
    yield ("msg", sse_json({"delta": f"{STREAM_TAG_SKILLS} 检索技能（多模型）…\n"}))
    planner_skills: Dict[str, List[dict]] = {}
    if planners:
        skills_tasks = [
            asyncio.to_thread(
                config.select_relevant_skills_func,
                message=message,
                model=planner.model,
                parameters=parameters,
                domain_ids=domain_ids,
            )
            for planner in planners
        ]
        skills_by_planner = await _gather_planner_lists(
            planners=planners,
            tasks=skills_tasks,
            task_id=task_id,
            run_id=run_id,
            safe_write_debug=safe_write_debug,
            failure_message="agent.think.retrieval.skills_failed",
        )
        for index, planner in enumerate(planners):
            planner_skills[str(planner.planner_id)] = list(skills_by_planner[index] or [])
        merged_skills = _merge_dicts_by_id(
            skills_by_planner,
            max_items=int(getattr(think_config, "max_skills", 6) or 6),
        )
    else:
        skills_by_planner = []
        merged_skills = await asyncio.to_thread(
            config.select_relevant_skills_func,
            message=message,
            model=model,
            parameters=parameters,
            domain_ids=domain_ids,
        )

    skills_hint = config.format_skills_for_prompt_func(merged_skills)
    if merged_skills:
        names = ", ".join(str(value.get("name") or "").strip() for value in merged_skills if isinstance(value, dict) and value.get("name"))
        if names:
            yield ("msg", sse_json({"delta": f"{STREAM_TAG_SKILLS} 已合并：{names}\n"}))
        else:
            yield ("msg", sse_json({"delta": f"{STREAM_TAG_SKILLS} 已合并：{len(merged_skills)} 个\n"}))

    # --- 检索：方案（多模型并行；基于各自技能匹配） ---
    yield ("msg", sse_json({"delta": f"{STREAM_TAG_SOLUTIONS} 匹配方案（多模型）…\n"}))
    planner_solutions: Dict[str, List[dict]] = {}
    if planners:
        solutions_tasks = [
            asyncio.to_thread(
                config.select_relevant_solutions_func,
                message=message,
                skills=planner_skills.get(str(planner.planner_id)) or [],
                model=planner.model,
                parameters=parameters,
                domain_ids=domain_ids,
                max_solutions=3,
            )
            for planner in planners
        ]
        solutions_by_planner = await _gather_planner_lists(
            planners=planners,
            tasks=solutions_tasks,
            task_id=task_id,
            run_id=run_id,
            safe_write_debug=safe_write_debug,
            failure_message="agent.think.retrieval.solutions_failed",
        )
        for index, planner in enumerate(planners):
            planner_solutions[str(planner.planner_id)] = list(solutions_by_planner[index] or [])
        merged_solutions = _merge_dicts_by_id(
            solutions_by_planner,
            max_items=int(THINK_MERGED_MAX_SOLUTIONS or 5),
        )
    else:
        solutions_by_planner = []
        merged_solutions = await asyncio.to_thread(
            config.select_relevant_solutions_func,
            message=message,
            skills=merged_skills or [],
            model=model,
            parameters=parameters,
            domain_ids=domain_ids,
            max_solutions=int(THINK_MERGED_MAX_SOLUTIONS or 5),
        )

    before_skill_ids = _extract_positive_id_set(list(merged_skills or []))

    tools_limit = int(getattr(think_config, "max_tools", 12) or 12)
    enriched = None
    async for event_type, event_payload in iter_planning_enrich_events(
        config=PlanningEnrichRunConfig(
            task_builder=lambda emit: prepare_planning_knowledge_think(
                message=message,
                model=model,
                parameters=parameters,
                graph_nodes=list(merged_graph_nodes or []),
                graph_hint=graph_hint,
                domain_ids=list(domain_ids or []),
                skills=list(merged_skills or []),
                skills_hint=skills_hint,
                solutions=list(merged_solutions or []),
                yield_func=emit,
                task_id=task_id,
                run_id=run_id,
                assess_knowledge_sufficiency_func=config.assess_knowledge_sufficiency_func,
                compose_skills_func=config.compose_skills_func,
                draft_skill_from_message_func=config.draft_skill_from_message_func,
                draft_solution_from_skills_func=config.draft_solution_from_skills_func,
                create_skill_func=config.create_skill_func,
                publish_skill_file_func=config.publish_skill_file_func,
                format_skills_for_prompt_func=config.format_skills_for_prompt_func,
                format_solutions_for_prompt_func=config.format_solutions_for_prompt_func,
                collect_tools_from_solutions_func=config.collect_tools_from_solutions_func,
                tools_limit=int(tools_limit),
            ),
            empty_result_error="think planning enrich 结果为空",
        )
    ):
        if event_type == "msg":
            yield ("msg", str(event_payload))
            continue
        enriched = event_payload
    if not isinstance(enriched, dict):
        raise RuntimeError("think planning enrich 结果为空")

    merged_skills = list(enriched.get("skills") or merged_skills or [])
    skills_hint = str(enriched.get("skills_hint") or skills_hint or "(无)")
    solutions_for_prompt = list(enriched.get("solutions_for_prompt") or merged_solutions or [])
    merged_solutions = list(solutions_for_prompt or [])
    draft_solution_id_value = parse_positive_int(enriched.get("draft_solution_id"), default=None)
    solutions_hint = str(enriched.get("solutions_hint") or "(无)")
    tools_hint = str(enriched.get("tools_hint") or "(无)")

    need_user_prompt = bool(enriched.get("need_user_prompt"))
    user_prompt_question = str(enriched.get("user_prompt_question") or "").strip()

    after_skill_ids = _extract_positive_id_set(list(merged_skills or []))
    added_skill_ids = after_skill_ids - before_skill_ids
    if added_skill_ids and planners:
        for planner in planners:
            planner_id = str(planner.planner_id)
            item_list = list(planner_skills.get(planner_id) or [])
            existing = _extract_positive_id_set(item_list)
            for skill in merged_skills or []:
                if not isinstance(skill, dict) or skill.get("id") is None:
                    continue
                skill_id = parse_positive_int(skill.get("id"), default=None)
                if skill_id is None:
                    continue
                if skill_id in added_skill_ids and skill_id not in existing:
                    item_list.append(skill)
                    existing.add(int(skill_id))
            planner_skills[planner_id] = item_list

    planner_hints: Dict[str, Dict[str, str]] = {}
    if planners:
        for planner in planners:
            planner_id = str(planner.planner_id)
            per_skills_hint = config.format_skills_for_prompt_func(planner_skills.get(planner_id) or [])
            per_solutions_hint = config.format_solutions_for_prompt_func(planner_solutions.get(planner_id) or [])
            per_tools_hint = config.collect_tools_from_solutions_func(
                planner_solutions.get(planner_id) or [],
                limit=int(tools_limit),
            )
            if (
                draft_solution_id_value is not None
                and not (planner_solutions.get(planner_id) or [])
            ):
                per_solutions_hint = solutions_hint
                per_tools_hint = config.collect_tools_from_solutions_func(
                    solutions_for_prompt or [],
                    limit=int(tools_limit),
                )
            planner_hints[planner_id] = {
                "skills_hint": per_skills_hint or "(无)",
                "solutions_hint": per_solutions_hint or "(无)",
                "tools_hint": per_tools_hint or "(无)",
            }

    safe_write_debug(
        task_id,
        run_id,
        message="agent.think.retrieval.merged",
        data={
            "domain_ids": list(domain_ids or []),
            "merged_graph_node_count": len(merged_graph_nodes or []),
            "merged_skill_count": len(merged_skills or []),
            "merged_solution_count": len(merged_solutions or []),
            "planner_hints_keys": list(planner_hints.keys()),
        },
    )

    yield (
        "done",
        {
            "graph_nodes": list(merged_graph_nodes or []),
            "graph_hint": str(graph_hint or ""),
            "memories_hint": "(无)",
            "domain_ids": list(domain_ids or []),
            "skills": list(merged_skills or []),
            "skills_hint": str(skills_hint or "(无)"),
            "solutions": list(merged_solutions or []),
            "solutions_hint": str(solutions_hint or "(无)"),
            "tools_hint": str(tools_hint or "(无)"),
            "draft_solution_id": draft_solution_id_value,
            "planner_hints": dict(planner_hints or {}),
            "need_user_prompt": bool(need_user_prompt),
            "user_prompt_question": str(user_prompt_question or ""),
        },
    )
