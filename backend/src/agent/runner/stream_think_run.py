"""
Think 模式流式执行入口。

与 stream_new_run.py（do 模式）类似，但使用多模型协作规划和执行。
"""

import asyncio
import logging
import os
import time
from typing import AsyncGenerator, Dict, List, Optional, Tuple

from backend.src.agent.runner import react_loop as react_loop_facade
from backend.src.agent.runner.feedback import append_task_feedback_step, is_task_feedback_step_title
from backend.src.agent.runner.react_loop import run_react_loop
from backend.src.agent.runner.stream_pump import pump_sync_generator
from backend.src.agent.runner.think_parallel_loop import run_think_parallel_loop
from backend.src.agent.runner.execution_pipeline import (
    create_sse_response,
    pump_async_task_messages,
    enter_pending_planning_waiting,
    prepare_planning_knowledge_think,
    run_finalization_sequence,
    handle_execution_exception,
    handle_stream_cancellation,
    build_base_agent_state,
    persist_agent_state,
)
from backend.src.agent.support import (
    _assess_knowledge_sufficiency,
    _filter_relevant_domains,
    _collect_tools_from_solutions,
    _compose_skills,
    _draft_skill_from_message,
    _format_graph_for_prompt,
    _format_skills_for_prompt,
    _format_solutions_for_prompt,
    _draft_solution_from_skills,
    _select_relevant_graph_nodes,
    _select_relevant_skills,
    _select_relevant_solutions,
)
from backend.src.agent.think import (
    ThinkConfig,
    ThinkPlanResult,
    get_default_think_config,
    create_think_config_from_dict,
    run_think_planning_sync,
    infer_executor_assignments,
    run_reflection,
    merge_fix_steps_into_plan,
)
from backend.src.agent.think.think_execution import _infer_executor_from_allow, build_executor_assignments_payload
from backend.src.api.schemas import AgentCommandStreamRequest
from backend.src.common.utils import error_response, now_iso
from backend.src.constants import (
    AGENT_DEFAULT_MAX_STEPS,
    AGENT_EXPERIMENT_DIR_REL,
    AGENT_PLAN_RESERVED_STEPS,
    AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
    AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_MESSAGE_LLM_CHAT_MESSAGE_MISSING,
    HTTP_STATUS_BAD_REQUEST,
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_STOPPED,
    RUN_STATUS_WAITING,
    STREAM_TAG_DOMAIN,
    STREAM_TAG_EXEC,
    STREAM_TAG_FAIL,
    STREAM_TAG_GRAPH,
    STREAM_TAG_REFLECTION,
    STREAM_TAG_SKILLS,
    STREAM_TAG_SOLUTIONS,
    STREAM_TAG_THINK,
    SSE_TYPE_MEMORY_ITEM,
    STEP_STATUS_FAILED,
    THINK_MERGED_MAX_SOLUTIONS,
    THINK_REFLECTION_MAX_ROUNDS,
)
from backend.src.services.debug.debug_output import write_task_debug_output
from backend.src.services.llm.llm_client import call_openai, resolve_default_model, sse_json
from backend.src.services.permissions.permission_checks import ensure_write_permission
from backend.src.repositories.task_runs_repo import update_task_run
from backend.src.repositories.skills_repo import create_skill
from backend.src.repositories.task_steps_repo import list_task_steps_for_run, mark_task_step_skipped
from backend.src.services.skills.skills_publish import publish_skill_file
from backend.src.services.tasks.task_run_lifecycle import (
    check_missing_artifacts,
    create_task_and_run_records_for_agent,
    enqueue_postprocess_thread,
    enqueue_review_on_feedback_waiting,
    enqueue_stop_task_run_records,
    finalize_run_and_task_status,
    mark_run_failed,
)

logger = logging.getLogger(__name__)


def _safe_write_debug(
    task_id: Optional[int],
    run_id: Optional[int],
    *,
    message: str,
    data: Optional[dict] = None,
    level: str = "debug",
) -> None:
    """调试输出。"""
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


def _create_llm_call_func(model: str, parameters: dict):
    """
    创建用于 Think 模式的 LLM 调用函数。

    返回签名为 (prompt, model, params) -> (response, record_id) 的函数。
    """

    def llm_call(prompt: str, call_model: str, call_params: dict) -> Tuple[str, Optional[int]]:
        merged_params = {**parameters, **call_params}
        text, record_id, err = call_openai(prompt, call_model or model, merged_params)
        if err:
            logger.warning("Think LLM call error: %s", err)
            return "", None
        return text or "", record_id

    return llm_call


def stream_agent_think_command(payload: AgentCommandStreamRequest):
    """
    Think 模式指令执行（SSE 流式）：
    - 创建 task/run
    - 检索（graph/memory/skills）
    - 多模型头脑风暴规划
    - 多 Executor 分工执行（按依赖并行调度；保留顺序收尾步骤）
    - 失败时触发反思机制
    """
    permission = ensure_write_permission()
    if permission:
        return permission

    message = (payload.message or "").strip()
    if not message:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            ERROR_MESSAGE_LLM_CHAT_MESSAGE_MISSING,
            HTTP_STATUS_BAD_REQUEST,
        )

    max_steps = payload.max_steps or AGENT_DEFAULT_MAX_STEPS
    dry_run = bool(payload.dry_run)
    model = (payload.model or "").strip() or resolve_default_model()
    parameters = payload.parameters or {"temperature": 0.2}

    # 解析 Think 配置
    think_config: ThinkConfig
    if payload.think_config:
        think_config = create_think_config_from_dict(payload.think_config, base_model=model)
        # 保底：反思机制需要 planners；执行需要 executors。若缺失则用默认补齐，避免“部分配置导致不可运行”。
        default_cfg = get_default_think_config(base_model=model)
        if not getattr(think_config, "planners", None):
            think_config.planners = default_cfg.planners
        if not getattr(think_config, "executors", None):
            think_config.executors = default_cfg.executors
        else:
            for role, exec_cfg in (default_cfg.executors or {}).items():
                if role not in think_config.executors:
                    think_config.executors[role] = exec_cfg
    else:
        think_config = get_default_think_config(base_model=model)

    async def gen() -> AsyncGenerator[str, None]:
        task_id: Optional[int] = None
        run_id: Optional[int] = None
        plan_items: List[dict] = []

        try:
            created_at = now_iso()
            workdir = os.getcwd()

            # 创建 task/run
            task_id, run_id = await asyncio.to_thread(
                create_task_and_run_records_for_agent,
                message=message,
                created_at=created_at,
            )

            yield sse_json({"type": "run_created", "task_id": task_id, "run_id": run_id})

            _safe_write_debug(
                task_id,
                run_id,
                message="agent.think.start",
                data={
                    "mode": "think",
                    "model": model,
                    "max_steps": max_steps,
                    "dry_run": dry_run,
                    "workdir": workdir,
                    "planner_count": think_config.get_planner_count(),
                },
            )

            yield sse_json({"delta": f"{STREAM_TAG_THINK} Think 模式启动，{think_config.get_planner_count()} 个规划者协作\n"})
            # 工具清单会在“方案匹配”之后汇总（方案提到的工具优先）
            tools_hint = "(无)"
            solutions_hint = "(无)"

            # ==============================
            # Think 多模型检索（并行 + 合并投票）
            # 文档约定：共享部分（图谱、领域）；差异部分（技能、方案、工具）按 Planner 分发。
            # ==============================

            def _vote_rank(values_by_planner: List[List]) -> List:
                """
                简单投票排序：
                - 计数：出现于多少个 Planner 的结果中
                - 排序：count desc，其次按首次出现位置稳定排序
                """
                counts: Dict = {}
                first_seen: Dict = {}
                for li, items in enumerate(values_by_planner or []):
                    seen_local = set()
                    for pi, raw in enumerate(items or []):
                        if raw is None:
                            continue
                        if raw in seen_local:
                            continue
                        seen_local.add(raw)
                        counts[raw] = int(counts.get(raw, 0)) + 1
                        if raw not in first_seen:
                            first_seen[raw] = (li, pi)
                ranked = sorted(
                    counts.keys(),
                    key=lambda k: (-int(counts.get(k, 0)), first_seen.get(k, (999, 999))),
                )
                return ranked

            def _merge_dicts_by_id(items_by_planner: List[List[dict]], *, max_items: int) -> List[dict]:
                id_lists: List[List[int]] = []
                by_id: Dict[int, dict] = {}
                for items in items_by_planner or []:
                    ids: List[int] = []
                    for item in items or []:
                        if not isinstance(item, dict) or item.get("id") is None:
                            continue
                        try:
                            sid = int(item.get("id"))
                        except Exception:
                            continue
                        if sid <= 0:
                            continue
                        ids.append(sid)
                        if sid not in by_id:
                            by_id[sid] = item
                    id_lists.append(ids)

                ranked_ids = _vote_rank(id_lists)
                selected: List[dict] = []
                for sid in ranked_ids[: max(0, int(max_items))]:
                    item = by_id.get(int(sid))
                    if item:
                        selected.append(item)
                return selected

            planners = list(getattr(think_config, "planners", []) or [])
            if not planners:
                yield sse_json({"delta": f"{STREAM_TAG_FAIL} Think 配置缺少 planners，回退为单模型检索\n"})
                planners = []

            # --- 检索：图谱（多模型并行） ---
            yield sse_json({"delta": f"{STREAM_TAG_GRAPH} 检索图谱（多模型）…\n"})
            if planners:
                graph_tasks = [
                    asyncio.to_thread(
                        _select_relevant_graph_nodes,
                        message=message,
                        model=p.model,
                        parameters=parameters,
                    )
                    for p in planners
                ]
                graph_results = await asyncio.gather(*graph_tasks, return_exceptions=True)
                graph_by_planner: List[List[dict]] = []
                graph_failures: List[dict] = []
                for i, res in enumerate(graph_results):
                    if isinstance(res, asyncio.CancelledError):
                        raise res
                    if isinstance(res, BaseException):
                        graph_failures.append(
                            {
                                "planner_id": str(getattr(planners[i], "planner_id", "")),
                                "model": str(getattr(planners[i], "model", "")),
                                "error": str(res),
                            }
                        )
                        graph_by_planner.append([])
                        continue
                    graph_by_planner.append(list(res or []))
                if graph_failures:
                    _safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.think.retrieval.graph_failed",
                        data={"failures": graph_failures},
                        level="warning",
                    )
                merged_graph_nodes = _merge_dicts_by_id(
                    graph_by_planner,
                    max_items=int(getattr(think_config, "max_graph_nodes", 10) or 10),
                )
            else:
                graph_by_planner = []
                merged_graph_nodes = await asyncio.to_thread(
                    _select_relevant_graph_nodes,
                    message=message,
                    model=model,
                    parameters=parameters,
                )

            graph_hint = _format_graph_for_prompt(merged_graph_nodes)
            if merged_graph_nodes:
                yield sse_json({"delta": f"{STREAM_TAG_GRAPH} 已合并：{len(merged_graph_nodes)} 个\n"})
            else:
                yield sse_json({"delta": f"{STREAM_TAG_GRAPH} 未命中\n"})

            if planners:
                _safe_write_debug(
                    task_id,
                    run_id,
                    message="agent.think.retrieval.graph_by_planner",
                    data={
                        "planners": [
                            {
                                "planner_id": p.planner_id,
                                "model": p.model,
                                "node_ids": [int(n.get("id")) for n in (graph_by_planner[i] or []) if n.get("id") is not None][:12],
                            }
                            for i, p in enumerate(planners)
                        ],
                        "merged_node_ids": [int(n.get("id")) for n in (merged_graph_nodes or []) if n.get("id") is not None],
                    },
                )

            # 文档约定：Memory 不参与检索与上下文注入（仅作为后处理沉淀原料）。
            memories_hint = "(无)"

            # --- 检索：领域（多模型并行 + 投票） ---
            yield sse_json({"delta": f"{STREAM_TAG_DOMAIN} 筛选领域（多模型）…\n"})
            if planners:
                domain_tasks = [
                    asyncio.to_thread(
                        _filter_relevant_domains,
                        message=message,
                        graph_hint=graph_hint,
                        model=p.model,
                        parameters=parameters,
                    )
                    for p in planners
                ]
                domain_results = await asyncio.gather(*domain_tasks, return_exceptions=True)
                domain_by_planner: List[List[str]] = []
                domain_failures: List[dict] = []
                for i, res in enumerate(domain_results):
                    if isinstance(res, asyncio.CancelledError):
                        raise res
                    if isinstance(res, BaseException):
                        domain_failures.append(
                            {
                                "planner_id": str(getattr(planners[i], "planner_id", "")),
                                "model": str(getattr(planners[i], "model", "")),
                                "error": str(res),
                            }
                        )
                        domain_by_planner.append([])
                        continue
                    domain_by_planner.append(list(res or []))
                if domain_failures:
                    _safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.think.retrieval.domain_failed",
                        data={"failures": domain_failures},
                        level="warning",
                    )
                ranked_domains = [str(d).strip() for d in _vote_rank(domain_by_planner) if str(d).strip()]
                # 若存在非 misc 领域，则丢弃 misc（避免“泛化领域”挤占名额）
                if any(d != "misc" for d in ranked_domains):
                    ranked_domains = [d for d in ranked_domains if d != "misc"]
                max_domains = 3
                try:
                    from backend.src.constants import AGENT_DOMAIN_PICK_MAX_DOMAINS

                    max_domains = int(AGENT_DOMAIN_PICK_MAX_DOMAINS or 3)
                except Exception:
                    max_domains = 3
                domain_ids = ranked_domains[:max_domains]
            else:
                domain_by_planner = []
                domain_ids = await asyncio.to_thread(
                    _filter_relevant_domains,
                    message=message,
                    graph_hint=graph_hint,
                    model=model,
                    parameters=parameters,
                )

            if domain_ids:
                yield sse_json({"delta": f"{STREAM_TAG_DOMAIN} 已选择：{', '.join(domain_ids)}\n"})
            else:
                yield sse_json({"delta": f"{STREAM_TAG_DOMAIN} 未命中，使用默认\n"})
                domain_ids = ["misc"]

            if planners:
                _safe_write_debug(
                    task_id,
                    run_id,
                    message="agent.think.retrieval.domain_by_planner",
                    data={
                        "planners": [
                            {"planner_id": p.planner_id, "model": p.model, "domain_ids": list(domain_by_planner[i] or [])}
                            for i, p in enumerate(planners)
                        ],
                        "merged_domain_ids": list(domain_ids or []),
                    },
                )

            # --- 检索：技能（多模型并行；每个 Planner 独立精选，合并后用于执行） ---
            yield sse_json({"delta": f"{STREAM_TAG_SKILLS} 检索技能（多模型）…\n"})
            planner_skills: Dict[str, List[dict]] = {}
            if planners:
                skills_tasks = [
                    asyncio.to_thread(
                        _select_relevant_skills,
                        message=message,
                        model=p.model,
                        parameters=parameters,
                        domain_ids=domain_ids,
                    )
                    for p in planners
                ]
                skills_results = await asyncio.gather(*skills_tasks, return_exceptions=True)
                skills_by_planner: List[List[dict]] = []
                skills_failures: List[dict] = []
                for i, res in enumerate(skills_results):
                    if isinstance(res, asyncio.CancelledError):
                        raise res
                    if isinstance(res, BaseException):
                        skills_failures.append(
                            {
                                "planner_id": str(getattr(planners[i], "planner_id", "")),
                                "model": str(getattr(planners[i], "model", "")),
                                "error": str(res),
                            }
                        )
                        skills_by_planner.append([])
                        continue
                    skills_by_planner.append(list(res or []))
                if skills_failures:
                    _safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.think.retrieval.skills_failed",
                        data={"failures": skills_failures},
                        level="warning",
                    )
                for i, p in enumerate(planners):
                    planner_skills[str(p.planner_id)] = list(skills_by_planner[i] or [])
                merged_skills = _merge_dicts_by_id(
                    skills_by_planner,
                    max_items=int(getattr(think_config, "max_skills", 6) or 6),
                )
            else:
                skills_by_planner = []
                merged_skills = await asyncio.to_thread(
                    _select_relevant_skills,
                    message=message,
                    model=model,
                    parameters=parameters,
                    domain_ids=domain_ids,
                )

            skills_hint = _format_skills_for_prompt(merged_skills)
            if merged_skills:
                names = ", ".join(str(s.get("name") or "").strip() for s in merged_skills if s.get("name"))
                if names:
                    yield sse_json({"delta": f"{STREAM_TAG_SKILLS} 已合并：{names}\n"})
                else:
                    yield sse_json({"delta": f"{STREAM_TAG_SKILLS} 已合并：{len(merged_skills)} 个\n"})

            # --- 检索：方案（多模型并行；基于各自技能匹配） ---
            yield sse_json({"delta": f"{STREAM_TAG_SOLUTIONS} 匹配方案（多模型）…\n"})
            planner_solutions: Dict[str, List[dict]] = {}
            if planners:
                solutions_tasks = [
                    asyncio.to_thread(
                        _select_relevant_solutions,
                        message=message,
                        skills=planner_skills.get(str(p.planner_id)) or [],
                        model=p.model,
                        parameters=parameters,
                        domain_ids=domain_ids,
                        max_solutions=3,
                    )
                    for p in planners
                ]
                solutions_results = await asyncio.gather(*solutions_tasks, return_exceptions=True)
                solutions_by_planner: List[List[dict]] = []
                solutions_failures: List[dict] = []
                for i, res in enumerate(solutions_results):
                    if isinstance(res, asyncio.CancelledError):
                        raise res
                    if isinstance(res, BaseException):
                        solutions_failures.append(
                            {
                                "planner_id": str(getattr(planners[i], "planner_id", "")),
                                "model": str(getattr(planners[i], "model", "")),
                                "error": str(res),
                            }
                        )
                        solutions_by_planner.append([])
                        continue
                    solutions_by_planner.append(list(res or []))
                if solutions_failures:
                    _safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.think.retrieval.solutions_failed",
                        data={"failures": solutions_failures},
                        level="warning",
                    )
                for i, p in enumerate(planners):
                    planner_solutions[str(p.planner_id)] = list(solutions_by_planner[i] or [])
                merged_solutions = _merge_dicts_by_id(
                    solutions_by_planner,
                    max_items=int(THINK_MERGED_MAX_SOLUTIONS or 5),
                )
            else:
                solutions_by_planner = []
                merged_solutions = await asyncio.to_thread(
                    _select_relevant_solutions,
                    message=message,
                    skills=merged_skills or [],
                    model=model,
                    parameters=parameters,
                    domain_ids=domain_ids,
                    max_solutions=int(THINK_MERGED_MAX_SOLUTIONS or 5),
                )

            # --- Think planning 前“知识增强”（对齐 docs/agent，收敛到 execution_pipeline）---
            # 说明：与 do 模式一致，包含：
            # - 知识充分性判断（compose_skills / create_draft_skill / ask_user）
            # - 无匹配方案时草拟 draft solution（Create 流程 A）
            before_skill_ids = {
                int(s.get("id"))
                for s in (merged_skills or [])
                if isinstance(s, dict) and isinstance(s.get("id"), int) and int(s.get("id")) > 0
            }

            tools_limit = int(getattr(think_config, "max_tools", 12) or 12)
            enrich_q: "asyncio.Queue[str]" = asyncio.Queue()

            def _emit_enrich(msg: str) -> None:
                try:
                    enrich_q.put_nowait(str(msg))
                except Exception:
                    return

            enrich_task = asyncio.create_task(
                prepare_planning_knowledge_think(
                    message=message,
                    model=model,
                    parameters=parameters,
                    graph_nodes=list(merged_graph_nodes or []),
                    graph_hint=graph_hint,
                    domain_ids=list(domain_ids or []),
                    skills=list(merged_skills or []),
                    skills_hint=skills_hint,
                    solutions=list(merged_solutions or []),
                    yield_func=_emit_enrich,
                    task_id=task_id,
                    run_id=run_id,
                    assess_knowledge_sufficiency_func=_assess_knowledge_sufficiency,
                    compose_skills_func=_compose_skills,
                    draft_skill_from_message_func=_draft_skill_from_message,
                    draft_solution_from_skills_func=_draft_solution_from_skills,
                    create_skill_func=create_skill,
                    publish_skill_file_func=publish_skill_file,
                    format_skills_for_prompt_func=_format_skills_for_prompt,
                    format_solutions_for_prompt_func=_format_solutions_for_prompt,
                    collect_tools_from_solutions_func=_collect_tools_from_solutions,
                    tools_limit=int(tools_limit),
                )
            )
            async for msg in pump_async_task_messages(enrich_task, enrich_q):
                yield msg
            enriched = await enrich_task

            merged_skills = list(enriched.get("skills") or merged_skills or [])
            skills_hint = str(enriched.get("skills_hint") or skills_hint or "(无)")
            solutions_for_prompt = list(enriched.get("solutions_for_prompt") or merged_solutions or [])
            merged_solutions = list(solutions_for_prompt or [])
            draft_solution_id = enriched.get("draft_solution_id")
            solutions_hint = str(enriched.get("solutions_hint") or "(无)")
            tools_hint = str(enriched.get("tools_hint") or "(无)")

            need_user_prompt = bool(enriched.get("need_user_prompt"))
            user_prompt_question = str(enriched.get("user_prompt_question") or "").strip()

            # docs/agent：知识不足需询问用户时，先进入 waiting；resume 后重新检索+规划再继续执行。
            if need_user_prompt and user_prompt_question and task_id is not None and run_id is not None:
                out_q: "asyncio.Queue[str]" = asyncio.Queue()

                def _emit_wait(msg: str) -> None:
                    try:
                        if msg:
                            out_q.put_nowait(str(msg))
                    except Exception:
                        return

                wait_task = asyncio.create_task(
                    enter_pending_planning_waiting(
                        task_id=int(task_id),
                        run_id=int(run_id),
                        mode="think",
                        message=message,
                        workdir=workdir,
                        model=model,
                        parameters=parameters,
                        max_steps=int(max_steps),
                        user_prompt_question=user_prompt_question,
                        tools_hint=tools_hint,
                        skills_hint=skills_hint,
                        solutions_hint=solutions_hint,
                        memories_hint=memories_hint,
                        graph_hint=graph_hint,
                        domain_ids=list(domain_ids or []),
                        skills=list(merged_skills or []),
                        solutions=list(merged_solutions or []),
                        draft_solution_id=int(draft_solution_id)
                        if isinstance(draft_solution_id, int) and int(draft_solution_id) > 0
                        else None,
                        think_config=payload.think_config,
                        yield_func=_emit_wait,
                        safe_write_debug_func=_safe_write_debug,
                    )
                )
                async for msg in pump_async_task_messages(wait_task, out_q):
                    yield msg
                await wait_task
                return

            # 把新创建的 draft skill 分发给所有 Planner（docs/agent：草稿技能属于 planning 兜底知识，应全局可见）
            after_skill_ids = {
                int(s.get("id"))
                for s in (merged_skills or [])
                if isinstance(s, dict) and isinstance(s.get("id"), int) and int(s.get("id")) > 0
            }
            added_skill_ids = after_skill_ids - before_skill_ids
            if added_skill_ids and planners:
                for p in planners:
                    pid = str(p.planner_id)
                    li = list(planner_skills.get(pid) or [])
                    existing = {
                        int(s.get("id"))
                        for s in li
                        if isinstance(s, dict) and isinstance(s.get("id"), int) and int(s.get("id")) > 0
                    }
                    for sk in merged_skills or []:
                        if not isinstance(sk, dict) or sk.get("id") is None:
                            continue
                        try:
                            sid = int(sk.get("id"))
                        except Exception:
                            continue
                        if sid in added_skill_ids and sid not in existing:
                            li.append(sk)
                            existing.add(sid)
                    planner_skills[pid] = li

            # --- 工具汇总：按 Planner 独立汇总（方案提到的工具优先）+ 全局汇总用于执行 ---
            planner_hints: Dict[str, Dict[str, str]] = {}
            if planners:
                for p in planners:
                    pid = str(p.planner_id)
                    per_skills_hint = _format_skills_for_prompt(planner_skills.get(pid) or [])
                    per_solutions_hint = _format_solutions_for_prompt(planner_solutions.get(pid) or [])
                    per_tools_hint = _collect_tools_from_solutions(
                        planner_solutions.get(pid) or [],
                        limit=int(tools_limit),
                    )
                    # docs/agent：方案草稿属于“本次 run 的 planning 兜底知识”，应对所有 Planner 可见。
                    if (
                        isinstance(draft_solution_id, int)
                        and int(draft_solution_id) > 0
                        and not (planner_solutions.get(pid) or [])
                    ):
                        per_solutions_hint = solutions_hint
                        per_tools_hint = _collect_tools_from_solutions(
                            solutions_for_prompt or [],
                            limit=int(tools_limit),
                        )
                    planner_hints[pid] = {
                        "skills_hint": per_skills_hint or "(无)",
                        "solutions_hint": per_solutions_hint or "(无)",
                        "tools_hint": per_tools_hint or "(无)",
                    }

            _safe_write_debug(
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

            # --- Think 模式规划（六阶段头脑风暴）---
            yield sse_json({"delta": f"{STREAM_TAG_THINK} 开始多模型协作规划…\n"})

            reserved = int(AGENT_PLAN_RESERVED_STEPS or 0)
            if reserved < 1:
                reserved = 1
            planning_max_steps = int(max_steps) - reserved if int(max_steps) > 1 else 1
            if planning_max_steps < 1:
                planning_max_steps = 1

            llm_call_func = _create_llm_call_func(model, parameters)

            # 收集规划进度消息
            progress_messages: List[str] = []

            def collect_progress(msg: str):
                progress_messages.append(msg)

            plan_started_at = time.monotonic()

            # 执行 Think 规划
            think_plan_result: ThinkPlanResult = await asyncio.to_thread(
                run_think_planning_sync,
                config=think_config,
                message=message,
                workdir=workdir,
                graph_hint=graph_hint,
                skills_hint=skills_hint,
                solutions_hint=solutions_hint,
                tools_hint=tools_hint,
                max_steps=planning_max_steps,
                llm_call_func=llm_call_func,
                yield_progress=collect_progress,
                planner_hints=planner_hints if isinstance(planner_hints, dict) else None,
            )

            # 输出规划进度
            for msg in progress_messages:
                yield sse_json({"delta": f"{msg}\n"})

            duration_ms = int((time.monotonic() - plan_started_at) * 1000)
            _safe_write_debug(
                task_id,
                run_id,
                message="agent.think.plan.done",
                data={
                    "duration_ms": duration_ms,
                    "steps": len(think_plan_result.plan_titles or []),
                    "winning_planner": think_plan_result.winning_planner_id,
                    "vote_records": think_plan_result.vote_records,
                },
                level="info",
            )

            if not think_plan_result.plan_titles:
                await asyncio.to_thread(
                    mark_run_failed,
                    task_id=int(task_id),
                    run_id=int(run_id),
                    reason="think_planning_empty",
                )
                yield sse_json({"message": "Think 模式规划失败：未生成有效计划"}, event="error")
                return

            plan_titles = think_plan_result.plan_titles
            plan_briefs = think_plan_result.plan_briefs
            plan_allows = think_plan_result.plan_allows
            plan_artifacts = think_plan_result.plan_artifacts

            # 构建 plan_items
            plan_items = []
            for i, title in enumerate(plan_titles):
                brief = plan_briefs[i] if i < len(plan_briefs) else ""
                allow = plan_allows[i] if i < len(plan_allows) else []
                plan_items.append({
                    "id": i + 1,
                    "title": title,
                    "brief": brief,
                    "allow": allow,
                    "status": "pending",
                })

            # 追加确认满意度步骤
            append_task_feedback_step(
                plan_titles=plan_titles,
                plan_items=plan_items,
                plan_allows=plan_allows,
                max_steps=int(max_steps) if isinstance(max_steps, int) else None,
            )

            yield sse_json({"type": "plan", "task_id": task_id, "items": plan_items})

            # 持久化 agent 运行态
            agent_state = {
                "message": message,
                "model": model,
                "parameters": parameters,
                "max_steps": max_steps,
                "workdir": workdir,
                "tools_hint": tools_hint,
                "skills_hint": skills_hint,
                "solutions_hint": solutions_hint,
                "memories_hint": memories_hint,
                "graph_hint": graph_hint,
                "task_feedback_asked": False,
                "last_user_input": None,
                "last_user_prompt": None,
                "context": {"last_llm_response": None},
                "observations": [],
                "step_order": 1,
                "paused": None,
                "mode": "think",
                # 用于后处理“方案沉淀/溯源”（docs/agent 依赖）
                "domain_ids": list(domain_ids or []),
                "skill_ids": [
                    s.get("id")
                    for s in (merged_skills or [])
                    if isinstance(s, dict) and isinstance(s.get("id"), int) and int(s.get("id")) > 0
                ],
                "solution_ids": [
                    s.get("id")
                    for s in (merged_solutions or [])
                    if isinstance(s, dict) and isinstance(s.get("id"), int) and int(s.get("id")) > 0
                ],
                # 规划阶段草拟的方案（Create 流程 A）：便于后处理覆盖/升级与调试溯源
                "draft_solution_id": int(draft_solution_id)
                if isinstance(draft_solution_id, int) and int(draft_solution_id) > 0
                else None,
                "think_config": payload.think_config,
                "winning_planner_id": think_plan_result.winning_planner_id,
                "alternative_plans": think_plan_result.alternative_plans,
                "vote_records": think_plan_result.vote_records,
                # docs/agent 规划输出字段别名（不影响既有读取逻辑）
                "plan_alternatives": think_plan_result.alternative_plans,
                "plan_votes": think_plan_result.vote_records,
            }

            try:
                updated_at = now_iso()
                await asyncio.to_thread(
                    update_task_run,
                    run_id=int(run_id),
                    agent_plan={
                        "titles": plan_titles,
                        "items": plan_items,
                        "allows": plan_allows,
                        "artifacts": plan_artifacts,
                    },
                    agent_state=agent_state,
                    updated_at=updated_at,
                )
            except Exception as exc:
                logger.exception("agent.think.state.persist_failed: %s", exc)

            if dry_run:
                yield sse_json({"delta": f"{STREAM_TAG_EXEC} dry_run: 已生成步骤，未执行。\n"})
                await asyncio.to_thread(
                    finalize_run_and_task_status,
                    task_id=int(task_id),
                    run_id=int(run_id),
                    run_status=RUN_STATUS_DONE,
                )
                return

            yield sse_json({"delta": f"{STREAM_TAG_EXEC} 开始执行…\n"})

            # 推断 Executor 分配（按 allow/title 推断执行者，用于并行调度与审计）
            executor_assignments = infer_executor_assignments(
                plan_titles=plan_titles,
                plan_allows=plan_allows,
                plan_artifacts=plan_artifacts,
            )
            # 持久化可读的分工表（用于中断恢复/评估输入；与 docs/agent 对齐）。
            if isinstance(agent_state, dict):
                agent_state["executor_assignments"] = build_executor_assignments_payload(
                    plan_titles=plan_titles,
                    plan_allows=plan_allows,
                )

            _safe_write_debug(
                task_id,
                run_id,
                message="agent.think.executor_assignments",
                data={
                    "assignments": [
                        {"step": a.step_index, "executor": a.executor}
                        for a in executor_assignments.assignments
                    ],
                },
            )

            # 执行上下文
            context: dict = {"last_llm_response": None}
            observations: List[str] = []
            agent_state["context"] = context
            agent_state["observations"] = observations

            def _resolve_step_llm_config(step_order: int, title: str, allow: List[str]):
                """
                Think 模式：按 executor 角色为每个步骤选择不同的模型/参数。
                """
                role = _infer_executor_from_allow(allow or [], title or "")
                cfg = think_config.get_executor(role) or think_config.get_executor("executor_code")

                resolved_model = model
                overrides: Dict = {}
                if cfg:
                    if isinstance(getattr(cfg, "model", None), str) and str(cfg.model).strip():
                        resolved_model = str(cfg.model).strip()
                    if getattr(cfg, "temperature", None) is not None:
                        overrides["temperature"] = float(cfg.temperature)
                    if getattr(cfg, "max_tokens", None) is not None:
                        overrides["max_tokens"] = int(cfg.max_tokens)
                return resolved_model, overrides

            base_dependencies = None
            try:
                if think_plan_result.elaboration and isinstance(think_plan_result.elaboration.dependencies, list):
                    base_dependencies = list(think_plan_result.elaboration.dependencies)
            except Exception:
                base_dependencies = None
            plan_modified = False

            # 反思循环：执行失败时触发多模型反思，最多反思 N 次
            reflection_count = 0
            max_reflection_rounds = int(THINK_REFLECTION_MAX_ROUNDS or 2)
            start_step_order = 1
            run_status = RUN_STATUS_DONE
            last_error = ""

            while True:
                has_feedback_tail = bool(plan_titles) and is_task_feedback_step_title(str(plan_titles[-1] or ""))
                parallel_end = len(plan_titles) - 1 if has_feedback_tail else len(plan_titles)
                tail_step_order = parallel_end + 1 if has_feedback_tail else None

                # 优先使用“详细阐述阶段”输出的显式依赖；当计划被反思插入步骤修改后，回退到本地推断（避免索引错位）。
                parallel_dependencies = None
                if not plan_modified and isinstance(base_dependencies, list) and base_dependencies:
                    parallel_dependencies = base_dependencies
                else:
                    try:
                        inferred = infer_executor_assignments(
                            plan_titles=plan_titles,
                            plan_allows=plan_allows,
                            plan_artifacts=plan_artifacts,
                        )
                        inferred_deps: List[dict] = []
                        for a in inferred.assignments or []:
                            deps = getattr(a, "depends_on", None)
                            if isinstance(deps, list) and deps:
                                inferred_deps.append(
                                    {"step_index": int(a.step_index), "depends_on": [int(d) for d in deps]}
                                )
                        parallel_dependencies = inferred_deps or None
                    except Exception:
                        parallel_dependencies = None

                # 先并行执行（排除确认满意度尾巴，保持反馈闭环语义仍由顺序执行器接管）
                inner_parallel = run_think_parallel_loop(
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
                    end_step_order_inclusive=int(parallel_end),
                    variables_source="agent_think_parallel",
                    step_llm_config_resolver=_resolve_step_llm_config,
                    dependencies=parallel_dependencies,
                    executor_roles=list((think_config.executors or {}).keys()),
                    llm_call=react_loop_facade.create_llm_call,
                    execute_step_action=react_loop_facade._execute_step_action,
                    safe_write_debug=_safe_write_debug,
                )

                exec_started_at = time.monotonic()
                parallel_result = None
                async for kind, payload_inner in pump_sync_generator(
                    inner=inner_parallel,
                    label="think_parallel",
                    poll_interval_seconds=AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
                    idle_timeout_seconds=AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
                ):
                    if kind == "msg":
                        if payload_inner:
                            yield str(payload_inner)
                        continue
                    if kind == "done":
                        parallel_result = payload_inner
                        break
                    if kind == "err":
                        if isinstance(payload_inner, BaseException):
                            raise payload_inner
                        raise RuntimeError(f"think_parallel 异常:{payload_inner}")

                if parallel_result is None:
                    raise RuntimeError("think_parallel 返回为空")

                run_status = str(parallel_result.run_status or "")
                last_step_order = int(getattr(parallel_result, "last_step_order", 0) or 0)

                # 并行阶段成功后，再顺序执行确认满意度（含评估门闩/不满意触发 replan 等后端闭环）。
                if run_status == RUN_STATUS_DONE and tail_step_order is not None:
                    inner_tail = run_react_loop(
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
                        start_step_order=int(tail_step_order),
                        variables_source="agent_think_react_tail",
                        step_llm_config_resolver=_resolve_step_llm_config,
                    )

                    tail_result = None
                    async for kind, payload_inner in pump_sync_generator(
                        inner=inner_tail,
                        label="think_react_tail",
                        poll_interval_seconds=AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
                        idle_timeout_seconds=AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
                    ):
                        if kind == "msg":
                            if payload_inner:
                                yield str(payload_inner)
                            continue
                        if kind == "done":
                            tail_result = payload_inner
                            break
                        if kind == "err":
                            if isinstance(payload_inner, BaseException):
                                raise payload_inner
                            raise RuntimeError(f"think_react_tail 异常:{payload_inner}")

                    if tail_result is None:
                        raise RuntimeError("think_react_tail 返回为空")

                    run_status = str(tail_result.run_status or "")
                    last_step_order = int(getattr(tail_result, "last_step_order", 0) or 0)

                duration_ms = int((time.monotonic() - exec_started_at) * 1000)

                _safe_write_debug(
                    task_id,
                    run_id,
                    message="agent.think.exec.done",
                    data={
                        "duration_ms": duration_ms,
                        "run_status": str(run_status),
                        "last_step_order": int(last_step_order),
                        "reflection_count": int(reflection_count),
                        "has_feedback_tail": bool(has_feedback_tail),
                    },
                    level="info",
                )

                # 如果不是失败，或者已达到反思次数上限，退出循环
                if run_status != RUN_STATUS_FAILED:
                    break

                if reflection_count >= max_reflection_rounds:
                    yield sse_json({"delta": f"{STREAM_TAG_FAIL} 已达反思次数上限（{max_reflection_rounds}次），停止执行\n"})
                    break

                # ========== 触发反思机制 ==========
                reflection_count += 1
                agent_state["reflection_count"] = int(reflection_count)
                yield sse_json({"delta": f"{STREAM_TAG_REFLECTION} 执行失败，启动第 {reflection_count} 次多模型反思…\n"})

                # 收集失败信息
                last_error = f"步骤 {last_step_order} 执行失败"
                observations_text = "\n".join(observations[-10:]) if observations else "(无观测)"

                # 计算已完成步骤
                done_step_indices = [
                    i for i, item in enumerate(plan_items)
                    if item.get("status") == "done"
                ]

                # 收集反思进度
                reflection_progress: List[str] = []

                def collect_reflection_progress(msg: str):
                    reflection_progress.append(msg)

                # 执行反思
                reflection_result = await asyncio.to_thread(
                    run_reflection,
                    config=think_config,
                    error=last_error,
                    observations=observations_text,
                    plan_titles=plan_titles,
                    done_step_indices=done_step_indices,
                    message=message,
                    llm_call_func=llm_call_func,
                    yield_progress=collect_reflection_progress,
                    max_fix_steps=3,
                )

                # 输出反思进度
                for msg in reflection_progress:
                    yield sse_json({"delta": f"{msg}\n"})

                _safe_write_debug(
                    task_id,
                    run_id,
                    message="agent.think.reflection.done",
                    data={
                        "reflection_count": reflection_count,
                        "fix_steps_count": len(reflection_result.fix_steps),
                        "winning_analysis": (
                            reflection_result.winning_analysis.to_dict()
                            if reflection_result.winning_analysis else None
                        ),
                    },
                    level="info",
                )
                # 记录反思摘要到 agent_state（用于评估/审计；避免只留在 debug 输出里难以结构化消费）
                try:
                    records = agent_state.get("reflection_records")
                    if not isinstance(records, list):
                        records = []
                    records.append(
                        {
                            "round": int(reflection_count),
                            "failed_step_order": int(last_step_order),
                            "error": str(last_error or "").strip(),
                            "winning_analysis": (
                                reflection_result.winning_analysis.to_dict()
                                if reflection_result.winning_analysis else None
                            ),
                            "fix_steps": [
                                {
                                    "title": str(s.get("title") or "").strip(),
                                    "brief": str(s.get("brief") or "").strip(),
                                    "allow": s.get("allow") if isinstance(s.get("allow"), list) else [],
                                }
                                for s in (reflection_result.fix_steps or [])
                                if isinstance(s, dict)
                            ],
                        }
                    )
                    agent_state["reflection_records"] = records
                except Exception:
                    pass

                # 如果没有生成修复步骤，停止反思
                if not reflection_result.fix_steps:
                    yield sse_json({"delta": f"{STREAM_TAG_FAIL} 反思未能生成修复步骤，停止执行\n"})
                    break

                # docs/agent：反思接管后，“失败步骤”不应继续阻塞依赖图与最终输出门闩。
                # 策略：
                # 1) 将该步骤在计划栏标记为 skipped（代表旧尝试已作废）
                # 2) 同步把对应 task_steps 从 failed 改为 skipped（保留 error 便于溯源；同时避免评估链路被“历史 failed”硬阻塞）
                # 3) 在其后插入修复步骤；默认追加 1 个“重试原步骤”的步骤提升收敛概率
                failed_step_index = max(0, int(last_step_order) - 1)
                failed_title = str(plan_titles[failed_step_index] or "").strip() if 0 <= failed_step_index < len(plan_titles) else ""
                raw_failed_allow = plan_allows[failed_step_index] if 0 <= failed_step_index < len(plan_allows) else []
                failed_allow: List[str] = (
                    [str(a).strip() for a in raw_failed_allow if str(a).strip()]
                    if isinstance(raw_failed_allow, list)
                    else []
                )

                allow_set = {str(a or "").strip().lower() for a in (failed_allow or []) if str(a or "").strip()}
                can_retry = bool(failed_title) and ("task_output" not in allow_set) and ("user_prompt" not in allow_set)

                max_fix_steps_value = 3
                fix_steps_for_merge = list(reflection_result.fix_steps or [])
                if can_retry:
                    # 预留 1 个名额给 retry，避免插入步数膨胀导致 max_steps 超限
                    fix_steps_for_merge = fix_steps_for_merge[: max(0, int(max_fix_steps_value) - 1)]
                    fix_steps_for_merge.append(
                        {
                            "title": failed_title,
                            "brief": "重试",
                            "allow": list(failed_allow or []),
                        }
                    )
                else:
                    fix_steps_for_merge = fix_steps_for_merge[: int(max_fix_steps_value)]

                if not fix_steps_for_merge:
                    yield sse_json({"delta": f"{STREAM_TAG_FAIL} 修复步骤为空，停止执行\n"})
                    break

                # 将“失败步骤”的 task_step 标记为 skipped（保留 error 字段用于溯源）
                try:
                    step_rows = await asyncio.to_thread(
                        list_task_steps_for_run,
                        task_id=int(task_id),
                        run_id=int(run_id),
                    )
                    target_id: Optional[int] = None
                    target_error: str = ""
                    for row in reversed(step_rows or []):
                        try:
                            if int(row["step_order"] or 0) != int(last_step_order):
                                continue
                            if str(row["status"] or "").strip() != str(STEP_STATUS_FAILED or "failed"):
                                continue
                            target_id = int(row["id"])
                            target_error = str(row["error"] or "").strip()
                            break
                        except Exception:
                            continue
                    if target_id is not None:
                        await asyncio.to_thread(
                            mark_task_step_skipped,
                            step_id=int(target_id),
                            error=target_error or "skipped_by_reflection",
                            finished_at=now_iso(),
                        )
                except Exception as exc:
                    _safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.think.reflection.mark_failed_step_skipped_failed",
                        data={"step_order": int(last_step_order), "error": str(exc)},
                        level="warning",
                    )

                # 合并修复步骤到计划
                new_titles, new_briefs, new_allows = merge_fix_steps_into_plan(
                    current_step_index=failed_step_index,
                    plan_titles=plan_titles,
                    plan_briefs=plan_briefs,
                    plan_allows=plan_allows,
                    fix_steps=fix_steps_for_merge,
                )

                plan_titles = new_titles
                plan_briefs = new_briefs
                plan_allows = new_allows

                # 重建 plan_items（保持已完成/失败状态；插入的修复步骤为 pending）
                old_plan_items = list(plan_items or [])
                insert_pos = failed_step_index + 1
                fix_count = len(fix_steps_for_merge or [])

                plan_items = []
                for i, title in enumerate(plan_titles):
                    brief = plan_briefs[i] if i < len(plan_briefs) else ""
                    allow = plan_allows[i] if i < len(plan_allows) else []
                    status = "pending"
                    if i in done_step_indices:
                        status = "done"
                    elif i == failed_step_index:
                        status = "skipped"
                    elif insert_pos <= i < insert_pos + fix_count:
                        status = "pending"
                    else:
                        # 尽量继承原计划栏状态（避免丢失用户可见信息）
                        old_index = i
                        if i >= insert_pos + fix_count:
                            old_index = i - fix_count
                        if 0 <= old_index < len(old_plan_items) and isinstance(old_plan_items[old_index], dict):
                            raw = str(old_plan_items[old_index].get("status") or "").strip() or "pending"
                            status = "pending" if raw in {"running", "waiting", "planned"} else raw
                    plan_items.append({
                        "id": i + 1,
                        "title": title,
                        "brief": brief,
                        "allow": allow,
                        "status": status,
                    })

                # 更新 agent_state
                agent_state["plan_titles"] = plan_titles
                agent_state["plan_briefs"] = plan_briefs
                agent_state["plan_allows"] = plan_allows
                try:
                    agent_state["executor_assignments"] = build_executor_assignments_payload(
                        plan_titles=plan_titles,
                        plan_allows=plan_allows,
                    )
                except Exception:
                    pass

                # 反思插入步骤后必须持久化最新 plan（否则中断恢复会回到旧 plan，导致 idx/步骤错位）
                try:
                    updated_at = now_iso()
                    await asyncio.to_thread(
                        update_task_run,
                        run_id=int(run_id),
                        agent_plan={
                            "titles": plan_titles,
                            "items": plan_items,
                            "allows": plan_allows,
                            "artifacts": plan_artifacts,
                        },
                        agent_state=agent_state,
                        updated_at=updated_at,
                    )
                except Exception as exc:
                    logger.exception("agent.think.reflection.plan.persist_failed: %s", exc)
                    _safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.think.reflection.plan.persist_failed",
                        data={"error": str(exc)},
                        level="warning",
                    )

                yield sse_json({"type": "plan", "task_id": task_id, "items": plan_items})
                yield sse_json({"delta": f"{STREAM_TAG_REFLECTION} 反思完成，继续从步骤 {last_step_order + 1} 执行…\n"})

                # 从失败步骤的下一步开始继续执行
                start_step_order = last_step_order + 1
                plan_modified = True

            # 统一后处理闭环（收敛到 execution_pipeline）
            out_q: "asyncio.Queue[str]" = asyncio.Queue()

            def _emit(msg: str) -> None:
                try:
                    out_q.put_nowait(str(msg))
                except Exception:
                    return

            final_task = asyncio.create_task(
                run_finalization_sequence(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    run_status=str(run_status),
                    agent_state=agent_state,
                    plan_items=plan_items,
                    plan_artifacts=plan_artifacts,
                    message=message,
                    workdir=workdir,
                    yield_func=_emit,
                )
            )
            async for msg in pump_async_task_messages(final_task, out_q):
                yield msg
            run_status = await final_task

        except (asyncio.CancelledError, GeneratorExit):
            handle_stream_cancellation(task_id=task_id, run_id=run_id, reason="agent_think_stream_cancelled")
            raise
        except Exception as exc:
            out_q = asyncio.Queue()
            err_task = asyncio.create_task(
                handle_execution_exception(
                    exc,
                    task_id=task_id,
                    run_id=run_id,
                    yield_func=_emit,
                    mode_prefix="agent.think",
                )
            )
            async for msg in pump_async_task_messages(err_task, out_q):
                yield msg
            await err_task

        try:
            yield sse_json({"type": "done"}, event="done")
        except BaseException:
            return

    return create_sse_response(gen)
