from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.src.agent.core.checkpoint_store import persist_checkpoint_async
from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.debug_utils import safe_write_debug
from backend.src.agent.runner.pending_planning_state import (
    build_initial_pending_state,
    build_planned_state_after_pending,
    build_waiting_followup_state,
)
from backend.src.agent.runner.planning_runner import run_do_planning_phase_with_stream
from backend.src.constants import (
    AGENT_KNOWLEDGE_SUFFICIENCY_KIND,
    AGENT_KNOWLEDGE_SUFFICIENCY_PROCEED_VALUE,
)
from backend.src.common.utils import parse_positive_int
from backend.src.services.llm.llm_client import sse_json
from backend.src.services.tasks.task_run_lifecycle import enqueue_postprocess_thread, mark_run_failed


def _normalize_plan(
    *,
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
    plan_artifacts: List[str],
) -> PlanStructure:
    return PlanStructure.from_legacy(
        plan_titles=list(plan_titles or []),
        plan_items=list(plan_items or []),
        plan_allows=[list(value or []) for value in (plan_allows or [])],
        plan_artifacts=list(plan_artifacts or []),
    )


def _normalize_user_input_text(value: str) -> str:
    return "".join(str(value or "").strip().lower().split())


def _looks_like_proceed_choice_value(value: str) -> bool:
    """
    判定 choice.value 是否表达“按当前信息继续”语义。
    """
    text = _normalize_user_input_text(str(value or ""))
    if not text:
        return False
    canonical = _normalize_user_input_text(str(AGENT_KNOWLEDGE_SUFFICIENCY_PROCEED_VALUE or ""))
    if canonical and text == canonical:
        return True
    return text in {
        "proceed",
        "proceedwithassumptions",
        "proceedwithcurrentinfo",
        "continuewithassumptions",
        "continuewithcurrentinfo",
    }


def _is_proceed_with_current_info_answer(*, user_input: str, paused: dict) -> bool:
    """
    识别“按当前信息继续执行”的用户选择语义。

    目的：
    - 用户已经明确接受“先按已知信息推进并在结果中列假设”时，
      避免再次进入 ask_user 循环；
    - 保持判定尽量基于 choices/value（结构化），文本关键词仅作兜底。
    """
    normalized_input = _normalize_user_input_text(str(user_input or ""))
    if not normalized_input:
        return False

    direct_markers = (
        "按当前已知信息继续执行",
        "按当前信息继续",
        "继续执行并明确列出关键假设",
        "按已有信息继续",
    )
    if any(_normalize_user_input_text(marker) in normalized_input for marker in direct_markers):
        return True

    choices = paused.get("choices") if isinstance(paused, dict) else None
    if _looks_like_proceed_choice_value(normalized_input):
        return True

    if not isinstance(choices, list):
        return False

    for item in choices:
        if not isinstance(item, dict):
            continue
        value_text = _normalize_user_input_text(str(item.get("value") or ""))
        if not value_text:
            continue
        if value_text != normalized_input:
            continue
        if _looks_like_proceed_choice_value(value_text):
            return True
        label_text = str(item.get("label") or "")
        if ("按当前" in label_text) or ("继续" in label_text):
            return True
    return False


def _ensure_single_feedback_tail(
    *,
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
) -> None:
    """
    统一规范“确认满意度”步骤：
    - 无论规划结果中是否出现同名步骤，都只保留一个；
    - 且该步骤必须位于计划尾部。
    """
    from backend.src.agent.runner.feedback import canonicalize_task_feedback_steps

    canonicalize_task_feedback_steps(
        plan_titles=plan_titles,
        plan_items=plan_items,
        plan_allows=plan_allows,
        keep_single_tail=True,
        feedback_asked=False,
        max_steps=None,
    )


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
    yield_func: Callable[[str], None] = lambda _msg: None,
    safe_write_debug_func: Optional[Callable[..., None]] = None,
    run_finalization_sequence_func: Optional[Callable[..., Any]] = None,
    yield_done_event_func: Optional[Callable[[Callable[[str], None]], None]] = None,
) -> str:
    """
    docs/agent：知识不足需询问用户时，先进入 waiting；resume 后重新检索+规划再继续执行。
    """
    from backend.src.agent.runner.react_step_executor import handle_user_prompt_action
    from backend.src.constants import ACTION_TYPE_USER_PROMPT, RUN_STATUS_WAITING

    run_finalization_sequence = run_finalization_sequence_func
    yield_done_event = yield_done_event_func
    if run_finalization_sequence is None or yield_done_event is None:
        from backend.src.agent.runner.execution_pipeline import (
            run_finalization_sequence as _run_finalization_sequence,
        )
        from backend.src.agent.runner.execution_pipeline import yield_done_event as _yield_done_event

        run_finalization_sequence = run_finalization_sequence or _run_finalization_sequence
        yield_done_event = yield_done_event or _yield_done_event

    question = str(user_prompt_question or "").strip()
    if not question:
        return RUN_STATUS_WAITING

    normalized_mode = str(mode or "").strip().lower() or "do"
    if normalized_mode not in {"do", "think"}:
        normalized_mode = "do"

    plan_titles = [f"user_prompt: {question}"]
    plan_items: List[dict] = [{"id": 1, "brief": "补充信息", "status": "pending"}]
    plan_allows = [[ACTION_TYPE_USER_PROMPT]]
    plan_artifacts: List[str] = []
    plan_struct = _normalize_plan(
        plan_titles=plan_titles,
        plan_items=plan_items,
        plan_allows=plan_allows,
        plan_artifacts=plan_artifacts,
    )
    plan_titles, plan_items, plan_allows, plan_artifacts = plan_struct.to_legacy_lists()

    yield_func(sse_json({"type": "plan", "task_id": int(task_id), "run_id": int(run_id), "items": plan_items}))

    agent_state = build_initial_pending_state(
        message=message,
        model=model,
        parameters=parameters,
        max_steps=max_steps,
        workdir=workdir,
        mode=normalized_mode,
        tools_hint=tools_hint,
        skills_hint=skills_hint,
        solutions_hint=solutions_hint,
        memories_hint=memories_hint,
        graph_hint=graph_hint,
        domain_ids=list(domain_ids or []),
        skills=list(skills or []),
        solutions=list(solutions or []),
        draft_solution_id=draft_solution_id,
        think_config=think_config,
    )

    payload_obj = {"question": question, "kind": AGENT_KNOWLEDGE_SUFFICIENCY_KIND}
    prompt_gen = handle_user_prompt_action(
        task_id=int(task_id),
        run_id=int(run_id),
        step_order=1,
        title=str(plan_titles[0] or ""),
        payload_obj=payload_obj,
        plan_struct=plan_struct,
        agent_state=agent_state,
        safe_write_debug=safe_write_debug_func or safe_write_debug,
    )

    run_status = RUN_STATUS_WAITING
    try:
        while True:
            event = next(prompt_gen)
            if event:
                yield_func(str(event))
    except StopIteration as stop:
        value = stop.value if isinstance(stop.value, tuple) else None
        run_status = value[0] if value and len(value) >= 1 else RUN_STATUS_WAITING

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
    """
    from backend.src.agent.runner.execution_pipeline import (
        prepare_planning_knowledge_do,
        prepare_planning_knowledge_think,
        retrieve_all_knowledge,
        yield_done_event,
    )
    from backend.src.constants import (
        ACTION_TYPE_USER_PROMPT,
        AGENT_MAX_STEPS_UNLIMITED,
        AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
        AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
        RUN_STATUS_FAILED,
        RUN_STATUS_RUNNING,
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

    force_proceed_with_assumptions = _is_proceed_with_current_info_answer(
        user_input=user_input_text,
        paused=paused if isinstance(paused, dict) else {},
    )
    if force_proceed_with_assumptions and isinstance(agent_state, dict):
        agent_state["knowledge_sufficiency_override"] = "proceed_with_assumptions"

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
        except (TypeError, ValueError, AttributeError):
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

    if need_user_prompt and user_prompt_question and force_proceed_with_assumptions:
        need_user_prompt = False
        user_prompt_question = ""
        yield_func(sse_json({"delta": f"{STREAM_TAG_EXEC} 已按你的选择继续执行，将基于当前信息推进并明确关键假设。\n"}))
        safe_debug(
            task_id,
            run_id,
            message="agent.knowledge_sufficiency.force_proceed",
            data={"user_input": user_input_text},
            level="info",
        )

    if need_user_prompt and user_prompt_question:
        from backend.src.agent.runner.react_step_executor import handle_user_prompt_action

        new_plan_titles = [pending_prompt_title, f"user_prompt: {user_prompt_question}"]
        new_plan_items = [pending_prompt_item, {"id": 2, "brief": "补充信息", "status": "pending"}]
        new_plan_allows = [list(pending_prompt_allow), [ACTION_TYPE_USER_PROMPT]]
        new_plan_artifacts: List[str] = []
        new_plan_struct = _normalize_plan(
            plan_titles=new_plan_titles,
            plan_items=new_plan_items,
            plan_allows=new_plan_allows,
            plan_artifacts=new_plan_artifacts,
        )
        new_plan_titles, new_plan_items, new_plan_allows, new_plan_artifacts = new_plan_struct.to_legacy_lists()

        yield_func(sse_json({"type": "plan", "task_id": int(task_id), "run_id": int(run_id), "items": new_plan_items}))

        agent_state = build_waiting_followup_state(
            agent_state=agent_state,
            mode=str(pending_mode),
            message=str(message_for_planning or "").strip(),
            tools_hint=str(enriched.get("tools_hint") or "(无)"),
            skills_hint=str(enriched.get("skills_hint") or "(无)"),
            solutions_hint=str(enriched.get("solutions_hint") or "(无)"),
            graph_hint=graph_hint,
            domain_ids=list(domain_ids or []),
            step_order=2,
        )

        payload_obj = {"question": user_prompt_question, "kind": AGENT_KNOWLEDGE_SUFFICIENCY_KIND}
        prompt_gen = handle_user_prompt_action(
            task_id=int(task_id),
            run_id=int(run_id),
            step_order=2,
            title=str(new_plan_titles[1] or ""),
            payload_obj=payload_obj,
            plan_struct=new_plan_struct,
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

    skills = list(enriched.get("skills") or skills or [])
    skills_hint = str(enriched.get("skills_hint") or skills_hint or "(无)")
    solutions_for_prompt = list(enriched.get("solutions_for_prompt") or solutions or [])
    draft_solution_id = enriched.get("draft_solution_id")
    draft_solution_id_value = parse_positive_int(draft_solution_id, default=None)
    solutions_hint = str(enriched.get("solutions_hint") or "(无)")
    tools_hint = str(enriched.get("tools_hint") or "(无)")

    planning_max_steps = int(AGENT_MAX_STEPS_UNLIMITED)
    planning_max_steps_value = int(planning_max_steps)

    append_feedback = append_task_feedback_step_func
    if append_feedback is None:
        from backend.src.agent.runner.feedback import append_task_feedback_step as _append

        append_feedback = _append

    def ensure_feedback_tail(*, titles: List[str], items: List[dict], allows: List[List[str]]) -> None:
        # 兼容旧注入（单测/运行时 patch）：先按旧逻辑尝试 append，再做规范化收敛。
        if callable(append_feedback):
            append_feedback(
                plan_titles=titles,
                plan_items=items,
                plan_allows=allows,
                max_steps=None,
            )
        _ensure_single_feedback_tail(
            plan_titles=titles,
            plan_items=items,
            plan_allows=allows,
        )

    if pending_mode == "think":
        from backend.src.agent.think import (
            create_think_config_from_dict,
            get_default_think_config,
            run_think_planning_sync,
        )
        from backend.src.agent.think.think_execution import build_executor_assignments_payload
        from backend.src.services.llm.llm_client import call_openai

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
                max_steps=planning_max_steps_value,
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

        ensure_feedback_tail(
            titles=plan_titles_gen,
            items=plan_items_gen,
            allows=plan_allows_gen,
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
        merged_plan_struct = _normalize_plan(
            plan_titles=merged_titles,
            plan_items=merged_items,
            plan_allows=merged_allows,
            plan_artifacts=plan_artifacts_new,
        )
        merged_titles, merged_items, merged_allows, plan_artifacts_new = merged_plan_struct.to_legacy_lists()

        yield_func(sse_json({"type": "plan", "task_id": int(task_id), "run_id": int(run_id), "items": merged_items}))

        resume_step_order = 2
        extra_state: Dict[str, Any] = {
            "winning_planner_id": getattr(think_plan_result, "winning_planner_id", None),
            "alternative_plans": getattr(think_plan_result, "alternative_plans", []) or [],
            "vote_records": getattr(think_plan_result, "vote_records", []) or [],
        }
        extra_state["plan_alternatives"] = list(extra_state.get("alternative_plans") or [])
        extra_state["plan_votes"] = list(extra_state.get("vote_records") or [])
        try:
            extra_state["executor_assignments"] = build_executor_assignments_payload(
                plan_titles=merged_titles,
                plan_allows=merged_allows,
            )
        except (TypeError, ValueError, AttributeError) as exc:
            safe_debug(
                int(task_id),
                int(run_id),
                message="agent.pending_planning.executor_assignments_failed",
                data={"error": str(exc)},
                level="warning",
            )

        agent_state = build_planned_state_after_pending(
            agent_state=agent_state,
            mode="think",
            message=message_for_planning,
            tools_hint=tools_hint,
            skills_hint=skills_hint,
            solutions_hint=solutions_hint,
            graph_hint=graph_hint,
            domain_ids=list(domain_ids or []),
            skills=list(skills or []),
            solutions=list(solutions_for_prompt or []),
            draft_solution_id=draft_solution_id_value,
            step_order=int(resume_step_order),
            extra_state=extra_state,
        )

        persist_error = await persist_checkpoint_async(
            run_id=int(run_id),
            status=RUN_STATUS_RUNNING,
            agent_plan=merged_plan_struct.to_agent_plan_payload(),
            agent_state=agent_state,
            task_id=int(task_id),
            safe_write_debug=safe_debug,
            where="resume_pending_planning.think.persist",
        )
        if persist_error:
            safe_debug(
                int(task_id),
                int(run_id),
                message="agent.pending_planning.persist_failed",
                data={"error": str(persist_error)},
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

    from backend.src.agent.planning_phase import PlanPhaseFailure

    try:
        if run_planning_phase_func is not None:
            # 兼容测试注入：保留原可注入入口。
            inner = run_planning_phase_func(
                task_id=int(task_id),
                run_id=int(run_id),
                message=message_for_planning,
                workdir=workdir,
                model=model,
                parameters=parameters,
                max_steps=planning_max_steps_value,
                tools_hint=tools_hint,
                skills_hint=skills_hint,
                solutions_hint=solutions_hint,
                memories_hint=memories_hint,
                graph_hint=graph_hint,
            )
            from backend.src.agent.runner.stream_pump import pump_sync_generator

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
        else:
            plan_result = await run_do_planning_phase_with_stream(
                task_id=int(task_id),
                run_id=int(run_id),
                message=message_for_planning,
                workdir=workdir,
                model=model,
                parameters=parameters,
                max_steps=planning_max_steps_value,
                tools_hint=tools_hint,
                skills_hint=skills_hint,
                solutions_hint=solutions_hint,
                memories_hint=memories_hint,
                graph_hint=graph_hint,
                yield_func=yield_func,
                safe_write_debug=safe_debug,
                debug_done_message="agent.plan_resume_pending.done",
                pump_label="planning_resume_pending",
                poll_interval_seconds=AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
                idle_timeout_seconds=AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
            )

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

    ensure_feedback_tail(
        titles=plan_titles_gen,
        items=plan_items_gen,
        allows=plan_allows_gen,
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
    merged_plan_struct = _normalize_plan(
        plan_titles=merged_titles,
        plan_items=merged_items,
        plan_allows=merged_allows,
        plan_artifacts=plan_artifacts_new,
    )
    merged_titles, merged_items, merged_allows, plan_artifacts_new = merged_plan_struct.to_legacy_lists()

    yield_func(sse_json({"type": "plan", "task_id": int(task_id), "run_id": int(run_id), "items": merged_items}))

    resume_step_order = 2
    agent_state = build_planned_state_after_pending(
        agent_state=agent_state,
        mode="do",
        message=message_for_planning,
        tools_hint=tools_hint,
        skills_hint=skills_hint,
        solutions_hint=solutions_hint,
        graph_hint=graph_hint,
        domain_ids=list(domain_ids or []),
        skills=list(skills or []),
        solutions=list(solutions_for_prompt or []),
        draft_solution_id=draft_solution_id_value,
        step_order=int(resume_step_order),
    )

    persist_error = await persist_checkpoint_async(
        run_id=int(run_id),
        status=RUN_STATUS_RUNNING,
        agent_plan=merged_plan_struct.to_agent_plan_payload(),
        agent_state=agent_state,
        task_id=int(task_id),
        safe_write_debug=safe_debug,
        where="resume_pending_planning.do.persist",
    )
    if persist_error:
        safe_debug(
            int(task_id),
            int(run_id),
            message="agent.pending_planning.persist_failed",
            data={"error": str(persist_error)},
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
