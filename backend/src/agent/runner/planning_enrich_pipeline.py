# -*- coding: utf-8 -*-
"""
Planning 前知识增强管道：
- 知识充分性评估
- compose_skills / create_draft_skill 分支
- solution draft 草拟
- 生成 planning 使用的 hints
"""

import asyncio
import sqlite3
from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.src.services.llm.llm_client import sse_json
from backend.src.agent.runner.debug_utils import safe_write_debug
from backend.src.common.utils import parse_positive_int


NON_FATAL_KNOWLEDGE_ERRORS = (sqlite3.Error, RuntimeError, TypeError, ValueError, OSError)


def _normalize_tools_limit(value: object, *, default: int = 8) -> int:
    normalized = parse_positive_int(value, default=None)
    if normalized is None:
        return int(default)
    return int(normalized)


def _normalize_optional_positive_id(value: object) -> Optional[int]:
    return parse_positive_int(value, default=None)


async def _publish_skill_draft_if_created(
    *,
    publish_func: Callable[..., Any],
    draft_skill_id: Optional[int],
    task_id: Optional[int],
    run_id: Optional[int],
    debug_message: str,
) -> Tuple[bool, Optional[str]]:
    if draft_skill_id is None:
        return False, None
    try:
        source_path, publish_err = await asyncio.to_thread(
            publish_func,
            int(draft_skill_id),
        )
        if publish_err:
            safe_write_debug(
                task_id,
                run_id,
                message=str(debug_message),
                data={"draft_skill_id": int(draft_skill_id), "error": str(publish_err)},
                level="warning",
            )
            return False, None
        return True, str(source_path or "") if source_path is not None else None
    except NON_FATAL_KNOWLEDGE_ERRORS as exc:
        safe_write_debug(
            task_id,
            run_id,
            message=str(debug_message),
            data={"draft_skill_id": int(draft_skill_id), "error": str(exc)},
            level="warning",
        )
        return False, None


async def _create_methodology_draft_skill_record(
    *,
    create_func: Callable[..., Any],
    skill_create_params_cls: object,
    task_id: Optional[int],
    run_id: Optional[int],
    name: str,
    description: str,
    steps: List[object],
    domain_id: str,
    create_failed_message: str,
) -> Optional[int]:
    if task_id is None or run_id is None:
        return None
    try:
        draft_skill_id = await asyncio.to_thread(
            create_func,
            skill_create_params_cls(
                name=str(name or ""),
                description=str(description or ""),
                steps=list(steps or []),
                task_id=int(task_id),
                domain_id=str(domain_id or "misc"),
                skill_type="methodology",
                status="draft",
                source_task_id=int(task_id),
                source_run_id=int(run_id),
            ),
        )
    except NON_FATAL_KNOWLEDGE_ERRORS as exc:
        safe_write_debug(
            task_id,
            run_id,
            message=str(create_failed_message or ""),
            data={"error": str(exc)},
            level="warning",
        )
        return None
    return _normalize_optional_positive_id(draft_skill_id)


def _append_methodology_draft_skill(
    *,
    skills: List[dict],
    draft_skill_id: Optional[int],
    domain_id: str,
    result_obj: object,
) -> bool:
    if draft_skill_id is None:
        return False
    skills.append(
        {
            "id": int(draft_skill_id),
            "name": str(getattr(result_obj, "name", "") or ""),
            "description": str(getattr(result_obj, "description", "") or ""),
            "steps": list(getattr(result_obj, "steps", []) or []),
            "domain_id": str(getattr(result_obj, "domain_id", "") or domain_id),
            "skill_type": "methodology",
            "status": "draft",
        }
    )
    return True


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
    draft_solution_from_skills_func: Optional[Callable[..., Any]] = None,
    create_skill_func: Optional[Callable[..., Any]] = None,
    publish_skill_file_func: Optional[Callable[..., Any]] = None,
    collect_tools_from_solutions_func: Optional[Callable[..., str]] = None,
    tools_limit: int = 8,
    debug_message_prefix: str = "agent.solution_draft",
) -> Dict[str, Any]:
    """
    planning 阶段“方案草稿（Create 流程 A）”兜底。
    """
    from backend.src.agent.support import _collect_tools_from_solutions, _draft_solution_from_skills
    from backend.src.constants import STREAM_TAG_SOLUTIONS
    from backend.src.services.skills.skills_draft import SkillCreateParams, create_skill
    from backend.src.services.skills.skills_publish import publish_skill_file

    draft_solution_func = draft_solution_from_skills_func or _draft_solution_from_skills
    create_func = create_skill_func or create_skill
    publish_func = publish_skill_file_func or publish_skill_file
    collect_tools_func = collect_tools_from_solutions_func or _collect_tools_from_solutions

    solutions_for_prompt = list(solutions or [])
    draft_solution_id: Optional[int] = None

    if task_id is None or run_id is None:
        return {"solutions_for_prompt": solutions_for_prompt, "draft_solution_id": None}

    if solutions_for_prompt or not skills:
        return {"solutions_for_prompt": solutions_for_prompt, "draft_solution_id": None}

    mode_tag = str(mode or "").strip().lower() or "do"

    tools_limit_value = _normalize_tools_limit(tools_limit, default=8)

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
    for skill in skills or []:
        if not isinstance(skill, dict):
            continue
        sid = _normalize_optional_positive_id(skill.get("id"))
        if sid is None or sid in seen:
            continue
        seen.add(sid)
        skill_ids_for_tags.append(int(sid))
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
    except NON_FATAL_KNOWLEDGE_ERRORS as exc:
        yield_func(sse_json({"delta": f"{STREAM_TAG_SOLUTIONS} 草稿方案落库失败，继续规划\n"}))
        safe_write_debug(
            task_id,
            run_id,
            message=f"{debug_message_prefix}.create_failed",
            data={"error": str(exc)},
            level="warning",
        )
        return {"solutions_for_prompt": solutions_for_prompt, "draft_solution_id": None}

    draft_solution_id_value = _normalize_optional_positive_id(draft_solution_id)
    if draft_solution_id_value is None:
        yield_func(sse_json({"delta": f"{STREAM_TAG_SOLUTIONS} 草稿方案落库失败，继续规划\n"}))
        safe_write_debug(
            task_id,
            run_id,
            message=f"{debug_message_prefix}.create_invalid_id",
            data={"draft_solution_id": draft_solution_id},
            level="warning",
        )
        return {"solutions_for_prompt": solutions_for_prompt, "draft_solution_id": None}

    try:
        _source_path, publish_err = await asyncio.to_thread(publish_func, int(draft_solution_id_value))
        if publish_err:
            safe_write_debug(
                task_id,
                run_id,
                message=f"{debug_message_prefix}.publish_failed",
                data={"draft_solution_id": int(draft_solution_id_value), "error": str(publish_err)},
                level="warning",
            )
    except NON_FATAL_KNOWLEDGE_ERRORS as exc:
        safe_write_debug(
            task_id,
            run_id,
            message=f"{debug_message_prefix}.publish_failed",
            data={"draft_solution_id": int(draft_solution_id_value), "error": str(exc)},
            level="warning",
        )

    solutions_for_prompt = [
        {
            "id": int(draft_solution_id_value),
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
                    f"#{int(draft_solution_id_value)} {getattr(draft_result, 'name', '')}\n"
                )
            }
        )
    )
    safe_write_debug(
        task_id,
        run_id,
        message=f"{debug_message_prefix}.created",
        data={
            "draft_solution_id": int(draft_solution_id_value),
            "name": str(getattr(draft_result, "name", "") or ""),
            "tool_names": list(getattr(draft_result, "tool_names", None) or []),
            "artifacts": list(getattr(draft_result, "artifacts", None) or []),
        },
        level="info",
    )

    return {
        "solutions_for_prompt": solutions_for_prompt,
        "draft_solution_id": int(draft_solution_id_value),
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
    assess_knowledge_sufficiency_func: Optional[Callable[..., Any]] = None,
    compose_skills_func: Optional[Callable[..., Any]] = None,
    draft_skill_from_message_func: Optional[Callable[..., Any]] = None,
    draft_solution_from_skills_func: Optional[Callable[..., Any]] = None,
    create_skill_func: Optional[Callable[..., Any]] = None,
    publish_skill_file_func: Optional[Callable[..., Any]] = None,
    format_skills_for_prompt_func: Optional[Callable[..., str]] = None,
    format_solutions_for_prompt_func: Optional[Callable[..., str]] = None,
    collect_tools_from_solutions_func: Optional[Callable[..., str]] = None,
    mode_tag: str = "do",
    tools_limit: int = 8,
    solution_draft_debug_prefix: str = "agent.solution_draft",
) -> Dict[str, Any]:
    """
    do 模式 planning 前的知识增强收敛点。
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
    from backend.src.services.skills.skills_draft import SkillCreateParams, create_skill
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
    tools_limit_value = _normalize_tools_limit(tools_limit, default=8)

    need_user_prompt = False
    user_prompt_question = ""

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
                draft_skill_id_value = await _create_methodology_draft_skill_record(
                    create_func=create_func,
                    skill_create_params_cls=SkillCreateParams,
                    task_id=task_id,
                    run_id=run_id,
                    name=str(getattr(compose_result, "name", "") or ""),
                    description=str(getattr(compose_result, "description", "") or ""),
                    steps=list(getattr(compose_result, "steps", []) or []),
                    domain_id=str(getattr(compose_result, "domain_id", "") or "misc"),
                    create_failed_message="agent.skill_compose.create_failed",
                )
                if task_id is not None and run_id is not None:
                    yield_func(
                        sse_json(
                            {
                                "delta": (
                                    f"{STREAM_TAG_KNOWLEDGE} 已创建组合技能："
                                    f"{getattr(compose_result, 'name', '')}"
                                    + (
                                        f"（草稿 #{int(draft_skill_id_value)}）\n"
                                        if draft_skill_id_value is not None
                                        else "（落库失败，仍可继续规划）\n"
                                    )
                                )
                            }
                        )
                    )

                    if draft_skill_id_value is not None:
                        published_ok, source_path = await _publish_skill_draft_if_created(
                            publish_func=publish_func,
                            draft_skill_id=draft_skill_id_value,
                            task_id=task_id,
                            run_id=run_id,
                            debug_message="agent.skill_compose.publish_failed",
                        )
                        if published_ok:
                            safe_write_debug(
                                task_id,
                                run_id,
                                message="agent.skill_compose.published",
                                data={
                                    "draft_skill_id": int(draft_skill_id_value),
                                    "source_path": source_path,
                                },
                                level="info",
                            )

                    safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.skill_compose.success",
                        data={
                            "draft_skill_id": draft_skill_id_value,
                            "name": str(getattr(compose_result, "name", "") or ""),
                            "source_skill_ids": list(getattr(compose_result, "source_skill_ids", []) or []),
                        },
                    )

                    if _append_methodology_draft_skill(
                        skills=skills,
                        draft_skill_id=draft_skill_id_value,
                        domain_id=str(getattr(compose_result, "domain_id", "") or "misc"),
                        result_obj=compose_result,
                    ):
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
            domain_candidate = list(domain_ids or ["misc"])
            domain_id = str((domain_candidate[0] if domain_candidate else "misc") or "").strip() or "misc"

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
                draft_skill_id_value = await _create_methodology_draft_skill_record(
                    create_func=create_func,
                    skill_create_params_cls=SkillCreateParams,
                    task_id=task_id,
                    run_id=run_id,
                    name=str(getattr(draft_result, "name", "") or ""),
                    description=str(getattr(draft_result, "description", "") or ""),
                    steps=list(getattr(draft_result, "steps", []) or []),
                    domain_id=str(getattr(draft_result, "domain_id", "") or domain_id),
                    create_failed_message="agent.skill_draft.create_failed",
                )

                yield_func(
                    sse_json(
                        {
                            "delta": (
                                f"{STREAM_TAG_KNOWLEDGE} 已草拟技能："
                                f"{getattr(draft_result, 'name', '')}"
                                + (
                                    f"（草稿 #{int(draft_skill_id_value)}）\n"
                                    if draft_skill_id_value is not None
                                    else "（落库失败，仍可继续规划）\n"
                                )
                            )
                        }
                    )
                )

                if draft_skill_id_value is not None:
                    await _publish_skill_draft_if_created(
                        publish_func=publish_func,
                        draft_skill_id=draft_skill_id_value,
                        task_id=task_id,
                        run_id=run_id,
                        debug_message="agent.skill_draft.publish_failed",
                    )

                    if _append_methodology_draft_skill(
                        skills=skills,
                        draft_skill_id=draft_skill_id_value,
                        domain_id=domain_id,
                        result_obj=draft_result,
                    ):
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
    draft_solution_id_value = _normalize_optional_positive_id(draft.get("draft_solution_id"))

    solutions_hint = format_solutions_func(solutions_for_prompt)
    if solutions:
        names = ", ".join(str(s.get("name") or "").strip() for s in solutions if isinstance(s, dict) and s.get("name"))
        if names:
            yield_func(sse_json({"delta": f"{STREAM_TAG_SOLUTIONS} 已加载：{names}\n"}))
        else:
            yield_func(sse_json({"delta": f"{STREAM_TAG_SOLUTIONS} 已加载：{len(solutions)} 个\n"}))
    else:
        if draft_solution_id_value is None:
            yield_func(sse_json({"delta": f"{STREAM_TAG_SOLUTIONS} 未命中\n"}))

    ids: List[int] = []
    for solution in (solutions or [])[:8]:
        if not isinstance(solution, dict):
            continue
        sid = _normalize_optional_positive_id(solution.get("id"))
        if sid is not None:
            ids.append(int(sid))
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
            "draft_solution_id": draft_solution_id_value,
        },
    )

    tools_hint = collect_tools_func(solutions_for_prompt or [], limit=int(tools_limit_value))

    return {
        "skills": skills,
        "skills_hint": skills_hint or "(无)",
        "solutions_for_prompt": solutions_for_prompt,
        "solutions_hint": solutions_hint or "(无)",
        "tools_hint": tools_hint or "(无)",
        "draft_solution_id": draft_solution_id_value,
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
    think 模式 planning 前的知识增强。
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
        tools_limit=_normalize_tools_limit(tools_limit, default=12),
        solution_draft_debug_prefix="agent.think.solution_draft",
    )
