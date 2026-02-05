import asyncio
import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from backend.src.api.schemas import AgentEvaluateStreamRequest
from backend.src.api.utils import ensure_write_permission
from backend.src.common.utils import error_response, extract_json_object, truncate_text
from backend.src.constants import (
    ERROR_CODE_INVALID_REQUEST,
    ERROR_MESSAGE_LLM_CALL_FAILED,
    HTTP_STATUS_BAD_REQUEST,
    SKILL_CATEGORY_CHOICES,
    AGENT_REVIEW_PASS_SCORE_THRESHOLD,
    AGENT_REVIEW_DISTILL_SCORE_THRESHOLD,
    AGENT_REVIEW_DISTILL_STATUS_ALLOW,
    AGENT_REVIEW_DISTILL_STATUS_DENY,
    AGENT_REVIEW_DISTILL_STATUS_MANUAL,
)
from backend.src.prompt.system_prompts import load_system_prompt
from backend.src.repositories.agent_reviews_repo import create_agent_review_record
from backend.src.repositories.task_outputs_repo import list_task_outputs_for_run
from backend.src.repositories.task_runs_repo import get_task_run
from backend.src.repositories.task_steps_repo import list_task_steps_for_run
from backend.src.repositories.tasks_repo import get_task
from backend.src.repositories.tool_call_records_repo import list_tool_calls_with_tool_name_by_run
from backend.src.services.llm.llm_client import call_openai, resolve_default_model, sse_json
from backend.src.services.skills.skills_publish import publish_skill_file
from backend.src.services.skills.skills_upsert import upsert_skill_from_agent_payload

router = APIRouter()


def _json_preview(value: Any, max_chars: int) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return truncate_text(value, max_chars)
    try:
        return truncate_text(json.dumps(value, ensure_ascii=False), max_chars)
    except Exception:
        return truncate_text(str(value), max_chars)


@router.post("/agent/evaluate/stream")
async def agent_evaluate_stream(payload: AgentEvaluateStreamRequest):
    """
    评估 Agent（MVP）：对某次 run 的执行过程做审查，输出问题清单/改进建议，并维护 0..N skills。
    """
    permission = ensure_write_permission()
    if permission:
        return permission

    run_id = int(payload.run_id)
    if run_id <= 0:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            "run_id 不合法",
            HTTP_STATUS_BAD_REQUEST,
        )

    user_note = str(payload.message or "").strip()
    requested_model = (payload.model or "").strip()
    parameters = payload.parameters or {"temperature": 0}

    run_row = await asyncio.to_thread(get_task_run, run_id=int(run_id))
    if not run_row:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            "run 不存在",
            HTTP_STATUS_BAD_REQUEST,
        )
    task_id = int(run_row["task_id"])

    # 评估模型选择：
    # - do：默认模型（或用户显式指定）
    # - think：优先使用 think_config.agents.evaluator（或用户显式指定），与 docs/agent 对齐
    state_obj = extract_json_object(run_row["agent_state"] or "") or {}
    mode = str(state_obj.get("mode") or "").strip().lower()
    if mode not in {"think", "do"}:
        mode = "do"

    model = requested_model
    if not model:
        if mode == "think":
            base_model = str(state_obj.get("model") or "").strip()
            if not base_model:
                base_model = await asyncio.to_thread(resolve_default_model)
            raw_cfg = state_obj.get("think_config")
            try:
                from backend.src.agent.think import create_think_config_from_dict, get_default_think_config

                think_cfg = (
                    create_think_config_from_dict(raw_cfg, base_model=base_model)
                    if isinstance(raw_cfg, dict) and raw_cfg
                    else get_default_think_config(base_model=base_model)
                )
                model = str(getattr(think_cfg, "evaluator_model", "") or "").strip() or base_model
            except Exception:
                model = base_model
        else:
            model = await asyncio.to_thread(resolve_default_model)

    async def gen():
        cancelled = False
        review_id: Optional[int] = None
        plan_items: List[dict] = []

        try:
            yield sse_json({"delta": "【评估】 读取记录…\n"})

            def _load_records():
                task_row = get_task(task_id=int(task_id))
                step_rows = list_task_steps_for_run(task_id=int(task_id), run_id=int(run_id))
                output_rows = list_task_outputs_for_run(task_id=int(task_id), run_id=int(run_id), order="ASC")
                tool_rows = list_tool_calls_with_tool_name_by_run(run_id=int(run_id), limit=50)
                return task_row, step_rows, output_rows, tool_rows

            task_row, step_rows, output_rows, tool_rows = await asyncio.to_thread(_load_records)

            # 评估过程计划（固定 4 步，避免“长时间无反馈”）
            plan_items = [
                {"id": 1, "brief": "读取记录", "status": "running"},
                {"id": 2, "brief": "评估分析", "status": "pending"},
                {"id": 3, "brief": "落库沉淀", "status": "pending"},
                {"id": 4, "brief": "输出结果", "status": "pending"},
            ]
            yield sse_json({"type": "plan", "task_id": task_id, "run_id": run_id, "items": plan_items})

            task_title = str(task_row["title"]) if task_row else ""
            run_meta = {
                "run_id": run_id,
                "status": run_row["status"],
                "started_at": run_row["started_at"],
                "finished_at": run_row["finished_at"],
                "summary": run_row["summary"],
                "mode": mode,
            }
            if mode == "think":
                vote_records = state_obj.get("vote_records")
                if vote_records is None:
                    vote_records = state_obj.get("plan_votes")
                alternative_plans = state_obj.get("alternative_plans")
                if alternative_plans is None:
                    alternative_plans = state_obj.get("plan_alternatives")
                run_meta["think"] = {
                    "think_config": state_obj.get("think_config"),
                    "winning_planner_id": state_obj.get("winning_planner_id"),
                    "vote_records": vote_records,
                    "alternative_plans": alternative_plans,
                    "reflection_count": state_obj.get("reflection_count"),
                    "reflection_records": state_obj.get("reflection_records"),
                    "executor_assignments": state_obj.get("executor_assignments"),
                }

            plan_obj = extract_json_object(run_row["agent_plan"] or "") or {}
            plan_compact = {
                "titles": plan_obj.get("titles"),
                "allows": plan_obj.get("allows"),
                "artifacts": plan_obj.get("artifacts"),
            }

            steps_compact: List[dict] = []
            for row in step_rows:
                action_type = None
                payload_preview = None
                try:
                    detail_obj = json.loads(row["detail"]) if row["detail"] else None
                    if isinstance(detail_obj, dict):
                        action_type = detail_obj.get("type")
                        payload_preview = detail_obj.get("payload")
                except Exception:
                    action_type = None
                    payload_preview = None

                steps_compact.append(
                    {
                        "step_id": row["id"],
                        "step_order": row["step_order"],
                        "title": row["title"],
                        "status": row["status"],
                        "action_type": action_type,
                        "payload": _json_preview(payload_preview, 360),
                        "result": _json_preview(row["result"], 520),
                        "error": truncate_text(str(row["error"] or ""), 260),
                    }
                )

            outputs_compact: List[dict] = []
            for row in output_rows:
                outputs_compact.append(
                    {
                        "output_id": row["id"],
                        "type": row["output_type"],
                        "content": truncate_text(str(row["content"] or ""), 900),
                        "created_at": row["created_at"],
                    }
                )

            tools_compact: List[dict] = []
            for row in tool_rows:
                tools_compact.append(
                    {
                        "tool_call_record_id": row["id"],
                        "tool_id": row["tool_id"],
                        "tool_name": row["tool_name"],
                        "reuse": bool(row["reuse"]),
                        "reuse_status": row["reuse_status"],
                        "input": truncate_text(str(row["input"] or ""), 360),
                        "output": truncate_text(str(row["output"] or ""), 520),
                        "created_at": row["created_at"],
                    }
                )

            # Step 1 done -> Step 2 running
            for it in plan_items:
                if it.get("status") == "running":
                    it["status"] = "done"
            if len(plan_items) >= 2:
                plan_items[1]["status"] = "running"
            yield sse_json({"type": "plan", "task_id": task_id, "run_id": run_id, "items": plan_items})

            prompt = load_system_prompt("agent_evaluate")
            if not prompt:
                # fallback：避免因文件缺失导致不可用
                prompt = (
                    "你是评估 Agent，只输出 JSON："
                    "{{\"status\":\"pass|needs_changes|fail\",\"summary\":\"\",\"issues\":[],\"next_actions\":[],\"skills\":[]}}。\n"
                    "输入：{task_title}\n{run_meta}\n{plan}\n{steps}\n{outputs}\n{tool_calls}\n"
                )

            skill_categories_text = "\n".join(f"- {c}" for c in SKILL_CATEGORY_CHOICES)
            prompt_text = prompt.format(
                skill_categories=skill_categories_text,
                pass_threshold=int(AGENT_REVIEW_PASS_SCORE_THRESHOLD),
                distill_threshold=int(AGENT_REVIEW_DISTILL_SCORE_THRESHOLD),
                user_note=user_note or "(无)",
                task_title=task_title,
                run_meta=json.dumps(run_meta, ensure_ascii=False),
                plan=json.dumps(plan_compact, ensure_ascii=False),
                steps=json.dumps(steps_compact, ensure_ascii=False),
                outputs=json.dumps(outputs_compact, ensure_ascii=False),
                tool_calls=json.dumps(tools_compact, ensure_ascii=False),
            )

            yield sse_json({"delta": "【评估】 正在审查…\n"})
            text, _, err = await asyncio.to_thread(call_openai, prompt_text, model, parameters)
            obj = extract_json_object(text or "") if not err else None
            llm_ok = isinstance(obj, dict)

            def _coerce_score(value: object) -> Optional[float]:
                try:
                    fv = float(value)  # type: ignore[arg-type]
                except Exception:
                    return None
                if fv < 0:
                    fv = 0.0
                if fv > 100:
                    fv = 100.0
                return float(fv)

            def _normalize_distill_status(value: object) -> str:
                v = str(value or "").strip().lower()
                if v in {
                    AGENT_REVIEW_DISTILL_STATUS_ALLOW,
                    AGENT_REVIEW_DISTILL_STATUS_DENY,
                    AGENT_REVIEW_DISTILL_STATUS_MANUAL,
                }:
                    return v
                return ""

            pass_threshold = float(AGENT_REVIEW_PASS_SCORE_THRESHOLD)
            distill_threshold = float(AGENT_REVIEW_DISTILL_SCORE_THRESHOLD)
            pass_score: Optional[float] = None
            distill_score: Optional[float] = None
            distill_status = ""
            distill_notes = ""

            if not llm_ok:
                status = "fail"
                summary = f"评估失败：{err or 'invalid_json'}"
                issues = [
                    {
                        "title": "评估失败",
                        "severity": "high",
                        "details": "Eval Agent 未能生成有效 JSON（可能是 LLM 配置/网络/提示词/返回格式问题）。",
                        "evidence": truncate_text(str(err or text or ""), 260),
                        "suggestion": "检查设置页的 LLM 配置（API Key/Base URL/Model），并重试 /eval。",
                    }
                ]
                next_actions = [{"title": "修复评估链路", "details": "确认 /agent/evaluate/stream 可用并能写入评估记录。"}]
                skills = []
                pass_score = 0.0
                distill_status = AGENT_REVIEW_DISTILL_STATUS_DENY
                distill_score = 0.0
                distill_notes = "评估 JSON 无效：禁止自动沉淀"
            else:
                status = str(obj.get("status") or "").strip() or "needs_changes"
                normalized_status = str(status or "").strip().lower()
                if normalized_status not in {"pass", "needs_changes", "fail"}:
                    normalized_status = "needs_changes"
                status = normalized_status
                summary = str(obj.get("summary") or "").strip()
                issues = obj.get("issues") if isinstance(obj.get("issues"), list) else []
                next_actions = (
                    obj.get("next_actions") if isinstance(obj.get("next_actions"), list) else []
                )
                skills = obj.get("skills") if isinstance(obj.get("skills"), list) else []

                pass_score = _coerce_score(obj.get("pass_score"))
                pass_threshold_value = _coerce_score(obj.get("pass_threshold"))
                if pass_threshold_value is not None:
                    pass_threshold = float(pass_threshold_value)

                distill_payload = obj.get("distill")
                if isinstance(distill_payload, dict):
                    distill_status = _normalize_distill_status(distill_payload.get("status"))
                    distill_score = _coerce_score(distill_payload.get("score"))
                    distill_threshold_value = _coerce_score(distill_payload.get("threshold"))
                    if distill_threshold_value is not None:
                        distill_threshold = float(distill_threshold_value)
                    distill_notes = str(
                        distill_payload.get("reason") or distill_payload.get("notes") or ""
                    ).strip()

                if not distill_status:
                    distill_status = _normalize_distill_status(obj.get("distill_status"))
                if distill_score is None:
                    distill_score = _coerce_score(obj.get("distill_score"))
                if not distill_notes:
                    distill_notes = str(obj.get("distill_notes") or "").strip()

                if pass_score is None:
                    if status == "pass":
                        pass_score = 100.0
                    elif status == "needs_changes":
                        pass_score = 70.0
                    else:
                        pass_score = 0.0

                # 兜底一致性：status=pass 但 score 未达门槛时，降级为 needs_changes
                if status == "pass" and pass_score is not None and pass_score < pass_threshold:
                    status = "needs_changes"
                    if not summary:
                        summary = "评分未达标：需补齐验证与修复后再交付。"

                # 知识沉淀前置条件：未通过任务评估(pass)则不允许沉淀（与 docs/agent 对齐）
                if status != "pass":
                    distill_status = AGENT_REVIEW_DISTILL_STATUS_DENY
                    distill_score = 0.0

                # backward compatible：旧评估没有 distill 字段时，默认按历史语义（pass=允许沉淀）
                if not distill_status:
                    distill_status = (
                        AGENT_REVIEW_DISTILL_STATUS_ALLOW if status == "pass" else AGENT_REVIEW_DISTILL_STATUS_DENY
                    )
                if distill_score is None:
                    distill_score = float(pass_score or 0.0) if status == "pass" else 0.0

                if (
                    distill_status == AGENT_REVIEW_DISTILL_STATUS_ALLOW
                    and distill_score is not None
                    and distill_score < distill_threshold
                ):
                    distill_status = AGENT_REVIEW_DISTILL_STATUS_MANUAL
                    if not distill_notes:
                        distill_notes = "distill_score 未达门槛：默认不自动沉淀"

            # Step 2 done/failed -> Step 3 running
            for it in plan_items:
                if it.get("status") == "running":
                    it["status"] = "done" if llm_ok else "failed"
            if len(plan_items) >= 3:
                plan_items[2]["status"] = "running"
            yield sse_json({"type": "plan", "task_id": task_id, "run_id": run_id, "items": plan_items})

            applied_skills: List[dict] = []
            if llm_ok and distill_status == AGENT_REVIEW_DISTILL_STATUS_ALLOW:
                for skill in skills:
                    skill_id, upsert_status, upsert_err = await asyncio.to_thread(
                        upsert_skill_from_agent_payload,
                        skill if isinstance(skill, dict) else {},
                        task_id=task_id,
                        run_id=run_id,
                    )
                    if upsert_status in {"created", "updated"} and skill_id:
                        source_path, publish_err = await asyncio.to_thread(publish_skill_file, int(skill_id))
                        applied_skills.append(
                            {
                                "skill_id": int(skill_id),
                                "status": upsert_status,
                                "name": str((skill or {}).get("name") or "").strip(),
                                "source_path": source_path,
                                "error": publish_err or upsert_err,
                            }
                        )
                    else:
                        applied_skills.append(
                            {
                                "skill_id": int(skill_id) if skill_id else None,
                                "status": upsert_status,
                                "name": str((skill or {}).get("name") or "").strip(),
                                "source_path": None,
                                "error": upsert_err,
                            }
                        )
            elif llm_ok and skills:
                # 评估通过但不建议沉淀：保留建议但不落库
                for skill in skills:
                    applied_skills.append(
                        {
                            "skill_id": None,
                            "status": f"skipped_distill_{distill_status}",
                            "name": str((skill or {}).get("name") or "").strip(),
                            "source_path": None,
                            "error": "distill_gate_blocked",
                        }
                    )

            review_id = await asyncio.to_thread(
                create_agent_review_record,
                task_id=int(task_id),
                run_id=int(run_id),
                status=str(status or ""),
                pass_score=pass_score,
                pass_threshold=pass_threshold,
                distill_status=str(distill_status or ""),
                distill_score=distill_score,
                distill_threshold=distill_threshold,
                distill_notes=distill_notes,
                summary=str(summary or ""),
                issues=issues,
                next_actions=next_actions,
                skills=applied_skills,
            )

            # Step 3 done -> Step 4 running
            for it in plan_items:
                if it.get("status") == "running":
                    it["status"] = "done"
            if len(plan_items) >= 4:
                plan_items[3]["status"] = "running"
            yield sse_json({"type": "plan", "task_id": task_id, "run_id": run_id, "items": plan_items})

            yield sse_json(
                {
                    "type": "review",
                    "review_id": review_id,
                    "task_id": task_id,
                    "run_id": run_id,
                    "status": status,
                    "pass_score": pass_score,
                    "pass_threshold": pass_threshold,
                    "distill_status": distill_status,
                    "distill_score": distill_score,
                    "distill_threshold": distill_threshold,
                    "distill_notes": distill_notes,
                    "summary": summary,
                    "issues": issues,
                    "next_actions": next_actions,
                    "skills": applied_skills,
                }
            )
            if summary:
                yield sse_json({"delta": f"【评估】 {summary}\n"})

            # Step 4 done
            for it in plan_items:
                if it.get("status") == "running":
                    it["status"] = "done"
            yield sse_json({"type": "plan", "task_id": task_id, "run_id": run_id, "items": plan_items})
        except Exception as exc:
            # 兜底：即使异常也尽量落库一条 fail 记录，避免前端误以为“评估没触发”。
            try:
                for it in plan_items:
                    if it.get("status") == "running":
                        it["status"] = "failed"
                yield sse_json({"type": "plan", "task_id": task_id, "run_id": run_id, "items": plan_items})
            except Exception:
                pass

            try:
                if review_id is None:
                    review_id = await asyncio.to_thread(
                        create_agent_review_record,
                        task_id=int(task_id),
                        run_id=int(run_id),
                        status="fail",
                        summary="评估异常：请查看 debug/后端日志",
                        issues=[
                            {
                                "title": "评估异常",
                                "severity": "high",
                                "details": "评估流式接口内部发生异常，导致未能完成评估。",
                                "evidence": truncate_text(str(exc), 260),
                                "suggestion": "检查后端日志与 LLM 配置后重试 /eval。",
                            }
                        ],
                        next_actions=[{"title": "修复评估链路", "details": "定位异常来源并补齐回归测试。"}],
                        skills=[],
                    )

                yield sse_json(
                    {
                        "type": "review",
                        "review_id": review_id,
                        "task_id": task_id,
                        "run_id": run_id,
                        "status": "fail",
                        "summary": "评估异常：请查看 debug/后端日志",
                        "issues": [
                            {
                                "title": "评估异常",
                                "severity": "high",
                                "details": "评估流式接口内部发生异常，导致未能完成评估。",
                                "evidence": truncate_text(str(exc), 260),
                                "suggestion": "检查后端日志与 LLM 配置后重试 /eval。",
                            }
                        ],
                        "next_actions": [{"title": "修复评估链路", "details": "定位异常来源并补齐回归测试。"}],
                        "skills": [],
                    }
                )
            except Exception:
                pass

            yield sse_json({"delta": f"【评估】 失败：{exc}\n"})
        except (asyncio.CancelledError, GeneratorExit):
            # SSE 客户端断开/主动取消时：不要再尝试 yield，否则会触发
            # “async generator ignored GeneratorExit/CancelledError” 类错误。
            cancelled = True
            raise
        finally:
            if not cancelled:
                try:
                    yield sse_json({"type": "done"}, event="done")
                except BaseException:
                    return

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(gen(), media_type="text/event-stream", headers=headers)
