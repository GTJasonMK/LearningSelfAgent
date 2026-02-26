import asyncio
from typing import List, Optional

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from backend.src.api.schemas import AgentEvaluateStreamRequest
from backend.src.api.utils import require_write_permission
from backend.src.common.utils import (
    error_response,
    extract_json_object,
    truncate_text,
)
from backend.src.constants import (
    ERROR_CODE_INVALID_REQUEST,
    HTTP_STATUS_BAD_REQUEST,
    AGENT_REVIEW_DISTILL_STATUS_ALLOW,
)
from backend.src.services.agent_review.review_decision import evaluate_review_decision
from backend.src.services.agent_review.review_prompt import (
    build_review_prompt_text,
    resolve_review_model,
)
from backend.src.services.agent_review.review_records import create_agent_review_record
from backend.src.services.agent_review.review_snapshot import (
    build_artifacts_check,
    build_run_meta,
    compact_outputs_for_review,
    compact_steps_for_review,
    compact_tools_for_review,
)
from backend.src.services.llm.llm_client import call_openai, sse_json
from backend.src.services.skills.skills_publish import publish_skill_file
from backend.src.services.skills.skills_upsert import upsert_skill_from_agent_payload
from backend.src.services.tasks.task_queries import (
    get_task,
    get_task_run,
    list_task_outputs_for_run,
    list_task_steps_for_run,
)
from backend.src.services.tools.tools_query import list_tool_calls_with_tool_name_by_run

router = APIRouter()


@router.post("/agent/evaluate/stream")
@require_write_permission
async def agent_evaluate_stream(payload: AgentEvaluateStreamRequest):
    """
    评估 Agent（MVP）：对某次 run 的执行过程做审查，输出问题清单/改进建议，并维护 0..N skills。
    """
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

    run_row = await asyncio.to_thread(get_task_run, run_id=run_id)
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

    model = await asyncio.to_thread(
        resolve_review_model,
        mode=mode,
        state_obj=state_obj if isinstance(state_obj, dict) else None,
        requested_model=requested_model,
    )

    async def gen():
        cancelled = False
        review_id: Optional[int] = None
        plan_items: List[dict] = []

        try:
            yield sse_json({"delta": "【评估】 读取记录…\n"})

            def _load_records():
                task_row = get_task(task_id=task_id)
                step_rows = list_task_steps_for_run(task_id=task_id, run_id=run_id)
                output_rows = list_task_outputs_for_run(task_id=task_id, run_id=run_id, order="ASC")
                tool_rows = list_tool_calls_with_tool_name_by_run(run_id=run_id, limit=50)
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

            plan_obj = extract_json_object(run_row["agent_plan"] or "") or {}
            plan_compact = {
                "titles": plan_obj.get("titles"),
                "allows": plan_obj.get("allows"),
                "artifacts": plan_obj.get("artifacts"),
            }
            plan_artifacts = plan_obj.get("artifacts") if isinstance(plan_obj.get("artifacts"), list) else []

            artifacts_check_workdir, artifacts_check_items, missing_artifacts = build_artifacts_check(
                plan_artifacts=plan_artifacts,
                state_obj=state_obj if isinstance(state_obj, dict) else None,
            )
            run_meta = build_run_meta(
                run_id=run_id,
                run_row=run_row,
                mode=mode,
                state_obj=state_obj if isinstance(state_obj, dict) else None,
                workdir=artifacts_check_workdir,
                artifacts_check_items=artifacts_check_items,
                missing_artifacts=missing_artifacts,
            )

            steps_compact = compact_steps_for_review(
                step_rows,
                payload_key="payload",
                result_key="result",
                error_key="error",
                max_items=None,
            )
            outputs_compact = compact_outputs_for_review(
                output_rows,
                content_key="content",
                max_items=None,
                content_max_chars=900,
            )
            tools_compact = compact_tools_for_review(tool_rows)

            # Step 1 done -> Step 2 running
            for it in plan_items:
                if it.get("status") == "running":
                    it["status"] = "done"
            if len(plan_items) >= 2:
                plan_items[1]["status"] = "running"
            yield sse_json({"type": "plan", "task_id": task_id, "run_id": run_id, "items": plan_items})

            prompt_text = build_review_prompt_text(
                task_title=task_title,
                run_meta=run_meta,
                plan_compact=plan_compact,
                steps_compact=steps_compact,
                outputs_compact=outputs_compact,
                tools_compact=tools_compact,
                user_note=user_note or "(无)",
            )

            yield sse_json({"delta": "【评估】 正在审查…\n"})
            text, _, err = await asyncio.to_thread(call_openai, prompt_text, model, parameters)
            obj = extract_json_object(text or "") if not err else None
            llm_ok = isinstance(obj, dict)
            skills = obj.get("skills") if llm_ok and isinstance(obj.get("skills"), list) else []
            decision = evaluate_review_decision(
                obj=obj,
                err=err,
                raw_text=str(text or ""),
                step_rows=step_rows,
                output_rows=output_rows,
                tool_rows=tool_rows,
                plan_artifacts=plan_artifacts,
                artifacts_check_items=artifacts_check_items,
                find_unverified_text_output_fn=lambda _rows: None,
            )
            status = decision["status"]
            summary = decision["summary"]
            issues = decision["issues"]
            next_actions = decision["next_actions"]
            pass_score = decision["pass_score"]
            pass_threshold = decision["pass_threshold"]
            distill_status = decision["distill_status"]
            distill_score = decision["distill_score"]
            distill_threshold = decision["distill_threshold"]
            distill_notes = decision["distill_notes"]
            distill_evidence_refs = decision["distill_evidence_refs"]

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
                task_id=task_id,
                run_id=run_id,
                status=str(status or ""),
                pass_score=pass_score,
                pass_threshold=pass_threshold,
                distill_status=str(distill_status or ""),
                distill_score=distill_score,
                distill_threshold=distill_threshold,
                distill_notes=distill_notes,
                distill_evidence_refs=distill_evidence_refs,
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
                    "distill_evidence_refs": distill_evidence_refs,
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
                        task_id=task_id,
                        run_id=run_id,
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
