import asyncio
import json
from typing import Generator, List, Optional

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from backend.src.agent.runner.stream_pump import pump_sync_generator
from backend.src.actions.executor import _execute_step_action
from backend.src.api.schemas import TaskExecuteRequest
from backend.src.api.tasks.route_common import ensure_task_exists_or_error
from backend.src.common.serializers import task_run_from_row, task_step_from_row
from backend.src.api.utils import now_iso, require_write_permission
from backend.src.common.errors import AppError
from backend.src.common.utils import action_type_from_step_detail, truncate_text
from backend.src.constants import (
    ACTION_TYPE_LLM_CALL,
    ACTION_TYPE_SHELL_COMMAND,
    ACTION_TYPE_TASK_OUTPUT,
    ACTION_TYPE_TOOL_CALL,
    AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
    AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_TASK_NOT_FOUND,
    HTTP_STATUS_NOT_FOUND,
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_RUNNING,
    RUN_STATUS_STOPPED,
    RUN_STATUS_WAITING,
    STATUS_DONE,
    STATUS_FAILED,
    STATUS_RUNNING,
    STATUS_STOPPED,
    STATUS_WAITING,
    STREAM_TAG_EXEC,
    STREAM_TAG_FAIL,
    STREAM_TAG_OK,
    STREAM_TAG_SKIP,
    STREAM_TAG_STEP,
    STREAM_RESULT_PREVIEW_MAX_CHARS,
    STEP_STATUS_DONE,
    STEP_STATUS_FAILED,
    STEP_STATUS_PLANNED,
    STEP_STATUS_RUNNING,
    STEP_STATUS_SKIPPED,
    STEP_STATUS_WAITING,
)
from backend.src.services.llm.llm_client import sse_json
from backend.src.services.output.output_format import format_visible_result
from backend.src.services.tasks.task_queries import (
    create_task_run as create_task_run_record,
)
from backend.src.services.tasks.task_queries import get_task as get_task_repo
from backend.src.services.tasks.task_queries import get_task_run
from backend.src.services.tasks.task_queries import get_task_step as get_task_step_repo
from backend.src.services.tasks.task_queries import list_task_steps_for_task
from backend.src.services.tasks.task_queries import mark_task_step_done
from backend.src.services.tasks.task_queries import mark_task_step_failed
from backend.src.services.tasks.task_queries import mark_task_step_running
from backend.src.services.tasks.task_queries import mark_task_step_skipped
from backend.src.services.tasks.task_queries import update_task as update_task_repo
from backend.src.services.tasks.task_queries import update_task_run as update_task_run_repo
from backend.src.services.tasks.task_queries import update_task_step as update_task_step_repo

router = APIRouter()

def _truncate_preview(text: str) -> str:
    return truncate_text(str(text or ""), STREAM_RESULT_PREVIEW_MAX_CHARS)


def _format_step_result_preview(action_type: Optional[str], result) -> Optional[str]:
    if not action_type or result is None:
        return None
    try:
        if action_type == ACTION_TYPE_TASK_OUTPUT and isinstance(result, dict):
            content = result.get("content") or ""
            content_preview = _truncate_preview(str(content))
            return f"output: {content_preview}" if content_preview else None
        if action_type == ACTION_TYPE_SHELL_COMMAND and isinstance(result, dict):
            stdout = _truncate_preview(str(result.get("stdout") or ""))
            stderr = _truncate_preview(str(result.get("stderr") or ""))
            if stdout:
                return f"stdout: {stdout}"
            if stderr:
                return f"stderr: {stderr}"
            return f"returncode: {result.get('returncode')}"
        if action_type == ACTION_TYPE_TOOL_CALL and isinstance(result, dict):
            out = _truncate_preview(str(result.get("output") or ""))
            return f"tool_output: {out}" if out else None
        if action_type == ACTION_TYPE_LLM_CALL and isinstance(result, dict):
            out = _truncate_preview(str(result.get("response") or ""))
            return f"llm: {out}" if out else None
    except Exception:
        return None
    return None


def _build_run_event(
    *,
    event_type: str,
    task_id: int,
    run_id: int,
    status: Optional[str] = None,
    created_at: Optional[str] = None,
) -> dict:
    payload = {
        "type": str(event_type or "").strip(),
        "task_id": int(task_id),
        "run_id": int(run_id),
    }
    if status is not None:
        payload["status"] = str(status or "").strip().lower()
    if created_at is not None:
        payload["created_at"] = str(created_at or "").strip() or None
    return payload


def _execute_task_with_messages(
    task_id: int, payload: Optional[TaskExecuteRequest] = None
) -> Generator[object, None, dict]:
    """
    复用 execute_task 的执行逻辑，并在关键节点产出可用于 SSE 的文本消息。

    说明：
    - 该 generator 会真正修改数据库（创建 run、更新 step 状态等）
    - yield 的内容包含两类：
      1) 字符串：用于桌宠气泡实时展示
      2) 结构化 dict：run_created/run_status 等前端状态事件
    - generator return 的 dict 与 execute_task 的返回结构一致
    """
    run_summary = payload.run_summary if payload else None
    max_retries = payload.max_retries if payload and payload.max_retries is not None else 0
    on_failure = payload.on_failure if payload and payload.on_failure else "stop"
    created_at = now_iso()

    task_row = get_task_repo(task_id=task_id)
    if not task_row:
        raise AppError(
            code=ERROR_CODE_NOT_FOUND,
            message=ERROR_MESSAGE_TASK_NOT_FOUND,
            status_code=HTTP_STATUS_NOT_FOUND,
        )

    run_id: Optional[int] = None
    run_id, _created, _updated = create_task_run_record(
        task_id=task_id,
        status=RUN_STATUS_RUNNING,
        summary=run_summary,
        started_at=created_at,
        finished_at=None,
        created_at=created_at,
        updated_at=created_at,
    )

    # 任务允许多次执行：每次开始执行都应标记为 running，但 started_at 仅在首次执行写入
    update_task_repo(task_id=task_id, status=STATUS_RUNNING, updated_at=created_at)

    step_rows = list_task_steps_for_task(task_id=task_id)

    executed_steps = []
    run_status = RUN_STATUS_DONE
    last_emitted_run_status = ""
    # 执行上下文：用于把上一步结果带入下一步（最低限度支持“两步规划：llm_call -> task_output”）
    context: dict = {"last_llm_response": None}

    def _emit_run_status(status: object) -> Optional[dict]:
        nonlocal last_emitted_run_status
        normalized = str(status or "").strip().lower()
        if not normalized or normalized == last_emitted_run_status:
            return None
        last_emitted_run_status = normalized
        return _build_run_event(
            event_type="run_status",
            task_id=int(task_id),
            run_id=int(run_id),
            status=normalized,
        )

    try:
        yield f"{STREAM_TAG_EXEC} 开始执行任务 #{task_id}: {task_row['title']}"
        yield _build_run_event(
            event_type="run_created",
            task_id=int(task_id),
            run_id=int(run_id),
            status=RUN_STATUS_RUNNING,
            created_at=created_at,
        )
        running_event = _emit_run_status(RUN_STATUS_RUNNING)
        if running_event is not None:
            yield running_event

        for step_row in step_rows:
            if step_row["status"] not in {STEP_STATUS_PLANNED, STEP_STATUS_RUNNING}:
                continue

            action_type = None
            if step_row["detail"]:
                action_type = action_type_from_step_detail(step_row["detail"])

            yield f"{STREAM_TAG_STEP} {step_row['title']}"

            attempts = 0
            step_completed = False
            while attempts <= max_retries:
                attempts += 1
                updated_at = now_iso()
                mark_task_step_running(
                    step_id=int(step_row["id"]),
                    run_id=int(run_id),
                    attempts=int(attempts),
                    started_at=updated_at,
                    updated_at=updated_at,
                )
                try:
                    result, error_message = _execute_step_action(task_id, run_id, step_row, context=context)
                except Exception as exc:
                    # 避免异常冒泡导致 run 永远停留在 running
                    result, error_message = None, f"step 执行异常: {exc}"

                updated_at = now_iso()
                if error_message:
                    if attempts <= max_retries:
                        update_task_step_repo(
                            step_id=int(step_row["id"]),
                            error=str(error_message),
                            updated_at=updated_at,
                        )
                    else:
                        if on_failure == "skip":
                            mark_task_step_skipped(
                                step_id=int(step_row["id"]),
                                error=str(error_message),
                                finished_at=updated_at,
                                updated_at=updated_at,
                            )
                            step_completed = True
                            yield f"{STREAM_TAG_SKIP} {step_row['title']}（{error_message}）"
                        else:
                            mark_task_step_failed(
                                step_id=int(step_row["id"]),
                                error=str(error_message),
                                finished_at=updated_at,
                                updated_at=updated_at,
                            )
                            run_status = RUN_STATUS_FAILED
                            step_completed = True
                            yield f"{STREAM_TAG_FAIL} {step_row['title']}（{error_message}）"
                            failed_event = _emit_run_status(RUN_STATUS_FAILED)
                            if failed_event is not None:
                                yield failed_event
                else:
                    result_value = None
                    if result is not None:
                        try:
                            result_value = json.dumps(result, ensure_ascii=False)
                        except Exception:
                            result_value = json.dumps({"text": str(result)}, ensure_ascii=False)
                    mark_task_step_done(
                        step_id=int(step_row["id"]),
                        result=result_value,
                        finished_at=updated_at,
                        updated_at=updated_at,
                    )
                    step_completed = True
                    yield f"{STREAM_TAG_OK} {step_row['title']}"
                    # 记录上下文：用于后续 task_output 自动填充
                    if action_type == ACTION_TYPE_LLM_CALL and isinstance(result, dict):
                        resp = result.get("response")
                        if isinstance(resp, str) and resp.strip():
                            context["last_llm_response"] = resp
                    if action_type == ACTION_TYPE_TASK_OUTPUT and isinstance(result, dict):
                        content_text = str(result.get("content") or "").strip()
                        if content_text:
                            # 对于 task_output，直接把可见结果输出到 SSE（比截断预览更符合桌宠使用）
                            yield format_visible_result(content_text)
                            preview = None
                        else:
                            preview = _format_step_result_preview(action_type, result)
                    else:
                        preview = _format_step_result_preview(action_type, result)
                    if preview:
                        yield preview
                if step_completed:
                    break

            row = get_task_step_repo(step_id=int(step_row["id"]))
            executed_steps.append(task_step_from_row(row))
            if run_status == RUN_STATUS_FAILED:
                break

        # 若仍存在 waiting 步骤，run 不应被标记为 done。
        # 典型场景：用户输入步骤尚未恢复，当前轮 execute 只是触发到等待态。
        if run_status == RUN_STATUS_DONE:
            try:
                latest_rows = list_task_steps_for_task(task_id=task_id)
            except Exception:
                latest_rows = []
            has_waiting_step = any(
                str((row or {}).get("status") or "").strip().lower() == STEP_STATUS_WAITING
                for row in (latest_rows or [])
            )
            if has_waiting_step:
                run_status = RUN_STATUS_WAITING
                yield f"{STREAM_TAG_EXEC} 检测到 waiting 步骤，等待用户输入后可继续执行"
                waiting_event = _emit_run_status(RUN_STATUS_WAITING)
                if waiting_event is not None:
                    yield waiting_event
            else:
                done_event = _emit_run_status(RUN_STATUS_DONE)
                if done_event is not None:
                    yield done_event
        elif run_status == RUN_STATUS_FAILED:
            failed_event = _emit_run_status(RUN_STATUS_FAILED)
            if failed_event is not None:
                yield failed_event
    except GeneratorExit:
        # SSE 客户端断开/上层主动关闭 generator 时：
        # - 必须把 run/task 收敛到 stopped，避免错误标记为 done；
        # - 并回退 running/waiting steps 为 planned，便于后续继续执行。
        run_status = RUN_STATUS_STOPPED
        raise
    except Exception as exc:
        run_status = RUN_STATUS_FAILED
        yield f"{STREAM_TAG_FAIL} 任务执行异常（{exc}）"
        failed_event = _emit_run_status(RUN_STATUS_FAILED)
        if failed_event is not None:
            yield failed_event
    finally:
        finished_at = now_iso()
        if run_id is not None:
            update_task_run_repo(run_id=int(run_id), status=run_status, updated_at=finished_at)
        # 只要一次执行尝试结束（无论成功/失败），任务就不应继续保持 running，避免前端一直显示“正在执行”
        if run_status == RUN_STATUS_DONE:
            task_final_status = STATUS_DONE
        elif run_status == RUN_STATUS_STOPPED:
            task_final_status = STATUS_STOPPED
        elif run_status == RUN_STATUS_WAITING:
            task_final_status = STATUS_WAITING
        else:
            task_final_status = STATUS_FAILED
        update_task_repo(task_id=task_id, status=task_final_status, updated_at=finished_at)
        if run_status == RUN_STATUS_STOPPED and run_id is not None:
            try:
                from backend.src.services.tasks.task_recovery import stop_task_run_records

                stop_task_run_records(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    reason="tasks_execute_stream_cancelled",
                )
            except Exception:
                pass

    eval_response = None
    skill_response = None
    graph_update = None
    if run_status in {RUN_STATUS_DONE, RUN_STATUS_FAILED, RUN_STATUS_STOPPED}:
        from backend.src.services.tasks.task_postprocess import postprocess_task_run

        eval_response, skill_response, graph_update = postprocess_task_run(
            task_row=task_row,
            task_id=task_id,
            run_id=run_id,
            run_status=run_status,
        )

    run_row = get_task_run(run_id=int(run_id))

    return {
        "run": task_run_from_row(run_row),
        "steps": executed_steps,
        "eval": eval_response,
        "skill": skill_response,
        "graph_update": graph_update,
    }


@router.post("/tasks/{task_id}/execute")
@require_write_permission
async def execute_task(task_id: int, payload: Optional[TaskExecuteRequest] = None) -> dict:
    task_exists_error = ensure_task_exists_or_error(task_id=task_id)
    if task_exists_error:
        return task_exists_error

    def _run() -> dict:
        inner = _execute_task_with_messages(task_id, payload)
        try:
            while True:
                next(inner)
        except StopIteration as exc:
            return exc.value or {}

    return await asyncio.to_thread(_run)


@router.post("/tasks/{task_id}/execute/stream")
@require_write_permission
async def execute_task_stream(task_id: int, payload: Optional[TaskExecuteRequest] = None):
    """
    执行任务（SSE 流式）：用于桌宠实时显示 step 进度。

    data: {"delta":"..."} 逐段输出；event: done 表示结束；event: error 表示失败。
    """
    task_exists_error = ensure_task_exists_or_error(task_id=task_id)
    if task_exists_error:
        return task_exists_error

    async def gen():
        cancelled = False
        try:
            inner = _execute_task_with_messages(task_id, payload)
            async for kind, payload_obj in pump_sync_generator(
                inner=inner,
                label=f"task_execute:{task_id}",
                poll_interval_seconds=AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
                idle_timeout_seconds=AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
            ):
                if kind == "msg":
                    msg = payload_obj
                    if isinstance(msg, dict):
                        msg_type = str(msg.get("type") or "").strip()
                        if msg_type:
                            yield sse_json(msg)
                            continue
                    if msg:
                        yield sse_json({"delta": f"{msg}\n"})
                    continue
                if kind == "done":
                    # 兜底：即便中间 run_status 丢失，也尽量在 done 前补一个最终状态事件。
                    if isinstance(payload_obj, dict):
                        run_obj = payload_obj.get("run") if isinstance(payload_obj.get("run"), dict) else None
                        run_id = int(run_obj.get("id") or 0) if isinstance(run_obj, dict) else 0
                        status = str(run_obj.get("status") or "").strip().lower() if isinstance(run_obj, dict) else ""
                        if run_id > 0 and status:
                            yield sse_json(
                                _build_run_event(
                                    event_type="run_status",
                                    task_id=int(task_id),
                                    run_id=int(run_id),
                                    status=status,
                                )
                            )
                    return
                if kind == "err":
                    yield sse_json({"message": f"任务执行失败:{payload_obj}"}, event="error")
                    return
        except (asyncio.CancelledError, GeneratorExit):
            # SSE 客户端断开/主动取消时：不要再尝试 yield，否则会触发
            # “async generator ignored GeneratorExit/CancelledError” 类错误。
            cancelled = True
            raise
        except Exception as exc:
            try:
                yield sse_json({"message": f"任务执行失败:{exc}"}, event="error")
            except BaseException:
                return
        finally:
            # done 事件的数据前端目前不会消费，但保留结构方便后续升级
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
