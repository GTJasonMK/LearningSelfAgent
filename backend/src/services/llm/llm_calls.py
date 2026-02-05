import json
import sqlite3
import time
from typing import Any, Callable, TypeVar

from backend.src.common.errors import AppError
from backend.src.common.serializers import llm_record_from_row
from backend.src.common.utils import dump_model, now_iso, render_prompt
from backend.src.constants import (
    ERROR_CODE_INVALID_REQUEST,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_LLM_CALL_FAILED,
    ERROR_MESSAGE_PROMPT_NOT_FOUND,
    ERROR_MESSAGE_PROMPT_RENDER_FAILED,
    HTTP_STATUS_BAD_REQUEST,
    HTTP_STATUS_NOT_FOUND,
    LLM_PROVIDER_OPENAI,
    LLM_STATUS_DRY_RUN,
    LLM_STATUS_ERROR,
    LLM_STATUS_RUNNING,
    LLM_STATUS_SUCCESS,
)
from backend.src.services.llm.llm_client import call_llm
from backend.src.storage import get_connection

T = TypeVar("T")


def _with_sqlite_locked_retry(op: Callable[[], T]) -> T:
    """
    SQLite 并发写入兜底：遇到短暂 locked 时做小幅退避重试。

    说明：
    - 依赖 storage.get_connection 的 busy_timeout 先等待；
    - 若仍抛出 locked，再做最多 3 次轻量重试，避免并行 Agent 把瞬时争用误判为“步骤失败”。
    """
    last_exc: Exception | None = None
    for attempt in range(0, 3):
        try:
            return op()
        except sqlite3.OperationalError as exc:
            last_exc = exc
            if "locked" in str(exc or "").lower() and attempt < 2:
                time.sleep(0.05 * (attempt + 1))
                continue
            raise
    raise RuntimeError(str(last_exc))


def create_llm_call(payload: Any) -> dict:
    """
    创建一次 LLM 调用并写入 llm_records（同步）。

    说明：
    - 这是“业务逻辑函数”，会被 Agent 执行链路复用；
    - API 层的权限校验（ensure_write_permission）应由路由函数负责；
    - 返回值保持与旧 /llm/calls 一致：成功时 {"record": ...}；失败时抛出 AppError（由 API 层捕获并转为错误响应）。
    """
    data = dump_model(payload)
    if not data.get("prompt") and data.get("template_id") is None:
        raise AppError(
            code=ERROR_CODE_INVALID_REQUEST,
            message=ERROR_MESSAGE_PROMPT_RENDER_FAILED,
            status_code=HTTP_STATUS_BAD_REQUEST,
        )

    prompt_text = data.get("prompt")
    if prompt_text is not None and not isinstance(prompt_text, str):
        prompt_text = str(prompt_text)
    template_id = data.get("template_id")
    if template_id is not None:
        with get_connection() as conn:
            template_row = conn.execute(
                "SELECT * FROM prompt_templates WHERE id = ?", (template_id,)
            ).fetchone()
        if not template_row:
            raise AppError(
                code=ERROR_CODE_NOT_FOUND,
                message=ERROR_MESSAGE_PROMPT_NOT_FOUND,
                status_code=HTTP_STATUS_NOT_FOUND,
            )
        rendered = render_prompt(
            template_row["template"],
            data.get("variables") if isinstance(data.get("variables"), dict) else None,
        )
        if rendered is None:
            raise AppError(
                code=ERROR_CODE_INVALID_REQUEST,
                message=ERROR_MESSAGE_PROMPT_RENDER_FAILED,
                status_code=HTTP_STATUS_BAD_REQUEST,
            )
        prompt_text = rendered

    if not prompt_text:
        raise AppError(
            code=ERROR_CODE_INVALID_REQUEST,
            message=ERROR_MESSAGE_PROMPT_RENDER_FAILED,
            status_code=HTTP_STATUS_BAD_REQUEST,
        )

    try:
        from backend.src.services.llm.llm_client import resolve_default_model, resolve_default_provider

        provider = data.get("provider") or resolve_default_provider()
        model = data.get("model") or resolve_default_model()
    except Exception:
        provider = data.get("provider")
        model = data.get("model")
    provider = provider or LLM_PROVIDER_OPENAI

    variables_value = json.dumps(data.get("variables")) if data.get("variables") else None
    parameters_value = json.dumps(data.get("parameters")) if data.get("parameters") else None
    created_at = now_iso()
    started_at = created_at
    updated_at = created_at
    status = LLM_STATUS_RUNNING

    def _insert_record() -> int:
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO llm_records (prompt, response, task_id, run_id, provider, model, prompt_template_id, variables, parameters, status, error, started_at, finished_at, created_at, updated_at, tokens_prompt, tokens_completion, tokens_total) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    prompt_text,
                    "",
                    data.get("task_id"),
                    data.get("run_id"),
                    provider,
                    model,
                    template_id,
                    variables_value,
                    parameters_value,
                    status,
                    None,
                    started_at,
                    None,
                    created_at,
                    updated_at,
                    None,
                    None,
                    None,
                ),
            )
            return int(cursor.lastrowid)

    record_id = _with_sqlite_locked_retry(_insert_record)

    if data.get("dry_run"):
        finished_at = now_iso()
        def _mark_dry_run():
            with get_connection() as conn:
                conn.execute(
                    "UPDATE llm_records SET status = ?, finished_at = ?, updated_at = ? WHERE id = ?",
                    (LLM_STATUS_DRY_RUN, finished_at, finished_at, record_id),
                )
                return conn.execute(
                    "SELECT * FROM llm_records WHERE id = ?", (record_id,)
                ).fetchone()

        row = _with_sqlite_locked_retry(_mark_dry_run)
        return {"record": llm_record_from_row(row)}

    parameters = data.get("parameters") if isinstance(data.get("parameters"), dict) else None
    try:
        response_text, tokens = call_llm(prompt_text, model, parameters, provider=provider)
        error_message = None
    except AppError as exc:
        response_text = None
        tokens = None
        error_message = exc.message or ERROR_MESSAGE_LLM_CALL_FAILED
    except Exception as exc:
        response_text = None
        tokens = None
        error_message = str(exc) or ERROR_MESSAGE_LLM_CALL_FAILED
    finished_at = now_iso()
    if error_message:
        def _mark_error():
            with get_connection() as conn:
                conn.execute(
                    "UPDATE llm_records SET status = ?, error = ?, finished_at = ?, updated_at = ? WHERE id = ?",
                    (
                        LLM_STATUS_ERROR,
                        error_message,
                        finished_at,
                        finished_at,
                        record_id,
                    ),
                )
                return conn.execute(
                    "SELECT * FROM llm_records WHERE id = ?", (record_id,)
                ).fetchone()

        row = _with_sqlite_locked_retry(_mark_error)
        return {"record": llm_record_from_row(row)}

    def _mark_success():
        with get_connection() as conn:
            conn.execute(
                "UPDATE llm_records SET response = ?, status = ?, finished_at = ?, updated_at = ?, tokens_prompt = ?, tokens_completion = ?, tokens_total = ? WHERE id = ?",
                (
                    response_text,
                    LLM_STATUS_SUCCESS,
                    finished_at,
                    finished_at,
                    tokens["prompt"] if tokens else None,
                    tokens["completion"] if tokens else None,
                    tokens["total"] if tokens else None,
                    record_id,
                ),
            )
            return conn.execute(
                "SELECT * FROM llm_records WHERE id = ?", (record_id,)
            ).fetchone()

    row = _with_sqlite_locked_retry(_mark_success)
    return {"record": llm_record_from_row(row)}
