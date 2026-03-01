import json
import os
import time
import threading
from typing import Any, Callable, TypeVar

from backend.src.common.app_error_utils import invalid_request_error, not_found_error
from backend.src.common.errors import AppError
from backend.src.common.sql import run_with_sqlite_locked_retry
from backend.src.common.serializers import llm_record_from_row
from backend.src.common.utils import dump_model, now_iso, render_prompt
from backend.src.constants import (
    ERROR_MESSAGE_LLM_CALL_FAILED,
    ERROR_MESSAGE_PROMPT_NOT_FOUND,
    ERROR_MESSAGE_PROMPT_RENDER_FAILED,
    LLM_PROVIDER_OPENAI,
    LLM_STATUS_DRY_RUN,
    LLM_STATUS_ERROR,
    LLM_STATUS_RUNNING,
    LLM_STATUS_SUCCESS,
)
from backend.src.services.llm.llm_client import call_llm, classify_llm_error_text
from backend.src.storage import get_connection

T = TypeVar("T")


def _read_int_env(name: str, default: int, *, min_value: int = 0) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return int(default)
    try:
        value = int(float(raw))
    except Exception:
        return int(default)
    if value < int(min_value):
        return int(min_value)
    return int(value)


LLM_CALL_MAX_ATTEMPTS = _read_int_env("LLM_CALL_MAX_ATTEMPTS", 3, min_value=1)
LLM_CALL_RETRY_BASE_SECONDS = 0.6
# create_llm_call 级硬超时（秒）：防止供应商 SDK 在少数情况下长期阻塞导致 run 永久 running。
LLM_CALL_HARD_TIMEOUT_SECONDS = _read_int_env("LLM_CALL_HARD_TIMEOUT_SECONDS", 30, min_value=5)


def _fetch_llm_record_by_id(conn, record_id: int):
    return conn.execute("SELECT * FROM llm_records WHERE id = ?", (record_id,)).fetchone()


def _with_sqlite_locked_retry(op: Callable[[], T]) -> T:
    """
    SQLite 并发写入兜底：遇到短暂 locked 时做小幅退避重试。

    说明：
    - 依赖 storage.get_connection 的 busy_timeout 先等待；
    - 若仍抛出 locked，再做最多 3 次轻量重试，避免并行 Agent 把瞬时争用误判为“步骤失败”。
    """
    return run_with_sqlite_locked_retry(op, attempts=3, base_delay_seconds=0.05)


def _call_llm_with_hard_timeout(
    *,
    prompt_text: str,
    model: str,
    parameters: Any,
    provider: str,
    timeout_seconds: int,
):
    """
    对 call_llm 增加线程级硬超时，避免单次 SDK 卡死拖垮整个 run。

    说明：
    - 超时时仅中断主流程，后台线程会作为 daemon 继续/退出，不阻塞当前请求返回；
    - 异常语义保持为普通 Exception，由上层重试/记录。
    """
    box: dict = {}

    def _worker():
        try:
            box["result"] = call_llm(prompt_text, model, parameters, provider=provider)
        except Exception as exc:  # pragma: no cover - 由调用方行为断言
            box["error"] = exc

    worker = threading.Thread(target=_worker, daemon=True)
    worker.start()
    worker.join(timeout=float(timeout_seconds))
    if worker.is_alive():
        raise TimeoutError(f"LLM call timeout after {int(timeout_seconds)}s")

    err = box.get("error")
    if err is not None:
        raise err
    return box.get("result")


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
        raise invalid_request_error(ERROR_MESSAGE_PROMPT_RENDER_FAILED)

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
            raise not_found_error(ERROR_MESSAGE_PROMPT_NOT_FOUND)
        rendered = render_prompt(
            template_row["template"],
            data.get("variables") if isinstance(data.get("variables"), dict) else None,
        )
        if rendered is None:
            raise invalid_request_error(ERROR_MESSAGE_PROMPT_RENDER_FAILED)
        prompt_text = rendered

    if not prompt_text:
        raise invalid_request_error(ERROR_MESSAGE_PROMPT_RENDER_FAILED)

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
                return _fetch_llm_record_by_id(conn, record_id)

        row = _with_sqlite_locked_retry(_mark_dry_run)
        return {"record": llm_record_from_row(row)}

    parameters = data.get("parameters") if isinstance(data.get("parameters"), dict) else None
    def _should_retry_llm_error(error_text: str) -> bool:
        kind = classify_llm_error_text(error_text)
        return kind in ("rate_limit", "transient")

    response_text = None
    tokens = None
    error_message = None
    for attempt in range(1, int(LLM_CALL_MAX_ATTEMPTS) + 1):
        try:
            call_result = _call_llm_with_hard_timeout(
                prompt_text=prompt_text,
                model=model,
                parameters=parameters,
                provider=provider,
                timeout_seconds=int(LLM_CALL_HARD_TIMEOUT_SECONDS),
            )
            if isinstance(call_result, tuple) and len(call_result) >= 2:
                response_text, tokens = call_result[0], call_result[1]
            else:
                raise RuntimeError(ERROR_MESSAGE_LLM_CALL_FAILED)
            error_message = None
            break
        except AppError as exc:
            error_message = exc.message or ERROR_MESSAGE_LLM_CALL_FAILED
        except Exception as exc:
            error_message = str(exc) or ERROR_MESSAGE_LLM_CALL_FAILED

        if attempt >= int(LLM_CALL_MAX_ATTEMPTS):
            break
        if not _should_retry_llm_error(error_message):
            break
        time.sleep(float(LLM_CALL_RETRY_BASE_SECONDS) * float(attempt))
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
                return _fetch_llm_record_by_id(conn, record_id)

        row = _with_sqlite_locked_retry(_mark_error)
        return {"record": llm_record_from_row(row)}

    def _mark_success():
        with get_connection() as conn:
            conn.execute(
                "UPDATE llm_records SET response = ?, status = ?, error = NULL, finished_at = ?, updated_at = ?, tokens_prompt = ?, tokens_completion = ?, tokens_total = ? WHERE id = ?",
                (
                    response_text,
                    LLM_STATUS_SUCCESS,
                    finished_at,
                    finished_at,
                    tokens.get("prompt") if isinstance(tokens, dict) else None,
                    tokens.get("completion") if isinstance(tokens, dict) else None,
                    tokens.get("total") if isinstance(tokens, dict) else None,
                    record_id,
                ),
            )
            return _fetch_llm_record_by_id(conn, record_id)

    row = _with_sqlite_locked_retry(_mark_success)
    return {"record": llm_record_from_row(row)}
