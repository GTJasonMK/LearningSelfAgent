# -*- coding: utf-8 -*-
"""
ReAct 循环步骤执行模块。

提供动作生成、步骤执行、观测生成等核心逻辑。
"""

import json
import logging
import os
import re
import time
import threading
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, TypeVar

from backend.src.agent.contracts.stream_events import (
    build_need_input_payload,
    generate_prompt_token,
)
from backend.src.agent.support import _truncate_observation
from backend.src.agent.runner.react_helpers import (
    call_llm_for_text,
    needs_nonempty_task_output_content,
    validate_and_normalize_action_text,
)
from backend.src.agent.runner.need_input_choices import resolve_need_input_choices
from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.plan_events import sse_plan_delta
from backend.src.agent.runner.react_state_manager import resolve_executor
from backend.src.common.utils import coerce_int, now_iso
from backend.src.constants import (
    ACTION_TYPE_FILE_WRITE,
    ACTION_TYPE_FILE_READ,
    ACTION_TYPE_FILE_APPEND,
    ACTION_TYPE_FILE_LIST,
    ACTION_TYPE_FILE_DELETE,
    ACTION_TYPE_HTTP_REQUEST,
    ACTION_TYPE_LLM_CALL,
    ACTION_TYPE_JSON_PARSE,
    ACTION_TYPE_MEMORY_WRITE,
    ACTION_TYPE_SHELL_COMMAND,
    ACTION_TYPE_TASK_OUTPUT,
    ACTION_TYPE_TOOL_CALL,
    ACTION_TYPE_USER_PROMPT,
    AGENT_REACT_ACTION_RETRY_MAX_ATTEMPTS,
    RUN_STATUS_FAILED,
    RUN_STATUS_WAITING,
    STATUS_WAITING,
    STEP_STATUS_RUNNING,
    STEP_STATUS_WAITING,
    STREAM_TAG_ASK,
    STREAM_TAG_FAIL,
    SSE_TYPE_MEMORY_ITEM,
    TASK_OUTPUT_TYPE_USER_PROMPT,
)
from backend.src.services.llm.llm_client import sse_json
from backend.src.services.llm.llm_client import classify_llm_error_text
from backend.src.services.output.output_format import format_visible_result
from backend.src.services.tasks.task_queries import (
    create_task_output,
    TaskStepCreateParams,
    create_task_step,
    mark_task_step_failed,
    update_task,
    update_task_run,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _read_int_env(name: str, default: int, *, min_value: int = 1) -> int:
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


REACT_LLM_INNER_RETRY_MAX_ATTEMPTS = _read_int_env(
    "AGENT_REACT_LLM_INNER_RETRY_MAX_ATTEMPTS",
    1,
    min_value=1,
)
REACT_LLM_INNER_HARD_TIMEOUT_SECONDS = _read_int_env(
    "AGENT_REACT_LLM_INNER_HARD_TIMEOUT_SECONDS",
    45,
    min_value=5,
)
REACT_LLM_ERROR_MAX_ATTEMPTS = _read_int_env(
    "AGENT_REACT_LLM_ERROR_MAX_ATTEMPTS",
    2,
    min_value=1,
)
REACT_ACTION_MAX_TOKENS = _read_int_env(
    "AGENT_REACT_ACTION_MAX_TOKENS",
    512,
    min_value=64,
)
REACT_ACTION_RETRY_MAX_TOKENS = _read_int_env(
    "AGENT_REACT_ACTION_RETRY_MAX_TOKENS",
    256,
    min_value=64,
)
SCRIPT_FILE_WRITE_TOKEN_FLOOR = 3072
SCRIPT_FILE_WRITE_RETRY_TOKEN_FLOOR = 2048
LLM_CALL_ACTION_TOKEN_FLOOR = 1024
LLM_CALL_ACTION_RETRY_TOKEN_FLOOR = 768
TEXT_OUTPUT_ACTION_TOKEN_FLOOR = 768
TEXT_OUTPUT_ACTION_RETRY_TOKEN_FLOOR = 512
REACT_BLOCKING_PROGRESS_INTERVAL_SECONDS = _read_int_env(
    "AGENT_REACT_BLOCKING_PROGRESS_INTERVAL_SECONDS",
    10,
    min_value=1,
)


def build_step_progress_payload(
    *,
    task_id: int,
    run_id: int,
    step_order: int,
    title: str,
    phase: str,
    status: str = "running",
    action_type: str = "",
    message: str = "",
    elapsed_ms: Optional[int] = None,
    tick: Optional[int] = None,
    attempt: Optional[int] = None,
    total_attempts: Optional[int] = None,
) -> dict:
    payload = {
        "type": "step_progress",
        "task_id": int(task_id),
        "run_id": int(run_id),
        "step_order": int(step_order),
        "title": str(title or ""),
        "phase": str(phase or "").strip() or "step",
        "status": str(status or "running").strip() or "running",
    }
    action_type_text = str(action_type or "").strip()
    if action_type_text:
        payload["action_type"] = action_type_text
    message_text = str(message or "").strip()
    if message_text:
        payload["message"] = message_text
    if elapsed_ms is not None:
        try:
            payload["elapsed_ms"] = int(elapsed_ms)
        except Exception:
            pass
    if tick is not None:
        try:
            payload["tick"] = int(tick)
        except Exception:
            pass
    if attempt is not None:
        try:
            payload["attempt"] = int(attempt)
        except Exception:
            pass
    if total_attempts is not None:
        try:
            payload["total_attempts"] = int(total_attempts)
        except Exception:
            pass
    return payload


def run_blocking_call_with_progress(
    *,
    func: Callable[[], T],
    start_payload: Optional[dict] = None,
    progress_payload_builder: Optional[Callable[[int, int], Optional[dict]]] = None,
    interval_seconds: Optional[float] = None,
    drain_events: Optional[Callable[[], List[dict]]] = None,
) -> Generator[str, None, T]:
    """在线程中执行阻塞调用，并周期性发出 step_progress 与子线程业务事件。"""
    if isinstance(start_payload, dict) and start_payload:
        yield sse_json(start_payload)

    box: Dict[str, Any] = {}
    done = threading.Event()

    def _worker() -> None:
        try:
            box["result"] = func()
        except BaseException as exc:  # noqa: BLE001
            box["error"] = exc
        finally:
            done.set()

    def _yield_drained_events() -> Generator[str, None, None]:
        if not callable(drain_events):
            return
        try:
            payloads = drain_events() or []
        except Exception:
            return
        for payload in payloads:
            if isinstance(payload, dict) and payload:
                yield sse_json(payload)

    worker = threading.Thread(target=_worker, daemon=True)
    worker.start()

    interval = float(interval_seconds or REACT_BLOCKING_PROGRESS_INTERVAL_SECONDS or 0)
    interval = interval if interval > 0 else float(REACT_BLOCKING_PROGRESS_INTERVAL_SECONDS)
    tick = 0
    started_at = time.monotonic()
    next_emit_at = started_at + interval

    while not done.wait(timeout=min(0.25, interval)):
        yield from _yield_drained_events()
        if not callable(progress_payload_builder):
            continue
        now_value = time.monotonic()
        if now_value < next_emit_at:
            continue
        tick += 1
        payload = progress_payload_builder(int(max(0.0, (now_value - started_at) * 1000)), tick)
        if isinstance(payload, dict) and payload:
            yield sse_json(payload)
        next_emit_at = now_value + interval

    yield from _yield_drained_events()
    error = box.get("error")
    if error is not None:
        raise error
    return box.get("result")


def _normalize_warning_items(raw_warnings: object) -> List[str]:
    if not isinstance(raw_warnings, list):
        return []
    items: List[str] = []
    for item in raw_warnings:
        text = str(item or "").strip()
        if not text or text in items:
            continue
        items.append(text)
    return items


def build_step_warning_payload(
    *,
    task_id: int,
    run_id: int,
    step_id: int,
    step_order: int,
    title: str,
    action_type: str,
    result: Optional[dict],
) -> Optional[dict]:
    if not isinstance(result, dict):
        return None

    warnings = _normalize_warning_items(result.get("warnings"))
    result_contract = result.get("result_contract") if isinstance(result.get("result_contract"), dict) else {}
    contract_status = str(result_contract.get("status") or "").strip().lower()
    if contract_status == "warn" and not warnings:
        warnings = [f"{str(action_type or '').strip() or 'step'} result_contract=warn"]
    if not warnings:
        return None

    payload = {
        "type": "step_warning",
        "level": "warning",
        "task_id": int(task_id),
        "run_id": int(run_id),
        "step_id": int(step_id),
        "step_order": int(step_order),
        "title": str(title or ""),
        "action_type": str(action_type or ""),
        "warning_count": len(warnings),
        "primary_warning": warnings[0],
        "warnings": warnings,
    }

    if str(action_type or "") == ACTION_TYPE_TOOL_CALL:
        tool_name = str(result.get("tool_name") or result.get("tool_id") or "").strip()
        if tool_name:
            payload["tool"] = tool_name
        attempts = result.get("attempts") if isinstance(result.get("attempts"), list) else []
        normalized_attempts = []
        failed_count = 0
        ok_count = 0
        for item in attempts[:5]:
            if not isinstance(item, dict):
                continue
            status = str(item.get("status") or "").strip().lower()
            if status == "failed":
                failed_count += 1
            elif status == "ok":
                ok_count += 1
            normalized_attempts.append(
                {
                    "host": str(item.get("host") or "").strip(),
                    "status": status,
                    "error_code": str(item.get("error_code") or "").strip(),
                    "reason": str(item.get("reason") or "").strip(),
                }
            )
        if normalized_attempts:
            payload["attempts"] = normalized_attempts
            payload["attempt_count"] = len(normalized_attempts)
            payload["failed_attempt_count"] = int(failed_count)
            payload["successful_attempt_count"] = int(ok_count)
            payload["fallback_used"] = bool(failed_count > 0 and ok_count > 0)
        protocol = result.get("protocol") if isinstance(result.get("protocol"), dict) else None
        if protocol:
            payload["protocol_source"] = str(protocol.get("source") or "").strip()

    return payload


def _run_with_optional_lock(db_lock: Optional[object], fn: Callable[[], object]) -> object:
    if db_lock is not None:
        with db_lock:
            return fn()
    return fn()


def _extract_declared_action_from_step_title(step_title: str) -> str:
    raw = str(step_title or "").strip()
    if not raw:
        return ""
    match = re.match(r"^([a-zA-Z_]+)\s*[:：]", raw)
    if not match:
        return ""
    token = str(match.group(1) or "").strip().lower()
    allowed = {
        ACTION_TYPE_FILE_WRITE,
        ACTION_TYPE_FILE_READ,
        ACTION_TYPE_FILE_APPEND,
        ACTION_TYPE_FILE_LIST,
        ACTION_TYPE_FILE_DELETE,
        ACTION_TYPE_HTTP_REQUEST,
        ACTION_TYPE_LLM_CALL,
        ACTION_TYPE_JSON_PARSE,
        ACTION_TYPE_MEMORY_WRITE,
        ACTION_TYPE_SHELL_COMMAND,
        ACTION_TYPE_TASK_OUTPUT,
        ACTION_TYPE_TOOL_CALL,
        ACTION_TYPE_USER_PROMPT,
    }
    return token if token in allowed else ""


def _extract_file_write_path_from_title(step_title: str) -> str:
    raw = str(step_title or "").strip()
    if not raw:
        return ""
    match = re.match(r"^file_write\s*[:：]\s*(\"[^\"]+\"|'[^']+'|\S+)", raw, flags=re.IGNORECASE)
    if not match:
        return ""
    path = str(match.group(1) or "").strip()
    if (path.startswith('"') and path.endswith('"')) or (path.startswith("'") and path.endswith("'")):
        path = path[1:-1].strip()
    return path


def _looks_like_script_file_path(path: str) -> bool:
    suffix = os.path.splitext(str(path or "").strip().lower())[1]
    return suffix in {
        ".py",
        ".js",
        ".ts",
        ".mjs",
        ".cjs",
        ".sh",
        ".ps1",
        ".bat",
        ".cmd",
        ".rb",
        ".php",
    }


def _collect_allowed_action_tokens(allowed_actions_text: str, step_title: str) -> set[str]:
    tokens: set[str] = set()
    declared = _extract_declared_action_from_step_title(step_title)
    if declared:
        tokens.add(declared)
    allowed_text = str(allowed_actions_text or "").strip().lower()
    if not allowed_text:
        return tokens
    for item in re.split(r"[\s,/|]+", allowed_text):
        token = str(item or "").strip().lower()
        if token:
            tokens.add(token)
    return tokens


def _allow_includes_action(allowed_actions_text: str, step_title: str, action_type: str) -> bool:
    token = str(action_type or "").strip().lower()
    if not token:
        return False
    return token in _collect_allowed_action_tokens(allowed_actions_text, step_title)


def _resolve_action_token_caps(
    *,
    step_title: str,
    allowed_actions_text: str,
    react_prompt: str,
) -> tuple[int, int]:
    initial_cap = int(REACT_ACTION_MAX_TOKENS)
    retry_cap = int(REACT_ACTION_RETRY_MAX_TOKENS)
    allowed_tokens = _collect_allowed_action_tokens(allowed_actions_text, step_title)
    file_write_path = _extract_file_write_path_from_title(step_title)

    if ACTION_TYPE_LLM_CALL in allowed_tokens:
        initial_cap = max(initial_cap, int(LLM_CALL_ACTION_TOKEN_FLOOR))
        retry_cap = max(retry_cap, int(LLM_CALL_ACTION_RETRY_TOKEN_FLOOR))
    if ACTION_TYPE_TASK_OUTPUT in allowed_tokens or ACTION_TYPE_MEMORY_WRITE in allowed_tokens:
        initial_cap = max(initial_cap, int(TEXT_OUTPUT_ACTION_TOKEN_FLOOR))
        retry_cap = max(retry_cap, int(TEXT_OUTPUT_ACTION_RETRY_TOKEN_FLOOR))
    if ACTION_TYPE_FILE_WRITE in allowed_tokens:
        initial_cap = max(initial_cap, int(LLM_CALL_ACTION_TOKEN_FLOOR))
        retry_cap = max(retry_cap, int(LLM_CALL_ACTION_RETRY_TOKEN_FLOOR))
        if _looks_like_script_file_path(file_write_path):
            initial_cap = max(initial_cap, int(SCRIPT_FILE_WRITE_TOKEN_FLOOR))
            retry_cap = max(retry_cap, int(SCRIPT_FILE_WRITE_RETRY_TOKEN_FLOOR))

    prompt_chars = len(str(react_prompt or ""))
    if prompt_chars >= 12000 and ACTION_TYPE_LLM_CALL in allowed_tokens:
        initial_cap = max(initial_cap, 1536)
        retry_cap = max(retry_cap, 1024)

    return int(initial_cap), int(retry_cap)


def _build_llm_call_contract_fallback_action(
    *,
    step_title: str,
    workdir: str,
    allowed_actions_text: str,
) -> Tuple[Optional[dict], Optional[str], Optional[dict], Optional[str]]:
    if not _allow_includes_action(allowed_actions_text, step_title, ACTION_TYPE_LLM_CALL):
        return None, None, None, "llm_call_not_allowed"

    step_goal = str(step_title or "").strip()
    for prefix in (f"{ACTION_TYPE_LLM_CALL}:", f"{ACTION_TYPE_LLM_CALL}："):
        if step_goal.startswith(prefix):
            step_goal = step_goal[len(prefix):].strip()
            break
    if not step_goal:
        step_goal = "分析最近一次真实观测并给出下一步所需结果"

    fallback_prompt = (
        f"请完成当前步骤：{step_goal}。\n"
        "系统会自动注入最近一次真实观测，请不要重复粘贴原文。\n"
        "要求：只基于真实观测分析；若数据不足，请明确指出缺失信息；禁止编造。"
    )
    fallback_obj = {
        "action": {
            "type": ACTION_TYPE_LLM_CALL,
            "payload": {
                "prompt": fallback_prompt,
            },
        }
    }
    normalized_obj, normalized_type, normalized_payload, normalized_err = validate_and_normalize_action_text(
        action_text=json.dumps(fallback_obj, ensure_ascii=False),
        step_title=step_title,
        workdir=workdir,
    )
    if normalized_err or not normalized_obj or not normalized_type:
        return None, None, None, str(normalized_err or "fallback_action_invalid")
    return normalized_obj, normalized_type, normalized_payload or {}, None


def _build_compact_action_retry_prompt(
    *,
    step_order: int,
    step_title: str,
    workdir: str,
    allowed_actions_text: str,
    last_error: str,
) -> str:
    declared_action = _extract_declared_action_from_step_title(step_title)
    file_write_path = _extract_file_write_path_from_title(step_title)
    allowed_text = str(allowed_actions_text or "").strip() or (declared_action or "按步骤允许动作")

    file_write_hint = ""
    if declared_action == ACTION_TYPE_FILE_WRITE:
        file_write_hint = (
            "- 当前步骤优先使用 file_write。若目标是脚本，必须直接输出可运行的真实脚本。\n"
            "- 禁止输出 skeleton/TODO/placeholder/sample source/假设数据结构/需根据观测调整 这类占位内容。\n"
            "- 若当前缺少足够真实样本支撑脚本实现，优先通过 plan_patch 在前置步骤补充样本或切换到更轻量路径；不要用骨架脚本假装完成。\n"
        )
        if file_write_path:
            file_write_hint += f"- file_write.path 优先使用：`{file_write_path}`。\n"

    llm_call_hint = ""
    if declared_action == ACTION_TYPE_LLM_CALL:
        llm_call_hint = (
            "- llm_call.prompt 只写分析指令，不要把网页正文/CSV/JSON 原文再次粘贴进 prompt。\n"
            "- 系统会自动注入最近一次真实观测；若观测不足，只需说明缺失信息。\n"
        )

    return (
        "你是本地 Agent 的动作生成器，当前处于超时降载重试模式。\n"
        "只输出单个 JSON 对象，禁止解释、禁止代码块。\n"
        "输出格式：{\"action\":{\"type\":\"...\",\"payload\":{...}},\"plan_patch\":null}\n"
        f"当前步骤序号：{int(step_order)}\n"
        f"当前步骤标题：{step_title}\n"
        f"允许动作：{allowed_text}\n"
        f"workdir：{workdir}\n"
        f"上次错误：{last_error}\n"
        "硬约束：\n"
        "- action.type 必须在允许动作中。\n"
        "- payload 只保留最少必要字段，避免冗余。\n"
        "- 如果是 shell_command，payload 必须包含 workdir。\n"
        f"{file_write_hint}"
        f"{llm_call_hint}"
        "现在请直接输出 JSON。"
    )


def _allow_includes_file_write(allowed_actions_text: str, step_title: str) -> bool:
    return _allow_includes_action(allowed_actions_text, step_title, ACTION_TYPE_FILE_WRITE)


def _build_file_write_timeout_fallback_action(
    *,
    step_title: str,
    workdir: str,
    allowed_actions_text: str,
) -> Tuple[Optional[dict], Optional[str], Optional[dict], Optional[str]]:
    if not _allow_includes_file_write(allowed_actions_text, step_title):
        return None, None, None, "file_write_not_allowed"
    path = _extract_file_write_path_from_title(step_title)
    if not path:
        return None, None, None, "missing_file_write_path_in_title"

    abs_path = os.path.normpath(os.path.join(str(workdir or ""), str(path)))
    content = ""
    try:
        if os.path.isfile(abs_path):
            with open(abs_path, "r", encoding="utf-8") as handle:
                content = handle.read()
    except Exception:
        content = ""
    if not str(content or "").strip():
        return None, None, None, "missing_existing_file_content_for_timeout_fallback"

    fallback_obj = {
        "action": {
            "type": ACTION_TYPE_FILE_WRITE,
            "payload": {
                "path": path,
                "content": str(content),
                "encoding": "utf-8",
            },
        }
    }
    normalized_obj, normalized_type, normalized_payload, normalized_err = validate_and_normalize_action_text(
        action_text=json.dumps(fallback_obj, ensure_ascii=False),
        step_title=step_title,
        workdir=workdir,
    )
    if normalized_err or not normalized_obj or not normalized_type:
        return None, None, None, str(normalized_err or "fallback_action_invalid")
    return normalized_obj, normalized_type, normalized_payload or {}, None


def generate_action_with_retry(
    *,
    llm_call: Callable[[dict], dict],
    react_prompt: str,
    task_id: int,
    run_id: int,
    step_order: int,
    step_title: str,
    workdir: str,
    model: str,
    react_params: dict,
    variables_source: str,
    allowed_actions_text: Optional[str] = None,
) -> Tuple[Optional[dict], Optional[str], Optional[dict], Optional[str], Optional[str]]:
    """
    生成 Action（支持自动重试）。

    Args:
        llm_call: LLM 调用函数
        react_prompt: ReAct 提示词
        task_id: 任务 ID
        run_id: 执行尝试 ID
        step_order: 步骤序号
        step_title: 步骤标题
        workdir: 工作目录
        model: 模型名称
        react_params: LLM 参数
        variables_source: 变量来源标识

    Returns:
        (action_obj, action_type, payload_obj, validate_error, last_action_text)
    """
    action_obj = None
    action_type = None
    payload_obj = None
    action_validate_error = None
    last_action_text = None
    prompt_for_attempt = react_prompt
    compact_prompt = ""
    transient_retry_count = 0

    retries = coerce_int(AGENT_REACT_ACTION_RETRY_MAX_ATTEMPTS or 0, default=0)
    for attempt in range(0, 1 + max(0, retries)):
        attempt_params = dict(react_params)
        if attempt > 0:
            # 重试时强制更稳定：降低温度，减少格式漂移
            attempt_params["temperature"] = 0

        initial_token_cap, retry_token_cap = _resolve_action_token_caps(
            step_title=str(step_title or ""),
            allowed_actions_text=str(allowed_actions_text or ""),
            react_prompt=str(react_prompt or ""),
        )
        token_cap = int(initial_token_cap if attempt == 0 else retry_token_cap)
        raw_max_tokens = attempt_params.get("max_tokens")
        try:
            current_max_tokens = int(float(raw_max_tokens))
        except Exception:
            current_max_tokens = 0
        if current_max_tokens <= 0 or current_max_tokens > token_cap:
            attempt_params["max_tokens"] = int(token_cap)

        # 逐次放宽单次调用硬超时，避免“第一次 30s 卡住 -> 后续仍然 30s 连续超时”。
        hard_timeout_seconds = int(REACT_LLM_INNER_HARD_TIMEOUT_SECONDS)
        if attempt > 0:
            hard_timeout_seconds = min(90, int(hard_timeout_seconds + 15 * attempt))

        prompt_text = compact_prompt or prompt_for_attempt
        prompt_chars = len(prompt_text)
        call_started_at = time.monotonic()
        logger.info(
            "[agent.react.action_gen.attempt] task_id=%s run_id=%s step_order=%s attempt=%s model=%s prompt_chars=%s max_tokens=%s timeout_s=%s compact=%s",
            int(task_id),
            int(run_id),
            int(step_order),
            int(attempt),
            str(model or ""),
            int(prompt_chars),
            int(coerce_int(attempt_params.get("max_tokens"), default=0)),
            int(hard_timeout_seconds),
            bool(compact_prompt),
        )
        action_text, action_error = call_llm_for_text(
            llm_call,
            prompt=prompt_text,
            task_id=int(task_id),
            run_id=int(run_id),
            model=model,
            parameters=attempt_params,
            variables={
                "source": variables_source if attempt == 0 else f"{variables_source}_retry{attempt}",
                "step_order": int(step_order),
                "attempt": int(attempt),
            },
            retry_max_attempts=int(REACT_LLM_INNER_RETRY_MAX_ATTEMPTS),
            hard_timeout_seconds=int(hard_timeout_seconds),
        )
        elapsed_ms = int(max(0.0, (time.monotonic() - call_started_at) * 1000))
        last_action_text = action_text

        if action_error or not action_text:
            action_validate_error = action_error or "empty_response"
            if action_error:
                error_kind = classify_llm_error_text(str(action_error or ""))
                logger.warning(
                    "[agent.react.action_gen.error] task_id=%s run_id=%s step_order=%s attempt=%s kind=%s elapsed_ms=%s error=%s",
                    int(task_id),
                    int(run_id),
                    int(step_order),
                    int(attempt),
                    str(error_kind or "unknown"),
                    int(elapsed_ms),
                    str(action_error or ""),
                )
                if error_kind in {"rate_limit", "transient"}:
                    # LLM 传输类错误由 create_llm_call 内部重试处理；
                    # 外层动作重试默认不再放大时延，避免单步进入“长时间心跳”。
                    max_attempts = coerce_int(REACT_LLM_ERROR_MAX_ATTEMPTS, default=1)
                    if (attempt + 1) >= max(1, int(max_attempts)):
                        break
                    transient_retry_count += 1
                    compact_prompt = _build_compact_action_retry_prompt(
                        step_order=int(step_order),
                        step_title=str(step_title or ""),
                        workdir=str(workdir or ""),
                        allowed_actions_text=str(allowed_actions_text or ""),
                        last_error=str(action_error or ""),
                    )
                    # 传输抖动类错误：继续重试当前步骤（不做额外提示词惩罚）。
                    continue
            else:
                logger.warning(
                    "[agent.react.action_gen.empty] task_id=%s run_id=%s step_order=%s attempt=%s elapsed_ms=%s",
                    int(task_id),
                    int(run_id),
                    int(step_order),
                    int(attempt),
                    int(elapsed_ms),
                )
        else:
            logger.info(
                "[agent.react.action_gen.ok] task_id=%s run_id=%s step_order=%s attempt=%s elapsed_ms=%s text_chars=%s",
                int(task_id),
                int(run_id),
                int(step_order),
                int(attempt),
                int(elapsed_ms),
                int(len(str(action_text or ""))),
            )
            action_obj, action_type, payload_obj, action_validate_error = validate_and_normalize_action_text(
                action_text=action_text,
                step_title=step_title,
                workdir=workdir,
            )

        if not action_validate_error and action_obj:
            break

        if attempt < retries:
            if compact_prompt:
                compact_prompt = _build_compact_action_retry_prompt(
                    step_order=int(step_order),
                    step_title=str(step_title or ""),
                    workdir=str(workdir or ""),
                    allowed_actions_text=str(allowed_actions_text or ""),
                    last_error=str(action_validate_error or "invalid_action"),
                )
            else:
                prompt_for_attempt = (
                    react_prompt
                    + f"\n上一次输出不合法（{action_validate_error}）。请严格只输出 JSON（不要代码块、不要解释）。\n"
                )

    # LLM 连续超时后的 file_write 兜底：
    # - 仅在发生过 transient 重试时触发，避免吞掉一次性偶发错误；
    # - 只允许复用“已存在且非空”的真实文件内容，禁止凭空生成占位脚本/占位数据，
    #   否则会把 timeout 伪装成成功步骤并污染后续执行。
    if (
        (not action_obj or action_validate_error)
        and transient_retry_count > 0
        and _allow_includes_file_write(str(allowed_actions_text or ""), str(step_title or ""))
    ):
        fallback_obj, fallback_type, fallback_payload, fallback_err = _build_file_write_timeout_fallback_action(
            step_title=str(step_title or ""),
            workdir=str(workdir or ""),
            allowed_actions_text=str(allowed_actions_text or ""),
        )
        if fallback_obj and fallback_type and not fallback_err:
            logger.warning(
                "[agent.react.action_gen.fallback] task_id=%s run_id=%s step_order=%s reason=%s fallback_type=%s",
                int(task_id),
                int(run_id),
                int(step_order),
                str(action_validate_error or "transient_timeout"),
                str(fallback_type),
            )
            return fallback_obj, fallback_type, fallback_payload or {}, None, last_action_text

    if (not action_obj or action_validate_error) and _allow_includes_action(
        str(allowed_actions_text or ""),
        str(step_title or ""),
        ACTION_TYPE_LLM_CALL,
    ):
        fallback_obj, fallback_type, fallback_payload, fallback_err = _build_llm_call_contract_fallback_action(
            step_title=str(step_title or ""),
            workdir=str(workdir or ""),
            allowed_actions_text=str(allowed_actions_text or ""),
        )
        if fallback_obj and fallback_type and not fallback_err:
            logger.warning(
                "[agent.react.action_gen.fallback] task_id=%s run_id=%s step_order=%s reason=%s fallback_type=%s",
                int(task_id),
                int(run_id),
                int(step_order),
                str(action_validate_error or "invalid_action"),
                str(fallback_type),
            )
            return fallback_obj, fallback_type, fallback_payload or {}, None, last_action_text

    return action_obj, action_type, payload_obj, action_validate_error, last_action_text


def build_observation_line(
    *,
    action_type: str,
    title: str,
    result: Optional[dict],
    context: Dict,
) -> Tuple[str, Optional[str]]:
    """
    根据执行结果构建观测行。

    Args:
        action_type: 动作类型
        title: 步骤标题
        result: 执行结果
        context: 上下文字典（会被修改以存储 last_llm_response）

    Returns:
        (observation_line, visible_content) 观测行和可见内容（用于 task_output）
    """
    visible_content = None

    if action_type == ACTION_TYPE_SHELL_COMMAND and isinstance(result, dict):
        stdout_raw = str(result.get("stdout") or "")
        stderr_raw = str(result.get("stderr") or "")
        stdout = _truncate_observation(stdout_raw)
        stderr = _truncate_observation(stderr_raw)

        auto_retry = result.get("auto_retry") if isinstance(result.get("auto_retry"), dict) else None
        retry_tail = ""
        parse_candidates = [stdout_raw.strip(), stderr_raw.strip()]
        if isinstance(auto_retry, dict):
            retry_trigger = str(auto_retry.get("trigger") or "").strip()
            retry_url = str(auto_retry.get("fallback_url") or "").strip()
            initial_stderr = str(auto_retry.get("initial_stderr") or "").strip()
            initial_stdout = str(auto_retry.get("initial_stdout") or "").strip()
            if retry_trigger:
                retry_tail = f" auto_retry={retry_trigger}"
                if retry_url:
                    retry_tail += f"({retry_url})"
            if initial_stderr:
                parse_candidates.append(initial_stderr)
            if initial_stdout:
                parse_candidates.append(initial_stdout)
            context["latest_shell_auto_retry"] = auto_retry

        obs_line = f"{title}: shell stdout={stdout} stderr={stderr}{retry_tail}".strip()
        parse_input = next((item for item in parse_candidates if str(item).strip()), "")
        if parse_input:
            context["latest_parse_input_text"] = parse_input

        parsed_output = result.get("parsed_output")
        if parsed_output is not None:
            try:
                parsed_text = json.dumps(parsed_output, ensure_ascii=False)
                context["latest_parse_input_text"] = parsed_text
                context["latest_script_json_output"] = parsed_output
                obs_line = (
                    f"{title}: script_run parsed_output="
                    f"{_truncate_observation(parsed_text)}{retry_tail}"
                ).strip()
            except Exception:
                pass

        artifacts = result.get("artifacts")
        if isinstance(artifacts, list):
            context["latest_script_artifacts"] = artifacts
            exists_count = 0
            for item in artifacts:
                if isinstance(item, dict) and bool(item.get("exists")):
                    exists_count += 1
            if exists_count > 0:
                obs_line += f" artifacts={exists_count}/{len(artifacts)}"

    elif action_type == ACTION_TYPE_LLM_CALL and isinstance(result, dict):
        resp = str(result.get("response") or "")
        context["last_llm_response"] = resp
        obs_line = f"{title}: llm={_truncate_observation(resp)}"

    elif action_type == ACTION_TYPE_TASK_OUTPUT and isinstance(result, dict):
        content_text = str(result.get("content") or "").strip()
        obs_line = f"{title}: output={_truncate_observation(content_text)}"
        if content_text:
            visible_content = content_text

    elif action_type == ACTION_TYPE_FILE_WRITE and isinstance(result, dict):
        path = str(result.get("path") or "").strip()
        size = result.get("bytes")
        tail = f"{size} bytes" if isinstance(size, int) else ""
        if path:
            context["latest_file_write_path"] = path
        warnings = result.get("warnings") if isinstance(result.get("warnings"), list) else []
        warn_tail = ""
        if warnings:
            warn_tail = f" warn={_truncate_observation(str(warnings[0] or ''))}"
        obs_line = f"{title}: file_write {path} {tail}{warn_tail}".strip()

    elif action_type == ACTION_TYPE_FILE_READ and isinstance(result, dict):
        path = str(result.get("path") or "").strip()
        size = result.get("bytes")
        tail = f"{size} bytes" if isinstance(size, int) else ""
        content_raw = str(result.get("content") or "")
        content = _truncate_observation(content_raw)
        obs_line = f"{title}: file_read {path} {tail} content={content}".strip()
        if content_raw.strip():
            context["latest_parse_input_text"] = content_raw.strip()

    elif action_type == ACTION_TYPE_FILE_APPEND and isinstance(result, dict):
        path = str(result.get("path") or "").strip()
        size = result.get("bytes")
        tail = f"{size} bytes" if isinstance(size, int) else ""
        obs_line = f"{title}: file_append {path} {tail}".strip()

    elif action_type == ACTION_TYPE_FILE_LIST and isinstance(result, dict):
        count = result.get("count")
        obs_line = f"{title}: file_list count={count}".strip()

    elif action_type == ACTION_TYPE_FILE_DELETE and isinstance(result, dict):
        deleted = result.get("deleted")
        obs_line = f"{title}: file_delete deleted={deleted}".strip()

    elif action_type == ACTION_TYPE_TOOL_CALL and isinstance(result, dict):
        tool_name = str(result.get("tool_name") or result.get("tool_id") or "")
        out = str(result.get("output") or "")
        warnings = _normalize_warning_items(result.get("warnings"))
        warn_tail = ""
        if warnings:
            warn_tail = f" warn={_truncate_observation(str(warnings[0] or ''))}"
        attempts = result.get("attempts") if isinstance(result.get("attempts"), list) else []
        attempt_tail = ""
        if attempts:
            failed_count = 0
            ok_count = 0
            hosts = []
            for item in attempts[:5]:
                if not isinstance(item, dict):
                    continue
                status = str(item.get("status") or "").strip().lower()
                host = str(item.get("host") or "").strip()
                if host and host not in hosts:
                    hosts.append(host)
                if status == "failed":
                    failed_count += 1
                elif status == "ok":
                    ok_count += 1
            attempt_tail = f" attempts={len(attempts)} failed={failed_count} ok={ok_count}"
            if hosts:
                attempt_tail += f" hosts={','.join(hosts[:3])}"
        obs_line = f"{title}: tool#{tool_name} output={_truncate_observation(out)}{warn_tail}{attempt_tail}"
        if out.strip():
            context["latest_parse_input_text"] = out.strip()
        tool_input = str(result.get("input") or "").strip()
        if tool_input.startswith("http://") or tool_input.startswith("https://"):
            context["latest_external_url"] = tool_input

    elif action_type == ACTION_TYPE_HTTP_REQUEST and isinstance(result, dict):
        status_code = result.get("status_code")
        size = result.get("bytes")
        tail = f"{size} bytes" if isinstance(size, int) else ""
        obs_line = f"{title}: http {status_code} {tail}".strip()
        content_raw = str(result.get("content") or "")
        if content_raw.strip():
            context["latest_parse_input_text"] = content_raw.strip()
        url_raw = str(result.get("url") or "").strip()
        if url_raw.startswith("http://") or url_raw.startswith("https://"):
            context["latest_external_url"] = url_raw

    elif action_type == ACTION_TYPE_JSON_PARSE and isinstance(result, dict):
        picked = result.get("picked")
        obs_line = f"{title}: json_parse picked={picked}".strip()

    elif action_type == ACTION_TYPE_MEMORY_WRITE and isinstance(result, dict):
        obs_line = f"{title}: memory#{result.get('id')}"

    else:
        obs_line = f"{title}: ok"

    return obs_line, visible_content


def record_invalid_action_step(
    *,
    task_id: int,
    run_id: int,
    step_order: int,
    title: str,
    executor: Optional[str] = None,
    error: str,
    last_action_text: Optional[str],
    safe_write_debug: Callable,
) -> Optional[int]:
    """
    为无效 action 创建失败步骤记录。

    Args:
        task_id: 任务 ID
        run_id: 执行尝试 ID
        step_order: 步骤序号
        title: 步骤标题
        error: 错误信息
        last_action_text: 最后的 action 文本
        safe_write_debug: 调试输出函数

    Returns:
        创建的 step_id，失败返回 None
    """
    step_created_at = now_iso()
    detail = json.dumps(
        {
            "type": "react_action_invalid",
            "payload": {
                "error": str(error or "invalid_action"),
                "last_action_text": _truncate_observation(str(last_action_text or "")),
            },
        },
        ensure_ascii=False,
    )

    try:
        step_id, _created, _updated = create_task_step(
            TaskStepCreateParams(
                task_id=int(task_id),
                run_id=int(run_id),
                title=title,
                status=STEP_STATUS_RUNNING,
                executor=str(executor) if executor is not None else None,
                detail=detail,
                result=None,
                error=None,
                attempts=1,
                started_at=step_created_at,
                finished_at=None,
                step_order=step_order,
                created_at=step_created_at,
                updated_at=step_created_at,
            )
        )
        mark_task_step_failed(
            step_id=int(step_id),
            error=f"action_invalid:{error or 'invalid_action'}",
            finished_at=now_iso(),
        )
        return step_id

    except Exception as exc:
        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.react.action_invalid.persist_failed",
            data={"step_order": int(step_order), "error": str(exc)},
            level="warning",
        )
        return None


def handle_user_prompt_action(
    *,
    task_id: int,
    run_id: int,
    step_order: int,
    title: str,
    payload_obj: dict,
    plan_struct: PlanStructure,
    agent_state: Dict,
    safe_write_debug: Callable,
    db_lock: Optional[object] = None,
) -> Generator[str, None, Tuple[str, bool]]:
    """
    处理 user_prompt 动作（暂停等待用户输入）。

    Args:
        task_id: 任务 ID
        run_id: 执行尝试 ID
        step_order: 步骤序号
        title: 步骤标题
        payload_obj: 动作载荷
        plan_struct: 计划结构
        agent_state: Agent 状态字典
        safe_write_debug: 调试输出函数

    Yields:
        SSE 事件

    Returns:
        (run_status, should_break) 运行状态和是否应该终止循环
    """
    question = str(payload_obj.get("question") or "").strip()
    if not question:
        plan_struct.set_step_status(step_order - 1, "failed")
        yield sse_plan_delta(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload(), indices=[step_order - 1])
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} user_prompt.question 不能为空\n"})
        return RUN_STATUS_FAILED, True

    kind = str(payload_obj.get("kind") or "").strip() or None
    normalized_choices = resolve_need_input_choices(
        raw_choices=payload_obj.get("choices"),
        question=question,
        kind=kind,
    )

    # 更新计划栏：当前步骤标记为 waiting
    plan_struct.set_step_status(step_order - 1, "waiting")
    yield sse_plan_delta(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload(), indices=[step_order - 1])

    # docs/agent：waiting 也应落库到 task_steps，便于中断恢复与审计。
    step_created_at = now_iso()
    try:
        detail = json.dumps({"type": ACTION_TYPE_USER_PROMPT, "payload": payload_obj}, ensure_ascii=False)

        executor_value = resolve_executor(agent_state, step_order)
        step_params = TaskStepCreateParams(
            task_id=int(task_id),
            run_id=int(run_id),
            title=title,
            status=STEP_STATUS_WAITING,
            executor=executor_value,
            detail=detail,
            result=None,
            error=None,
            attempts=1,
            started_at=step_created_at,
            finished_at=None,
            step_order=int(step_order),
            created_at=step_created_at,
            updated_at=step_created_at,
        )
        step_id, _created, _updated = _run_with_optional_lock(
            db_lock,
            lambda: create_task_step(step_params),
        )
    except Exception as exc:
        step_id = None
        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.user_prompt.step_persist_failed",
            data={"step_order": int(step_order), "error": str(exc)},
            level="warning",
        )

    # 写入输出记录
    created_at = step_created_at
    try:
        _run_with_optional_lock(
            db_lock,
            lambda:
            create_task_output(
                task_id=int(task_id),
                run_id=int(run_id),
                output_type=TASK_OUTPUT_TYPE_USER_PROMPT,
                content=question,
                created_at=created_at,
            ),
        )
    except Exception as exc:
        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.user_prompt.output_write_failed",
            data={"step_order": int(step_order), "error": str(exc)},
            level="warning",
        )

    safe_write_debug(
        task_id=int(task_id),
        run_id=int(run_id),
        message="agent.waiting_input",
        data={"step_order": int(step_order), "question": question, "choices_count": len(normalized_choices or [])},
        level="info",
    )

    prompt_token = generate_prompt_token(
        task_id=int(task_id),
        run_id=int(run_id),
        step_order=int(step_order),
        question=question,
        created_at=created_at,
    )

    # 持久化暂停状态
    agent_state["paused"] = {
        "question": question,
        "step_order": step_order,
        "step_title": title,
        "created_at": created_at,
        "prompt_token": prompt_token,
    }
    if kind:
        agent_state["paused"]["kind"] = kind
    if normalized_choices:
        agent_state["paused"]["choices"] = normalized_choices
    if step_id is not None:
        agent_state["paused"]["step_id"] = int(step_id)
    agent_state["step_order"] = step_order
    updated_at = now_iso()

    try:
        def _persist_waiting_state() -> None:
                update_task_run(
                    run_id=int(run_id),
                    status=RUN_STATUS_WAITING,
                    agent_plan=plan_struct.to_agent_plan_payload(),
                    agent_state=agent_state,
                    updated_at=updated_at,
                )
                update_task(task_id=int(task_id), status=STATUS_WAITING, updated_at=updated_at)
        _run_with_optional_lock(db_lock, _persist_waiting_state)
    except Exception as exc:
        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.waiting_state.persist_failed",
            data={"step_order": int(step_order), "error": str(exc)},
            level="error",
        )

    need_input_payload = build_need_input_payload(
        task_id=int(task_id),
        run_id=int(run_id),
        question=question,
        kind=kind,
        choices=normalized_choices,
        prompt_token=prompt_token,
        session_key=str(agent_state.get("session_key") or "") if isinstance(agent_state, dict) else "",
    )

    yield sse_json(need_input_payload)
    yield sse_json({"delta": f"{STREAM_TAG_ASK} {question}\n"})

    return RUN_STATUS_WAITING, True


def handle_task_output_fallback(
    *,
    llm_call: Callable[[dict], dict],
    react_prompt: str,
    task_id: int,
    run_id: int,
    step_order: int,
    title: str,
    workdir: str,
    model: str,
    react_params: dict,
    variables_source: str,
    payload_obj: dict,
    context: Dict,
    safe_write_debug: Callable,
) -> Tuple[Optional[dict], Optional[str], Optional[dict], Optional[str]]:
    """
    处理 task_output.content 为空的情况，强制让模型补齐。

    Args:
        llm_call: LLM 调用函数
        react_prompt: ReAct 提示词
        task_id: 任务 ID
        run_id: 执行尝试 ID
        step_order: 步骤序号
        title: 步骤标题
        workdir: 工作目录
        model: 模型名称
        react_params: LLM 参数
        variables_source: 变量来源标识
        payload_obj: 原始载荷
        context: 上下文字典
        safe_write_debug: 调试输出函数

    Returns:
        (action_obj, action_type, payload_obj, error) 新的动作或错误信息
    """
    if not needs_nonempty_task_output_content(payload_obj, context):
        return None, None, None, None

    safe_write_debug(
        task_id=int(task_id),
        run_id=int(run_id),
        message="agent.task_output.content_missing",
        data={"step_order": int(step_order), "title": title},
        level="warning",
    )

    force_content_prompt = (
        react_prompt
        + "\n补充约束：你选择了 task_output，但当前没有可用的上一条 llm_call 输出用于补齐。"
        "因此你必须在 task_output.payload.content 中写入非空的最终结论（不要返回空字符串）。只输出 JSON。\n"
    )

    forced_text, forced_err = call_llm_for_text(
        llm_call,
        prompt=force_content_prompt,
        task_id=int(task_id),
        run_id=int(run_id),
        model=model,
        parameters=react_params,
        variables={
            "source": f"{variables_source}_force_task_output",
            "step_order": int(step_order),
        },
        retry_max_attempts=int(REACT_LLM_INNER_RETRY_MAX_ATTEMPTS),
        hard_timeout_seconds=int(REACT_LLM_INNER_HARD_TIMEOUT_SECONDS),
    )

    forced_obj, forced_type, forced_payload, forced_validate_error = validate_and_normalize_action_text(
        action_text=forced_text or "",
        step_title=title,
        workdir=workdir,
    )

    if forced_err or forced_validate_error or forced_type != ACTION_TYPE_TASK_OUTPUT:
        return None, None, None, "task_output.content 为空且无法自动补齐"

    if needs_nonempty_task_output_content(forced_payload or {}, context):
        return None, None, None, "task_output.content 仍为空"

    return forced_obj, forced_type, forced_payload or {}, None


def yield_memory_write_event(
    *,
    task_id: int,
    run_id: int,
    result: dict,
) -> str:
    """
    生成记忆写入的 SSE 事件。

    Args:
        task_id: 任务 ID
        run_id: 执行尝试 ID
        result: 记忆写入结果

    Returns:
        SSE JSON 字符串
    """
    return sse_json({
        "type": SSE_TYPE_MEMORY_ITEM,
        "task_id": int(task_id),
        "run_id": int(run_id),
        "item": result,
    })


def yield_visible_result(content: str) -> str:
    """
    生成可见结果的 SSE 事件。

    Args:
        content: 结果内容

    Returns:
        SSE JSON 字符串
    """
    return sse_json({"delta": f"{format_visible_result(content)}\n"})
