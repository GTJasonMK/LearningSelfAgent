from __future__ import annotations

import threading

from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

from backend.src.common.task_error_codes import format_task_error

from backend.src.actions.handlers.file_append import execute_file_append
from backend.src.actions.handlers.file_delete import execute_file_delete
from backend.src.actions.handlers.file_list import execute_file_list
from backend.src.actions.handlers.file_read import execute_file_read
from backend.src.actions.handlers.file_write import execute_file_write
from backend.src.actions.handlers.http_request import execute_http_request
from backend.src.actions.handlers.json_parse import execute_json_parse
from backend.src.services.permissions.permissions_store import is_action_enabled
from backend.src.actions.handlers.llm_call import execute_llm_call
from backend.src.actions.handlers.memory_write import execute_memory_write
from backend.src.actions.handlers.shell_command import execute_shell_command
from backend.src.actions.handlers.task_output import execute_task_output
from backend.src.actions.handlers.tool_call import execute_tool_call
from backend.src.constants import (
    ACTION_TYPE_FILE_APPEND,
    ACTION_TYPE_FILE_DELETE,
    ACTION_TYPE_FILE_LIST,
    ACTION_TYPE_FILE_READ,
    ACTION_TYPE_FILE_WRITE,
    ACTION_TYPE_HTTP_REQUEST,
    ACTION_TYPE_JSON_PARSE,
    ACTION_TYPE_LLM_CALL,
    ACTION_TYPE_MEMORY_WRITE,
    ACTION_TYPE_SHELL_COMMAND,
    ACTION_TYPE_TASK_OUTPUT,
    ACTION_TYPE_TOOL_CALL,
    ACTION_TYPE_USER_PROMPT,
    AGENT_REACT_OBSERVATION_MAX_CHARS,
)

# Action 执行函数统一签名：尽量让 executor 不需要知道每个 action 的“特殊参数形态”。
ActionExecutor = Callable[
    [int, int, dict, dict, Optional[dict]],
    Tuple[Optional[dict], Optional[str]],
]


@dataclass(frozen=True)
class ActionTypeSpec:
    """
    Action 类型定义（用于计划/校验/执行的统一来源）。

    设计目标（对应质量报告 P2#7）：
    - 新增 action 类型时，尽量只改这一处（注册 spec），避免在 6 个文件里散落硬编码；
    - 允许 alias：兼容模型输出的同义写法（tool/tool_call、cmd/shell_command 等）。
    """

    action_type: str
    allowed_payload_keys: Set[str]
    aliases: Set[str]
    executor: ActionExecutor
    # validate_payload：只做“结构与关键字段”校验；执行错误由 executor 返回。
    validate_payload: Callable[[dict], Optional[str]]


def _require_nonempty_string(value: object, error_message: str) -> Optional[str]:
    if not isinstance(value, str) or not value.strip():
        return error_message
    return None


def _validate_optional_string_field(payload: dict, key: str, error_message: str) -> Optional[str]:
    if key in payload and payload.get(key) is not None and not isinstance(payload.get(key), str):
        return error_message
    return None


def _validate_optional_positive_int_field(payload: dict, key: str, *, action_name: str) -> Optional[str]:
    if key not in payload:
        return None
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        parsed = int(value)
    except Exception:
        return format_task_error(code="invalid_action_payload", message=f"{action_name}.{key} 必须为正整数或空")
    if parsed <= 0:
        return format_task_error(code="invalid_action_payload", message=f"{action_name}.{key} 必须为正整数或空")
    return None


def _validate_required_path_field(payload: dict, action_name: str) -> Optional[str]:
    return _require_nonempty_string(payload.get("path"), f"{action_name}.path 不能为空")


def _validate_llm_call(payload: dict) -> Optional[str]:
    if not str(payload.get("prompt") or "").strip() and payload.get("template_id") is None:
        return "llm_call.prompt 不能为空"
    return None


def _validate_memory_write(payload: dict) -> Optional[str]:
    return _require_nonempty_string(payload.get("content"), "memory_write.content 不能为空")


def _validate_task_output(payload: dict) -> Optional[str]:
    # content 允许为空：执行阶段会尝试用 last_llm_response 补齐；如仍为空则由 react_loop 强制重问。
    return _validate_optional_string_field(payload, "content", "task_output.content 必须是字符串")


def _validate_tool_call(payload: dict) -> Optional[str]:
    input_error = _require_nonempty_string(payload.get("input"), "tool_call.input 不能为空")
    if input_error:
        return input_error
    for key in ("tool_id", "task_id", "run_id", "skill_id"):
        field_error = _validate_optional_positive_int_field(payload, key, action_name="tool_call")
        if field_error:
            return field_error
    # output 允许为空：执行器会在运行时尝试执行工具并填充 output
    return _validate_optional_string_field(payload, "output", "tool_call.output 必须是字符串")


def _validate_shell_command(payload: dict) -> Optional[str]:
    command = payload.get("command")
    script = payload.get("script")

    script_text = str(script or "").strip() if isinstance(script, str) else ""
    has_script = bool(script_text)

    has_command = False
    command_present = command is not None
    if isinstance(command, str):
        if command.strip():
            has_command = True
    elif isinstance(command, list):
        if command:
            head = command[0]
            if isinstance(head, str) and head.strip():
                has_command = True

    if command_present and not has_command and not has_script:
        return "shell_command.command 不能为空"
    if not has_command and not has_script:
        return "shell_command.command/script 不能为空"

    args = payload.get("args")
    if args is not None:
        if not isinstance(args, list):
            return "shell_command.args 必须是字符串数组"
        for idx, item in enumerate(args):
            if not isinstance(item, str):
                return f"shell_command.args[{idx}] 必须是字符串"

    required_args = payload.get("required_args")
    if required_args is not None:
        if not isinstance(required_args, list):
            return "shell_command.required_args 必须是字符串数组"
        for idx, item in enumerate(required_args):
            if not isinstance(item, str) or not str(item).strip():
                return f"shell_command.required_args[{idx}] 不能为空"

    expected_outputs = payload.get("expected_outputs")
    if expected_outputs is not None:
        if not isinstance(expected_outputs, list):
            return "shell_command.expected_outputs 必须是字符串数组"
        for idx, item in enumerate(expected_outputs):
            if not isinstance(item, str) or not str(item).strip():
                return f"shell_command.expected_outputs[{idx}] 不能为空"

    for key in ("parse_json_output", "discover_required_args", "stdin_from_context"):
        value = payload.get(key)
        if value is not None and not isinstance(value, bool):
            return f"shell_command.{key} 必须是布尔值"

    emit_as = payload.get("emit_as")
    if emit_as is not None and (not isinstance(emit_as, str) or not emit_as.strip()):
        return "shell_command.emit_as 必须是非空字符串"

    workdir = payload.get("workdir")
    if not isinstance(workdir, str) or not workdir.strip():
        return "shell_command.workdir 不能为空"
    return None


def _validate_file_write(payload: dict) -> Optional[str]:
    path_error = _validate_required_path_field(payload, "file_write")
    if path_error:
        return path_error
    # content 允许为空/缺失：执行器会把 None 视为 ""，但若提供则必须是字符串类型
    return _validate_optional_string_field(payload, "content", "file_write.content 必须是字符串")


def _validate_file_read(payload: dict) -> Optional[str]:
    return _validate_required_path_field(payload, "file_read")


def _validate_http_request(payload: dict) -> Optional[str]:
    url_error = _require_nonempty_string(payload.get("url"), "http_request.url 不能为空")
    if url_error:
        return url_error
    fallback_urls = payload.get("fallback_urls")
    if fallback_urls is not None:
        if isinstance(fallback_urls, str):
            if not fallback_urls.strip():
                return "http_request.fallback_urls 不能为空字符串"
        elif isinstance(fallback_urls, list):
            if not fallback_urls:
                return "http_request.fallback_urls 不能为空数组"
            for idx, item in enumerate(fallback_urls):
                if not isinstance(item, str) or not item.strip():
                    return f"http_request.fallback_urls[{idx}] 不能为空字符串"
        else:
            return "http_request.fallback_urls 必须是字符串或字符串数组"
    strict_status_code = payload.get("strict_status_code")
    if strict_status_code is not None and not isinstance(strict_status_code, bool):
        return "http_request.strict_status_code 必须是布尔值"
    return None


def _validate_file_append(payload: dict) -> Optional[str]:
    path_error = _validate_required_path_field(payload, "file_append")
    if path_error:
        return path_error
    # content 允许为空/缺失：执行器会把 None 视为 ""，但若提供则必须是字符串类型
    return _validate_optional_string_field(payload, "content", "file_append.content 必须是字符串")


def _validate_file_list(payload: dict) -> Optional[str]:
    return _validate_required_path_field(payload, "file_list")


def _validate_file_delete(payload: dict) -> Optional[str]:
    return _validate_required_path_field(payload, "file_delete")


def _validate_json_parse(payload: dict) -> Optional[str]:
    return _require_nonempty_string(payload.get("text"), "json_parse.text 不能为空")


def _validate_user_prompt(payload: dict) -> Optional[str]:
    question_error = _require_nonempty_string(payload.get("question"), "user_prompt.question 不能为空")
    if question_error:
        return question_error

    if "kind" in payload and payload.get("kind") is not None:
        kind = payload.get("kind")
        if not isinstance(kind, str):
            return "user_prompt.kind 必须是字符串"

    if "choices" in payload and payload.get("choices") is not None:
        choices = payload.get("choices")
        if not isinstance(choices, list):
            return "user_prompt.choices 必须是数组"
        for idx, item in enumerate(choices):
            if isinstance(item, str):
                if not item.strip():
                    return f"user_prompt.choices[{idx}] 不能为空"
                continue
            if not isinstance(item, dict):
                return f"user_prompt.choices[{idx}] 必须是对象或字符串"
            label = item.get("label")
            if not isinstance(label, str) or not label.strip():
                return f"user_prompt.choices[{idx}].label 不能为空"
            if "value" in item and item.get("value") is not None:
                value = item.get("value")
                if not isinstance(value, str) or not value.strip():
                    return f"user_prompt.choices[{idx}].value 不能为空"
    return None


def _truncate_text_with_tail(text: str, max_chars: int) -> str:
    raw = str(text or "")
    if max_chars <= 0 or len(raw) <= max_chars:
        return raw
    half = max(64, int((max_chars - 8) / 2))
    if half * 2 >= len(raw):
        return raw
    return f"{raw[:half]}\n...\n{raw[-half:]}"


def _inject_latest_parse_input_prompt(prompt: str, context: Optional[dict]) -> tuple[str, bool]:
    """
    为 llm_call 自动注入最近一次真实观测，减少“提示词未携带数据”导致的空转。
    """
    if not isinstance(context, dict):
        return prompt, False
    base = str(prompt or "").strip()
    if not base:
        return prompt, False

    parse_text = str(context.get("latest_parse_input_text") or "").strip()
    if not parse_text:
        return prompt, False

    marker = "【可用观测数据（自动注入）】"
    if marker in base or parse_text in base:
        return prompt, False

    limit = int(AGENT_REACT_OBSERVATION_MAX_CHARS or 4000)
    snippet = _truncate_text_with_tail(parse_text, max_chars=max(800, limit))
    injected_prompt = (
        f"{base}\n\n"
        f"{marker}\n"
        f"{snippet}\n"
        "【约束】仅可基于上述真实观测进行计算/抽取；若数据不足，请先继续抓取或读取。"
    )
    return injected_prompt, True


def _exec_llm_call(task_id: int, run_id: int, step_row: dict, payload: dict, context: Optional[dict]):
    _ = step_row
    patched_payload = dict(payload or {})
    prompt = str(patched_payload.get("prompt") or "").strip()
    if prompt:
        injected_prompt, injected = _inject_latest_parse_input_prompt(prompt, context)
        if injected:
            patched_payload["prompt"] = injected_prompt
            if isinstance(context, dict):
                context["llm_prompt_auto_observation_injected"] = True
    return execute_llm_call(task_id, run_id, patched_payload)


def _exec_memory_write(task_id: int, run_id: int, step_row: dict, payload: dict, context: Optional[dict]):
    _ = run_id
    _ = step_row
    _ = context
    return execute_memory_write(task_id, payload)


def _exec_task_output(task_id: int, run_id: int, step_row: dict, payload: dict, context: Optional[dict]):
    return execute_task_output(task_id, run_id, payload, context=context, step_row=step_row)


def _exec_tool_call(task_id: int, run_id: int, step_row: dict, payload: dict, context: Optional[dict]):
    _ = context
    return execute_tool_call(task_id, run_id, step_row, payload)


def _exec_shell_command(task_id: int, run_id: int, step_row: dict, payload: dict, context: Optional[dict]):
    return execute_shell_command(
        int(task_id),
        int(run_id),
        step_row,
        payload,
        context=context,
    )


def _exec_file_write(task_id: int, run_id: int, step_row: dict, payload: dict, context: Optional[dict]):
    _ = task_id
    _ = run_id
    _ = step_row
    return execute_file_write(payload, context=context)


def _exec_file_append(task_id: int, run_id: int, step_row: dict, payload: dict, context: Optional[dict]):
    _ = task_id
    _ = run_id
    _ = step_row
    _ = context
    return execute_file_append(payload)


def _exec_file_list(task_id: int, run_id: int, step_row: dict, payload: dict, context: Optional[dict]):
    _ = task_id
    _ = run_id
    _ = step_row
    _ = context
    return execute_file_list(payload)


def _exec_file_delete(task_id: int, run_id: int, step_row: dict, payload: dict, context: Optional[dict]):
    _ = task_id
    _ = run_id
    _ = step_row
    _ = context
    return execute_file_delete(payload)


def _exec_json_parse(task_id: int, run_id: int, step_row: dict, payload: dict, context: Optional[dict]):
    _ = task_id
    _ = run_id
    _ = step_row
    return execute_json_parse(payload, context=context)


def _exec_file_read(task_id: int, run_id: int, step_row: dict, payload: dict, context: Optional[dict]):
    _ = task_id
    _ = run_id
    _ = step_row
    _ = context
    return execute_file_read(payload)


def _exec_http_request(task_id: int, run_id: int, step_row: dict, payload: dict, context: Optional[dict]):
    _ = task_id
    _ = run_id
    _ = step_row
    _ = context
    return execute_http_request(payload)


def _exec_user_prompt(task_id: int, run_id: int, step_row: dict, payload: dict, context: Optional[dict]):
    # user_prompt 在 ReAct 循环里被“短路处理”（暂停等待用户输入），此处通常不会执行到。
    _ = task_id
    _ = run_id
    _ = step_row
    _ = context
    question = str(payload.get("question") or "").strip()
    if not question:
        return None, "user_prompt.question 不能为空"
    return {"question": question}, None


_SPECS: Dict[str, ActionTypeSpec] = {}
_ALIASES: Dict[str, str] = {}
_REGISTRY_FROZEN = False
_REGISTRY_LOCK = threading.Lock()
_PAYLOAD_REQUIRED_KEYS: Dict[str, Set[str]] = {
    ACTION_TYPE_MEMORY_WRITE: {"content"},
    ACTION_TYPE_FILE_READ: {"path"},
    ACTION_TYPE_FILE_LIST: {"path"},
    ACTION_TYPE_FILE_DELETE: {"path"},
    ACTION_TYPE_USER_PROMPT: {"question"},
}
_PAYLOAD_REQUIRED_ONE_OF_KEYS: Dict[str, List[List[str]]] = {
    ACTION_TYPE_LLM_CALL: [["prompt", "template_id"]],
    ACTION_TYPE_TASK_OUTPUT: [["content", "run_id"]],
    ACTION_TYPE_TOOL_CALL: [["tool_id", "tool_name"], ["input"]],
    ACTION_TYPE_SHELL_COMMAND: [["command", "script"], ["workdir"]],
    ACTION_TYPE_HTTP_REQUEST: [["url"]],
    ACTION_TYPE_FILE_WRITE: [["path"]],
    ACTION_TYPE_FILE_APPEND: [["path"]],
    ACTION_TYPE_JSON_PARSE: [["text"]],
}


def register_action_type(spec: ActionTypeSpec) -> None:
    key = str(spec.action_type or "").strip()
    if not key:
        return
    with _REGISTRY_LOCK:
        if _REGISTRY_FROZEN:
            raise RuntimeError(f"注册表已冻结，无法注册新 action 类型: {key}")
        _SPECS[key] = spec
        for alias in spec.aliases or set():
            value = str(alias or "").strip()
            if not value:
                continue
            _ALIASES[value] = key


def _freeze_registry() -> None:
    """冻结注册表，之后不再允许新增注册。运行时只读访问无需加锁。"""
    global _REGISTRY_FROZEN
    with _REGISTRY_LOCK:
        _REGISTRY_FROZEN = True


def normalize_action_type(value: str) -> Optional[str]:
    """
    归一化 action.type：
    - 小写
    - '-' -> '_'
    - alias 映射
    """
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw.replace("-", "_").strip().lower()
    normalized = _ALIASES.get(normalized, normalized)
    return normalized if normalized in _SPECS else None


def get_action_spec(action_type: str) -> Optional[ActionTypeSpec]:
    return _SPECS.get(action_type)


def list_action_types() -> List[str]:
    # 保持稳定顺序：用于 prompt（减少模型“换行/排序差异”带来的波动）。
    preferred = [
        ACTION_TYPE_LLM_CALL,
        ACTION_TYPE_MEMORY_WRITE,
        ACTION_TYPE_TASK_OUTPUT,
        ACTION_TYPE_TOOL_CALL,
        ACTION_TYPE_HTTP_REQUEST,
        ACTION_TYPE_SHELL_COMMAND,
        ACTION_TYPE_FILE_LIST,
        ACTION_TYPE_FILE_READ,
        ACTION_TYPE_FILE_APPEND,
        ACTION_TYPE_FILE_WRITE,
        ACTION_TYPE_FILE_DELETE,
        ACTION_TYPE_JSON_PARSE,
        ACTION_TYPE_USER_PROMPT,
    ]
    seen: List[str] = []
    for t in preferred:
        if t in _SPECS and t not in seen:
            seen.append(t)
    for t in sorted(_SPECS.keys()):
        if t not in seen:
            seen.append(t)
    return seen


def action_types_line() -> str:
    return " / ".join(list_action_types())


def action_payload_keys_guide() -> str:
    """
    返回运行时权威的 action payload 字段白名单说明。

    用途：
    - 注入到 ReAct 提示词，减少“提示词示例字段”与执行白名单漂移；
    - 提升模型在新增字段（如 http_request.fallback_urls）时的命中率。
    """
    lines: List[str] = []
    for action_type in list_action_types():
        spec = get_action_spec(action_type)
        if not spec:
            continue
        keys = sorted(str(k) for k in (spec.allowed_payload_keys or set()) if str(k).strip())
        keys_text = ", ".join(keys) if keys else "(无)"
        lines.append(f"- {action_type}: {keys_text}")
    return "\n".join(lines) if lines else "(无)"


def export_action_contract_schema() -> Dict[str, object]:
    """
    导出运行时 action 契约（JSON Schema 2020-12）。

    说明：
    - `additionalProperties=false` 由白名单驱动，避免字段漂移；
    - `x-required-one-of` 为扩展约束（Schema 原生难以直接表达“二选一”可读规则）。
    """
    payload_defs: Dict[str, dict] = {}
    action_one_of: List[dict] = []
    for action_type in list_action_types():
        spec = get_action_spec(action_type)
        if not spec:
            continue
        allowed_keys = sorted(str(key) for key in (spec.allowed_payload_keys or set()) if str(key).strip())
        required_keys = sorted(
            key for key in (_PAYLOAD_REQUIRED_KEYS.get(action_type) or set()) if key in set(allowed_keys)
        )
        payload_schema = {
            "type": "object",
            "additionalProperties": False,
            "properties": {key: {} for key in allowed_keys},
            "required": required_keys,
            "x-required-one-of": list(_PAYLOAD_REQUIRED_ONE_OF_KEYS.get(action_type) or []),
            "x-aliases": sorted(str(alias) for alias in (spec.aliases or set()) if str(alias).strip()),
        }
        payload_defs[action_type] = payload_schema
        action_one_of.append(
            {
                "type": "object",
                "required": ["action"],
                "properties": {
                    "action": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["type", "payload"],
                        "properties": {
                            "type": {"const": action_type},
                            "payload": {"$ref": f"#/$defs/payloads/{action_type}"},
                        },
                    }
                },
            }
        )

    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "AgentActionContract",
        "type": "object",
        "description": "Agent ReAct action object contract",
        "$defs": {"payloads": payload_defs},
        "oneOf": action_one_of,
    }


def validate_action_object(action_obj: dict) -> Optional[str]:
    """
    统一校验 ReAct action 结构：
    - 只校验结构与关键字段（payload 是否 dict、必填字段是否存在等）
    """
    if not isinstance(action_obj, dict):
        return "action 输出不是对象"
    action = action_obj.get("action")
    if not isinstance(action, dict):
        return "缺少 action"
    action_type = normalize_action_type(action.get("type"))
    if not action_type:
        raw_type = str(action.get("type") or "").strip().lower().replace("-", "_")
        if raw_type == "plan_patch":
            return format_task_error(
                code="plan_patch_not_action",
                message="action.type 非法: plan_patch（plan_patch 不是可执行的 action，请输出可执行的 action）",
            )
        return f"action.type 非法: {action.get('type')}"
    payload = action.get("payload")
    if not isinstance(payload, dict):
        return "action.payload 不是对象"
    spec = get_action_spec(action_type)
    if not spec:
        return f"action.type 非法: {action.get('type')}"
    if not is_action_enabled(action_type):
        return f"action 已禁用: {action_type}"
    return spec.validate_payload(payload)


def _register_builtin_specs() -> None:
    # 与 executor._ALLOWED_KEYS_BY_TYPE 对齐：新增字段必须先登记，否则执行阶段会被白名单拒绝。
    register_action_type(
        ActionTypeSpec(
            action_type=ACTION_TYPE_LLM_CALL,
            allowed_payload_keys={
                "prompt",
                "template_id",
                "variables",
                "provider",
                "model",
                "parameters",
                "task_id",
                "run_id",
                "dry_run",
            },
            aliases={"llm", "chat", "llmcall"},
            executor=_exec_llm_call,
            validate_payload=_validate_llm_call,
        )
    )
    register_action_type(
        ActionTypeSpec(
            action_type=ACTION_TYPE_MEMORY_WRITE,
            allowed_payload_keys={"content", "memory_type", "tags", "task_id"},
            aliases={"memory", "memorywrite"},
            executor=_exec_memory_write,
            validate_payload=_validate_memory_write,
        )
    )
    register_action_type(
        ActionTypeSpec(
            action_type=ACTION_TYPE_TASK_OUTPUT,
            allowed_payload_keys={"output_type", "content", "run_id"},
            aliases={"output", "taskoutput"},
            executor=_exec_task_output,
            validate_payload=_validate_task_output,
        )
    )
    register_action_type(
        ActionTypeSpec(
            action_type=ACTION_TYPE_TOOL_CALL,
            allowed_payload_keys={
                "tool_id",
                "tool_name",
                "tool_description",
                "tool_version",
                "tool_metadata",
                "task_id",
                "skill_id",
                "run_id",
                "reuse",
                "reuse_status",
                "reuse_notes",
                "input",
                "output",
            },
            aliases={"tool", "toolcall"},
            executor=_exec_tool_call,
            validate_payload=_validate_tool_call,
        )
    )
    register_action_type(
        ActionTypeSpec(
            action_type=ACTION_TYPE_SHELL_COMMAND,
            allowed_payload_keys={
                "command",
                "script",
                "args",
                "required_args",
                "discover_required_args",
                "expected_outputs",
                "parse_json_output",
                "emit_as",
                "workdir",
                "timeout_ms",
                "stdin",
                "stdin_from_context",
            },
            aliases={"shell", "cmd", "command", "script_run"},
            executor=_exec_shell_command,
            validate_payload=_validate_shell_command,
        )
    )
    register_action_type(
        ActionTypeSpec(
            action_type=ACTION_TYPE_HTTP_REQUEST,
            allowed_payload_keys={
                "url",
                "method",
                "headers",
                "params",
                "data",
                "json",
                "timeout_ms",
                "timeout",
                "allow_redirects",
                "encoding",
                "max_bytes",
                "strict_business_success",
                "strict_status_code",
                "fallback_urls",
            },
            aliases={"http", "http_request", "request"},
            executor=_exec_http_request,
            validate_payload=_validate_http_request,
        )
    )
    register_action_type(
        ActionTypeSpec(
            action_type=ACTION_TYPE_FILE_LIST,
            allowed_payload_keys={"path", "pattern", "recursive", "max_entries"},
            aliases={"list_file", "list_files", "listdir"},
            executor=_exec_file_list,
            validate_payload=_validate_file_list,
        )
    )
    register_action_type(
        ActionTypeSpec(
            action_type=ACTION_TYPE_FILE_READ,
            allowed_payload_keys={"path", "encoding", "max_bytes"},
            aliases={"read_file", "readfile"},
            executor=_exec_file_read,
            validate_payload=_validate_file_read,
        )
    )
    register_action_type(
        ActionTypeSpec(
            action_type=ACTION_TYPE_FILE_APPEND,
            allowed_payload_keys={"path", "content", "encoding"},
            aliases={"append_file", "appendfile"},
            executor=_exec_file_append,
            validate_payload=_validate_file_append,
        )
    )
    register_action_type(
        ActionTypeSpec(
            action_type=ACTION_TYPE_FILE_WRITE,
            allowed_payload_keys={"path", "content", "encoding"},
            aliases={"file", "write_file", "writefile"},
            executor=_exec_file_write,
            validate_payload=_validate_file_write,
        )
    )
    register_action_type(
        ActionTypeSpec(
            action_type=ACTION_TYPE_FILE_DELETE,
            allowed_payload_keys={"path", "recursive"},
            aliases={"delete_file", "remove_file", "remove"},
            executor=_exec_file_delete,
            validate_payload=_validate_file_delete,
        )
    )
    register_action_type(
        ActionTypeSpec(
            action_type=ACTION_TYPE_JSON_PARSE,
            allowed_payload_keys={"text", "pick_keys"},
            aliases={"parse_json"},
            executor=_exec_json_parse,
            validate_payload=_validate_json_parse,
        )
    )
    register_action_type(
        ActionTypeSpec(
            action_type=ACTION_TYPE_USER_PROMPT,
            allowed_payload_keys={"question", "kind", "choices"},
            aliases={"ask", "user"},
            executor=_exec_user_prompt,
            validate_payload=_validate_user_prompt,
        )
    )


_register_builtin_specs()
_freeze_registry()
