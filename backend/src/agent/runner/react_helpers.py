import json
import re
from typing import Callable, Dict, List, Optional, Tuple

from backend.src.actions.registry import (
    action_payload_keys_guide,
    action_types_line,
    normalize_action_type,
)
from backend.src.agent.support import (
    _extract_json_object,
    coerce_file_write_payload_path_from_title,
    _validate_action,
)
from backend.src.agent.core.context_budget import apply_context_budget_pipeline
from backend.src.common.errors import AppError
from backend.src.common.utils import now_iso
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
    AGENT_EXPERIMENT_DIR_REL,
    AGENT_REACT_STEP_PROMPT_TEMPLATE,
    ASSISTANT_OUTPUT_STYLE_GUIDE,
    AGENT_SHELL_COMMAND_DEFAULT_TIMEOUT_MS,
    ERROR_MESSAGE_LLM_CALL_FAILED,
    TASK_OUTPUT_TYPE_TEXT,
)


def _extract_prefixed_value(title: str, prefix: str) -> str:
    raw = str(title or "").strip()
    if not raw:
        return ""
    m = re.match(rf"^{re.escape(str(prefix))}[:：]\s*(\"[^\"]+\"|'[^']+'|\S+)", raw)
    if not m:
        return ""
    value = str(m.group(1) or "").strip()
    if (value.startswith("\"") and value.endswith("\"")) or (value.startswith("'") and value.endswith("'")):
        value = value[1:-1].strip()
    return value


def _looks_like_url(value: str) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    return bool(re.match(r"^https?://", raw, re.IGNORECASE))


def _normalize_llm_parameters(raw_params: object) -> Optional[dict]:
    if not isinstance(raw_params, dict):
        return None

    normalized: dict = {}
    for key, value in raw_params.items():
        key_text = str(key or "").strip()
        if not key_text:
            continue
        if value is None:
            continue
        normalized[key_text] = value

    if "max_tokens" not in normalized and "max_output_tokens" in normalized:
        raw_value = normalized.get("max_output_tokens")
        try:
            max_tokens = int(raw_value) if raw_value is not None else None
        except Exception:
            max_tokens = None
        if isinstance(max_tokens, int) and max_tokens > 0:
            normalized["max_tokens"] = max_tokens
    normalized.pop("max_output_tokens", None)

    return normalized or None


def extract_llm_call_text(resp) -> Tuple[Optional[str], Optional[str]]:
    """
    统一解析 create_llm_call 的返回值。

    返回：(text, error_message)
    """
    text, err, _llm_id = extract_llm_call_text_and_id(resp)
    return text, err


def extract_llm_call_text_and_id(resp) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """
    统一解析 create_llm_call 的返回值，并提取 llm_records.id。

    返回：(text, error_message, llm_id)
    """
    text = None
    err = None
    llm_id = None
    if isinstance(resp, dict):
        record = resp.get("record")
        if isinstance(record, dict):
            try:
                llm_id = int(record.get("id")) if record.get("id") is not None else None
            except (TypeError, ValueError):
                llm_id = None
            if str(record.get("status") or "").strip() == "error":
                err = str(record.get("error") or "").strip() or ERROR_MESSAGE_LLM_CALL_FAILED
            text = record.get("response")
        else:
            err = ERROR_MESSAGE_LLM_CALL_FAILED
    else:
        err = ERROR_MESSAGE_LLM_CALL_FAILED
    return text, err, llm_id


def call_llm_for_text(
    llm_call: Callable[[dict], dict],
    *,
    prompt: str,
    task_id: int,
    run_id: int,
    model: str,
    parameters: dict,
    variables: Optional[dict] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    统一 LLM 调用 + 错误收敛，避免在 react_loop 里重复 try/except。
    """
    text, err, _llm_id = call_llm_for_text_with_id(
        llm_call,
        prompt=prompt,
        task_id=task_id,
        run_id=run_id,
        model=model,
        parameters=parameters,
        variables=variables,
    )
    return text, err


def call_llm_for_text_with_id(
    llm_call: Callable[[dict], dict],
    *,
    prompt: str,
    task_id: int,
    run_id: int,
    model: str,
    parameters: dict,
    variables: Optional[dict] = None,
) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """
    call_llm_for_text 的扩展版本：额外返回 llm_records.id，便于链路调试与溯源。
    """
    try:
        resp = llm_call(
            {
                "prompt": prompt,
                "task_id": int(task_id),
                "run_id": int(run_id),
                "model": model,
                "parameters": parameters,
                "variables": variables or {},
            }
        )
        return extract_llm_call_text_and_id(resp)
    except AppError as exc:
        return None, str(exc.message or "").strip() or ERROR_MESSAGE_LLM_CALL_FAILED, None
    except (TypeError, ValueError, KeyError, AttributeError, RuntimeError) as exc:
        return None, str(exc) or ERROR_MESSAGE_LLM_CALL_FAILED, None


def build_react_step_prompt(
    *,
    workdir: str,
    message: str,
    plan: str,
    step_index: int,
    step_title: str,
    allowed_actions: str,
    observations: str,
    recent_source_failures: str,
    graph: str,
    tools: str,
    skills: str,
    memories: str,
    now_utc: Optional[str] = None,
    disallow_plan_patch: bool = False,
    capability_hint: str = "",
    budget_meta_sink: Optional[dict] = None,
) -> str:
    """
    构建 ReAct step prompt（统一模板与运行时 action 契约注入）。

    目的：
    - 避免 react_loop_impl / think_parallel_loop 各自拼接导致字段约束漂移；
    - payload 字段白名单统一来自 action registry，和执行器保持单一来源。
    """
    sections, budget_meta = apply_context_budget_pipeline(
        {
            "observations": str(observations),
            "recent_source_failures": str(recent_source_failures),
            "graph": str(graph),
            "tools": str(tools),
            "skills": str(skills),
            "solutions": "",
            "memories": str(memories),
        }
    )
    if isinstance(budget_meta_sink, dict):
        budget_meta_sink.clear()
        budget_meta_sink.update(dict(budget_meta or {}))
    prompt = AGENT_REACT_STEP_PROMPT_TEMPLATE.format(
        now=str(now_utc or now_iso()),
        workdir=str(workdir),
        agent_workspace=AGENT_EXPERIMENT_DIR_REL,
        message=str(message),
        plan=str(plan),
        step_index=int(step_index),
        step_title=str(step_title),
        allowed_actions=str(allowed_actions),
        observations=str(sections.get("observations") or ""),
        recent_source_failures=str(sections.get("recent_source_failures") or ""),
        graph=str(sections.get("graph") or ""),
        tools=str(sections.get("tools") or ""),
        skills=str(sections.get("skills") or ""),
        memories=str(sections.get("memories") or ""),
        output_style=ASSISTANT_OUTPUT_STYLE_GUIDE,
        action_types_line=action_types_line(),
        action_payload_keys_guide=action_payload_keys_guide(),
    )
    if disallow_plan_patch:
        prompt += "\n额外约束：当前为 Think 并行执行阶段，不支持 plan_patch。请不要输出 plan_patch 字段（或始终为 null）。\n"
    capability_text = str(capability_hint or "").strip()
    if capability_text:
        prompt += f"\n额外约束：本步骤能力标签为「{capability_text}」，优先选择与该能力匹配的 action。\n"
    return prompt


def normalize_action_obj_for_execution(
    *,
    action_obj: dict,
    step_title: str,
    workdir: str,
) -> tuple[Optional[dict], Optional[str], Optional[dict], Optional[str]]:
    """
    对 LLM 输出的 action 做一次“可执行归一化”，再交给 _validate_action 校验。

    关键点：
    - shell_command 的 workdir/timeout_ms 允许模型省略（由后端兜底），否则校验会提前失败；
    - file_write 的 path 允许模型省略（若标题以 file_write:xxx 声明了路径），否则校验会提前失败；
    - task_output 的 output_type/content 允许省略（content 是否允许为空取决于上下文，另行处理）；
    - llm_call 禁止 provider/model，统一走后端默认配置，避免模型写错导致不可用。
    """
    if not isinstance(action_obj, dict):
        return None, None, None, "action 输出不是对象"
    action = action_obj.get("action")
    if not isinstance(action, dict):
        return None, None, None, "缺少 action"
    raw_action_type = action.get("type")
    if not isinstance(raw_action_type, str) or not raw_action_type.strip():
        return None, None, None, "action.type 不能为空"

    # 归一化 action.type（支持 alias），确保后续兜底逻辑与 allow gate 一致。
    # 注意：未知类型不强行改写，交给 _validate_action 给出明确错误。
    normalized_action_type = normalize_action_type(raw_action_type)
    action_type = normalized_action_type or str(raw_action_type).strip()
    if normalized_action_type:
        action["type"] = normalized_action_type

    payload_raw = action.get("payload")
    if not isinstance(payload_raw, dict):
        return None, None, None, "action.payload 不是对象"

    payload_obj = dict(payload_raw)

    if action_type == ACTION_TYPE_LLM_CALL:
        # 强制走后端默认配置，避免模型不匹配
        payload_obj.pop("model", None)
        payload_obj.pop("provider", None)
        normalized_params = _normalize_llm_parameters(payload_obj.get("parameters"))
        if isinstance(normalized_params, dict):
            payload_obj["parameters"] = normalized_params
        else:
            payload_obj.pop("parameters", None)

    if action_type == ACTION_TYPE_SHELL_COMMAND:
        # workdir 在 Windows/WSL 下很容易被模型漏填；缺失会导致 _validate_action 直接失败
        payload_obj.setdefault("workdir", workdir)
        payload_obj.setdefault("timeout_ms", AGENT_SHELL_COMMAND_DEFAULT_TIMEOUT_MS)

    if action_type == ACTION_TYPE_TASK_OUTPUT:
        payload_obj.setdefault("output_type", TASK_OUTPUT_TYPE_TEXT)
        payload_obj.setdefault("content", "")

    if action_type in {
        ACTION_TYPE_FILE_READ,
        ACTION_TYPE_FILE_LIST,
        ACTION_TYPE_FILE_APPEND,
        ACTION_TYPE_FILE_DELETE,
    }:
        # 兼容：模型可能只在 title 中写了路径（file_read:xxx），payload.path 漏填。
        current_path = str(payload_obj.get("path") or "").strip()
        if not current_path:
            extracted = _extract_prefixed_value(step_title, action_type)
            if extracted:
                payload_obj["path"] = extracted

    if action_type == ACTION_TYPE_HTTP_REQUEST:
        # 兼容：模型可能只在 title 中写了 URL（http_request:https://...），payload.url 漏填。
        current_url = str(payload_obj.get("url") or "").strip()
        if not current_url:
            extracted = _extract_prefixed_value(step_title, action_type)
            if extracted and _looks_like_url(extracted):
                payload_obj["url"] = extracted

    if action_type == ACTION_TYPE_FILE_WRITE:
        payload_obj.setdefault("encoding", "utf-8")
        payload_obj = coerce_file_write_payload_path_from_title(step_title, payload_obj)

    if action_type == ACTION_TYPE_JSON_PARSE:
        # 兼容：模型可能用 json/content/raw 字段而非 text。
        text = payload_obj.get("text")
        if not isinstance(text, str) or not text.strip():
            for key in ("json", "content", "raw"):
                alt = payload_obj.get(key)
                if isinstance(alt, str) and alt.strip():
                    payload_obj["text"] = alt
                    break

    if action_type == ACTION_TYPE_MEMORY_WRITE:
        # 兼容：模型可能用 text/message/value 字段而非 content。
        content = payload_obj.get("content")
        if not isinstance(content, str) or not content.strip():
            for key in ("text", "message", "value"):
                alt = payload_obj.get(key)
                if isinstance(alt, str) and alt.strip():
                    payload_obj["content"] = alt
                    break

    if action_type == ACTION_TYPE_USER_PROMPT:
        # 兼容：模型可能用 prompt/content/text/message 字段而非 question。
        question = payload_obj.get("question")
        if not isinstance(question, str) or not question.strip():
            for key in ("prompt", "content", "text", "message"):
                alt = payload_obj.get(key)
                if isinstance(alt, str) and alt.strip():
                    payload_obj["question"] = alt
                    break

    if action_type == ACTION_TYPE_TOOL_CALL:
        # 兼容：模型可能只在 title 中写了工具名（tool_call:web_fetch ...），payload.tool_name 漏填。
        current_tool_name = str(payload_obj.get("tool_name") or "").strip()
        if not current_tool_name:
            extracted_tool_name = _extract_prefixed_value(step_title, ACTION_TYPE_TOOL_CALL)
            if extracted_tool_name:
                payload_obj["tool_name"] = extracted_tool_name

        # tool_call.exec 需要工作目录与超时等信息；模型经常漏填或用别名字段，这里做兼容兜底。
        meta = payload_obj.get("tool_metadata")
        if isinstance(meta, dict):
            exec_spec = meta.get("exec")
            if isinstance(exec_spec, dict):
                if exec_spec.get("timeout_ms") is None and isinstance(
                    exec_spec.get("timeout"), (int, float)
                ):
                    raw = exec_spec.get("timeout")
                    try:
                        raw_num = float(raw)
                        exec_spec["timeout_ms"] = (
                            int(raw_num * 1000) if 0 < raw_num < 1000 else int(raw_num)
                        )
                    except (TypeError, ValueError):
                        pass
                if not exec_spec.get("workdir"):
                    exec_spec["workdir"] = workdir
                if not exec_spec.get("type"):
                    if exec_spec.get("command") or exec_spec.get("args") or exec_spec.get("shell"):
                        exec_spec["type"] = "shell"
                if (
                    not exec_spec.get("command")
                    and isinstance(exec_spec.get("shell"), str)
                    and exec_spec.get("shell").strip()
                ):
                    exec_spec["command"] = exec_spec.get("shell").strip()
                if isinstance(exec_spec.get("args"), str) and exec_spec.get("args").strip():
                    if not exec_spec.get("command"):
                        exec_spec["command"] = exec_spec.get("args").strip()
                    exec_spec.pop("args", None)
                if (
                    isinstance(exec_spec.get("command"), list)
                    and exec_spec.get("command")
                    and not exec_spec.get("args")
                ):
                    exec_spec["args"] = exec_spec.get("command")
                    exec_spec.pop("command", None)
                exec_spec.setdefault("timeout_ms", AGENT_SHELL_COMMAND_DEFAULT_TIMEOUT_MS)
                meta["exec"] = exec_spec
            payload_obj["tool_metadata"] = meta

    action["payload"] = payload_obj
    action_obj["action"] = action
    return action_obj, action_type, payload_obj, None


def validate_and_normalize_action_text(
    *,
    action_text: str,
    step_title: str,
    workdir: str,
) -> tuple[Optional[dict], Optional[str], Optional[dict], Optional[str]]:
    """
    action_text(JSON) -> action_obj/action_type/payload_obj。
    """
    action_obj = _extract_json_object(action_text)
    if not action_obj:
        return None, None, None, "action 输出不是有效 JSON"

    action_obj, action_type, payload_obj, normalize_err = normalize_action_obj_for_execution(
        action_obj=action_obj,
        step_title=step_title,
        workdir=workdir,
    )
    if normalize_err:
        return None, None, None, normalize_err

    action_validate_error = _validate_action(action_obj)
    if action_validate_error:
        return None, None, None, action_validate_error

    return action_obj, action_type, payload_obj, None


def needs_nonempty_task_output_content(payload_obj: dict, context: Dict) -> bool:
    """
    判断 task_output.content 是否必须非空：
    - 若 payload.content 非空 => ok
    - 若已有 last_llm_response 可用于补齐 => ok
    - 否则 => 必须让模型补齐 content（否则执行器会报错，且用户看不到最终结果）
    """
    try:
        content = str(payload_obj.get("content") or "").strip()
    except (AttributeError, TypeError, ValueError):
        content = ""
    if content:
        return False
    try:
        last_llm = str((context or {}).get("last_llm_response") or "").strip()
    except (AttributeError, TypeError, ValueError):
        last_llm = ""
    return not bool(last_llm)


def json_dumps_or_fallback(value: dict) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return json.dumps({"text": str(value)}, ensure_ascii=False)


__all__ = [
    "call_llm_for_text",
    "call_llm_for_text_with_id",
    "extract_llm_call_text",
    "extract_llm_call_text_and_id",
    "json_dumps_or_fallback",
    "needs_nonempty_task_output_content",
    "normalize_action_obj_for_execution",
    "validate_and_normalize_action_text",
]
