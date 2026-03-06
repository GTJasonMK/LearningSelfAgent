import os
import json
import re
from typing import Callable, Dict, List, Optional, Tuple

from backend.src.actions.registry import (
    action_payload_keys_guide,
    action_types_line,
    normalize_action_type,
)
from backend.src.actions.handlers.common_utils import parse_command_tokens, resolve_path_with_workdir
from backend.src.agent.support import (
    _extract_json_object,
    coerce_file_write_payload_path_from_title,
    extract_file_write_target_path,
    looks_like_file_path,
    _validate_action,
)
from backend.src.agent.core.context_budget import apply_context_budget_pipeline
from backend.src.agent.runner.goal_progress import summarize_task_grounding_for_prompt
from backend.src.common.errors import AppError
from backend.src.common.task_error_codes import format_task_error
from backend.src.common.utils import coerce_int, now_iso
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



def _truncate_sample_text_for_prompt(text: str, max_chars: int = 2200) -> str:
    raw = str(text or "").strip()
    if not raw or len(raw) <= max_chars:
        return raw
    head = max(320, int((max_chars - 8) / 2))
    tail = head
    if head + tail >= len(raw):
        return raw
    return f"{raw[:head]}\n...\n{raw[-tail:]}"

def normalize_allow_actions(raw_actions: object) -> list[str]:
    """
    归一化计划 allow 列表：
    - 统一小写
    - 去空/去重
    - 保持原始顺序（首个出现优先）
    """
    if not isinstance(raw_actions, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in raw_actions:
        text = normalize_action_type(item)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _normalize_direct_user_prompt_payload(raw_payload: object) -> Optional[dict]:
    if not isinstance(raw_payload, dict):
        return None
    question = str(raw_payload.get("question") or "").strip()
    if not question:
        return None
    payload: dict = {"question": question}
    kind = str(raw_payload.get("kind") or "").strip()
    if kind:
        payload["kind"] = kind
    choices = raw_payload.get("choices")
    if isinstance(choices, list) and choices:
        payload["choices"] = list(choices)
    return payload


def resolve_direct_user_prompt_payload(
    *,
    step_title: str,
    allowed_actions: object,
    step_prompt: Optional[dict] = None,
) -> Optional[dict]:
    """
    判定是否可走“确定性 user_prompt 直通”分支。

    约束：
    - allow 仅包含 user_prompt；
    - step_title 明确以 user_prompt: 前缀声明问题文本。
    """
    normalized_allow = normalize_allow_actions(allowed_actions)
    if normalized_allow != [ACTION_TYPE_USER_PROMPT]:
        return None
    prompt_payload = _normalize_direct_user_prompt_payload(step_prompt)
    if prompt_payload:
        return prompt_payload

    raw_title = str(step_title or "").strip()
    if not raw_title:
        return None
    m = re.match(r"^user_prompt\s*[:：]\s*(.+)$", raw_title, re.IGNORECASE)
    if not m:
        return None
    question = str(m.group(1) or "").strip()
    if not question:
        return None
    return {"question": question}


def _looks_like_url(value: str) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    return bool(re.match(r"^https?://", raw, re.IGNORECASE))


def _looks_like_python_script_token(value: str) -> bool:
    token = str(value or "").strip().lower()
    return bool(token) and token.endswith(".py")


def _is_python_executable_token(value: str) -> bool:
    token = os.path.splitext(os.path.basename(str(value or "").strip()))[0].lower()
    return token in {"python", "python3", "py"}


def _extract_script_run_from_command(command: object) -> Tuple[str, List[str]]:
    """
    从 shell_command.command 中提取 script_run 结构：
    - 支持 ["python", "x.py", ...] / "python x.py ..."
    - 支持直接执行脚本 ["x.py", ...]（默认由执行器补 python）
    """
    tokens = parse_command_tokens(command)
    if not tokens:
        return "", []

    head = str(tokens[0] or "").strip()
    head_lower = head.lower()
    if head_lower in {"python", "python3", "py"} and len(tokens) >= 2:
        script = str(tokens[1] or "").strip()
        if _looks_like_python_script_token(script):
            return script, [str(item) for item in tokens[2:]]
        return "", []

    if _looks_like_python_script_token(head):
        return head, [str(item) for item in tokens[1:]]
    return "", []


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
    retry_max_attempts: Optional[int] = None,
    hard_timeout_seconds: Optional[int] = None,
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
        retry_max_attempts=retry_max_attempts,
        hard_timeout_seconds=hard_timeout_seconds,
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
    retry_max_attempts: Optional[int] = None,
    hard_timeout_seconds: Optional[int] = None,
) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """
    call_llm_for_text 的扩展版本：额外返回 llm_records.id，便于链路调试与溯源。
    """
    try:
        payload = {
            "prompt": prompt,
            "task_id": int(task_id),
            "run_id": int(run_id),
            "model": model,
            "parameters": parameters,
            "variables": variables or {},
        }
        if retry_max_attempts is not None:
            payload["retry_max_attempts"] = int(retry_max_attempts)
        if hard_timeout_seconds is not None:
            payload["hard_timeout_seconds"] = int(hard_timeout_seconds)
        resp = llm_call(payload)
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
    latest_parse_input_text: str = "",
    latest_external_url: str = "",
    now_utc: Optional[str] = None,
    disallow_plan_patch: bool = False,
    capability_hint: str = "",
    execution_hint: str = "",
    budget_meta_sink: Optional[dict] = None,
    recent_step_feedback: str = "",
    retry_requirements: str = "",
    failure_guidance: str = "",
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
    execution_text = str(execution_hint or "").strip()
    if execution_text:
        prompt += f"\n执行修复约束：\n{execution_text}\n"
    feedback_text = str(recent_step_feedback or "").strip()
    if feedback_text:
        prompt += f"\n最近步骤反馈（用于避免重复失败）：\n{feedback_text}\n"
    retry_text = str(retry_requirements or "").strip()
    if retry_text:
        prompt += f"\n当前重试约束（下一轮必须满足）：\n{retry_text}\n"
    failure_guidance_text = str(failure_guidance or "").strip()
    if failure_guidance_text:
        prompt += f"\n失败修复策略提示（保留自主选择空间）：\n{failure_guidance_text}\n"

    task_grounding_text = summarize_task_grounding_for_prompt(message)
    if task_grounding_text and task_grounding_text != "(无)":
        prompt += f"\n原任务不可变约束：\n{task_grounding_text}\n"

    file_write_target = str(extract_file_write_target_path(step_title) or "").strip()
    if file_write_target and os.path.splitext(file_write_target)[1].lower() in {".py", ".js", ".ts", ".mjs", ".cjs", ".sh", ".ps1", ".bat", ".cmd", ".rb", ".php"}:
        prompt += (
            "\n脚本 file_write 额外约束：\n"
            "- 只能写入基于最近真实观测可直接运行的真实脚本；禁止 skeleton/TODO/placeholder/sample source。\n"
            "- 禁止写‘假设数据结构’‘需根据观测调整’这类占位解析逻辑。\n"
            "- 若当前没有足够样本支撑脚本实现，优先通过 plan_patch 在前置步骤获取真实样本后再写脚本。\n"
        )

    allow_text = str(allowed_actions or "").lower()
    title_text = str(step_title or "").lower()
    sample_sensitive = any(
        token in allow_text or title_text.startswith(f"{token}:")
        for token in (ACTION_TYPE_FILE_WRITE, ACTION_TYPE_SHELL_COMMAND, ACTION_TYPE_JSON_PARSE, ACTION_TYPE_LLM_CALL)
    )
    latest_sample = str(latest_parse_input_text or "").strip()
    latest_url = str(latest_external_url or "").strip()
    if sample_sensitive and latest_sample:
        prompt += (
            "\n最近真实样本（自动注入，优先使用）：\n"
            f"{_truncate_sample_text_for_prompt(latest_sample)}\n"
            "样本使用约束：\n"
            "- 当前动作若要写脚本、解析数据或生成中间结果，必须直接基于上述真实样本实现。\n"
            "- 禁止输出‘假设数据结构’‘需根据观测调整’‘先尝试常见结构’这类占位逻辑。\n"
            "- 若上述样本仍不足以支持当前动作，请不要编造；改为 plan_patch 增补更具体的抓取/读取/转换步骤。\n"
        )
        if latest_url:
            prompt += f"- 最近样本来源：{latest_url}\n"
    return prompt


def build_execution_constraints_hint(
    *,
    agent_state: Optional[Dict],
    step_order: int,
) -> str:
    """
    将 agent_state.execution_constraints 转换为 prompt 约束文本。

    目标：
    - 在“失败后重规划”场景持续施加修复约束，避免重复生成高耦合脚本；
    - 约束带有 step 有效期，到期后自动失效。
    """
    if not isinstance(agent_state, dict):
        return ""
    constraints = agent_state.get("execution_constraints")
    if not isinstance(constraints, dict):
        return ""

    current_step = coerce_int(step_order, default=0)
    hints: List[str] = []

    low_param_until = coerce_int(constraints.get("prefer_low_param_scripts_until_step"), default=0)
    if low_param_until >= current_step > 0:
        hints.append(
            "- 若需要写脚本：优先低参数设计（必填参数 <= 2），避免生成需要大量手工参数映射的脚本。"
        )

    materialize_until = coerce_int(constraints.get("require_script_materialization_until_step"), default=0)
    if materialize_until >= current_step > 0:
        hints.append(
            "- shell_command/tool_call 若引用本地脚本，必须先执行 file_write/file_append 落盘该脚本。"
        )

    exclusive_until = coerce_int(constraints.get("enforce_exclusive_input_args_until_step"), default=0)
    if exclusive_until >= current_step > 0:
        hints.append(
            "- 若脚本参数存在互斥输入（如 --in-json/--in-csv），只能选择其一，禁止同时传入。"
        )

    compact_action_until = coerce_int(constraints.get("prefer_compact_action_prompt_until_step"), default=0)
    if compact_action_until >= current_step > 0:
        hints.append(
            "- 当前阶段动作生成需极简：只输出单个 JSON，payload 仅保留必要字段，禁止解释和冗余内容。"
        )

    grounded_script_until = coerce_int(constraints.get("require_grounded_script_file_write_until_step"), default=0)
    if grounded_script_until >= current_step > 0:
        hints.append(
            "- 若当前是脚本类 file_write：只能写入基于最近真实观测可直接运行的真实脚本；禁止 skeleton/TODO/placeholder/假设结构。若缺少样本，优先先获取样本或切换到更轻量路径。"
        )

    switch_action_path_until = coerce_int(constraints.get("prefer_action_path_switch_until_step"), default=0)
    if switch_action_path_until >= current_step > 0:
        hints.append(
            "- 若同类动作连续超时，请优先通过 plan_patch 切换到更轻量路径（tool_call/shell_command）产出中间结果后再继续。"
        )

    if not hints:
        return ""
    return "\n".join(hints)


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

        # 结构化 script_run 归一化：
        # - 若模型直接给 script/args，保留并清洗；
        # - 若模型给的是 python xxx.py ... 的 command，自动拆成 script + args；
        # - 目的：让执行器可做参数契约预检，避免“运行后才报 argparse 缺参”。
        script_value = str(payload_obj.get("script") or "").strip()
        if not script_value:
            script_value = _extract_prefixed_value(step_title, "script_run")
        command_value = payload_obj.get("command")
        extracted_script, extracted_args = _extract_script_run_from_command(command_value)

        args_value = payload_obj.get("args")
        normalized_args: List[str] = []
        if isinstance(args_value, str) and args_value.strip():
            normalized_args = parse_command_tokens(args_value)
        elif isinstance(args_value, list):
            normalized_args = [str(item) for item in args_value if str(item).strip()]

        # 纠正模型常见错误：script 写成 python，可从 command/args 中回收真实脚本路径。
        if extracted_script and (not script_value or _is_python_executable_token(script_value)):
            script_value = extracted_script
        if script_value and _is_python_executable_token(script_value) and normalized_args:
            first_arg = str(normalized_args[0] or "").strip()
            if _looks_like_python_script_token(first_arg):
                script_value = first_arg
                normalized_args = [str(item) for item in normalized_args[1:] if str(item).strip()]
        if script_value:
            if normalized_args and str(normalized_args[0] or "").strip() == script_value:
                normalized_args = normalized_args[1:]
            if extracted_args and not normalized_args:
                normalized_args = list(extracted_args)

            payload_obj["script"] = script_value
            payload_obj["args"] = list(normalized_args)

            required_args = payload_obj.get("required_args")
            if isinstance(required_args, str) and required_args.strip():
                payload_obj["required_args"] = [required_args.strip()]
            elif not isinstance(required_args, list):
                payload_obj.pop("required_args", None)

            payload_obj["discover_required_args"] = True
            payload_obj.pop("command", None)

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

        title_target = extract_file_write_target_path(step_title)
        current_path = str(payload_obj.get("path") or "").strip()
        if (
            title_target
            and current_path
            and looks_like_file_path(title_target)
            and looks_like_file_path(current_path)
            and title_target != current_path
        ):
            return None, None, None, format_task_error(
                code="file_write_path_conflict",
                message=(
                    "file_write.title 与 payload.path 冲突；"
                    f"title={title_target}，payload.path={current_path}。"
                    "请保持同一步只写一个目标文件，并确保 title 与 payload.path 一致。"
                ),
            )

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

    # 运行前置契约校验：尽早拦截“shell_command 引用不存在脚本”，
    # 让模型在同一步重生成为 file_write/shell_command 的正确顺序，而不是先执行失败再 replan。
    if action_type == ACTION_TYPE_SHELL_COMMAND and isinstance(payload_obj, dict):
        script_path = str(payload_obj.get("script") or "").strip()
        if script_path:
            resolved = resolve_path_with_workdir(script_path, workdir)
            if resolved and not os.path.exists(resolved):
                return (
                    None,
                    None,
                    None,
                    format_task_error(
                        code="script_missing",
                        message=f"shell_command 引用脚本不存在：{resolved}（请先 file_write 再执行）",
                    ),
                )

    return action_obj, action_type, payload_obj, None


def _map_tool_exec_dependency_error_code(error_text: str) -> str:
    lowered = str(error_text or "").strip().lower()
    if "依赖未绑定" in lowered or "unbound" in lowered:
        return "script_dependency_unbound"
    if "脚本不存在" in lowered or "not found" in lowered or "missing" in lowered:
        return "script_missing"
    return "tool_exec_contract_error"


def validate_runtime_action_contracts(
    *,
    task_id: int,
    run_id: int,
    step_title: str,
    action_type: str,
    payload_obj: Optional[dict],
    workdir: str,
) -> Optional[str]:
    """
    在步骤真正落库/执行前做运行时契约校验，尽早把结构性错误收敛为 action_invalid。

    目前覆盖：
    - tool_call 缺少 exec 定义
    - tool_call.exec 引用本地脚本，但脚本未落盘或未被当前 run 的 file_write/file_append 绑定
    """
    if action_type != ACTION_TYPE_TOOL_CALL or not isinstance(payload_obj, dict):
        return None

    from backend.src.actions.handlers.tool_call import (
        _enforce_tool_exec_script_dependency,
        _resolve_tool_exec_spec,
    )

    exec_spec = _resolve_tool_exec_spec(payload_obj)
    if exec_spec is None:
        return format_task_error(
            code="missing_tool_exec_spec",
            message=(
                "tool_call 缺少可执行定义：请在 tool_metadata.exec 中提供 "
                "type=shell，且包含 command(str) 或 args(list)，可选 timeout_ms，建议 workdir；"
                "并把 output 留空让系统真实执行"
            ),
        )

    dependency_error = _enforce_tool_exec_script_dependency(
        task_id=int(task_id),
        run_id=int(run_id),
        step_row={"id": None, "title": str(step_title or "")},
        exec_spec=exec_spec,
        tool_input=str(payload_obj.get("input") or ""),
    )
    if not dependency_error:
        return None
    return format_task_error(
        code=_map_tool_exec_dependency_error_code(dependency_error),
        message=str(dependency_error),
    )


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
    "validate_runtime_action_contracts",
]
