import json
import ntpath
import os
import queue
import re
import threading
import time
from dataclasses import dataclass
from typing import Generator, List, Optional

from backend.src.agent.json_utils import _extract_json_object
from backend.src.agent.plan_utils import (
    _normalize_plan_titles,
    drop_non_artifact_file_write_steps,
    extract_file_write_target_path,
    is_bootstrap_script_file_write_step,
    reorder_script_file_writes_before_exec_steps,
    repair_plan_artifacts_with_file_write_steps,
    sanitize_plan_brief,
)
from backend.src.agent.support import _truncate_observation
from backend.src.agent.source_failure_summary import summarize_recent_source_failures_for_prompt
from backend.src.agent.core.context_budget import apply_context_budgets
from backend.src.actions.registry import action_types_line
from backend.src.services.llm.llm_calls import create_llm_call
from backend.src.constants import (
    ACTION_TYPE_FILE_READ,
    ACTION_TYPE_FILE_WRITE,
    ACTION_TYPE_HTTP_REQUEST,
    ACTION_TYPE_JSON_PARSE,
    ACTION_TYPE_LLM_CALL,
    ACTION_TYPE_SHELL_COMMAND,
    ACTION_TYPE_TASK_OUTPUT,
    ACTION_TYPE_TOOL_CALL,
    AGENT_EXPERIMENT_DIR_REL,
    AGENT_PLAN_HEARTBEAT_INTERVAL_SECONDS,
    AGENT_PLAN_MAX_WAIT_SECONDS,
    AGENT_PLAN_PROMPT_TEMPLATE,
    AGENT_REPLAN_PROMPT_TEMPLATE,
    ERROR_MESSAGE_LLM_API_KEY_MISSING,
    ERROR_MESSAGE_LLM_CALL_FAILED,
    STREAM_TAG_PLAN,
)
from backend.src.services.debug.safe_debug import safe_write_debug as _safe_write_debug
from backend.src.services.llm.llm_client import sse_json
from backend.src.agent.runner.react_helpers import call_llm_for_text_with_id
from backend.src.agent.runner.goal_progress import detect_task_grounding_drift, summarize_task_grounding_for_prompt
from backend.src.agent.runner.step_feedback import (
    summarize_failure_guidance_for_prompt,
    summarize_recent_step_feedback_for_prompt,
    summarize_retry_requirements_for_prompt,
)
from backend.src.common.task_error_codes import extract_task_error_code, is_source_failure_error_code

# 仅在用户明确表达“保存到某目录/某路径下”时启用的输出目录提示抽取。
# 目的：让 plan 阶段 deterministic 地把 file_write/artifacts 绑定到用户指定目录，避免落盘到错误位置。
_OUTPUT_DIR_QUOTED_RE = re.compile(
    r"""["'](?P<path>[^"']+)["']\s*(?:目录|文件夹|下|中)""",
    re.IGNORECASE,
)
_OUTPUT_DIR_BARE_WIN_RE = re.compile(
    r"""(?P<path>[A-Za-z]:[\\/][^\s"']+)\s*(?:目录|文件夹|下|中)""",
    re.IGNORECASE,
)


def _is_windows_abs_path(value: str) -> bool:
    return bool(re.match(r"^[A-Za-z]:[\\/]", str(value or "").strip()))


def _extract_output_dir_from_message(*, message: str, workdir: str) -> Optional[str]:
    """
    从用户消息中抽取“目标输出目录”，并尽量转换为相对 workdir 的路径。

    示例：
    - message: 保存在 "E:\\code\\LearningSelfAgent\\test" 目录中
    - workdir : E:\\code\\LearningSelfAgent
    - return  : test
    """
    text = str(message or "")
    if not text.strip():
        return None

    raw = None
    m = _OUTPUT_DIR_QUOTED_RE.search(text)
    if m:
        raw = str(m.group("path") or "").strip()
    if not raw:
        m = _OUTPUT_DIR_BARE_WIN_RE.search(text)
        if m:
            raw = str(m.group("path") or "").strip()
    if not raw:
        return None

    # 仅处理“目录”语义：如果看起来是文件（有扩展名），直接忽略，避免误伤。
    base = raw.replace("\\", "/").rstrip("/")
    if "." in base.split("/")[-1]:
        return None

    # Windows 路径：优先做 workdir 内相对化（大小写不敏感）。
    if _is_windows_abs_path(raw) and _is_windows_abs_path(workdir):
        try:
            wd_norm = ntpath.normcase(ntpath.normpath(str(workdir)))
            raw_norm = ntpath.normcase(ntpath.normpath(str(raw)))
            if raw_norm.startswith(wd_norm):
                rel = ntpath.relpath(raw_norm, wd_norm)
                rel = rel.replace("\\", "/").strip()
                if rel in {".", ""}:
                    return None
                return rel
        except Exception:
            return None

    # WSL 场景：workdir 可能是 /mnt/<drive>/...，但用户输入仍是 Windows 盘符路径（E:\...）。
    # 这里将 raw 映射为 /mnt/<drive>/... 后再做相对化，避免把盘符路径当成普通字符串而忽略用户的目录意图。
    if _is_windows_abs_path(raw):
        try:
            wd_posix = str(workdir or "").replace("\\", "/").rstrip("/")
            m = re.match(r"^/mnt/(?P<drive>[A-Za-z])(?:/|$)", wd_posix)
            if m:
                wd_drive = str(m.group("drive") or "").lower()
                m2 = re.match(r"^(?P<drive>[A-Za-z]):[\\/](?P<rest>.*)$", str(raw))
                if m2:
                    raw_drive = str(m2.group("drive") or "").lower()
                    if raw_drive == wd_drive:
                        rest = str(m2.group("rest") or "").replace("\\", "/").lstrip("/")
                        raw_posix = f"/mnt/{raw_drive}/{rest}" if rest else f"/mnt/{raw_drive}"
                        wd_norm = os.path.normpath(wd_posix)
                        raw_norm = os.path.normpath(raw_posix)
                        if raw_norm.startswith(wd_norm):
                            rel = os.path.relpath(raw_norm, wd_norm).replace("\\", "/").strip()
                            if rel in {".", ""}:
                                return None
                            return rel
        except Exception:
            return None

    return None


def _rewrite_file_write_title_path(*, title: str, new_path: str) -> str:
    """
    把 file_write 步骤 title 中的目标路径替换为 new_path，保留其余描述。
    仅用于 plan 阶段的确定性修正。
    """
    raw = str(title or "").strip()
    if not raw:
        return raw
    m = re.match(r"^(file_write[:：]\s*)(\"[^\"]+\"|'[^']+'|\S+)(.*)$", raw)
    if not m:
        return raw
    prefix = str(m.group(1) or "")
    suffix = str(m.group(3) or "")
    return f"{prefix}{new_path}{suffix}".strip()


def _apply_output_dir_hint_to_plan(
    *,
    output_dir_rel: str,
    titles: List[str],
    artifacts: List[str],
) -> tuple[List[str], List[str], bool]:
    """
    将“用户指定输出目录（相对 workdir）”应用到计划：
    - artifacts 中的纯文件名 -> output_dir_rel/文件名
    - file_write:xxx 中的纯文件名 -> file_write:output_dir_rel/xxx
    """
    rel = str(output_dir_rel or "").strip().strip("/").strip()
    if not rel:
        return titles, artifacts, False

    changed = False

    new_artifacts: List[str] = []
    for a in artifacts or []:
        v = str(a or "").strip()
        if not v:
            continue
        if "/" not in v and "\\" not in v:
            new_artifacts.append(f"{rel}/{v}")
            changed = True
        else:
            new_artifacts.append(v)

    new_titles: List[str] = []
    for t in titles or []:
        value = str(t or "").strip()
        target = extract_file_write_target_path(value)
        if target and "/" not in target and "\\" not in target:
            # 只对“纯文件名”的写入目标做前缀修正，避免误改已显式指定子目录的计划。
            new_target = f"{rel}/{target}"
            new_titles.append(_rewrite_file_write_title_path(title=value, new_path=new_target))
            changed = True
        else:
            new_titles.append(value)

    return new_titles, new_artifacts, changed


def _is_plain_json_object_text(text: str) -> bool:
    """
    判断 LLM 输出是否为“纯 JSON 对象文本”。

    目的：
    - 降低“代码块包裹/前后附加解释”导致解析歧义；
    - 规划阶段优先要求可稳定解析的最小输出格式。
    """
    raw = str(text or "").strip()
    if not raw:
        return False
    if "```" in raw:
        return False
    return raw.startswith("{") and raw.endswith("}")


_STRUCTURED_DATA_EXTS = {".csv", ".tsv", ".json", ".ndjson", ".xlsx", ".parquet"}
_SCRIPT_FILE_EXTS = {".py", ".js", ".ts", ".mjs", ".cjs", ".sh", ".ps1", ".bat", ".cmd", ".rb", ".php"}
_SAMPLE_ACQUIRE_ACTIONS = {ACTION_TYPE_TOOL_CALL, ACTION_TYPE_HTTP_REQUEST, ACTION_TYPE_FILE_READ, ACTION_TYPE_SHELL_COMMAND}
_PRE_SAMPLE_BLOCKED_ACTIONS = {ACTION_TYPE_LLM_CALL, ACTION_TYPE_JSON_PARSE, ACTION_TYPE_TASK_OUTPUT}


def _has_real_sample(*, context: Optional[dict] = None) -> bool:
    if not isinstance(context, dict):
        return False
    return bool(str(context.get("latest_parse_input_text") or "").strip())


def _detect_sample_kind(sample_text: str) -> str:
    raw = str(sample_text or "").strip()
    lowered = raw.lower()
    if not raw:
        return "none"
    if raw.startswith("{") or raw.startswith("["):
        return "json"
    if raw.startswith("<") and "<html" in lowered[:200]:
        return "html"
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    if len(lines) >= 2 and "," in lines[0] and "," in lines[1]:
        return "csv"
    return "text"


def _summarize_real_sample_status(*, context: Optional[dict] = None) -> str:
    if not isinstance(context, dict):
        return "- sample_available=no\n- reason=no_context"
    sample_text = str(context.get("latest_parse_input_text") or "").strip()
    latest_external_url = str(context.get("latest_external_url") or "").strip()
    if not sample_text:
        lines = ["- sample_available=no"]
        if latest_external_url:
            lines.append(f"- latest_external_url={latest_external_url}")
            lines.append("- note=已有来源线索，但尚无可直接解析的真实样本")
        else:
            lines.append("- note=当前没有可直接解析的真实样本")
        return "\n".join(lines)

    lines = ["- sample_available=yes"]
    lines.append(f"- sample_kind={_detect_sample_kind(sample_text)}")
    if latest_external_url:
        lines.append(f"- sample_source={latest_external_url}")
    lines.append(f"- sample_preview={_truncate_observation(sample_text)}")
    return "\n".join(lines)


def _text_indicates_structured_data(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(
        token in lowered
        for token in ("csv", "json", "tsv", "ndjson", "xlsx", "parquet", "表格", "结构化", "字段", "列")
    )



def _plan_targets_structured_data(*, titles: List[str], artifacts: List[str], message: str = "") -> bool:
    if _text_indicates_structured_data(message):
        return True
    for item in artifacts or []:
        ext = os.path.splitext(str(item or "").strip().lower())[1]
        if ext in _STRUCTURED_DATA_EXTS:
            return True
    for title in titles or []:
        target = extract_file_write_target_path(str(title or ""))
        ext = os.path.splitext(str(target or "").strip().lower())[1]
        if ext in _STRUCTURED_DATA_EXTS:
            return True
        if _text_indicates_structured_data(title):
            return True
    return False


def _is_script_file_write_step(*, title: str, allow: List[str]) -> bool:
    allow_set = set(allow or [])
    if ACTION_TYPE_FILE_WRITE not in allow_set:
        return False
    target = extract_file_write_target_path(str(title or ""))
    ext = os.path.splitext(str(target or "").strip().lower())[1]
    return ext in _SCRIPT_FILE_EXTS


def _is_sample_acquire_step(*, title: str, allow: List[str]) -> bool:
    _ = title
    allow_set = set(allow or [])
    return bool(allow_set & _SAMPLE_ACQUIRE_ACTIONS)


def _has_grounded_external_source(*, message: str, observations: List[str], context: Optional[dict]) -> bool:
    candidates: List[str] = [str(message or "").strip()]
    for item in observations or []:
        candidates.append(str(item or "").strip())
    if isinstance(context, dict):
        for key in ("latest_external_url", "latest_source_url", "source_url"):
            value = str(context.get(key) or "").strip()
            if value:
                candidates.append(value)
    for text in candidates:
        if re.search(r"https?://[^\s\"'<>]+", str(text or "")):
            return True
    return False



def _validate_source_discovery_contract(
    *,
    titles: List[str],
    allows: List[List[str]],
    message: str,
    error: str,
    observations: List[str],
    context: Optional[dict],
) -> Optional[str]:
    code = extract_task_error_code(str(error or ""))
    if not code or not is_source_failure_error_code(code):
        return None
    if _has_grounded_external_source(message=message, observations=observations, context=context):
        return None
    if not titles or not allows:
        return None

    first_title = str(titles[0] or "").strip().lower()
    first_allow_set = set(allows[0] or [])
    if ACTION_TYPE_HTTP_REQUEST in first_allow_set or first_title.startswith("http_request:"):
        return "最近失败属于来源发现/可用性问题，且当前没有可用具体 URL/host 依据；剩余计划的首个抓取步骤不能直接是 http_request，应先继续发现来源。"
    return None



def _validate_structured_artifact_sample_contract(
    *,
    titles: List[str],
    allows: List[List[str]],
    artifacts: List[str],
    real_sample_available: bool,
    message: str = "",
) -> Optional[str]:
    if real_sample_available:
        return None
    if not _plan_targets_structured_data(titles=titles, artifacts=artifacts, message=message):
        return None

    first_acquire_idx: Optional[int] = None
    for idx, (title, allow) in enumerate(zip(titles or [], allows or [])):
        if _is_sample_acquire_step(title=str(title or ""), allow=list(allow or [])):
            first_acquire_idx = idx
            break
    if first_acquire_idx is None:
        return "当前尚无真实样本，但剩余计划没有优先安排获取样本步骤。对于结构化数据任务，应先获取样本再解析/写文件。"

    bootstrap_script_count = 0
    for idx in range(0, int(first_acquire_idx)):
        title = str(titles[idx] or "") if idx < len(titles) else ""
        allow = list(allows[idx] or []) if idx < len(allows) else []
        allow_set = set(allow or [])
        if ACTION_TYPE_FILE_WRITE in allow_set:
            if _is_script_file_write_step(title=title, allow=allow):
                if not is_bootstrap_script_file_write_step(title=title, brief=""):
                    return "当前尚无真实样本，首次获取样本前只允许抓取/读取类脚本；不要先写解析、校验或格式化脚本。"
                bootstrap_script_count += 1
                if bootstrap_script_count > 1:
                    return "当前尚无真实样本，首次获取样本前最多只能准备一个抓取/读取脚本；不要先连续写多个脚本。"
                continue
            return "当前尚无真实样本，首次获取样本前禁止写结果文件或非脚本类 file_write。"
        if allow_set & _PRE_SAMPLE_BLOCKED_ACTIONS:
            return "当前尚无真实样本，首次获取样本前不要先做 llm_call/json_parse/task_output；请先拿到真实样本。"
    return None


class PlanPhaseFailure(RuntimeError):
    """
    规划阶段失败：由上层负责收敛 task/run 状态并向前端输出 error event。
    """

    def __init__(self, *, reason: str, public_message: str):
        super().__init__(reason)
        self.reason = reason
        self.public_message = public_message


@dataclass
class PlanPhaseResult:
    plan_titles: List[str]
    plan_briefs: List[str]
    plan_allows: List[List[str]]
    plan_artifacts: List[str]
    plan_items: List[dict]
    plan_llm_id: Optional[int]


def run_replan_phase(
    *,
    task_id: int,
    run_id: int,
    message: str,
    workdir: str,
    model: str,
    parameters: dict,
    max_steps: int,
    tools_hint: str,
    skills_hint: str,
    solutions_hint: str,
    memories_hint: str,
    graph_hint: str,
    plan_titles: List[str],
    plan_artifacts: List[str],
    done_steps: List[str],
    error: str,
    observations: List[str],
    failure_signatures: Optional[dict] = None,
    context: Optional[dict] = None,
) -> Generator[str, None, PlanPhaseResult]:
    """
    重新规划（Replan）：
    - 基于已有计划与已完成步骤，生成“剩余步骤”计划
    - 产出结构与 run_planning_phase 一致的 PlanPhaseResult
    """
    yield sse_json({"delta": f"{STREAM_TAG_PLAN} 重新规划…\n"})

    obs_text = "\n".join(f"- {_truncate_observation(o)}" for o in observations[-5:]) or "(无)"
    source_failure_summary = summarize_recent_source_failures_for_prompt(
        observations=list(observations or []),
        error=str(error or ""),
        failure_signatures=failure_signatures if isinstance(failure_signatures, dict) else None,
    )
    agent_state_like = {
        "step_feedback_history": (failure_signatures or {}).get("step_feedback_history")
        if isinstance(failure_signatures, dict)
        else None,
        "pending_retry_requirements": (failure_signatures or {}).get("pending_retry_requirements")
        if isinstance(failure_signatures, dict)
        else None,
    }
    recent_step_feedback = summarize_recent_step_feedback_for_prompt(agent_state_like)
    retry_requirements = summarize_retry_requirements_for_prompt(agent_state_like)
    failure_guidance = summarize_failure_guidance_for_prompt(agent_state_like)
    real_sample_status = _summarize_real_sample_status(context=context)
    sections = apply_context_budgets(
        {
            "observations": obs_text,
            "recent_source_failures": source_failure_summary,
            "recent_step_feedback": recent_step_feedback,
            "retry_requirements": retry_requirements,
            "failure_guidance": failure_guidance,
            "tools": tools_hint,
            "skills": skills_hint,
            "solutions": solutions_hint,
            "memories": memories_hint,
            "graph": graph_hint,
        }
    )
    replan_prompt = AGENT_REPLAN_PROMPT_TEMPLATE.format(
        message=message,
        workdir=workdir,
        agent_workspace=AGENT_EXPERIMENT_DIR_REL,
        plan=json.dumps(plan_titles, ensure_ascii=False),
        done_steps=json.dumps(done_steps, ensure_ascii=False),
        error=str(error or ""),
        observations=str(sections.get("observations") or ""),
        recent_source_failures=str(sections.get("recent_source_failures") or ""),
        real_sample_status=str(real_sample_status or ""),
        recent_step_feedback=str(sections.get("recent_step_feedback") or ""),
        retry_requirements=str(sections.get("retry_requirements") or ""),
        failure_guidance=str(sections.get("failure_guidance") or ""),
        tools=str(sections.get("tools") or ""),
        skills=str(sections.get("skills") or ""),
        solutions=str(sections.get("solutions") or ""),
        memories=str(sections.get("memories") or ""),
        graph=str(sections.get("graph") or ""),
        action_types_line=action_types_line(),
    )
    task_grounding_text = summarize_task_grounding_for_prompt(message)
    if task_grounding_text and task_grounding_text != "(无)":
        replan_prompt += f"\n原任务不可变约束（必须继承，不可改题）：\n{task_grounding_text}\n"

    text, err, llm_id = call_llm_for_text_with_id(
        create_llm_call,
        prompt=replan_prompt,
        task_id=int(task_id),
        run_id=int(run_id),
        model=model,
        parameters=parameters,
        variables={"source": "agent_replan"},
    )
    if err or not text:
        raise PlanPhaseFailure(
            reason=f"replan_llm_failed:{err or 'empty_response'}",
            public_message=f"{ERROR_MESSAGE_LLM_CALL_FAILED}:{err or 'empty_response'}",
        )

    plan_text = str(text or "")
    if not _is_plain_json_object_text(plan_text):
        _safe_write_debug(
            int(task_id),
            int(run_id),
            message="agent.replan.non_plain_json_response",
            data={"llm_id": llm_id, "head": plan_text[:240]},
            level="warning",
        )
        reprompt = (
            replan_prompt
            + "\n\n补充约束：你上一轮输出不是纯 JSON 对象（可能包含代码块或说明文字）。"
            "请只输出一个 JSON 对象，不要 markdown 代码块、不要额外解释。"
        )
        retry_params = dict(parameters or {})
        retry_params["temperature"] = 0
        retry_text, retry_err, retry_llm_id = call_llm_for_text_with_id(
            create_llm_call,
            prompt=reprompt,
            task_id=int(task_id),
            run_id=int(run_id),
            model=model,
            parameters=retry_params,
            variables={"source": "agent_replan_plain_json_retry"},
        )
        if not retry_err and retry_text:
            plan_text = str(retry_text)
            if retry_llm_id is not None:
                llm_id = retry_llm_id

    plan = _extract_json_object(plan_text)
    if not plan:
        raise PlanPhaseFailure(reason="replan_invalid_json", public_message="重新规划输出不是有效 JSON")

    plan_titles_new, plan_briefs, plan_allows, plan_artifacts_new, plan_error = _normalize_plan_titles(
        plan, max_steps=max_steps
    )
    if plan_error or not plan_titles_new or not plan_allows:
        raise PlanPhaseFailure(
            reason=f"replan_invalid:{plan_error or 'empty_plan'}",
            public_message=f"重新规划输出不合法: {plan_error or 'empty_plan'}",
        )

    output_dir_rel = _extract_output_dir_from_message(message=message, workdir=workdir)
    if output_dir_rel:
        plan_titles_new, plan_artifacts_new, changed = _apply_output_dir_hint_to_plan(
            output_dir_rel=output_dir_rel,
            titles=plan_titles_new,
            artifacts=plan_artifacts_new,
        )
        if changed:
            yield sse_json({"delta": f"{STREAM_TAG_PLAN} 已应用输出目录：{output_dir_rel}\n"})
            _safe_write_debug(
                int(task_id),
                int(run_id),
                message="agent.replan.output_dir_applied",
                data={"output_dir_rel": output_dir_rel, "artifacts": list(plan_artifacts_new or [])},
                level="info",
            )

    # 输出步骤必须是最后一步且仅出现一次
    if plan_allows and ACTION_TYPE_TASK_OUTPUT not in set(plan_allows[-1] or []):
        raise PlanPhaseFailure(
            reason="replan_invalid:output_not_last",
            public_message="重新规划输出不合法: 输出步骤必须是最后一步",
        )
    if any(ACTION_TYPE_TASK_OUTPUT in set(a or []) for a in plan_allows[:-1]):
        raise PlanPhaseFailure(
            reason="replan_invalid:task_output_not_last",
            public_message="重新规划输出不合法: task_output 只能出现在最后一步",
        )

    real_sample_available = _has_real_sample(context=context)
    grounding_error = _validate_structured_artifact_sample_contract(
        titles=plan_titles_new,
        allows=plan_allows,
        artifacts=list(plan_artifacts or []) + list(plan_artifacts_new or []),
        real_sample_available=real_sample_available,
        message=message,
    )
    source_discovery_error = _validate_source_discovery_contract(
        titles=plan_titles_new,
        allows=plan_allows,
        message=message,
        error=error,
        observations=observations,
        context=context,
    )
    task_grounding_error = detect_task_grounding_drift(
        message=message,
        plan_titles=plan_titles_new,
        plan_briefs=plan_briefs,
    )
    contract_error = grounding_error or source_discovery_error or task_grounding_error
    if contract_error:
        _safe_write_debug(
            int(task_id),
            int(run_id),
            message="agent.replan.grounding_invalid",
            data={
                "error": contract_error,
                "sample_available": bool(real_sample_available),
                "plan": list(plan_titles_new or []),
                "has_grounded_external_source": _has_grounded_external_source(
                    message=message,
                    observations=observations,
                    context=context,
                ),
                "task_grounding_error": task_grounding_error,
            },
            level="warning",
        )
        retry_constraints = []
        if grounding_error:
            retry_constraints.extend(
                [
                    "若当前没有真实样本，先安排获取样本步骤。",
                    "获取样本前最多只允许一个抓取/读取脚本 file_write；不要先写解析脚本、校验脚本或结果文件。",
                ]
            )
        if source_discovery_error:
            retry_constraints.extend(
                [
                    "若当前没有可用具体 URL/host 依据，首个抓取步骤不能直接是 http_request。",
                    "应先安排来源发现步骤（如 tool_call:web_fetch），或先拿到真实 URL 再做 http_request。",
                ]
            )
        if task_grounding_error:
            retry_constraints.extend(
                [
                    "可以更换来源、搜索词和实现路径，但不得改写原任务的单位、时间范围或输出格式。",
                    "若用户明确要求元/克、最近三个月、CSV 等口径，剩余计划和搜索步骤必须继续继承这些约束。",
                ]
            )
        retry_constraints.append("只输出剩余步骤 JSON。")
        numbered_retry_constraints = "".join(
            f"{idx}) {item}\n" for idx, item in enumerate(retry_constraints, start=1)
        )
        reprompt = (
            replan_prompt
            + "\n\n补充约束：你上一轮剩余计划不合法，因为 "
            + contract_error
            + "\n请重写剩余计划，并满足：\n"
            + numbered_retry_constraints
            + f"上一轮输出（供参考）：{json.dumps(plan, ensure_ascii=False)}\n"
        )
        retry_params = dict(parameters or {})
        retry_params["temperature"] = 0
        retry_text, retry_err, retry_llm_id = call_llm_for_text_with_id(
            create_llm_call,
            prompt=reprompt,
            task_id=int(task_id),
            run_id=int(run_id),
            model=model,
            parameters=retry_params,
            variables={"source": "agent_replan_grounding_retry"},
        )
        retry_plan = _extract_json_object(retry_text or "")
        if retry_err or not retry_plan:
            raise PlanPhaseFailure(
                reason=f"replan_grounding_retry_failed:{retry_err or 'invalid_json'}",
                public_message=f"重新规划输出不合法: {contract_error}",
            )
        plan = retry_plan
        if retry_llm_id is not None:
            llm_id = retry_llm_id
        plan_titles_new, plan_briefs, plan_allows, plan_artifacts_new, plan_error = _normalize_plan_titles(
            plan, max_steps=max_steps
        )
        if plan_error or not plan_titles_new or not plan_allows:
            raise PlanPhaseFailure(
                reason=f"replan_invalid:{plan_error or 'empty_plan'}",
                public_message=f"重新规划输出不合法: {plan_error or 'empty_plan'}",
            )
        if output_dir_rel:
            plan_titles_new, plan_artifacts_new, _changed = _apply_output_dir_hint_to_plan(
                output_dir_rel=output_dir_rel,
                titles=plan_titles_new,
                artifacts=plan_artifacts_new,
            )
        if plan_allows and ACTION_TYPE_TASK_OUTPUT not in set(plan_allows[-1] or []):
            raise PlanPhaseFailure(
                reason="replan_invalid:output_not_last",
                public_message="重新规划输出不合法: 输出步骤必须是最后一步",
            )
        if any(ACTION_TYPE_TASK_OUTPUT in set(a or []) for a in plan_allows[:-1]):
            raise PlanPhaseFailure(
                reason="replan_invalid:task_output_not_last",
                public_message="重新规划输出不合法: task_output 只能出现在最后一步",
            )
        grounding_error = _validate_structured_artifact_sample_contract(
            titles=plan_titles_new,
            allows=plan_allows,
            artifacts=list(plan_artifacts or []) + list(plan_artifacts_new or []),
            real_sample_available=real_sample_available,
        )
        source_discovery_error = _validate_source_discovery_contract(
            titles=plan_titles_new,
            allows=plan_allows,
            message=message,
            error=error,
            observations=observations,
            context=context,
        )
        task_grounding_error = detect_task_grounding_drift(
            message=message,
            plan_titles=plan_titles_new,
            plan_briefs=plan_briefs,
        )
        contract_error = grounding_error or source_discovery_error or task_grounding_error
        if contract_error:
            raise PlanPhaseFailure(
                reason="replan_invalid_grounding",
                public_message=f"重新规划输出不合法: {contract_error}",
            )

    # 合并 artifacts（保留已有声明）
    # 合并 artifacts（保留已有声明）
    merged_artifacts = []
    for item in (plan_artifacts or []) + (plan_artifacts_new or []):
        rel = str(item or "").strip()
        if rel and rel not in merged_artifacts:
            merged_artifacts.append(rel)

    # 重新规划时同样需要保证 artifacts <-> file_write 对齐
    if merged_artifacts:
        (
            plan_titles_new,
            plan_briefs,
            plan_allows,
            merged_artifacts,
            repair_err,
            patched_count,
        ) = repair_plan_artifacts_with_file_write_steps(
            titles=plan_titles_new,
            briefs=plan_briefs,
            allows=plan_allows,
            artifacts=merged_artifacts,
            max_steps=max_steps,
        )
        if repair_err:
            raise PlanPhaseFailure(
                reason=f"replan_repair_failed:{repair_err}",
                public_message=(
                    "重新规划输出不合法: artifacts 数量大于 file_write 步骤数，"
                    f"artifacts={len(merged_artifacts)}; {repair_err}"
                ),
            )
        if patched_count:
            yield sse_json({"delta": f"{STREAM_TAG_PLAN} 自动修复：补齐 {patched_count} 个写文件步骤\n"})
        _safe_write_debug(
            int(task_id),
            int(run_id),
            message="agent.replan.repair",
            data={
                "artifacts": list(merged_artifacts or []),
                "file_write_steps": int(
                    sum(1 for allow in plan_allows if ACTION_TYPE_FILE_WRITE in set(allow or []))
                ),
                "patched_count": int(patched_count or 0),
            },
            level="info",
        )
        (
            plan_titles_new,
            plan_briefs,
            plan_allows,
            removed_count,
        ) = drop_non_artifact_file_write_steps(
            titles=plan_titles_new,
            briefs=plan_briefs,
            allows=plan_allows,
            artifacts=merged_artifacts,
        )
        if removed_count:
            yield sse_json({"delta": f"{STREAM_TAG_PLAN} 已移除 {removed_count} 个无效写文件步骤\n"})
            _safe_write_debug(
                int(task_id),
                int(run_id),
                message="agent.replan.drop_non_artifact_file_write",
                data={"removed_count": int(removed_count)},
                level="info",
            )

        (
            plan_titles_new,
            plan_briefs,
            plan_allows,
            moved_count,
        ) = reorder_script_file_writes_before_exec_steps(
            titles=plan_titles_new,
            briefs=plan_briefs,
            allows=plan_allows,
        )
        if moved_count:
            yield sse_json({"delta": f"{STREAM_TAG_PLAN} 自动修复：前置 {moved_count} 个脚本写入步骤（避免先执行后写文件）\n"})
            _safe_write_debug(
                int(task_id),
                int(run_id),
                message="agent.replan.reorder_script_file_write",
                data={"moved_count": int(moved_count or 0)},
                level="info",
            )

        post_repair_grounding_error = _validate_structured_artifact_sample_contract(
            titles=plan_titles_new,
            allows=plan_allows,
            artifacts=list(merged_artifacts or []),
            real_sample_available=real_sample_available,
            message=message,
        )
        post_repair_source_discovery_error = _validate_source_discovery_contract(
            titles=plan_titles_new,
            allows=plan_allows,
            message=message,
            error=error,
            observations=observations,
            context=context,
        )
        post_repair_task_grounding_error = detect_task_grounding_drift(
            message=message,
            plan_titles=plan_titles_new,
            plan_briefs=plan_briefs,
        )
        post_repair_contract_error = (
            post_repair_grounding_error
            or post_repair_source_discovery_error
            or post_repair_task_grounding_error
        )
        if post_repair_contract_error:
            raise PlanPhaseFailure(
                reason="replan_invalid_post_repair_contract",
                public_message=f"重新规划输出不合法: {post_repair_contract_error}",
            )

    plan_items = []
    for idx, brief in enumerate(plan_briefs or [], start=1):
        text = sanitize_plan_brief(
            str(brief or "").strip(),
            fallback_title=plan_titles_new[idx - 1],
        )
        plan_items.append({"id": idx, "brief": text, "status": "pending"})

    return PlanPhaseResult(
        plan_titles=plan_titles_new,
        plan_briefs=plan_briefs,
        plan_allows=plan_allows,
        plan_artifacts=merged_artifacts,
        plan_items=plan_items,
        plan_llm_id=llm_id,
    )


def run_planning_phase(
    *,
    task_id: int,
    run_id: int,
    message: str,
    workdir: str,
    model: str,
    parameters: dict,
    max_steps: int,
    tools_hint: str,
    skills_hint: str,
    solutions_hint: str,
    memories_hint: str,
    graph_hint: str,
) -> Generator[str, None, PlanPhaseResult]:
    """
    规划阶段（plan）：
    - 调用 LLM 生成 plan JSON
    - 心跳（避免 UI 误判卡死）
    - 解析与校验 allow/artifacts
    - artifacts 与 file_write 步骤不匹配时做自动修复（必要时触发一次重规划）
    - 产出 UI 计划栏 items（brief + status）

    产出：
    - yield SSE 字符串（通过 sse_json）
    - return PlanPhaseResult（通过 yield from 获取）
    """
    yield sse_json({"delta": f"{STREAM_TAG_PLAN} 正在规划…\n"})

    output_dir_rel = _extract_output_dir_from_message(message=message, workdir=workdir)
    sections = apply_context_budgets(
        {
            "tools": tools_hint,
            "skills": skills_hint,
            "solutions": solutions_hint,
            "memories": memories_hint,
            "graph": graph_hint,
        }
    )
    plan_prompt = AGENT_PLAN_PROMPT_TEMPLATE.format(
        message=message,
        max_steps=max_steps,
        agent_workspace=AGENT_EXPERIMENT_DIR_REL,
        tools=str(sections.get("tools") or ""),
        skills=str(sections.get("skills") or ""),
        solutions=str(sections.get("solutions") or ""),
        memories=str(sections.get("memories") or ""),
        graph=str(sections.get("graph") or ""),
        action_types_line=action_types_line(),
    )
    if output_dir_rel:
        plan_prompt = (
            f"用户指定输出目录（相对 workdir）：{output_dir_rel}\n"
            "请确保所有需要落盘的文件（artifacts/file_write）都写入该目录下。\n\n"
            + plan_prompt
        )

    task_grounding_text = summarize_task_grounding_for_prompt(message)
    if task_grounding_text and task_grounding_text != "(无)":
        plan_prompt += f"\n原任务不可变约束（规划时必须继承）：\n{task_grounding_text}\n"

    # 规划阶段可能耗时较长：放到后台线程执行，并周期性输出心跳，避免桌宠一直停在“正在规划”
    plan_queue: "queue.Queue[tuple[Optional[str], Optional[str], Optional[int]]]" = queue.Queue(maxsize=1)

    def _run_plan_in_thread():
        text, err, llm_id = call_llm_for_text_with_id(
            create_llm_call,
            prompt=plan_prompt,
            task_id=int(task_id),
            run_id=int(run_id),
            model=model,
            parameters=parameters,
            variables={"source": "agent_plan"},
        )
        try:
            plan_queue.put((text, err, llm_id), timeout=1)
        except Exception as exc:
            _safe_write_debug(
                int(task_id),
                int(run_id),
                message="agent.plan.queue_put_failed",
                data={"error": str(exc), "llm_id": llm_id},
                level="warning",
            )

    threading.Thread(target=_run_plan_in_thread, daemon=True).start()

    start_ts = time.time()
    plan_text: Optional[str] = None
    error_message: Optional[str] = None
    plan_llm_id: Optional[int] = None

    while True:
        try:
            plan_text, error_message, plan_llm_id = plan_queue.get(
                timeout=float(AGENT_PLAN_HEARTBEAT_INTERVAL_SECONDS)
            )
            break
        except queue.Empty:
            elapsed = int(time.time() - start_ts)
            if elapsed >= AGENT_PLAN_MAX_WAIT_SECONDS:
                raise PlanPhaseFailure(
                    reason=f"plan_timeout>{AGENT_PLAN_MAX_WAIT_SECONDS}s",
                    public_message=(
                        f"规划超时（>{AGENT_PLAN_MAX_WAIT_SECONDS}s）。请检查 LLM 配置（主面板 -> 设置）或网络/代理。"
                    ),
                )
            # 输出轻量心跳（点点点），避免刷屏
            yield sse_json({"delta": "…"})

    yield sse_json({"delta": f"\n{STREAM_TAG_PLAN} 完成，正在解析…\n"})

    if error_message or not plan_text:
        if error_message == ERROR_MESSAGE_LLM_API_KEY_MISSING:
            error_message = f"{ERROR_MESSAGE_LLM_API_KEY_MISSING}（请到主面板 -> 设置 配置）"
        raise PlanPhaseFailure(
            reason=f"plan_llm_failed:{error_message or 'empty_response'}",
            public_message=f"{ERROR_MESSAGE_LLM_CALL_FAILED}:{error_message or 'empty_response'}",
        )

    plan_text_value = str(plan_text or "")
    if not _is_plain_json_object_text(plan_text_value):
        _safe_write_debug(
            int(task_id),
            int(run_id),
            message="agent.plan.non_plain_json_response",
            data={"llm_id": plan_llm_id, "head": plan_text_value[:240]},
            level="warning",
        )
        reprompt = (
            plan_prompt
            + "\n\n补充约束：你上一轮输出不是纯 JSON 对象（可能包含代码块或说明文字）。"
            "请只输出一个 JSON 对象，不要 markdown 代码块、不要额外解释。"
        )
        retry_params = dict(parameters or {})
        retry_params["temperature"] = 0
        retry_text, retry_err, retry_llm_id = call_llm_for_text_with_id(
            create_llm_call,
            prompt=reprompt,
            task_id=int(task_id),
            run_id=int(run_id),
            model=model,
            parameters=retry_params,
            variables={"source": "agent_plan_plain_json_retry"},
        )
        if not retry_err and retry_text:
            plan_text_value = str(retry_text)
            if retry_llm_id is not None:
                plan_llm_id = retry_llm_id

    plan = _extract_json_object(plan_text_value)
    if not plan:
        raise PlanPhaseFailure(reason="plan_invalid_json", public_message="规划输出不是有效 JSON")

    plan_titles, plan_briefs, plan_allows, plan_artifacts, plan_error = _normalize_plan_titles(
        plan, max_steps=max_steps
    )
    if plan_error or not plan_titles or not plan_allows:
        raise PlanPhaseFailure(
            reason=f"plan_invalid:{plan_error or 'empty_plan'}",
            public_message=f"规划输出不合法: {plan_error or 'empty_plan'}",
        )

    # 用户指定“保存到某目录”时，对计划做一次确定性路径修正（避免 LLM 忽略目录导致落盘位置错误）。
    if output_dir_rel:
        plan_titles, plan_artifacts, changed = _apply_output_dir_hint_to_plan(
            output_dir_rel=output_dir_rel,
            titles=plan_titles,
            artifacts=plan_artifacts,
        )
        if changed:
            yield sse_json({"delta": f"{STREAM_TAG_PLAN} 已应用输出目录：{output_dir_rel}\n"})
            _safe_write_debug(
                int(task_id),
                int(run_id),
                message="agent.plan.output_dir_applied",
                data={"output_dir_rel": output_dir_rel, "artifacts": list(plan_artifacts or [])},
                level="info",
            )

    # 计划需要一个明确的“输出最终结果”步骤：用 allow=task_output 作为硬约束，而不是靠关键词猜测
    if not any(ACTION_TYPE_TASK_OUTPUT in allow for allow in plan_allows):
        raise PlanPhaseFailure(
            reason="plan_missing_task_output",
            public_message="规划输出不合法: 缺少 allow=task_output 的输出步骤",
        )

    # 输出步骤必须是“最后一步”（避免出现“先输出结果、后又继续执行”的反直觉流程）
    if plan_allows and ACTION_TYPE_TASK_OUTPUT not in set(plan_allows[-1] or []):
        yield sse_json({"delta": f"{STREAM_TAG_PLAN} 规划不合规，正在重新规划…\n"})
        reprompt = (
            plan_prompt
            + "\n\n补充约束：allow=task_output 的输出步骤必须是最后一步（最后一个 plan item）。"
            "请在不超过 max_steps 的前提下重写计划，并确保最后一步为输出。\n"
            "只输出 JSON。\n"
            f"上一轮输出（供参考）：{json.dumps(plan, ensure_ascii=False)}\n"
        )
        fixed_params = dict(parameters or {})
        fixed_params["temperature"] = 0
        fixed_text, fixed_err, _fixed_llm_id = call_llm_for_text_with_id(
            create_llm_call,
            prompt=reprompt,
            task_id=int(task_id),
            run_id=int(run_id),
            model=model,
            parameters=fixed_params,
            variables={"source": "agent_plan_fix_output_last"},
        )
        if fixed_err or not fixed_text:
            raise PlanPhaseFailure(
                reason=f"plan_invalid:output_not_last:{fixed_err or 'empty_response'}",
                public_message="规划输出不合法: 输出步骤必须是最后一步",
            )
        fixed_plan = _extract_json_object(fixed_text)
        if not fixed_plan:
            raise PlanPhaseFailure(reason="plan_invalid_json", public_message="规划输出不是有效 JSON")
        plan_titles, plan_briefs, plan_allows, plan_artifacts, plan_error = _normalize_plan_titles(
            fixed_plan, max_steps=max_steps
        )
        if plan_error or not plan_titles or not plan_allows:
            raise PlanPhaseFailure(
                reason=f"plan_invalid:{plan_error or 'empty_plan'}",
                public_message=f"规划输出不合法: {plan_error or 'empty_plan'}",
            )

    # 仅允许最后一步包含 task_output（避免出现多个输出步骤）
    if any(ACTION_TYPE_TASK_OUTPUT in set(a or []) for a in plan_allows[:-1]):
        raise PlanPhaseFailure(
            reason="plan_invalid:task_output_not_last",
            public_message="规划输出不合法: task_output 只能出现在最后一步",
        )

    # 若声明了 artifacts：需要确保有足够的 file_write 步骤覆盖写文件动作
    if plan_artifacts:
        file_steps = sum(1 for allow in plan_allows if ACTION_TYPE_FILE_WRITE in set(allow or []))
        if file_steps < len(plan_artifacts):
            (
                plan_titles,
                plan_briefs,
                plan_allows,
                plan_artifacts,
                repair_err,
                patched_count,
            ) = repair_plan_artifacts_with_file_write_steps(
                titles=plan_titles,
                briefs=plan_briefs,
                allows=plan_allows,
                artifacts=plan_artifacts,
                max_steps=max_steps,
            )

            if repair_err:
                # 若无法本地修复（通常是 plan 已达 max_steps），尝试一次“自我修复式重规划”，避免直接失败
                yield sse_json({"delta": f"{STREAM_TAG_PLAN} 规划不合规，正在重新规划…\n"})
                reprompt = (
                    plan_prompt
                    + "\n\n补充约束：你上一轮规划不合法，因为 artifacts 数量大于 file_write 步骤数。"
                    "请在不超过 max_steps 的前提下重写计划：\n"
                    "1) 如果 artifacts 有 N 个文件，则 plan 中至少需要 N 个 allow 包含 file_write 的步骤（每个文件一个步骤）。\n"
                    "2) 每个写文件步骤 title 必须包含 `file_write:相对路径`。\n"
                    "3) 必须包含 allow=task_output 的输出步骤。\n"
                    "只输出 JSON。\n"
                    f"上一轮输出（供参考）：{json.dumps(plan, ensure_ascii=False)}\n"
                )
                fixed_params = dict(parameters or {})
                fixed_params["temperature"] = 0
                fixed_text, fixed_err, _fixed_llm_id = call_llm_for_text_with_id(
                    create_llm_call,
                    prompt=reprompt,
                    task_id=int(task_id),
                    run_id=int(run_id),
                    model=model,
                    parameters=fixed_params,
                    variables={"source": "agent_plan_repair"},
                )

                fixed_plan = _extract_json_object(fixed_text or "")
                if fixed_err or not fixed_plan:
                    raise PlanPhaseFailure(
                        reason=f"plan_repair_failed:{fixed_err or 'invalid_json'}",
                        public_message=(
                            "规划输出不合法: artifacts 数量大于 file_write 步骤数，"
                            f"artifacts={len(plan_artifacts)} file_write_steps={file_steps}; {repair_err}"
                        ),
                    )

                (
                    plan_titles,
                    plan_briefs,
                    plan_allows,
                    plan_artifacts,
                    plan_error,
                ) = _normalize_plan_titles(fixed_plan, max_steps=max_steps)
                if plan_error or not plan_titles or not plan_allows:
                    raise PlanPhaseFailure(
                        reason=f"plan_invalid:{plan_error or 'empty_plan'}",
                        public_message=f"规划输出不合法: {plan_error or 'empty_plan'}",
                    )
                if not any(ACTION_TYPE_TASK_OUTPUT in allow for allow in plan_allows):
                    raise PlanPhaseFailure(
                        reason="plan_missing_task_output",
                        public_message="规划输出不合法: 缺少 allow=task_output 的输出步骤",
                    )

                (
                    plan_titles,
                    plan_briefs,
                    plan_allows,
                    plan_artifacts,
                    repair_err,
                    patched_count,
                ) = repair_plan_artifacts_with_file_write_steps(
                    titles=plan_titles,
                    briefs=plan_briefs,
                    allows=plan_allows,
                    artifacts=plan_artifacts,
                    max_steps=max_steps,
                )
                if repair_err:
                    raise PlanPhaseFailure(
                        reason=f"plan_repair_failed:{repair_err}",
                        public_message=(
                            "规划输出不合法: artifacts 数量大于 file_write 步骤数，"
                            f"artifacts={len(plan_artifacts)} file_write_steps={file_steps}; {repair_err}"
                        ),
                    )

            if patched_count:
                yield sse_json({"delta": f"{STREAM_TAG_PLAN} 自动修复：补齐 {patched_count} 个写文件步骤\n"})
            _safe_write_debug(
                int(task_id),
                int(run_id),
                message="agent.plan.repair",
                data={
                    "artifacts": list(plan_artifacts or []),
                    "file_write_steps": int(
                        sum(1 for allow in plan_allows if ACTION_TYPE_FILE_WRITE in set(allow or []))
                    ),
                    "patched_file_write_steps": int(patched_count or 0),
                    "error": repair_err,
                },
                level="info" if not repair_err else "warning",
            )

            (
                plan_titles,
                plan_briefs,
                plan_allows,
                removed_count,
            ) = drop_non_artifact_file_write_steps(
                titles=plan_titles,
                briefs=plan_briefs,
                allows=plan_allows,
                artifacts=plan_artifacts,
            )
            if removed_count:
                yield sse_json({"delta": f"{STREAM_TAG_PLAN} 已移除 {removed_count} 个无效写文件步骤\n"})
                _safe_write_debug(
                    int(task_id),
                    int(run_id),
                    message="agent.plan.drop_non_artifact_file_write",
                    data={"removed_count": int(removed_count)},
                    level="info",
                )

            (
                plan_titles,
                plan_briefs,
                plan_allows,
                moved_count,
            ) = reorder_script_file_writes_before_exec_steps(
                titles=plan_titles,
                briefs=plan_briefs,
                allows=plan_allows,
            )
            if moved_count:
                yield sse_json({"delta": f"{STREAM_TAG_PLAN} 自动修复：前置 {moved_count} 个脚本写入步骤（避免先执行后写文件）\n"})
                _safe_write_debug(
                    int(task_id),
                    int(run_id),
                    message="agent.plan.reorder_script_file_write",
                    data={"moved_count": int(moved_count or 0)},
                    level="info",
                )

    # 生成 UI plan items（用于桌宠左侧计划栏）
    plan_items: List[dict] = []
    for i, title in enumerate(plan_titles, start=1):
        raw_brief = ""
        if isinstance(plan_briefs, list) and i - 1 < len(plan_briefs):
            raw_brief = str(plan_briefs[i - 1] or "").strip()
        brief = sanitize_plan_brief(raw_brief, fallback_title=title)
        plan_items.append({"id": i, "brief": brief, "status": "pending"})

    yield sse_json({"type": "plan", "task_id": task_id, "items": plan_items})

    return PlanPhaseResult(
        plan_titles=list(plan_titles),
        plan_briefs=list(plan_briefs or []),
        plan_allows=list(plan_allows or []),
        plan_artifacts=list(plan_artifacts or []),
        plan_items=plan_items,
        plan_llm_id=plan_llm_id,
    )
