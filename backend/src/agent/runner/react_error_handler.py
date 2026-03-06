# -*- coding: utf-8 -*-
"""
ReAct 循环错误处理模块。

提供动作验证失败、allow 约束失败、步骤执行失败的统一处理逻辑。
"""

import json
import logging
import os
import re
from typing import Callable, Dict, Generator, List, Optional, Tuple

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.support import _truncate_observation, extract_file_write_target_path, looks_like_file_path
from backend.src.agent.runner.react_error_policy import (
    should_fail_fast_on_step_error,
    should_force_replan_on_action_error,
)
from backend.src.agent.runner.react_state_manager import (
    ReplanContext,
    prepare_replan_context,
    persist_loop_state,
    resolve_executor,
)
from backend.src.agent.runner.plan_events import sse_plan, sse_plan_delta
from backend.src.agent.runner.react_step_executor import record_invalid_action_step
from backend.src.agent.runner.attempt_controller import (
    build_unreachable_proof_event,
    classify_failure_class,
    rotate_strategy,
    strategy_meta,
)
from backend.src.agent.runner.step_feedback import build_step_feedback, register_step_feedback
from backend.src.constants import (
    ACTION_TYPE_FILE_LIST,
    ACTION_TYPE_FILE_READ,
    ACTION_TYPE_FILE_WRITE,
    ACTION_TYPE_HTTP_REQUEST,
    ACTION_TYPE_SHELL_COMMAND,
    ACTION_TYPE_TOOL_CALL,
    AGENT_REACT_REPEAT_FAILURE_MAX,
    RUN_STATUS_FAILED,
    STREAM_TAG_EXEC,
    STREAM_TAG_FAIL,
)
from backend.src.common.utils import coerce_int
from backend.src.common.task_error_codes import extract_task_error_code
from backend.src.services.llm.llm_client import classify_llm_error_text, sse_json

logger = logging.getLogger(__name__)


def _derive_step_failure_repair_hint(*, error_code: str, step_error: str) -> str:
    code = str(error_code or "").strip().lower()
    text = str(step_error or "").strip()
    lowered = text.lower()

    if code in {"script_missing", "script_dependency_unbound"}:
        path_match = re.search(r"脚本(?:不存在|依赖未绑定):\s*([^\s（。；]+)", text)
        if path_match:
            path = str(path_match.group(1) or "").strip()
            if path:
                return f"必须先 file_write 创建并落盘脚本 `{path}`，再执行 shell_command。"
        return "必须先 file_write 创建并落盘缺失脚本，再执行 shell_command。"

    if code == "file_write_path_conflict":
        return "file_write 的 title 路径必须与 payload.path 完全一致；不要把脚本路径和结果文件路径写在同一步。"

    if code == "file_write_content_path_mismatch":
        return "脚本代码必须写入脚本文件（如 .py）；CSV/JSON 等结果文件只能写入最终产物内容。"
    if code == "csv_artifact_quality_failed":
        return "CSV 结果文件必须包含真实数据行并通过最小质量校验；不要生成只有表头或占位值的伪结果。"

    file_not_found = re.search(
        r"FileNotFoundError:\s*(?:\[Errno\s+\d+\][^:]*:\s*)?'([^']+)'",
        text,
        flags=re.IGNORECASE,
    )
    if file_not_found:
        path = str(file_not_found.group(1) or "").strip()
        if path:
            return f"依赖输入文件缺失：先生成 `{path}`，或改用已存在文件参数后再执行。"
        return "依赖输入文件缺失：请先生成输入文件后再执行。"

    if "specify exactly one of --in-json or --in-csv" in lowered:
        return "脚本参数为互斥输入：仅保留 --in-json 或 --in-csv 之一，不能同时为空或同时提供。"

    return ""


def _extract_missing_args_count(step_error: str) -> int:
    text = str(step_error or "")
    patterns = [
        r"脚本参数缺失（([^）]+)）",
        r"Missing required args\s*\(([^)]+)\)",
        r"missing args?\s*\(([^)]+)\)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        body = str(match.group(1) or "").strip()
        if not body:
            continue
        parts = [item.strip() for item in re.split(r"[，,、]", body) if str(item).strip()]
        if parts:
            return len(parts)
    return 0


def _update_execution_constraints(
    *,
    agent_state: Dict,
    step_order: int,
    error_code: str,
    step_error: str,
) -> None:
    if not isinstance(agent_state, dict):
        return
    constraints = agent_state.get("execution_constraints")
    if not isinstance(constraints, dict):
        constraints = {}
        agent_state["execution_constraints"] = constraints

    code = str(error_code or "").strip().lower()
    text = str(step_error or "").strip().lower()
    current_step = coerce_int(step_order, default=0)
    if current_step <= 0:
        return

    if code == "script_args_missing":
        missing_count = _extract_missing_args_count(step_error)
        if missing_count >= 3:
            prev = coerce_int(constraints.get("prefer_low_param_scripts_until_step"), default=0)
            constraints["prefer_low_param_scripts_until_step"] = max(prev, int(current_step + 6))
            constraints["prefer_low_param_scripts_reason"] = (
                f"script_args_missing_many:{int(missing_count)}"
            )

    if code in {"script_missing", "script_dependency_unbound"}:
        prev = coerce_int(constraints.get("require_script_materialization_until_step"), default=0)
        constraints["require_script_materialization_until_step"] = max(prev, int(current_step + 4))
        constraints["require_script_materialization_reason"] = code

    if code in {"file_write_path_conflict", "file_write_content_path_mismatch"}:
        prev = coerce_int(constraints.get("require_grounded_script_file_write_until_step"), default=0)
        constraints["require_grounded_script_file_write_until_step"] = max(prev, int(current_step + 4))
        constraints["require_grounded_script_file_write_reason"] = code

    if "exactly one of --in-json or --in-csv" in text or "provide exactly one of --text or --base64" in text:
        prev = coerce_int(constraints.get("enforce_exclusive_input_args_until_step"), default=0)
        constraints["enforce_exclusive_input_args_until_step"] = max(prev, int(current_step + 4))
        constraints["enforce_exclusive_input_args_reason"] = "exclusive_input_args"


def _update_action_invalid_constraints(
    *,
    agent_state: Dict,
    step_order: int,
    step_title: str,
    action_validate_error: str,
    failure_hit_count: int,
) -> str:
    if not isinstance(agent_state, dict):
        return ""
    constraints = agent_state.get("execution_constraints")
    if not isinstance(constraints, dict):
        constraints = {}
        agent_state["execution_constraints"] = constraints

    current_step = coerce_int(step_order, default=0)
    if current_step <= 0:
        return ""

    error_text = str(action_validate_error or "").strip()
    error_code = extract_task_error_code(error_text)
    error_kind = classify_llm_error_text(error_text)

    if error_code == "file_write_path_conflict":
        return "file_write 的 title 路径必须与 payload.path 一致；同一步只写一个目标文件。"

    if error_code == "file_write_content_path_mismatch":
        return "当前 file_write 存在路径与内容类型错配：脚本写到 .py，结果写到 .csv/.json，不要混在同一步。"
    title_text = str(step_title or "").strip().lower()
    failure_count = max(0, coerce_int(failure_hit_count, default=0))

    if error_kind not in {"rate_limit", "transient"}:
        if "json" in error_text.lower():
            prev = coerce_int(constraints.get("prefer_compact_action_prompt_until_step"), default=0)
            constraints["prefer_compact_action_prompt_until_step"] = max(prev, int(current_step + 3))
            constraints["prefer_compact_action_reason"] = "action_json_invalid"
            return "动作输出需严格精简为单个 JSON；不要解释、不要代码块。"
        return ""

    prev = coerce_int(constraints.get("prefer_compact_action_prompt_until_step"), default=0)
    constraints["prefer_compact_action_prompt_until_step"] = max(prev, int(current_step + 4))
    constraints["prefer_compact_action_reason"] = "action_generation_transient_timeout"

    is_file_write_step = title_text.startswith(f"{ACTION_TYPE_FILE_WRITE}:") or title_text.startswith(
        f"{ACTION_TYPE_FILE_WRITE}："
    )
    if is_file_write_step:
        prev_grounded = coerce_int(constraints.get("require_grounded_script_file_write_until_step"), default=0)
        constraints["require_grounded_script_file_write_until_step"] = max(prev_grounded, int(current_step + 5))
        constraints["require_grounded_script_file_write_reason"] = "file_write_action_invalid_transient"
        prev_switch = coerce_int(constraints.get("prefer_action_path_switch_until_step"), default=0)
        constraints["prefer_action_path_switch_until_step"] = max(prev_switch, int(current_step + 4))
        constraints["prefer_action_path_switch_reason"] = "file_write_transient"
        if failure_count >= 2:
            constraints["prefer_action_path_switch_until_step"] = max(
                coerce_int(constraints.get("prefer_action_path_switch_until_step"), default=0),
                int(current_step + 6),
            )
            constraints["prefer_action_path_switch_reason"] = "file_write_transient_repeat"
            return (
                "file_write 连续超时：禁止输出骨架/TODO；若当前缺少真实样本，重规划时优先先获取样本，"
                "或切换到 http_request/tool_call/shell_command 等更轻量路径。"
            )
        return "file_write 超时：禁止输出骨架/TODO；若缺少真实样本，优先先获取样本或切换到更轻量路径。"

    if failure_count >= 2:
        prev_switch = coerce_int(constraints.get("prefer_action_path_switch_until_step"), default=0)
        constraints["prefer_action_path_switch_until_step"] = max(prev_switch, int(current_step + 4))
        constraints["prefer_action_path_switch_reason"] = "action_invalid_transient_repeat"
        return "动作生成连续超时：优先选择更轻量的动作路径（tool_call/shell_command），再回到当前步骤。"

    return "动作生成超时：请输出最小 JSON 并减少非必要字段。"


def _normalize_failure_signature(*, action_type: str, step_error: str) -> str:
    action = str(action_type or "").strip().lower() or "unknown_action"
    error_text = str(step_error or "").strip()
    error_code = extract_task_error_code(error_text)
    if error_code:
        return f"{action}|code:{error_code}"

    head = error_text.splitlines()[0].strip() if error_text else "unknown_error"
    lowered = head.lower()
    # 归一化路径/日期/数字，减少“同类错误因动态文本不同”导致的签名漂移。
    lowered = re.sub(r"[a-z]:\\\\[^\\s]+", "<path>", lowered)
    lowered = re.sub(r"\b\d{4}-\d{2}-\d{2}\b", "<date>", lowered)
    lowered = re.sub(r"\b\d+\b", "<n>", lowered)
    lowered = " ".join(lowered.split())
    if not lowered:
        lowered = "unknown_error"
    return f"{action}|msg:{lowered[:180]}"


def _record_failure_signature(*, agent_state: Dict, action_type: str, step_error: str) -> tuple[str, int]:
    signature = _normalize_failure_signature(action_type=action_type, step_error=step_error)
    stats = agent_state.get("failure_signatures") if isinstance(agent_state, dict) else None
    if not isinstance(stats, dict):
        stats = {}

    existing = stats.get(signature)
    if isinstance(existing, dict):
        count = coerce_int(existing.get("count"), default=0) + 1
    else:
        count = 1

    stats[signature] = {"count": coerce_int(count, default=1)}

    # 防止状态无限膨胀：仅保留最近 20 个错误签名。
    if len(stats) > 20:
        removable = [k for k in stats.keys() if k != signature]
        for key in removable[: max(0, len(stats) - 20)]:
            stats.pop(key, None)

    agent_state["failure_signatures"] = stats

    # 失败预算按“连续失败”统计，而不是按全局累计统计。
    # 否则会出现：前面某步偶发失败 + 后面不同步骤再失败 => 被误判为“连续失败”。
    streak_signature = str(agent_state.get("failure_streak_signature") or "")
    streak_count = coerce_int(agent_state.get("failure_streak_count"), default=0)
    if signature and signature == streak_signature:
        streak_count += 1
    else:
        streak_count = 1
    agent_state["failure_streak_signature"] = signature
    agent_state["failure_streak_count"] = int(max(1, streak_count))

    return signature, int(max(1, streak_count))


def clear_failure_streak(agent_state: Dict) -> None:
    """在一次成功执行后清空失败连击计数。"""
    if not isinstance(agent_state, dict):
        return
    agent_state.pop("failure_streak_signature", None)
    agent_state.pop("failure_streak_count", None)


def _resolve_action_invalid_repeat_limit() -> int:
    """
    action_invalid 专用失败预算（可单独配置）。

    优先级：
    1) AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX（推荐）
    2) AGENT_REACT_REPEAT_FAILURE_MAX（通用预算）
    """
    base_global_limit = coerce_int(AGENT_REACT_REPEAT_FAILURE_MAX, default=0)
    default_limit = 2
    if base_global_limit > 0:
        default_limit = min(int(default_limit), int(base_global_limit))

    raw = str(os.getenv("AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX") or "").strip()
    if raw:
        try:
            parsed = int(float(raw))
        except Exception:
            parsed = int(default_limit)
    else:
        parsed = int(default_limit)
    if parsed < 0:
        return 0
    return int(parsed)


def _extract_script_path_from_action_text(last_action_text: Optional[str]) -> str:
    text = str(last_action_text or "").strip()
    if not text:
        return ""

    def _normalize_candidate(value: object) -> str:
        candidate = str(value or "").strip().strip("`").strip().strip("\"'")
        if not candidate:
            return ""
        candidate = re.split(r"[；;，,。)\]]", candidate, maxsplit=1)[0]
        return str(candidate or "").strip()

    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            action = obj.get("action")
            if isinstance(action, dict):
                payload = action.get("payload")
                if isinstance(payload, dict):
                    script = _normalize_candidate(payload.get("script"))
                    if script:
                        return script
    except Exception:
        pass

    # 弱兜底：从原始 JSON 文本中提取 script 字段。
    match = re.search(r'"script"\s*:\s*"([^"]+)"', text)
    if not match:
        return ""
    raw = str(match.group(1) or "")
    try:
        decoded = json.loads(f"\"{raw}\"")
    except Exception:
        decoded = raw
    return _normalize_candidate(decoded)


def _extract_script_path_from_action_error(action_validate_error: str) -> str:
    text = str(action_validate_error or "").strip()
    if not text:
        return ""

    patterns = (
        r"shell_command\s*引用脚本不存在[:：]\s*([^\s（(；;]+)",
        r"脚本(?:不存在|依赖未绑定)[:：]\s*([^\s（(；;]+)",
        r"missing\s+script[:：]\s*([^\s（(；;]+)",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        value = str(match.group(1) or "").strip().strip("`").strip().strip("\"'")
        if value:
            return value
    return ""


def _is_file_write_step_title(title: str) -> bool:
    return bool(re.match(r"^file_write\s*[:：]", str(title or "").strip(), flags=re.IGNORECASE))


def _extract_file_write_path_from_action_text(last_action_text: Optional[str]) -> str:
    text = str(last_action_text or "").strip()
    if not text:
        return ""

    def _normalize_path(value: object) -> str:
        candidate = str(value or "").strip().strip("`").strip().strip("\"'")
        if not candidate:
            return ""
        candidate = re.split(r"[；;，,。)\]]", candidate, maxsplit=1)[0]
        return str(candidate or "").strip()

    try:
        obj = json.loads(text)
    except Exception:
        obj = None
    if isinstance(obj, dict):
        action = obj.get("action")
        if isinstance(action, dict):
            action_type = str(action.get("type") or "").strip().lower()
            payload = action.get("payload")
            if action_type == ACTION_TYPE_FILE_WRITE and isinstance(payload, dict):
                candidate = _normalize_path(payload.get("path"))
                if candidate:
                    return candidate

    match = re.search(r'"path"\s*:\s*"([^"]+)"', text)
    if not match:
        return ""
    raw = str(match.group(1) or "")
    try:
        decoded = json.loads(f'"{raw}"')
    except Exception:
        decoded = raw
    return _normalize_path(decoded)


def _resolve_file_write_target_path(*, title: str, last_action_text: Optional[str] = None) -> str:
    declared = str(extract_file_write_target_path(title) or "").strip()
    if declared and looks_like_file_path(declared):
        return declared
    candidate = _extract_file_write_path_from_action_text(last_action_text)
    if candidate and looks_like_file_path(candidate):
        return candidate
    return ""


def _is_blocking_artifact_materialization_step(
    *,
    action_type: str,
    title: str,
    last_action_text: Optional[str] = None,
) -> bool:
    normalized_action = str(action_type or "").strip().lower()
    if normalized_action != ACTION_TYPE_FILE_WRITE and not _is_file_write_step_title(title):
        return False
    target_path = _resolve_file_write_target_path(title=title, last_action_text=last_action_text)
    if target_path:
        return True
    return True


def _is_shell_command_artifact_step(*, action_type: str, step_detail: str) -> bool:
    normalized_action = str(action_type or "").strip().lower()
    if normalized_action != ACTION_TYPE_SHELL_COMMAND:
        return False
    detail_text = str(step_detail or "").strip()
    if not detail_text:
        return False
    try:
        detail_obj = json.loads(detail_text)
    except Exception:
        return False
    if not isinstance(detail_obj, dict):
        return False
    payload = detail_obj.get("payload")
    if not isinstance(payload, dict):
        return False
    expected_outputs = payload.get("expected_outputs")
    if isinstance(expected_outputs, list):
        for item in expected_outputs:
            if looks_like_file_path(str(item or "").strip()):
                return True
    return False


def _is_blocking_step_failure(
    *,
    action_type: str,
    title: str,
    error_code: str,
    step_detail: str = "",
) -> bool:
    structural_codes = {
        "script_args_missing",
        "script_arg_contract_mismatch",
        "missing_expected_artifact",
        "csv_artifact_quality_failed",
        "script_output_not_json",
        "script_missing",
        "script_dependency_unbound",
        "dependency_missing",
        "http_response_empty",
        "no_structured_data_extracted",
    }
    code = str(error_code or "").strip().lower()
    if code in structural_codes:
        return True
    if _is_blocking_artifact_materialization_step(
        action_type=action_type,
        title=title,
    ):
        return True
    return _is_shell_command_artifact_step(
        action_type=action_type,
        step_detail=step_detail,
    )


_SOURCE_RECOVERY_TITLE_KEYWORDS = (
    "抓取",
    "获取",
    "搜索",
    "查找",
    "读取",
    "列出",
    "拉取",
    "下载",
    "fetch",
    "search",
    "read",
    "list",
    "load",
    "crawl",
    "scrape",
    "query",
)


def _title_looks_like_source_recovery(title: str) -> bool:
    text = str(title or "").strip()
    if not text:
        return False
    lowered = text.lower()
    return any(keyword in text or keyword in lowered for keyword in _SOURCE_RECOVERY_TITLE_KEYWORDS)


def _is_source_recovery_step(*, title: str, allow: List[str]) -> bool:
    allow_set = {str(item or "").strip().lower() for item in (allow or []) if str(item or "").strip()}
    if ACTION_TYPE_HTTP_REQUEST in allow_set:
        return True
    if ACTION_TYPE_FILE_READ in allow_set or ACTION_TYPE_FILE_LIST in allow_set:
        return True
    if ACTION_TYPE_TOOL_CALL in allow_set and _title_looks_like_source_recovery(title):
        return True
    return False


def _is_source_grounding_blocked_after_failure(
    *,
    failure_class: str,
    current_idx: int,
    plan_struct: PlanStructure,
    context: Optional[Dict],
) -> bool:
    """
    当“取源失败”且当前没有任何真实样本时，后续只能继续执行新的取源步骤。

    否则若继续执行 llm/json/shell/file_write/task_output 等下游步骤，
    实际上只是把“上游无数据”的问题延后放大，形成伪进展。
    """
    if str(failure_class or "").strip().lower() != "source_unavailable":
        return False
    if isinstance(context, dict) and str(context.get("latest_parse_input_text") or "").strip():
        return False

    for next_idx in range(max(0, int(current_idx)) + 1, int(plan_struct.step_count)):
        step = plan_struct.get_step(next_idx)
        if step is None:
            continue
        status = str(step.status or "").strip().lower()
        if status in {"done", "skipped", "failed"}:
            continue
        if _is_source_recovery_step(title=str(step.title or ""), allow=list(step.allow or [])):
            return False
        return True
    return False


def _resolve_safe_missing_script_target(*, script_path: str, workdir: str) -> str:
    raw = str(script_path or "").strip()
    if not raw:
        return ""

    base_dir = os.path.abspath(str(workdir or os.getcwd()))
    project_root = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "..",
            "..",
        )
    )
    target = os.path.abspath(raw if os.path.isabs(raw) else os.path.join(base_dir, raw))
    if not str(target).lower().endswith(".py"):
        return ""

    allowed_roots = [base_dir, project_root]
    allowed = False
    for root in allowed_roots:
        try:
            common = os.path.commonpath([os.path.abspath(root), target])
        except Exception:
            continue
        if common == os.path.abspath(root):
            allowed = True
            break
    if not allowed:
        return ""
    return target


def _build_missing_script_fallback_source(script_name: str) -> str:
    safe_name = str(script_name or "missing_script.py").strip() or "missing_script.py"
    return "\n".join(
        [
            "#!/usr/bin/env python3",
            "\"\"\"Auto-generated fallback script for a missing planned script.\"\"\"",
            "from __future__ import annotations",
            "",
            "import argparse",
            "import csv",
            "import json",
            "import os",
            "import sys",
            "",
            "",
            "def _to_float(value):",
            "    text = str(value or \"\").strip().replace(\",\", \"\")",
            "    if not text:",
            "        return None",
            "    try:",
            "        return float(text)",
            "    except Exception:",
            "        return None",
            "",
            "",
            "def _collect_csv_metrics(path, encoding):",
            "    rows = []",
            "    with open(path, \"r\", encoding=encoding, errors=\"replace\", newline=\"\") as handle:",
            "        rows = list(csv.reader(handle))",
            "    data_rows = rows[1:] if len(rows) > 1 else []",
            "    numeric_rows = 0",
            "    for row in data_rows:",
            "        has_numeric = any(_to_float(cell) is not None for cell in row)",
            "        if has_numeric:",
            "            numeric_rows += 1",
            "    total_rows = len(data_rows)",
            "    ratio = float(numeric_rows) / float(total_rows) if total_rows > 0 else 0.0",
            "    return total_rows, numeric_rows, ratio",
            "",
            "",
            "def main():",
            "    parser = argparse.ArgumentParser(description=\"Auto fallback validator\")",
            "    parser.add_argument(\"--in-csv\", dest=\"in_csv\", default=\"\")",
            "    parser.add_argument(\"--csv\", dest=\"csv\", default=\"\")",
            "    parser.add_argument(\"--input\", dest=\"input_path\", default=\"\")",
            "    parser.add_argument(\"--in-json\", dest=\"in_json\", default=\"\")",
            "    parser.add_argument(\"--out-json\", dest=\"out_json\", default=\"\")",
            "    parser.add_argument(\"--output\", dest=\"output\", default=\"\")",
            "    parser.add_argument(\"--encoding\", dest=\"encoding\", default=\"utf-8\")",
            "    args, _unknown = parser.parse_known_args()",
            "",
            "    result = {",
            "        \"ok\": True,",
            "        \"source\": \"auto_generated_missing_script_fallback\",",
            f"        \"script\": {json.dumps(safe_name)},",
            "        \"rows\": 0,",
            "        \"numeric_rows\": 0,",
            "        \"numeric_ratio\": 0.0,",
            "    }",
            "",
            "    in_csv = args.in_csv or args.csv or args.input_path",
            "    in_json = args.in_json",
            "    if in_csv:",
            "        if not os.path.exists(in_csv):",
            "            result[\"ok\"] = False",
            "            result[\"error\"] = f\"input_csv_not_found:{in_csv}\"",
            "        else:",
            "            rows, numeric_rows, numeric_ratio = _collect_csv_metrics(in_csv, args.encoding)",
            "            result[\"rows\"] = int(rows)",
            "            result[\"numeric_rows\"] = int(numeric_rows)",
            "            result[\"numeric_ratio\"] = float(round(numeric_ratio, 6))",
            "            result[\"input\"] = str(in_csv)",
            "    elif in_json:",
            "        if not os.path.exists(in_json):",
            "            result[\"ok\"] = False",
            "            result[\"error\"] = f\"input_json_not_found:{in_json}\"",
            "        else:",
            "            with open(in_json, \"r\", encoding=\"utf-8\", errors=\"replace\") as handle:",
            "                obj = json.load(handle)",
            "            if isinstance(obj, list):",
            "                result[\"rows\"] = len(obj)",
            "            elif isinstance(obj, dict):",
            "                result[\"rows\"] = len(obj)",
            "            result[\"input\"] = str(in_json)",
            "    else:",
            "        result[\"ok\"] = False",
            "        result[\"error\"] = \"missing_input\"",
            "",
            "    out_json = args.out_json or (args.output if str(args.output).lower().endswith(\".json\") else \"\")",
            "    if out_json:",
            "        out_dir = os.path.dirname(os.path.abspath(out_json))",
            "        if out_dir:",
            "            os.makedirs(out_dir, exist_ok=True)",
            "        with open(out_json, \"w\", encoding=\"utf-8\", newline=\"\\n\") as handle:",
            "            json.dump(result, handle, ensure_ascii=False, indent=2)",
            "",
            "    print(json.dumps(result, ensure_ascii=False))",
            "    return 0 if bool(result.get(\"ok\")) else 1",
            "",
            "",
            "if __name__ == \"__main__\":",
            "    raise SystemExit(main())",
            "",
        ]
    )


def _auto_materialize_missing_script(
    *,
    action_validate_error: str,
    last_action_text: Optional[str],
    workdir: str,
) -> Tuple[bool, str, str]:
    path_from_error = _extract_script_path_from_action_error(action_validate_error)
    path_from_action = _extract_script_path_from_action_text(last_action_text)
    raw_script_path = str(path_from_error or path_from_action or "").strip()
    if not raw_script_path:
        return False, "", "script_path_unresolved"

    target = _resolve_safe_missing_script_target(script_path=raw_script_path, workdir=workdir)
    if not target:
        return False, "", "script_path_outside_workdir_or_non_python"

    if os.path.exists(target):
        return True, target, "already_exists"

    try:
        os.makedirs(os.path.dirname(target), exist_ok=True)
        script_source = _build_missing_script_fallback_source(os.path.basename(target))
        with open(target, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(script_source)
    except Exception as exc:
        return False, target, f"write_failed:{exc}"

    return True, target, "created"


def _bind_auto_materialized_script_path(context: Dict, script_path: str) -> None:
    if not isinstance(context, dict):
        return
    path_text = str(script_path or "").strip()
    if not path_text:
        return
    existing = context.get("shell_dependency_auto_bind_paths")
    bound: List[str] = []
    if isinstance(existing, list):
        for item in existing:
            value = str(item or "").strip()
            if value:
                bound.append(value)
    if path_text not in bound:
        bound.append(path_text)
    context["shell_dependency_auto_bind_paths"] = bound



def handle_action_invalid(
    *,
    task_id: int,
    run_id: int,
    step_order: int,
    idx: int,
    title: str,
    message: str,
    workdir: str,
    model: str,
    react_params: dict,
    tools_hint: str,
    skills_hint: str,
    memories_hint: str,
    graph_hint: str,
    action_validate_error: str,
    last_action_text: Optional[str],
    plan_struct: PlanStructure,
    agent_state: Dict,
    context: Dict,
    observations: List[str],
    max_steps_limit: Optional[int],
    run_replan_and_merge: Callable,
    safe_write_debug: Callable,
) -> Generator[str, None, Tuple[str, Optional[int]]]:
    """
    处理 action 验证失败的情况。

    Yields:
        SSE 事件

    Returns:
        (run_status, next_idx)
        run_status 为空字符串表示继续执行，非空表示终止
        next_idx 为 None 表示正常递增，否则跳转到指定索引
    """
    will_continue = step_order < plan_struct.step_count
    failure_signature, failure_hit_count = _record_failure_signature(
        agent_state=agent_state,
        action_type="action_invalid",
        step_error=str(action_validate_error or ""),
    )
    action_error_code = extract_task_error_code(str(action_validate_error or ""))
    blocking_artifact_step = _is_blocking_artifact_materialization_step(
        action_type="",
        title=title,
        last_action_text=last_action_text,
    )
    repair_hint = _update_action_invalid_constraints(
        agent_state=agent_state,
        step_order=int(step_order),
        step_title=str(title or ""),
        action_validate_error=str(action_validate_error or ""),
        failure_hit_count=int(failure_hit_count),
    )
    action_failure_class = classify_failure_class(str(action_validate_error or ""))
    feedback = build_step_feedback(
        message=message,
        step_order=int(step_order),
        title=str(title or ""),
        action_type="action_invalid",
        status="invalid",
        error_message=str(action_validate_error or ""),
        failure_class=str(action_failure_class or ""),
        failure_signature=str(failure_signature or ""),
        context=context,
        strategy_fingerprint=str(agent_state.get("strategy_fingerprint") or ""),
        attempt_index=int(coerce_int(agent_state.get("attempt_index"), default=0)),
        previous_goal_progress_score=coerce_int(
            ((agent_state.get("goal_progress") if isinstance(agent_state.get("goal_progress"), dict) else {}) or {}).get("score"),
            default=0,
        ),
    )
    register_step_feedback(agent_state, feedback)

    if action_error_code in {"script_missing", "script_dependency_unbound"}:
        missing_script_path = _extract_script_path_from_action_error(str(action_validate_error or ""))
        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.react.action_invalid.missing_script_detected",
            data={
                "step_order": int(step_order),
                "error_code": str(action_error_code or ""),
                "script_path": str(missing_script_path or ""),
                "strategy": "replan_instead_of_auto_materialize",
            },
            level="warning",
        )
        if missing_script_path:
            observations.append(f"{title}: MISSING_SCRIPT {str(missing_script_path or '')}")
    repeat_failure_limit = _resolve_action_invalid_repeat_limit()
    repeat_failure_exceeded = repeat_failure_limit > 0 and coerce_int(
        failure_hit_count, default=0
    ) >= repeat_failure_limit
    if repeat_failure_exceeded:
        agent_state["critical_failure"] = True
        agent_state["critical_failure_reason"] = "action_invalid_repeat_failure_budget_exceeded"

    # 更新计划栏状态
    plan_struct.set_step_status(idx, "failed")
    yield sse_plan_delta(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload(), indices=[idx])

    # 输出错误信息
    if action_validate_error in {"empty_response", "action 输出不是有效 JSON"}:
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} action 输出不是有效 JSON\n"})
    else:
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} action 不合法（{action_validate_error}）\n"})

    # 调试输出
    safe_write_debug(
        task_id=int(task_id),
        run_id=int(run_id),
        message="agent.react.action_invalid",
        data={
            "step_order": int(step_order),
            "error": str(action_validate_error),
            "last_action_text": _truncate_observation(str(last_action_text or "")),
            "will_continue": bool(will_continue),
            "failure_signature": str(failure_signature or ""),
            "failure_hit_count": coerce_int(failure_hit_count, default=0),
            "failure_budget": coerce_int(repeat_failure_limit, default=0),
            "failure_budget_exceeded": bool(repeat_failure_exceeded),
            "blocking_artifact_step": bool(blocking_artifact_step),
        },
        level="warning",
    )

    # 记录失败步骤
    executor_value = resolve_executor(agent_state, step_order)

    record_invalid_action_step(
        task_id=task_id,
        run_id=run_id,
        step_order=step_order,
        title=title,
        executor=executor_value,
        error=action_validate_error,
        last_action_text=last_action_text,
        safe_write_debug=safe_write_debug,
    )

    # 添加观测
    observations.append(f"{title}: FAIL action_invalid {action_validate_error}")
    if repair_hint:
        observations.append(f"REPAIR_HINT: {repair_hint}")

    # 失败状态必须先落盘，再决定是否 replan/fail-fast。
    persist_loop_state(
        run_id=run_id,
        plan_struct=plan_struct,
        agent_state=agent_state,
        step_order=step_order + 1,
        observations=observations,
        context=context,
        safe_write_debug=safe_write_debug,
        task_id=task_id,
        where="after_action_invalid",
        # action 无效/失败后的状态变更属于关键节点：必须落盘。
        force=True,
    )

    if repeat_failure_exceeded:
        failure_class_for_proof = classify_failure_class(str(action_validate_error or ""))
        agent_state["last_failure_class"] = str(failure_class_for_proof or "")
        yield sse_json(
            build_unreachable_proof_event(
                agent_state=agent_state,
                task_id=int(task_id),
                run_id=int(run_id),
                reason="action_invalid_repeat_failure_budget_exceeded",
                failure_class=str(failure_class_for_proof or ""),
                error_message=str(action_validate_error or ""),
            )
        )
        yield sse_json(
            {
                "delta": (
                    f"{STREAM_TAG_FAIL} 同类 action 无效失败已连续出现 {int(failure_hit_count)} 次，"
                    "停止自动重规划并终止本轮执行。\n"
                )
            }
        )
        return RUN_STATUS_FAILED, None

    # 判断是否需要强制 replan
    error_kind = classify_llm_error_text(str(action_validate_error or ""))
    transient_action_error = error_kind in {"rate_limit", "transient"}
    force_replan = should_force_replan_on_action_error(str(action_validate_error or ""))
    if blocking_artifact_step and not transient_action_error:
        force_replan = True
    failure_class = classify_failure_class(str(action_validate_error or ""))
    agent_state["last_failure_class"] = str(failure_class or "")

    # 尝试 replan
    if force_replan or not will_continue:
        replan_ctx = prepare_replan_context(
            step_order=step_order,
            agent_state=agent_state,
            max_steps_limit=max_steps_limit,
            plan_titles=plan_struct.get_titles(),
        )

        if replan_ctx.can_replan:
            yield sse_json(
                rotate_strategy(
                    agent_state=agent_state,
                    plan_struct=plan_struct,
                    reason="action_invalid_replan",
                    failure_class=str(failure_class or ""),
                )
            )
            sse_notice = f"{STREAM_TAG_EXEC} action 不合法，重新规划剩余步骤…" if force_replan else f"{STREAM_TAG_EXEC} 动作解析失败，重新规划剩余步骤…"

            replan_result = yield from run_replan_and_merge(
                task_id=int(task_id),
                run_id=int(run_id),
                message=message,
                workdir=workdir,
                model=model,
                react_params=react_params,
                max_steps_value=replan_ctx.max_steps_value,
                tools_hint=tools_hint,
                skills_hint=skills_hint,
                memories_hint=memories_hint,
                graph_hint=graph_hint,
                plan_struct=plan_struct,
                agent_state=agent_state,
                context=context,
                observations=observations,
                done_count=replan_ctx.done_count,
                error=str(action_validate_error or "action_invalid"),
                sse_notice=sse_notice,
                replan_attempts=replan_ctx.replan_attempts,
                safe_write_debug=safe_write_debug,
            )

            if replan_result:
                # replan 成功，替换计划
                plan_struct.replace_from(replan_result.plan_struct)

                yield sse_plan(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload())

                persist_loop_state(
                    run_id=run_id,
                    plan_struct=plan_struct,
                    agent_state=agent_state,
                    step_order=step_order,
                    observations=observations,
                    context=context,
                    safe_write_debug=safe_write_debug,
                    task_id=task_id,
                    where="after_action_invalid_replan",
                    # replan 合并属于关键状态：必须落盘，保证 resume 与审计一致。
                    force=True,
                )

                # 跳转到已完成步骤的下一步
                return "", replan_ctx.done_count

    if will_continue:
        if force_replan:
            yield sse_json({"delta": f"{STREAM_TAG_FAIL} 动作生成失败且无法恢复，终止本轮执行。\n"})
            return RUN_STATUS_FAILED, None
        if transient_action_error:
            yield sse_json(
                rotate_strategy(
                    agent_state=agent_state,
                    plan_struct=plan_struct,
                    reason="action_invalid_transient_retry",
                    failure_class=str(failure_class or ""),
                )
            )
            yield sse_json({"delta": f"{STREAM_TAG_EXEC} 动作生成超时/抖动，重试当前步骤…\n"})
            return "", idx
        if blocking_artifact_step:
            yield sse_json({"delta": f"{STREAM_TAG_FAIL} 关键产物步骤未恢复，已终止本轮执行。\n"})
            return RUN_STATUS_FAILED, None
        # 继续下一步
        return "", idx + 1

    # 计划耗尽，终止
    return RUN_STATUS_FAILED, None


def handle_allow_failure(
    *,
    task_id: int,
    run_id: int,
    step_order: int,
    idx: int,
    title: str,
    message: str,
    workdir: str,
    model: str,
    react_params: dict,
    tools_hint: str,
    skills_hint: str,
    memories_hint: str,
    graph_hint: str,
    allow_err: str,
    plan_struct: PlanStructure,
    agent_state: Dict,
    context: Dict,
    observations: List[str],
    max_steps_limit: Optional[int],
    run_replan_and_merge: Callable,
    safe_write_debug: Callable,
) -> Generator[str, None, Tuple[str, Optional[int]]]:
    """
    处理 allow 约束验证失败的情况。

    Returns:
        (run_status, next_idx)
    """
    will_continue = step_order < plan_struct.step_count
    failure_class = classify_failure_class(str(allow_err or "allow_failed"))
    agent_state["last_failure_class"] = str(failure_class or "")
    blocking_artifact_step = _is_blocking_artifact_materialization_step(
        action_type="",
        title=title,
    )

    # 更新计划栏状态（与 handle_action_invalid / handle_step_failure 保持一致：使用 idx 而非 step_order-1）
    plan_struct.set_step_status(idx, "failed")
    yield sse_plan_delta(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload(), indices=[idx])

    yield sse_json({"delta": f"{STREAM_TAG_FAIL} {allow_err or 'action.type 不在 allow 内'}\n"})

    safe_write_debug(
        task_id=int(task_id),
        run_id=int(run_id),
        message="agent.react.allow_failed",
        data={"step_order": int(step_order), "error": str(allow_err or ""), "will_continue": bool(will_continue), "blocking_artifact_step": bool(blocking_artifact_step)},
        level="warning",
    )

    observations.append(f"{title}: FAIL allow {allow_err or 'action.type_not_allowed'}")

    # 持久化状态
    persist_loop_state(
        run_id=run_id,
        plan_struct=plan_struct,
        agent_state=agent_state,
        step_order=step_order + 1,
        observations=observations,
        context=context,
        safe_write_debug=safe_write_debug,
        task_id=task_id,
        where="after_allow_failed",
        # allow 失败会影响后续 resume/诊断：必须落盘。
        force=True,
    )

    if will_continue:
        # 统一策略：优先尝试 replan 修复（与 handle_step_failure 保持一致），
        # 避免 allow 失败后盲目跳下一步导致连锁失败。
        replan_ctx = prepare_replan_context(
            step_order=step_order,
            agent_state=agent_state,
            max_steps_limit=max_steps_limit,
            plan_titles=plan_struct.get_titles(),
        )

        if replan_ctx.can_replan:
            yield sse_json(
                rotate_strategy(
                    agent_state=agent_state,
                    plan_struct=plan_struct,
                    reason="allow_failure_replan",
                    failure_class=str(failure_class or ""),
                )
            )
            replan_result = yield from run_replan_and_merge(
                task_id=int(task_id),
                run_id=int(run_id),
                message=message,
                workdir=workdir,
                model=model,
                react_params=react_params,
                max_steps_value=replan_ctx.max_steps_value,
                tools_hint=tools_hint,
                skills_hint=skills_hint,
                memories_hint=memories_hint,
                graph_hint=graph_hint,
                plan_struct=plan_struct,
                agent_state=agent_state,
                context=context,
                observations=observations,
                done_count=replan_ctx.done_count,
                error=str(allow_err or "allow_failed"),
                sse_notice=f"{STREAM_TAG_EXEC} allow 约束未满足，重新规划剩余步骤…",
                replan_attempts=replan_ctx.replan_attempts,
                safe_write_debug=safe_write_debug,
            )

            if replan_result:
                plan_struct.replace_from(replan_result.plan_struct)
                return "", replan_ctx.done_count

        if blocking_artifact_step:
            yield sse_json({"delta": f"{STREAM_TAG_FAIL} 关键产物步骤 allow 约束失败且重规划未恢复，已终止本轮执行。\n"})
            return RUN_STATUS_FAILED, None
        # replan 不可用或失败，降级跳下一步
        return "", idx + 1

    # 计划耗尽，尝试 replan
    replan_ctx = prepare_replan_context(
        step_order=step_order,
        agent_state=agent_state,
        max_steps_limit=max_steps_limit,
        plan_titles=plan_struct.get_titles(),
    )

    if replan_ctx.can_replan:
        yield sse_json(
            rotate_strategy(
                agent_state=agent_state,
                plan_struct=plan_struct,
                reason="allow_failure_replan_tail",
                failure_class=str(failure_class or ""),
            )
        )
        replan_result = yield from run_replan_and_merge(
            task_id=int(task_id),
            run_id=int(run_id),
            message=message,
            workdir=workdir,
            model=model,
            react_params=react_params,
            max_steps_value=replan_ctx.max_steps_value,
            tools_hint=tools_hint,
            skills_hint=skills_hint,
            memories_hint=memories_hint,
            graph_hint=graph_hint,
            plan_struct=plan_struct,
            agent_state=agent_state,
            observations=observations,
            done_count=replan_ctx.done_count,
            error=str(allow_err or "allow_failed"),
            sse_notice=f"{STREAM_TAG_EXEC} allow 约束未满足，重新规划剩余步骤…",
            replan_attempts=replan_ctx.replan_attempts,
            safe_write_debug=safe_write_debug,
        )

        if replan_result:
            plan_struct.replace_from(replan_result.plan_struct)
            return "", replan_ctx.done_count

    return RUN_STATUS_FAILED, None


def handle_step_failure(
    *,
    task_id: int,
    run_id: int,
    step_id: int,
    step_order: int,
    idx: int,
    title: str,
    message: str,
    workdir: str,
    model: str,
    react_params: dict,
    tools_hint: str,
    skills_hint: str,
    memories_hint: str,
    graph_hint: str,
    action_type: str,
    step_detail: str = "",
    step_error: str,
    plan_struct: PlanStructure,
    agent_state: Dict,
    context: Dict,
    observations: List[str],
    max_steps_limit: Optional[int],
    run_replan_and_merge: Callable,
    safe_write_debug: Callable,
    mark_task_step_failed: Callable,
    finished_at: str,
) -> Generator[str, None, Tuple[str, Optional[int]]]:
    """
    处理步骤执行失败的情况。

    Returns:
        (run_status, next_idx)
    """
    will_continue = step_order < plan_struct.step_count
    failure_signature, failure_hit_count = _record_failure_signature(
        agent_state=agent_state,
        action_type=str(action_type or ""),
        step_error=str(step_error or ""),
    )
    repeat_failure_limit = coerce_int(AGENT_REACT_REPEAT_FAILURE_MAX, default=0)
    if repeat_failure_limit < 0:
        repeat_failure_limit = 0
    non_retriable_failure = should_fail_fast_on_step_error(str(step_error or ""))
    repeat_failure_exceeded = repeat_failure_limit > 0 and coerce_int(
        failure_hit_count, default=0
    ) >= repeat_failure_limit
    if non_retriable_failure:
        repeat_failure_exceeded = True
    if repeat_failure_exceeded:
        agent_state["critical_failure"] = True
        agent_state["critical_failure_reason"] = (
            "non_retriable_step_error" if non_retriable_failure else "repeat_failure_budget_exceeded"
        )

    blocking_failure = _is_blocking_step_failure(
        action_type=action_type,
        title=title,
        error_code=extract_task_error_code(str(step_error or "")) or "step_failed",
        step_detail=step_detail,
    )

    safe_write_debug(
        task_id=int(task_id),
        run_id=int(run_id),
        message="agent.step.failed",
        data={
            "step_id": int(step_id),
            "step_order": int(step_order),
            "title": title,
            "action_type": action_type,
            "error": step_error,
            "will_continue": bool(will_continue),
            "failure_signature": failure_signature,
            "failure_hit_count": coerce_int(failure_hit_count, default=0),
            "failure_budget": coerce_int(repeat_failure_limit, default=0),
            "failure_budget_exceeded": bool(repeat_failure_exceeded),
            "non_retriable_failure": bool(non_retriable_failure),
            "blocking_failure": bool(blocking_failure),
        },
        level="warning",
    )

    mark_task_step_failed(
        step_id=int(step_id),
        error=str(step_error),
        finished_at=finished_at,
    )

    yield sse_json({"delta": f"{STREAM_TAG_FAIL} {title}（{step_error}）\n"})

    plan_struct.set_step_status(idx, "failed")
    yield sse_plan_delta(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload(), indices=[idx])

    error_code = extract_task_error_code(str(step_error or "")) or "step_failed"
    _update_execution_constraints(
        agent_state=agent_state,
        step_order=int(step_order),
        error_code=str(error_code or ""),
        step_error=str(step_error or ""),
    )
    observations.append(f"{title}: FAIL {step_error}")
    repair_hint = _derive_step_failure_repair_hint(
        error_code=error_code,
        step_error=str(step_error or ""),
    )
    if repair_hint:
        observations.append(f"REPAIR_HINT: {repair_hint}")
    # 失败后清空"最近可解析源"，避免后续 json_parse 继续消费陈旧结果。
    if isinstance(context, dict):
        context.pop("latest_parse_input_text", None)

    failure_class = classify_failure_class(str(step_error or ""))
    source_grounding_blocked = _is_source_grounding_blocked_after_failure(
        failure_class=str(failure_class or ""),
        current_idx=int(idx),
        plan_struct=plan_struct,
        context=context,
    )
    blocking_failure = bool(blocking_failure or source_grounding_blocked)
    agent_state["last_failure_class"] = str(failure_class or "")
    strategy_info = strategy_meta(agent_state)
    logger.warning(
        "[agent.react.step_failed] task_id=%s run_id=%s step_id=%s step_order=%s action_type=%s code=%s non_retriable=%s budget_exceeded=%s error=%s",
        int(task_id),
        int(run_id),
        int(step_id),
        int(step_order),
        str(action_type or ""),
        str(error_code or ""),
        bool(non_retriable_failure),
        bool(repeat_failure_exceeded),
        str(step_error or ""),
    )
    # 关键语义：步骤级失败是“可观测事件”，不应伪装为 SSE transport error。
    # 否则前端会提前终止流，造成状态不同步（例如仍在 replan 却已提示任务结束）。
    yield sse_json(
        {
            "type": "step_error",
            "level": "warning",
            "code": error_code,
            "error_code": error_code,
            "task_id": int(task_id),
            "run_id": int(run_id),
            "step_id": int(step_id),
            "step_order": int(step_order),
            "action_type": str(action_type or ""),
            "message": str(step_error or ""),
            "error_message": str(step_error or ""),
            "phase": "react_step_execution",
            "recoverable": not bool(repeat_failure_exceeded),
            "retryable": not bool(repeat_failure_exceeded),
            "non_retriable_failure": bool(non_retriable_failure),
            "failure_signature": str(failure_signature or ""),
            "failure_hit_count": int(coerce_int(failure_hit_count, default=0)),
            "failure_class": str(failure_class or ""),
            "blocking_failure": bool(blocking_failure),
            "source_grounding_blocked": bool(source_grounding_blocked),
            "strategy_fingerprint": str(strategy_info.get("strategy_fingerprint") or ""),
            "attempt_index": int(coerce_int(strategy_info.get("attempt_index"), default=0)),
        }
    )

    # 持久化失败状态
    persist_loop_state(
        run_id=run_id,
        plan_struct=plan_struct,
        agent_state=agent_state,
        step_order=step_order + 1,
        observations=observations,
        context=context,
        safe_write_debug=safe_write_debug,
        task_id=task_id,
        where="after_step_failed",
        # 步骤失败结算必须落盘：避免节流吞掉 failed 状态，影响可恢复性。
        force=True,
    )

    if repeat_failure_exceeded:
        proof_event = build_unreachable_proof_event(
            agent_state=agent_state,
            task_id=int(task_id),
            run_id=int(run_id),
            reason="non_retriable_step_error" if non_retriable_failure else "repeat_failure_budget_exceeded",
            failure_class=str(failure_class or ""),
            error_message=str(step_error or ""),
        )
        yield sse_json(proof_event)
        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.failure.non_retriable" if non_retriable_failure else "agent.failure_budget.exceeded",
            data={
                "signature": failure_signature,
                "count": int(failure_hit_count),
                "budget": int(repeat_failure_limit),
                "step_order": int(step_order),
                "action_type": str(action_type or ""),
                "error_code": str(error_code or ""),
                "non_retriable_failure": bool(non_retriable_failure),
                "proof_id": str(proof_event.get("proof_id") or ""),
            },
            level="warning",
        )
        if non_retriable_failure:
            yield sse_json({"delta": f"{STREAM_TAG_FAIL} 检测到不可重试的契约错误，终止本轮执行。\n"})
        else:
            yield sse_json(
                {
                    "delta": (
                        f"{STREAM_TAG_FAIL} 同类失败已连续出现 {int(failure_hit_count)} 次，"
                        "停止自动重试并终止本轮执行。\n"
                    )
                }
            )
        return RUN_STATUS_FAILED, None

    # 失败后优先尝试 replan
    if will_continue:
        # 这类“结构性执行错误”若重规划也失败，不应继续跳过到下一步，
        # 否则会出现“关键步骤失败但流程继续推进”的伪进展。
        no_skip_on_replan_failure = bool(blocking_failure)
        if source_grounding_blocked:
            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.source_grounding.blocked",
                data={
                    "step_id": int(step_id),
                    "step_order": int(step_order),
                    "failure_class": str(failure_class or ""),
                    "error_code": str(error_code or ""),
                    "reason": "source_failed_without_grounded_sample",
                },
                level="warning",
            )

        replan_ctx = prepare_replan_context(
            step_order=step_order,
            agent_state=agent_state,
            max_steps_limit=max_steps_limit,
            plan_titles=plan_struct.get_titles(),
        )

        if replan_ctx.can_replan:
            replan_result = yield from run_replan_and_merge(
                task_id=int(task_id),
                run_id=int(run_id),
                message=message,
                workdir=workdir,
                model=model,
                react_params=react_params,
                max_steps_value=replan_ctx.max_steps_value,
                tools_hint=tools_hint,
                skills_hint=skills_hint,
                memories_hint=memories_hint,
                graph_hint=graph_hint,
                plan_struct=plan_struct,
                agent_state=agent_state,
                context=context,
                observations=observations,
                done_count=replan_ctx.done_count,
                error=str(step_error),
                sse_notice="",  # 静默 replan
                replan_attempts=replan_ctx.replan_attempts,
                safe_write_debug=safe_write_debug,
            )

            if replan_result:
                plan_struct.replace_from(replan_result.plan_struct)
                return "", replan_ctx.done_count

        # replan 失败或不可用：结构性错误不允许跳过继续执行，直接终止并暴露错误。
        if no_skip_on_replan_failure:
            yield sse_json(
                {
                    "delta": (
                        f"{STREAM_TAG_FAIL} 关键步骤失败且重规划未恢复（code={error_code}），"
                        "已终止本轮执行。\n"
                    )
                }
            )
            return RUN_STATUS_FAILED, None

        # 非关键错误：允许降级继续下一步（保持原有容错行为）
        return "", idx + 1

    # 计划耗尽，尝试最后的 replan
    replan_ctx = prepare_replan_context(
        step_order=step_order,
        agent_state=agent_state,
        max_steps_limit=max_steps_limit,
        plan_titles=plan_struct.get_titles(),
    )

    if replan_ctx.remaining_limit is not None and replan_ctx.remaining_limit <= 0:
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} 计划已耗尽且剩余步数不足以继续（max_steps={max_steps_limit}）。\n"})
        return RUN_STATUS_FAILED, None

    if not replan_ctx.can_replan:
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} 计划已耗尽且重新规划次数已达上限。\n"})
        return RUN_STATUS_FAILED, None

    yield sse_json(
        rotate_strategy(
            agent_state=agent_state,
            plan_struct=plan_struct,
            reason="step_failure_replan_tail",
            failure_class=str(failure_class or ""),
        )
    )
    replan_result = yield from run_replan_and_merge(
        task_id=int(task_id),
        run_id=int(run_id),
        message=message,
        workdir=workdir,
        model=model,
        react_params=react_params,
        max_steps_value=replan_ctx.max_steps_value,
        tools_hint=tools_hint,
        skills_hint=skills_hint,
        memories_hint=memories_hint,
        graph_hint=graph_hint,
        plan_struct=plan_struct,
        agent_state=agent_state,
        observations=observations,
        done_count=replan_ctx.done_count,
        error=str(step_error),
        sse_notice=f"{STREAM_TAG_EXEC} 计划已耗尽，重新规划剩余步骤…",
        replan_attempts=replan_ctx.replan_attempts,
        safe_write_debug=safe_write_debug,
    )

    if replan_result:
        plan_struct.replace_from(replan_result.plan_struct)
        return "", replan_ctx.done_count

    return RUN_STATUS_FAILED, None
