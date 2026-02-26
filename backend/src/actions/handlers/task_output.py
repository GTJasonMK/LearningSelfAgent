import os
import re
from datetime import date
from typing import Dict, List, Optional, Tuple

from backend.src.actions.handlers.common_utils import load_json_object
from backend.src.common.serializers import task_output_from_row
from backend.src.common.utils import dedupe_keep_order
from backend.src.constants import (
    AGENT_ARTIFACT_CSV_MAX_PLACEHOLDER_RATIO,
    AGENT_ARTIFACT_CSV_MIN_DATE_SPAN_DAYS,
    AGENT_ARTIFACT_CSV_MIN_NUMERIC_RATIO,
    AGENT_ARTIFACT_CSV_MIN_NUMERIC_ROWS,
    AGENT_ARTIFACT_CSV_MIN_ROWS,
    AGENT_ARTIFACT_CSV_QUALITY_HARD_FAIL_DEFAULT,
    AGENT_ARTIFACT_CSV_QUALITY_GATE_DEFAULT,
    TASK_OUTPUT_TYPE_TEXT,
)
from backend.src.common.path_utils import normalize_windows_abs_path_on_posix
from backend.src.repositories.task_outputs_repo import create_task_output, get_task_output
from backend.src.repositories.task_steps_repo import list_task_steps_for_run
from backend.src.repositories.tasks_repo import task_exists


def _create_task_output_record(task_id: int, payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    写入 task_outputs 并返回序列化后的 output。

    说明：
    - 这是 actions 执行链路内部使用的轻量写入函数；
    - API 层的 /tasks/{task_id}/outputs 仍由路由对外提供。
    """
    if not task_exists(task_id=int(task_id)):
        return None, "task_not_found"

    output_id, _ = create_task_output(
        task_id=int(task_id),
        run_id=payload.get("run_id"),
        output_type=str(payload.get("output_type") or ""),
        content=str(payload.get("content") or ""),
    )
    row = get_task_output(output_id=int(output_id))
    return (task_output_from_row(row) if row else None), None


def _truncate_text(value: object, max_chars: int = 120) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if len(raw) <= int(max_chars):
        return raw
    return f"{raw[:max(0, int(max_chars) - 1)]}…"


def _looks_like_missing_url_text(text: object) -> bool:
    raw = str(text or "").strip().lower()
    if not raw:
        return False
    return "no url provided" in raw or "url required" in raw or "缺少url" in raw


def _build_step_result_summary(action_type: str, result_obj: Optional[dict]) -> str:
    if not isinstance(result_obj, dict):
        return ""

    if action_type == "shell_command":
        auto_retry = result_obj.get("auto_retry") if isinstance(result_obj.get("auto_retry"), dict) else None
        if isinstance(auto_retry, dict):
            retry_trigger = str(auto_retry.get("trigger") or "").strip()
            retry_url = str(auto_retry.get("fallback_url") or "").strip()
            first_error = _truncate_text(auto_retry.get("initial_stderr") or auto_retry.get("initial_stdout"), 80)
            parts = ["shell"]
            if retry_trigger:
                parts.append(f"auto_retry={retry_trigger}")
            if retry_url:
                parts.append(f"url={retry_url}")
            if first_error:
                parts.append(f"first_error={first_error}")
            return " ".join(parts).strip()

        stderr = _truncate_text(result_obj.get("stderr"), 80)
        stdout = _truncate_text(result_obj.get("stdout"), 80)
        if stderr:
            return f"shell stderr={stderr}"
        if stdout:
            return f"shell stdout={stdout}"
        return "shell"

    if action_type == "tool_call":
        tool_id = result_obj.get("tool_id")
        tool_output = _truncate_text(result_obj.get("output"), 80)
        if tool_id is not None and tool_output:
            return f"tool#{tool_id} output={tool_output}"
        if tool_id is not None:
            return f"tool#{tool_id}"
        return "tool_call"

    if action_type == "http_request":
        status_code = result_obj.get("status_code")
        bytes_value = result_obj.get("bytes")
        if status_code is not None:
            return f"http {status_code} bytes={bytes_value}"
        return "http_request"

    if action_type in {"file_write", "file_append", "file_read"}:
        path_text = str(result_obj.get("path") or "").strip()
        bytes_value = result_obj.get("bytes")
        if path_text:
            return f"{action_type} {path_text} bytes={bytes_value}"
        return action_type

    if action_type == "json_parse":
        return f"json_parse picked={result_obj.get('picked')}"

    return ""


def _collect_task_output_evidence(
    *,
    task_id: int,
    run_id: int,
    current_step_id: Optional[int],
) -> Dict[str, List]:
    """
    收集 task_output 可引用的执行证据（step/tool/artifact）。

    附加信息：
    - flags: 真实性校验信号（例如 shell 无 URL 首次失败已被观测）。
    """
    step_refs: List[dict] = []
    tool_call_record_ids: List[int] = []
    artifact_paths: List[str] = []
    evidence_flags: List[str] = []

    try:
        rows = list_task_steps_for_run(task_id=int(task_id), run_id=int(run_id))
    except Exception:
        return {
            "steps": step_refs,
            "tool_calls": tool_call_record_ids,
            "artifacts": artifact_paths,
            "flags": evidence_flags,
        }

    for row in rows or []:
        if not row:
            continue
        try:
            row_id = int(row["id"]) if row["id"] is not None else None
        except Exception:
            row_id = None
        if current_step_id is not None and row_id == int(current_step_id):
            continue

        status = str(row["status"] or "").strip().lower() if "status" in row.keys() else ""
        if status != "done":
            continue

        title = str(row["title"] or "").strip() if "title" in row.keys() else ""
        detail_obj = load_json_object(row["detail"] if "detail" in row.keys() else None)
        action_type = str(detail_obj.get("type") or "").strip().lower() if isinstance(detail_obj, dict) else ""
        result_obj = load_json_object(row["result"] if "result" in row.keys() else None)

        if row_id is not None:
            step_item = {"id": int(row_id), "title": title}
            step_summary = _build_step_result_summary(action_type, result_obj)
            if step_summary:
                step_item["summary"] = step_summary
            step_refs.append(step_item)

        if action_type == "tool_call" and isinstance(result_obj, dict):
            call_id = result_obj.get("id")
            try:
                if call_id is not None:
                    tool_call_record_ids.append(int(call_id))
            except Exception:
                pass

        if action_type in {"file_write", "file_append"} and isinstance(result_obj, dict):
            path_text = str(result_obj.get("path") or "").strip()
            if path_text:
                artifact_paths.append(path_text)

        if action_type == "shell_command" and isinstance(result_obj, dict):
            shell_markers = [
                str(result_obj.get("stderr") or ""),
                str(result_obj.get("stdout") or ""),
            ]
            auto_retry = result_obj.get("auto_retry") if isinstance(result_obj.get("auto_retry"), dict) else None
            if isinstance(auto_retry, dict):
                evidence_flags.append("shell_auto_retry_used")
                retry_trigger = str(auto_retry.get("trigger") or "").strip()
                if retry_trigger:
                    evidence_flags.append(f"shell_auto_retry:{retry_trigger}")
                retry_url = str(auto_retry.get("fallback_url") or "").strip()
                if retry_url:
                    evidence_flags.append(f"shell_auto_retry_url:{retry_url}")
                shell_markers.append(str(auto_retry.get("initial_stderr") or ""))
                shell_markers.append(str(auto_retry.get("initial_stdout") or ""))
            if any(_looks_like_missing_url_text(item) for item in shell_markers):
                evidence_flags.append("shell_missing_url_observed")

    # 去重 + 控制长度（保留最近证据）
    step_refs = dedupe_keep_order(step_refs)[-6:]
    tool_call_record_ids = dedupe_keep_order(tool_call_record_ids)[-6:]
    artifact_paths = dedupe_keep_order(artifact_paths)[-6:]
    evidence_flags = dedupe_keep_order(evidence_flags)[-12:]
    return {
        "steps": step_refs,
        "tool_calls": tool_call_record_ids,
        "artifacts": artifact_paths,
        "flags": evidence_flags,
    }


def _format_evidence_block(evidence: Dict[str, List]) -> str:
    steps = evidence.get("steps") if isinstance(evidence.get("steps"), list) else []
    tool_calls = evidence.get("tool_calls") if isinstance(evidence.get("tool_calls"), list) else []
    artifacts = evidence.get("artifacts") if isinstance(evidence.get("artifacts"), list) else []

    lines: List[str] = ["[证据引用]"]
    if steps:
        step_text = "；".join(
            f"step#{int(item.get('id'))}:{str(item.get('title') or '').strip()}"
            for item in steps
            if isinstance(item, dict) and item.get("id") is not None
        )
        if step_text:
            lines.append(f"- steps: {step_text}")

        step_results = "；".join(
            f"step#{int(item.get('id'))}:{str(item.get('summary') or '').strip()}"
            for item in steps
            if isinstance(item, dict) and item.get("id") is not None and str(item.get("summary") or "").strip()
        )
        if step_results:
            lines.append(f"- step_results: {step_results}")
    if tool_calls:
        tools_text = ", ".join(f"#{int(i)}" for i in tool_calls)
        lines.append(f"- tool_call_records: {tools_text}")
    if artifacts:
        artifact_text = "；".join(f"`{str(path)}`" for path in artifacts if str(path).strip())
        if artifact_text:
            lines.append(f"- artifacts: {artifact_text}")

    return "\n".join(lines)


def _merge_content_with_evidence(
    *,
    content: str,
    evidence_block: str,
    has_evidence: bool,
) -> str:
    current = str(content or "").strip()
    if has_evidence:
        if "证据引用" in current:
            return current
        return f"{current}\n\n{evidence_block}".strip()

    fallback = "[证据引用]\n- 无（建议补齐 step/tool/artifact 证据，避免与实际执行不一致）"
    if not current:
        return fallback
    if "证据引用" in current:
        return current
    return f"{current}\n\n{fallback}".strip()


def _parse_numeric(text: str) -> Optional[float]:
    raw = str(text or "").strip()
    if not raw:
        return None
    raw = raw.replace(",", "")
    raw = raw.replace("，", "")
    raw = raw.replace("元/克", "")
    raw = raw.replace("元", "")
    if raw.endswith("%"):
        raw = raw[:-1]
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _parse_iso_date(text: str) -> Optional[date]:
    raw = str(text or "").strip()
    if not raw:
        return None
    normalized = raw.replace("/", "-").replace(".", "-")
    if not re.match(r"^\d{4}-\d{1,2}-\d{1,2}$", normalized):
        return None
    parts = normalized.split("-")
    try:
        y = int(parts[0])
        m = int(parts[1])
        d = int(parts[2])
        return date(y, m, d)
    except Exception:
        return None


def _load_csv_quality_stats(path: str) -> Dict[str, object]:
    rows_total = 0
    numeric_rows = 0
    placeholder_rows = 0
    date_values: List[date] = []
    placeholders = ("暂无", "n/a", "na", "none", "null", "无数据", "待补充", "tbd")

    with open(path, "r", encoding="utf-8", errors="ignore") as handle:
        raw_lines = [str(line or "").strip() for line in handle.readlines()]

    clean_lines = [line for line in raw_lines if line]
    if not clean_lines:
        return {
            "rows_total": 0,
            "numeric_rows": 0,
            "placeholder_rows": 0,
            "numeric_ratio": 0.0,
            "placeholder_ratio": 0.0,
            "date_span_days": 0,
            "issues": ["csv_empty"],
        }

    # 允许第一行是表头
    data_lines = clean_lines[1:] if clean_lines and ("日期" in clean_lines[0] or "date" in clean_lines[0].lower()) else clean_lines

    for line in data_lines:
        if not line:
            continue
        cells = [cell.strip() for cell in re.split(r"[,，]\s*", line) if str(cell).strip()]
        if not cells:
            continue
        rows_total += 1

        text_joined = " ".join(cells).lower()
        if any(mark in text_joined for mark in placeholders):
            placeholder_rows += 1

        numeric_hit = False
        for cell in cells[1:] if len(cells) > 1 else cells:
            if _parse_numeric(cell) is not None:
                numeric_hit = True
                break
        if numeric_hit:
            numeric_rows += 1

        date_candidate = _parse_iso_date(cells[0])
        if date_candidate is not None:
            date_values.append(date_candidate)

    numeric_ratio = float(numeric_rows) / float(rows_total) if rows_total > 0 else 0.0
    placeholder_ratio = float(placeholder_rows) / float(rows_total) if rows_total > 0 else 0.0
    span_days = 0
    if len(date_values) >= 2:
        span_days = abs((max(date_values) - min(date_values)).days)

    issues: List[str] = []
    if rows_total < int(AGENT_ARTIFACT_CSV_MIN_ROWS):
        issues.append("rows_insufficient")
    if numeric_rows < int(AGENT_ARTIFACT_CSV_MIN_NUMERIC_ROWS):
        issues.append("numeric_rows_insufficient")
    if numeric_ratio < float(AGENT_ARTIFACT_CSV_MIN_NUMERIC_RATIO):
        issues.append("numeric_ratio_low")
    if placeholder_ratio > float(AGENT_ARTIFACT_CSV_MAX_PLACEHOLDER_RATIO):
        issues.append("placeholder_ratio_high")
    if span_days < int(AGENT_ARTIFACT_CSV_MIN_DATE_SPAN_DAYS):
        issues.append("date_span_too_short")

    return {
        "rows_total": rows_total,
        "numeric_rows": numeric_rows,
        "placeholder_rows": placeholder_rows,
        "numeric_ratio": round(numeric_ratio, 4),
        "placeholder_ratio": round(placeholder_ratio, 4),
        "date_span_days": span_days,
        "issues": issues,
    }


def _enforce_csv_artifact_quality(
    *,
    task_id: int,
    run_id: int,
    context: Optional[dict],
    evidence: Dict[str, List],
) -> Optional[str]:
    strict = (context or {}).get("enforce_csv_artifact_quality")
    if strict is None:
        strict = AGENT_ARTIFACT_CSV_QUALITY_GATE_DEFAULT
    if not bool(strict):
        return None

    artifacts = evidence.get("artifacts") if isinstance(evidence.get("artifacts"), list) else []
    if not artifacts:
        return None

    csv_paths: List[str] = []
    for item in artifacts:
        raw = str(item or "").strip()
        if raw.lower().endswith(".csv"):
            csv_paths.append(raw)
    if not csv_paths:
        return None

    rows = []
    try:
        rows = list_task_steps_for_run(task_id=int(task_id), run_id=int(run_id))
    except Exception:
        rows = []

    last_workdir = os.getcwd()
    for row in rows or []:
        detail_obj = load_json_object(row["detail"] if row and "detail" in row.keys() else None)
        if not isinstance(detail_obj, dict):
            continue
        if str(detail_obj.get("type") or "").strip().lower() != "shell_command":
            continue
        payload_obj = detail_obj.get("payload") if isinstance(detail_obj.get("payload"), dict) else {}
        candidate = normalize_windows_abs_path_on_posix(str(payload_obj.get("workdir") or "").strip())
        if candidate:
            last_workdir = candidate

    failed_reasons: List[str] = []
    for raw_path in csv_paths:
        norm = normalize_windows_abs_path_on_posix(raw_path)
        if not os.path.isabs(norm):
            norm = os.path.abspath(os.path.join(last_workdir, norm))
        if not os.path.exists(norm):
            failed_reasons.append(f"csv_missing:{raw_path}")
            continue
        try:
            stats = _load_csv_quality_stats(norm)
        except Exception as exc:
            failed_reasons.append(f"csv_unreadable:{raw_path}:{exc}")
            continue
        issues = stats.get("issues") if isinstance(stats.get("issues"), list) else []
        if issues:
            issue_text = ",".join(str(item) for item in issues)
            failed_reasons.append(f"csv_quality_failed:{raw_path}:{issue_text}")

    if failed_reasons:
        return "；".join(failed_reasons)
    return None


def execute_task_output(
    task_id: int,
    run_id: int,
    payload: dict,
    *,
    context: Optional[dict] = None,
    step_row: Optional[dict] = None,
) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 task_output：
    - content 允许为空：会尝试用 context.last_llm_response 自动补齐
    - 可选开启 evidence：自动绑定 step/tool/artifact 证据（作为可观测性与评估依据）
    """
    content = payload.get("content")
    if not isinstance(content, str) or not content.strip():
        last_llm = (context or {}).get("last_llm_response")
        if isinstance(last_llm, str) and last_llm.strip():
            payload["content"] = last_llm
        else:
            return None, "task_output.content 不能为空"

    output_type = payload.get("output_type")
    if not isinstance(output_type, str) or not output_type.strip():
        payload["output_type"] = TASK_OUTPUT_TYPE_TEXT

    if bool((context or {}).get("enforce_task_output_evidence")):
        current_step_id = None
        if isinstance(step_row, dict):
            try:
                if step_row.get("id") is not None:
                    current_step_id = int(step_row.get("id"))
            except Exception:
                current_step_id = None

        evidence = _collect_task_output_evidence(
            task_id=int(task_id),
            run_id=int(run_id),
            current_step_id=current_step_id,
        )
        has_evidence = bool(
            (evidence.get("steps") or [])
            or (evidence.get("tool_calls") or [])
            or (evidence.get("artifacts") or [])
        )
        evidence_block = _format_evidence_block(evidence)
        payload["content"] = _merge_content_with_evidence(
            content=str(payload.get("content") or ""),
            evidence_block=evidence_block,
            has_evidence=has_evidence,
        )

        csv_quality_error = _enforce_csv_artifact_quality(
            task_id=int(task_id),
            run_id=int(run_id),
            context=context,
            evidence=evidence,
        )
        if csv_quality_error:
            warn_line = f"[产物质量提示] CSV 产物质量校验未通过：{csv_quality_error}"
            payload["content"] = f"{str(payload.get('content') or '').strip()}\n\n{warn_line}".strip()
            if isinstance(context, dict):
                items = context.get("quality_warnings")
                if not isinstance(items, list):
                    items = []
                items.append(warn_line)
                context["quality_warnings"] = items
            hard_fail = (context or {}).get("enforce_csv_artifact_quality_hard_fail")
            if hard_fail is None:
                hard_fail = AGENT_ARTIFACT_CSV_QUALITY_HARD_FAIL_DEFAULT
            if bool(hard_fail):
                return None, f"csv_artifact_quality_failed:{csv_quality_error}"

    payload.setdefault("run_id", run_id)
    output, err = _create_task_output_record(task_id, payload)
    if err:
        return None, err
    return output, None
