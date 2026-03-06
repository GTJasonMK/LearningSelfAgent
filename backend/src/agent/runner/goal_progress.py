# -*- coding: utf-8 -*-
"""目标进展评估与任务口径提取。"""

from __future__ import annotations

import json
import re
from typing import Dict, List, Optional, Set

from backend.src.common.utils import coerce_int

_STOP_WORDS = {
    "请你", "帮我", "最近", "三个月", "保存", "文件", "然后", "这个", "那个", "需要", "可以", "进行",
    "一下", "当前", "最终", "数据", "结果", "最近的", "请", "帮", "我", "把", "并", "为", "到", "与",
}
_FILE_TYPES = ("csv", "json", "txt", "md", "html", "xlsx")
_UNIT_CONFLICT_TERMS = {
    "元/克": ["usd/oz", "usd", "美元/盎司", "美元每盎司", "xau", "ounce", "oz", "盎司"],
    "美元": ["元/克", "元每克", "人民币/克", "cny/g", "cny per gram"],
}


def _extract_keywords(text: str) -> List[str]:
    raw = str(text or "").strip().lower()
    if not raw:
        return []
    tokens: List[str] = []
    for item in re.findall(r"[a-z0-9_./-]{3,}", raw):
        value = str(item or "").strip().lower()
        if not value or value in _STOP_WORDS:
            continue
        tokens.append(value)
    for block in re.findall(r"[\u4e00-\u9fff]{2,12}", raw):
        text_block = str(block or "").strip()
        if not text_block:
            continue
        if text_block not in _STOP_WORDS:
            tokens.append(text_block)
        max_len = min(4, len(text_block))
        for n in range(2, max_len + 1):
            for idx in range(0, len(text_block) - n + 1):
                piece = text_block[idx : idx + n]
                if piece in _STOP_WORDS:
                    continue
                tokens.append(piece)
    out: List[str] = []
    seen: Set[str] = set()
    for token in sorted(tokens, key=len, reverse=True):
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
        if len(out) >= 24:
            break
    return out


def _collect_text_fragments(title: str, action_type: str, result: Dict, visible_content: str, context: Optional[Dict]) -> str:
    parts: List[str] = [str(title or ""), str(action_type or ""), str(visible_content or "")]
    if isinstance(result, dict) and result:
        try:
            parts.append(json.dumps(result, ensure_ascii=False, sort_keys=True))
        except Exception:
            parts.append(str(result))
    if isinstance(context, dict):
        for key in (
            "latest_external_url",
            "latest_parse_input_text",
            "latest_script_json_output",
            "latest_task_output_content",
        ):
            value = context.get(key)
            if value is None:
                continue
            try:
                parts.append(json.dumps(value, ensure_ascii=False, sort_keys=True))
            except Exception:
                parts.append(str(value))
        artifacts = context.get("latest_shell_artifacts")
        if isinstance(artifacts, list):
            parts.extend(str(item or "") for item in artifacts if str(item or "").strip())
    return "\n".join(part for part in parts if str(part or "").strip()).lower()


def _extract_requested_file_type(message: str) -> str:
    lowered = str(message or "").lower()
    for item in _FILE_TYPES:
        if item in lowered:
            return item
    return ""


def _collect_artifact_like_strings(result: Dict, context: Optional[Dict]) -> List[str]:
    items: List[str] = []

    def _walk(value: object) -> None:
        if isinstance(value, dict):
            for nested in value.values():
                _walk(nested)
            return
        if isinstance(value, list):
            for nested in value:
                _walk(nested)
            return
        text = str(value or "").strip()
        if not text:
            return
        lowered = text.lower()
        if any(lowered.endswith(f".{ext}") for ext in _FILE_TYPES):
            items.append(text)
            return
        if "/" in text or "\\" in text:
            items.append(text)

    _walk(result)
    if isinstance(context, dict):
        _walk(context.get("latest_shell_artifacts"))
    out: List[str] = []
    for item in items:
        if item not in out:
            out.append(item)
    return out[:10]


def extract_task_requirements(message: str) -> Dict:
    text = str(message or "").strip().lower()
    file_type = _extract_requested_file_type(text)
    unit = ""
    if "元/克" in text or "元每克" in text or "人民币/克" in text or "cny/g" in text:
        unit = "元/克"
    elif "usd/oz" in text or "美元/盎司" in text or "美元每盎司" in text:
        unit = "美元/盎司"
    elif "usd" in text or "美元" in text:
        unit = "美元"
    time_range = ""
    for marker in ("最近三个月", "最近一个月", "最近一年", "近三个月", "近一个月", "近一年"):
        if marker in text:
            time_range = marker
            break
    needs_tabular = bool(file_type in {"csv", "xlsx"} or any(token in text for token in ("表格", "按天", "按月", "列", "csv")))
    return {
        "file_type": file_type,
        "unit": unit,
        "time_range": time_range,
        "needs_tabular": needs_tabular,
    }


def summarize_task_grounding_for_prompt(message: str) -> str:
    raw_message = str(message or "").strip()
    requirements = extract_task_requirements(raw_message)
    lines: List[str] = []
    if raw_message:
        compact = re.sub(r"\s+", " ", raw_message)
        lines.append(f"- original_task={compact[:180]}")
    file_type = str(requirements.get("file_type") or "").strip().lower()
    if file_type:
        lines.append(f"- output_file_type={file_type}")
    unit = str(requirements.get("unit") or "").strip()
    if unit:
        lines.append(f"- unit={unit}")
    time_range = str(requirements.get("time_range") or "").strip()
    if time_range:
        lines.append(f"- time_range={time_range}")
    if bool(requirements.get("needs_tabular")):
        lines.append("- needs_tabular=yes")
    lines.append("- invariant_policy=允许更换来源、搜索词和实现路径，但不得改写用户要求的目标对象、单位、时间范围和最终产物格式")
    conflicts = _UNIT_CONFLICT_TERMS.get(unit) or []
    if conflicts:
        lines.append(f"- avoid_conflicting_units={', '.join(conflicts[:6])}")
    return "\n".join(lines) if lines else "(无)"


def detect_task_grounding_drift(*, message: str, plan_titles: List[str], plan_briefs: Optional[List[str]] = None) -> Optional[str]:
    requirements = extract_task_requirements(message)
    unit = str(requirements.get("unit") or "").strip()
    if not unit:
        return None
    combined = "\n".join([str(item or "") for item in list(plan_titles or []) + list(plan_briefs or [])]).lower()
    if not combined.strip():
        return None
    hits: List[str] = []
    for token in _UNIT_CONFLICT_TERMS.get(unit) or []:
        lowered = str(token or "").strip().lower()
        if lowered and lowered in combined and token not in hits:
            hits.append(token)
    if hits:
        return f"剩余计划改写了任务单位口径：用户要求 {unit}，但计划出现了 {', '.join(hits[:4])}"
    return None


def _score_requirement_matches(requirements: Dict, evidence_text: str, artifact_like: List[str]) -> tuple[int, List[str]]:
    score = 0
    matched: List[str] = []
    file_type = str(requirements.get("file_type") or "").strip().lower()
    if file_type and any(str(item).lower().endswith(f".{file_type}") for item in artifact_like):
        score += 20
        matched.append(f"file_type:{file_type}")
    unit = str(requirements.get("unit") or "").strip()
    if unit and unit.lower() in evidence_text:
        score += 18
        matched.append(f"unit:{unit}")
    time_range = str(requirements.get("time_range") or "").strip()
    if time_range and time_range in evidence_text:
        score += 12
        matched.append(f"time_range:{time_range}")
    if bool(requirements.get("needs_tabular")) and ("csv" in evidence_text or "列" in evidence_text or "header" in evidence_text):
        score += 10
        matched.append("tabular")
    return score, matched


def evaluate_goal_progress(
    *,
    message: str,
    title: str,
    action_type: str,
    result: Optional[Dict],
    error_message: str,
    visible_content: str,
    context: Optional[Dict],
    previous_score: Optional[int] = None,
) -> Dict:
    result_obj = result if isinstance(result, dict) else {}
    evidence_text = _collect_text_fragments(title, action_type, result_obj, visible_content, context)
    keywords = _extract_keywords(message)
    matched = [token for token in keywords if token and token in evidence_text]
    artifact_like = _collect_artifact_like_strings(result_obj, context)
    requirements = extract_task_requirements(message)
    requested_file_type = _extract_requested_file_type(message)
    matched_file_type = bool(requested_file_type and any(str(item).lower().endswith(f".{requested_file_type}") for item in artifact_like))
    has_visible_content = bool(str(visible_content or "").strip())
    has_external_evidence = bool(str(result_obj.get("output") or result_obj.get("response") or "").strip())
    if not has_external_evidence and isinstance(context, dict):
        has_external_evidence = bool(str(context.get("latest_external_url") or "").strip())

    requirement_score, matched_requirements = _score_requirement_matches(requirements, evidence_text, artifact_like)
    overlap_score = min(45, len(matched) * 12)
    artifact_score = min(25, len(artifact_like) * 10)
    visible_score = 20 if has_visible_content else 0
    evidence_score = 10 if has_external_evidence else 0
    format_score = 15 if matched_file_type else 0
    score = int(max(0, min(100, overlap_score + artifact_score + visible_score + evidence_score + format_score + requirement_score)))

    previous = int(coerce_int(previous_score, default=-1)) if previous_score is not None else -1
    if str(error_message or "").strip():
        state = "regressed" if previous > 0 else "none"
        score = max(0, min(score, max(0, previous - 15) if previous >= 0 else 0))
        reason = "当前步骤失败，未形成可验证目标进展。"
    elif matched_file_type and (has_visible_content or artifact_like) and len(matched) >= 1:
        state = "complete" if score >= 70 else "strong"
        score = max(score, 85 if state == "complete" else 65)
        reason = "已生成与任务要求匹配的目标格式产物。"
    elif score >= 60:
        state = "strong"
        reason = "已拿到较强证据，接近最终交付。"
    elif score >= 25:
        state = "partial"
        reason = "已有部分有效证据，但尚未满足最终交付条件。"
    else:
        state = "none"
        reason = "当前步骤尚未提供足够的目标相关证据。"

    return {
        "state": state,
        "score": int(score),
        "reason": reason,
        "matched_keywords": matched[:8],
        "matched_requirements": matched_requirements[:8],
        "task_requirements": requirements,
        "matched_keyword_count": len(matched),
        "artifact_like": artifact_like,
        "requested_file_type": requested_file_type,
        "matched_file_type": matched_file_type,
    }
