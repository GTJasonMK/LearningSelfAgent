from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

from backend.src.common.utils import truncate_text
from backend.src.constants import (
    AGENT_REVIEW_DISTILL_STATUS_ALLOW,
    AGENT_REVIEW_DISTILL_STATUS_DENY,
    AGENT_REVIEW_DISTILL_STATUS_MANUAL,
)


def _append_note(existing: str, note: str) -> str:
    """
    拼接 distill_notes，避免重复追加同一条门槛说明。
    """
    base = str(existing or "").strip()
    n = str(note or "").strip()
    if not n:
        return base
    if n in base:
        return base
    return f"{base}；{n}" if base else n


def filter_evidence_refs(
    raw: object,
    *,
    valid_step_ids: Set[int],
    valid_output_ids: Set[int],
    valid_tool_call_record_ids: Set[int],
    valid_artifact_paths: Set[str],
    artifact_exists_by_path: Optional[Dict[str, bool]] = None,
    max_items: int = 8,
) -> List[dict]:
    """
    清洗证据引用（evidence_refs / distill.evidence_refs）。

    规则：
    - raw 必须是 list，否则返回空数组；
    - 仅允许 kind：step/output/tool_call/artifact；
    - step_id/output_id/tool_call_record_id/path 必须可解析且存在于有效集合，否则丢弃；
    - 限制最大条数 max_items，避免评估记录膨胀；
    - artifact 引用可额外补齐 exists（优先使用 artifact_exists_by_path 的真实检查结果）。
    """
    if not isinstance(raw, list):
        return []

    out: List[dict] = []
    for it in raw:
        if not isinstance(it, dict):
            continue
        kind = str(it.get("kind") or "").strip().lower()

        if kind == "step":
            try:
                sid = int(it.get("step_id"))
            except Exception:
                continue
            if sid not in valid_step_ids:
                continue
            ref = {"kind": "step", "step_id": sid}
            if it.get("step_order") is not None:
                try:
                    ref["step_order"] = int(it.get("step_order"))
                except Exception:
                    pass
            out.append(ref)

        elif kind == "output":
            try:
                oid = int(it.get("output_id"))
            except Exception:
                continue
            if oid not in valid_output_ids:
                continue
            out.append({"kind": "output", "output_id": oid})

        elif kind == "tool_call":
            try:
                cid = int(it.get("tool_call_record_id"))
            except Exception:
                continue
            if cid not in valid_tool_call_record_ids:
                continue
            out.append({"kind": "tool_call", "tool_call_record_id": cid})

        elif kind == "artifact":
            path = str(it.get("path") or "").strip()
            if not path:
                continue
            if path not in valid_artifact_paths:
                continue
            ref = {"kind": "artifact", "path": path}

            exists_value: Optional[bool] = None
            if isinstance(artifact_exists_by_path, dict) and path in artifact_exists_by_path:
                exists_value = bool(artifact_exists_by_path.get(path))
            elif isinstance(it.get("exists"), bool):
                exists_value = bool(it.get("exists"))

            if exists_value is not None:
                ref["exists"] = bool(exists_value)
            out.append(ref)

        if len(out) >= int(max_items):
            break

    return out


def normalize_issues(
    raw: object,
    *,
    valid_step_ids: Set[int],
    valid_output_ids: Set[int],
    valid_tool_call_record_ids: Set[int],
    valid_artifact_paths: Set[str],
    artifact_exists_by_path: Optional[Dict[str, bool]] = None,
    max_items: int = 50,
) -> List[dict]:
    """
    归一化 issues：
    - 清洗 evidence_refs；
    - 截断 evidence_quote；
    - 保留最多 max_items 条。
    """
    if not isinstance(raw, list):
        return []

    normalized: List[dict] = []
    for issue in raw:
        if not isinstance(issue, dict):
            continue

        refs = filter_evidence_refs(
            issue.get("evidence_refs"),
            valid_step_ids=valid_step_ids,
            valid_output_ids=valid_output_ids,
            valid_tool_call_record_ids=valid_tool_call_record_ids,
            valid_artifact_paths=valid_artifact_paths,
            artifact_exists_by_path=artifact_exists_by_path,
            max_items=8,
        )

        item = dict(issue)
        item["evidence_refs"] = refs

        quote = str(item.get("evidence_quote") or item.get("evidence") or "").strip()
        if quote:
            item["evidence_quote"] = truncate_text(quote, 120)
        normalized.append(item)
        if len(normalized) >= int(max_items):
            break

    return normalized


def apply_distill_gate(
    *,
    review_status: str,
    pass_score: Optional[float],
    distill_status: str,
    distill_score: Optional[float],
    distill_threshold: float,
    distill_notes: str,
    distill_evidence_refs: List[dict],
) -> Tuple[str, float, str]:
    """
    应用知识沉淀门槛（distill gate）。

    规则（与 docs/agent 对齐）：
    - review_status != pass：强制 distill=deny（不自动沉淀）；
    - distill.status 缺失/非法：默认 distill=manual（不自动沉淀）并写明原因；
    - distill=allow 但 distill_score < distill_threshold：降级 manual；
    - distill=allow 但缺少可定位 distill_evidence_refs：降级 manual。

    返回：(distill_status, distill_score, distill_notes)
    """
    status = str(review_status or "").strip().lower()
    ds = str(distill_status or "").strip().lower()
    if ds not in {
        AGENT_REVIEW_DISTILL_STATUS_ALLOW,
        AGENT_REVIEW_DISTILL_STATUS_DENY,
        AGENT_REVIEW_DISTILL_STATUS_MANUAL,
    }:
        ds = ""

    score_value: float
    if distill_score is None:
        score_value = float(pass_score or 0.0) if status == "pass" else 0.0
    else:
        try:
            score_value = float(distill_score)
        except Exception:
            score_value = float(pass_score or 0.0) if status == "pass" else 0.0

    notes = str(distill_notes or "").strip()

    # 未通过任务评估：禁止自动沉淀
    if status != "pass":
        return AGENT_REVIEW_DISTILL_STATUS_DENY, 0.0, notes

    # pass 但 distill.status 缺失：默认不自动沉淀（manual）
    if not ds:
        ds = AGENT_REVIEW_DISTILL_STATUS_MANUAL
        notes = _append_note(notes, "缺少 distill.status：默认不自动沉淀")

    # allow 但未达阈值：默认不自动沉淀（manual）
    if ds == AGENT_REVIEW_DISTILL_STATUS_ALLOW:
        try:
            if float(score_value) < float(distill_threshold):
                ds = AGENT_REVIEW_DISTILL_STATUS_MANUAL
                notes = _append_note(notes, "distill_score 未达门槛：默认不自动沉淀")
        except Exception:
            pass

    # allow 必须有可定位证据
    if ds == AGENT_REVIEW_DISTILL_STATUS_ALLOW and not (distill_evidence_refs or []):
        ds = AGENT_REVIEW_DISTILL_STATUS_MANUAL
        notes = _append_note(notes, "distill 缺少可定位 evidence_refs：默认不自动沉淀")

    return ds, float(score_value), notes

