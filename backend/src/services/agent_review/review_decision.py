from __future__ import annotations

from typing import Callable, List, Optional

from backend.src.common.utils import truncate_text
from backend.src.constants import (
    AGENT_REVIEW_DISTILL_SCORE_THRESHOLD,
    AGENT_REVIEW_DISTILL_STATUS_ALLOW,
    AGENT_REVIEW_DISTILL_STATUS_DENY,
    AGENT_REVIEW_DISTILL_STATUS_MANUAL,
    AGENT_REVIEW_PASS_SCORE_THRESHOLD,
)
from backend.src.services.agent_review.review_normalize import (
    apply_distill_gate,
    filter_evidence_refs,
    normalize_issues,
)


def evaluate_review_decision(
    *,
    obj: object,
    err: Optional[str],
    raw_text: str,
    step_rows: List[dict],
    output_rows: List[dict],
    tool_rows: List[dict],
    plan_artifacts: List[str],
    artifacts_check_items: List[dict],
    find_unverified_text_output_fn: Callable[[List[dict]], Optional[dict]],
) -> dict:
    def _coerce_score(value: object) -> Optional[float]:
        try:
            fv = float(value)  # type: ignore[arg-type]
        except Exception:
            return None
        if fv < 0:
            fv = 0.0
        if fv > 100:
            fv = 100.0
        return float(fv)

    def _normalize_distill_status(value: object) -> str:
        v = str(value or "").strip().lower()
        if v in {
            AGENT_REVIEW_DISTILL_STATUS_ALLOW,
            AGENT_REVIEW_DISTILL_STATUS_DENY,
            AGENT_REVIEW_DISTILL_STATUS_MANUAL,
        }:
            return v
        return ""

    pass_threshold = float(AGENT_REVIEW_PASS_SCORE_THRESHOLD)
    distill_threshold = float(AGENT_REVIEW_DISTILL_SCORE_THRESHOLD)
    pass_score: Optional[float] = None
    distill_score: Optional[float] = None
    distill_status = ""
    distill_notes = ""
    distill_evidence_refs: List[dict] = []
    raw_distill_evidence_refs: object = None

    if not isinstance(obj, dict):
        status = "fail"
        summary = f"评估失败：{err or 'invalid_json'}"
        issues = [
            {
                "title": "评估失败",
                "severity": "high",
                "details": "Eval Agent 未能生成有效 JSON（可能是 LLM 配置/网络/提示词/返回格式问题）。",
                "evidence_quote": truncate_text(str(err or raw_text or ""), 260),
                "evidence_refs": [],
                "suggestion": "检查设置页的 LLM 配置（API Key/Base URL/Model），并在桌宠中用 /eval run_id 复现错误。",
            }
        ]
        next_actions = [{"title": "修复评估链路", "details": "确认 /agent/evaluate/stream 可用并能写入评估记录。"}]
        pass_score = 0.0
        distill_status = AGENT_REVIEW_DISTILL_STATUS_DENY
        distill_score = 0.0
        distill_notes = "评估 JSON 无效：禁止自动沉淀"
    else:
        status = str(obj.get("status") or "").strip() or "needs_changes"
        normalized_status = str(status or "").strip().lower()
        if normalized_status not in {"pass", "needs_changes", "fail"}:
            normalized_status = "needs_changes"
        status = normalized_status

        summary = str(obj.get("summary") or "").strip()
        issues = obj.get("issues") if isinstance(obj.get("issues"), list) else []
        next_actions = obj.get("next_actions") if isinstance(obj.get("next_actions"), list) else []

        pass_score = _coerce_score(obj.get("pass_score"))
        pass_threshold_value = _coerce_score(obj.get("pass_threshold"))
        if pass_threshold_value is not None:
            pass_threshold = float(pass_threshold_value)

        distill_payload = obj.get("distill")
        if isinstance(distill_payload, dict):
            distill_status = _normalize_distill_status(distill_payload.get("status"))
            distill_score = _coerce_score(distill_payload.get("score"))
            distill_threshold_value = _coerce_score(distill_payload.get("threshold"))
            if distill_threshold_value is not None:
                distill_threshold = float(distill_threshold_value)
            distill_notes = str(
                distill_payload.get("reason") or distill_payload.get("notes") or ""
            ).strip()
            raw_distill_evidence_refs = distill_payload.get("evidence_refs")

        if not distill_status:
            distill_status = _normalize_distill_status(obj.get("distill_status"))
        if distill_score is None:
            distill_score = _coerce_score(obj.get("distill_score"))
        if not distill_notes:
            distill_notes = str(obj.get("distill_notes") or "").strip()

        if pass_score is None:
            if status == "pass":
                pass_score = 100.0
            elif status == "needs_changes":
                pass_score = 70.0
            else:
                pass_score = 0.0

        # 兜底一致性：status=pass 但 score 未达门槛时，降级为 needs_changes
        if status == "pass" and pass_score is not None and pass_score < pass_threshold:
            status = "needs_changes"
            if not summary:
                summary = "评分未达标：需补齐验证与修复后再交付。"

        # 结果质量提示：若最终文本缺少可验证证据，追加高优先级问题，但不硬性否决评估结果。
        unverified_output = find_unverified_text_output_fn(output_rows)
        if isinstance(unverified_output, dict):
            gate_note = "最终输出缺少可验证证据：建议补齐 step/tool/artifact 证据后再交付。"
            if summary and gate_note not in summary:
                summary = f"{summary}（{gate_note}）"
            elif not summary:
                summary = gate_note

            evidence_refs = []
            output_id = unverified_output.get("output_id")
            if output_id is not None:
                evidence_refs.append({"kind": "output", "output_id": int(output_id)})

            issue_payload = {
                "title": "最终输出缺少可验证证据",
                "severity": "high",
                "details": "最终输出缺少可定位的 step/tool/artifact 证据链，可能导致结果与实际执行不一致。",
                "evidence_refs": evidence_refs,
                "evidence_quote": str(unverified_output.get("content_preview") or ""),
                "suggestion": "在输出最终结论前，先执行可验证步骤并附上证据引用。",
            }
            existing_issues = issues if isinstance(issues, list) else []
            issues = [issue_payload]
            issues.extend(existing_issues)

        valid_step_ids = set()
        valid_output_ids = set()
        valid_tool_call_ids = set()
        try:
            valid_step_ids = {int(r["id"]) for r in (step_rows or []) if r and r["id"] is not None}
        except Exception:
            valid_step_ids = set()
        try:
            valid_output_ids = {int(r["id"]) for r in (output_rows or []) if r and r["id"] is not None}
        except Exception:
            valid_output_ids = set()
        try:
            valid_tool_call_ids = {int(r["id"]) for r in (tool_rows or []) if r and r["id"] is not None}
        except Exception:
            valid_tool_call_ids = set()
        artifact_paths = set()
        try:
            artifact_paths = {str(x).strip() for x in (plan_artifacts or []) if str(x).strip()}
        except Exception:
            artifact_paths = set()

        artifact_exists_by_path: dict[str, bool] = {}
        for it in artifacts_check_items or []:
            if not isinstance(it, dict):
                continue
            p = str(it.get("path") or "").strip()
            if not p:
                continue
            exists_value = it.get("exists")
            if isinstance(exists_value, bool):
                artifact_exists_by_path[p] = bool(exists_value)

        distill_evidence_refs = filter_evidence_refs(
            raw_distill_evidence_refs,
            valid_step_ids=valid_step_ids,
            valid_output_ids=valid_output_ids,
            valid_tool_call_record_ids=valid_tool_call_ids,
            valid_artifact_paths=artifact_paths,
            artifact_exists_by_path=artifact_exists_by_path,
            max_items=8,
        )
        distill_status, distill_score, distill_notes = apply_distill_gate(
            review_status=status,
            pass_score=pass_score,
            distill_status=distill_status,
            distill_score=distill_score,
            distill_threshold=distill_threshold,
            distill_notes=distill_notes,
            distill_evidence_refs=distill_evidence_refs,
        )

        issues = normalize_issues(
            issues,
            valid_step_ids=valid_step_ids,
            valid_output_ids=valid_output_ids,
            valid_tool_call_record_ids=valid_tool_call_ids,
            valid_artifact_paths=artifact_paths,
            artifact_exists_by_path=artifact_exists_by_path,
            max_items=50,
        )

    return {
        "status": status,
        "summary": summary,
        "issues": issues,
        "next_actions": next_actions,
        "pass_score": pass_score,
        "pass_threshold": pass_threshold,
        "distill_status": distill_status,
        "distill_score": distill_score,
        "distill_threshold": distill_threshold,
        "distill_notes": distill_notes,
        "distill_evidence_refs": distill_evidence_refs,
    }
