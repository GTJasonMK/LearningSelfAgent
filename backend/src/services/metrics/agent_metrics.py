from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from backend.src.common.utils import extract_json_object, now_iso
from backend.src.storage import get_connection


def _chunked(values: List[int], chunk_size: int = 900) -> List[List[int]]:
    out: List[List[int]] = []
    if chunk_size <= 0:
        chunk_size = 900
    for i in range(0, len(values), chunk_size):
        out.append(values[i : i + chunk_size])
    return out


def _resolve_since_iso(*, since_days: int) -> Tuple[int, Optional[str]]:
    try:
        days = int(since_days)
    except Exception:
        days = 30
    if days <= 0:
        days = 30
    try:
        now_dt = datetime.fromisoformat(now_iso().replace("Z", "+00:00"))
        since_dt = now_dt - timedelta(days=int(days))
        since = since_dt.isoformat().replace("+00:00", "Z")
    except Exception:
        since = None
    return days, since


def _classify_distill_block_reason(*, distill_status: str, distill_notes: str) -> str:
    """
    将“未自动沉淀”的原因做粗粒度归类（用于趋势观测）。

    说明：
    - 当前仅基于 distill_notes 的关键词做启发式分类；
    - 目标是“可观测性”，而不是严格审计（审计请以 review 记录原文为准）。
    """
    ds = str(distill_status or "").strip().lower()
    notes = str(distill_notes or "")

    if "缺少可定位 evidence_refs" in notes:
        return "missing_evidence_refs"
    if "distill_score 未达门槛" in notes:
        return "score_below_threshold"
    if "缺少 distill.status" in notes:
        return "missing_distill_status"

    # deny 在 pass 场景下通常意味着 evaluator 主观拒绝沉淀（一次性/不稳定）
    if ds == "deny":
        return "evaluator_denied"

    return "other"


def compute_agent_metrics(*, since_days: int = 30) -> dict:
    """
    聚合 Agent 运行指标（P3：可观测性）。

    指标口径（简化但可用）：
    - runs：task_runs.summary LIKE 'agent_%' 的记录
    - success_rate：done / (done + failed)，忽略 stopped/waiting（它们代表“可继续/中断”）
    - avg_steps：task_steps 数量均值
    - avg_tokens：llm_records.tokens_total 均值
    - avg_replan_attempts：agent_state.replan_attempts（do）
    - avg_reflection_count：agent_state.reflection_count（think）
    - reuse_rate：tool_call_records.reuse / tool_call_records 总量
    - reuse_pass_rate：reuse_status=pass / (pass+fail)
    """
    days, since = _resolve_since_iso(since_days=since_days)

    with get_connection() as conn:
        where = "summary LIKE 'agent_%'"
        params: List = []
        if since:
            where += " AND created_at >= ?"
            params.append(str(since))

        run_rows = conn.execute(
            f"SELECT id, status, agent_state, created_at FROM task_runs WHERE {where} ORDER BY id ASC",
            params,
        ).fetchall()

        run_ids: List[int] = []
        runs: List[dict] = []
        for row in run_rows or []:
            try:
                rid = int(row["id"])
            except Exception:
                continue
            if rid <= 0:
                continue
            run_ids.append(rid)
            state_obj = extract_json_object(row["agent_state"] or "") or {}
            mode = str(state_obj.get("mode") or "").strip().lower() or "do"
            replan_attempts = 0
            reflection_count = 0
            try:
                replan_attempts = int(state_obj.get("replan_attempts") or 0)
            except Exception:
                replan_attempts = 0
            try:
                reflection_count = int(state_obj.get("reflection_count") or 0)
            except Exception:
                reflection_count = 0
            runs.append(
                {
                    "run_id": rid,
                    "status": str(row["status"] or "").strip(),
                    "mode": mode,
                    "replan_attempts": replan_attempts,
                    "reflection_count": reflection_count,
                }
            )

        # step counts（分块避免超出 SQLite 变量上限）
        step_counts: Dict[int, int] = {}
        token_sums: Dict[int, int] = {}
        if run_ids:
            for chunk in _chunked(run_ids):
                placeholders = ",".join(["?"] * len(chunk))
                rows = conn.execute(
                    f"SELECT run_id, COUNT(*) AS c FROM task_steps WHERE run_id IN ({placeholders}) GROUP BY run_id",
                    chunk,
                ).fetchall()
                for r in rows or []:
                    try:
                        step_counts[int(r["run_id"])] = int(r["c"] or 0)
                    except Exception:
                        continue

                rows = conn.execute(
                    f"SELECT run_id, COALESCE(SUM(tokens_total), 0) AS t FROM llm_records WHERE run_id IN ({placeholders}) GROUP BY run_id",
                    chunk,
                ).fetchall()
                for r in rows or []:
                    try:
                        token_sums[int(r["run_id"])] = int(r["t"] or 0)
                    except Exception:
                        continue

        # tool reuse stats（时间窗内）
        if since:
            tool_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS calls,
                    COALESCE(SUM(reuse), 0) AS reuse_calls,
                    COALESCE(SUM(CASE WHEN reuse_status = 'pass' THEN 1 ELSE 0 END), 0) AS pass_calls,
                    COALESCE(SUM(CASE WHEN reuse_status = 'fail' THEN 1 ELSE 0 END), 0) AS fail_calls
                FROM tool_call_records
                WHERE created_at >= ?
                """,
                (str(since),),
            ).fetchone()
        else:
            tool_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS calls,
                    COALESCE(SUM(reuse), 0) AS reuse_calls,
                    COALESCE(SUM(CASE WHEN reuse_status = 'pass' THEN 1 ELSE 0 END), 0) AS pass_calls,
                    COALESCE(SUM(CASE WHEN reuse_status = 'fail' THEN 1 ELSE 0 END), 0) AS fail_calls
                FROM tool_call_records
                """,
            ).fetchone()

        calls = int(tool_row["calls"] or 0) if tool_row else 0
        reuse_calls = int(tool_row["reuse_calls"] or 0) if tool_row else 0
        reuse_rate = (float(reuse_calls) / float(calls)) if calls else 0.0
        pass_calls = int(tool_row["pass_calls"] or 0) if tool_row else 0
        fail_calls = int(tool_row["fail_calls"] or 0) if tool_row else 0
        denom = pass_calls + fail_calls
        reuse_pass_rate = (float(pass_calls) / float(denom)) if denom else 0.0

        # review stats（时间窗内）
        if since:
            review_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COALESCE(SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END), 0) AS pass_count,
                    COALESCE(SUM(CASE WHEN status = 'pass' AND distill_status = 'allow' THEN 1 ELSE 0 END), 0) AS distill_allow_count,
                    COALESCE(SUM(CASE WHEN status = 'pass' AND distill_status = 'manual' THEN 1 ELSE 0 END), 0) AS distill_manual_count,
                    COALESCE(SUM(CASE WHEN status = 'pass' AND distill_status = 'deny' THEN 1 ELSE 0 END), 0) AS distill_deny_count,
                    COALESCE(SUM(CASE WHEN status = 'pass' AND distill_status = 'allow' AND distill_evidence_refs IS NOT NULL AND TRIM(distill_evidence_refs) NOT IN ('', '[]', 'null') THEN 1 ELSE 0 END), 0) AS distill_allow_with_evidence_count
                FROM agent_review_records
                WHERE created_at >= ?
                """,
                (str(since),),
            ).fetchone()
        else:
            review_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COALESCE(SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END), 0) AS pass_count,
                    COALESCE(SUM(CASE WHEN status = 'pass' AND distill_status = 'allow' THEN 1 ELSE 0 END), 0) AS distill_allow_count,
                    COALESCE(SUM(CASE WHEN status = 'pass' AND distill_status = 'manual' THEN 1 ELSE 0 END), 0) AS distill_manual_count,
                    COALESCE(SUM(CASE WHEN status = 'pass' AND distill_status = 'deny' THEN 1 ELSE 0 END), 0) AS distill_deny_count,
                    COALESCE(SUM(CASE WHEN status = 'pass' AND distill_status = 'allow' AND distill_evidence_refs IS NOT NULL AND TRIM(distill_evidence_refs) NOT IN ('', '[]', 'null') THEN 1 ELSE 0 END), 0) AS distill_allow_with_evidence_count
                FROM agent_review_records
                """,
            ).fetchone()

        review_total = int(review_row["total"] or 0) if review_row else 0
        review_pass = int(review_row["pass_count"] or 0) if review_row else 0
        distill_allow = int(review_row["distill_allow_count"] or 0) if review_row else 0
        distill_manual = int(review_row["distill_manual_count"] or 0) if review_row else 0
        distill_deny = int(review_row["distill_deny_count"] or 0) if review_row else 0
        distill_allow_with_evidence = (
            int(review_row["distill_allow_with_evidence_count"] or 0) if review_row else 0
        )
        distill_rate = (float(distill_allow) / float(review_pass)) if review_pass else 0.0
        evidence_coverage = (float(distill_allow_with_evidence) / float(distill_allow)) if distill_allow else 0.0

        # distill block reasons（仅在 pass 场景观察：任务完成但不沉淀）
        if since:
            rows = conn.execute(
                """
                SELECT distill_status, distill_notes
                FROM agent_review_records
                WHERE status = 'pass' AND created_at >= ? AND distill_status IN ('manual', 'deny')
                """,
                (str(since),),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT distill_status, distill_notes
                FROM agent_review_records
                WHERE status = 'pass' AND distill_status IN ('manual', 'deny')
                """,
            ).fetchall()

        distill_block_reasons: Dict[str, int] = {}
        for r in rows or []:
            reason = _classify_distill_block_reason(
                distill_status=str(r["distill_status"] or ""),
                distill_notes=str(r["distill_notes"] or ""),
            )
            distill_block_reasons[reason] = int(distill_block_reasons.get(reason, 0)) + 1

    # runs 聚合
    by_status: Dict[str, int] = {}
    by_mode: Dict[str, int] = {}
    done = 0
    failed = 0
    stopped = 0
    waiting = 0
    total_steps = 0
    total_tokens = 0
    total_replan = 0
    total_reflection = 0

    for r in runs:
        status = str(r.get("status") or "").strip().lower() or "unknown"
        by_status[status] = by_status.get(status, 0) + 1
        mode = str(r.get("mode") or "").strip().lower() or "do"
        by_mode[mode] = by_mode.get(mode, 0) + 1

        if status == "done":
            done += 1
        elif status == "failed":
            failed += 1
        elif status == "stopped":
            stopped += 1
        elif status == "waiting":
            waiting += 1

        rid = int(r.get("run_id") or 0)
        total_steps += int(step_counts.get(rid) or 0)
        total_tokens += int(token_sums.get(rid) or 0)
        total_replan += int(r.get("replan_attempts") or 0)
        total_reflection += int(r.get("reflection_count") or 0)

    denom_success = done + failed
    success_rate = (float(done) / float(denom_success)) if denom_success else 0.0

    total_runs = len(runs)
    avg_steps = (float(total_steps) / float(total_runs)) if total_runs else 0.0
    avg_tokens = (float(total_tokens) / float(total_runs)) if total_runs else 0.0
    avg_replan_attempts = (float(total_replan) / float(total_runs)) if total_runs else 0.0
    avg_reflection_count = (float(total_reflection) / float(total_runs)) if total_runs else 0.0

    return {
        "ok": True,
        "since_days": int(days),
        "since": since,
        "at": now_iso(),
        "runs": {
            "total": int(total_runs),
            "by_status": by_status,
            "by_mode": by_mode,
            "done": int(done),
            "failed": int(failed),
            "stopped": int(stopped),
            "waiting": int(waiting),
            "success_rate": round(success_rate, 4),
            "avg_steps": round(avg_steps, 4),
            "avg_tokens_total": round(avg_tokens, 4),
            "avg_replan_attempts": round(avg_replan_attempts, 4),
            "avg_reflection_count": round(avg_reflection_count, 4),
        },
        "tool_calls": {
            "calls": int(calls),
            "reuse_calls": int(reuse_calls),
            "reuse_rate": round(reuse_rate, 4),
            "reuse_pass_calls": int(pass_calls),
            "reuse_fail_calls": int(fail_calls),
            "reuse_pass_rate": round(reuse_pass_rate, 4),
        },
        "reviews": {
            "total": int(review_total),
            "pass": int(review_pass),
            "distill_allow": int(distill_allow),
            "distill_rate_among_pass": round(distill_rate, 4),
            "distill_allow_with_evidence": int(distill_allow_with_evidence),
            "distill_evidence_coverage_among_allow": round(evidence_coverage, 4),
            "distill_manual": int(distill_manual),
            "distill_deny": int(distill_deny),
            "distill_block_reasons_among_pass": distill_block_reasons,
        },
    }
