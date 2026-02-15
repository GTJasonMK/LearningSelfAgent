from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter

from backend.src.api.utils import error_response, parse_json_value
from backend.src.constants import (
    ERROR_CODE_INVALID_REQUEST,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_RECORD_NOT_FOUND,
    HTTP_STATUS_BAD_REQUEST,
    HTTP_STATUS_NOT_FOUND,
    RUN_STATUS_RUNNING,
    RUN_STATUS_WAITING,
)
from backend.src.repositories.task_runs_repo import (
    fetch_agent_run_with_task_title_by_statuses,
    fetch_latest_agent_run_with_task_title,
    get_task_run_with_task_title,
)
from backend.src.storage import get_connection

router = APIRouter()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    """
    解析 now_iso() 生成的时间戳（以 Z 结尾），或 sqlite 中已有的 ISO 字符串。
    """
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _row_to_run_meta(row, task_title: str, is_current: bool) -> dict:
    keys = set(row.keys()) if hasattr(row, "keys") else set()
    return {
        "run_id": _safe_int(row["id"]),
        "task_id": _safe_int(row["task_id"]),
        "task_title": task_title,
        "status": row["status"],
        "summary": row["summary"],
        "mode": row["mode"] if "mode" in keys else None,
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "is_current": bool(is_current),
    }


def _compute_plan_snapshot(*, agent_plan: Optional[dict], agent_state: Optional[dict]) -> dict:
    """
    为前端 UI 生成“计划快照”（不依赖 task_steps 表）。

    说明：
    - plan_items.status 来自 runner 在执行过程中的 plan_delta 更新 + 落库；
    - 该快照的目标是“更好展示进度”，不是严格审计（审计以 task_steps 为准）。
    """
    plan_obj = agent_plan if isinstance(agent_plan, dict) else {}
    titles = plan_obj.get("titles") if isinstance(plan_obj.get("titles"), list) else []
    items = plan_obj.get("items") if isinstance(plan_obj.get("items"), list) else []
    allows = plan_obj.get("allows") if isinstance(plan_obj.get("allows"), list) else []

    total = len(titles)
    by_status: dict[str, int] = {}

    def _norm_status(value: Any) -> str:
        raw = str(value or "").strip().lower()
        if not raw:
            return "pending"
        if raw in {"planned", "queued"}:
            return "pending"
        return raw

    for raw in items:
        it = raw if isinstance(raw, dict) else {}
        st = _norm_status(it.get("status"))
        by_status[st] = int(by_status.get(st, 0)) + 1

    done = int(by_status.get("done", 0))
    running = int(by_status.get("running", 0))
    waiting = int(by_status.get("waiting", 0))
    failed = int(by_status.get("failed", 0))
    skipped = int(by_status.get("skipped", 0))
    pending = int(by_status.get("pending", 0) + by_status.get("planned", 0))

    # 当前步骤：优先从 agent_state.step_order 读取（1-based）；缺失时根据 plan_items.status 推断。
    step_order = None
    if isinstance(agent_state, dict):
        try:
            step_order = int(agent_state.get("step_order") or 0)
        except Exception:
            step_order = None
    if step_order is not None and step_order <= 0:
        step_order = None
    if step_order is None:
        # 推断规则：
        # 1) running -> waiting -> 第一个非终态（done/failed/skipped/stopped/cancelled 以外）
        for wanted in ("running", "waiting"):
            for idx, raw in enumerate(items or []):
                it = raw if isinstance(raw, dict) else {}
                if _norm_status(it.get("status")) == wanted:
                    step_order = int(idx) + 1
                    break
            if step_order is not None:
                break
    if step_order is None:
        for idx, raw in enumerate(items or []):
            it = raw if isinstance(raw, dict) else {}
            st = _norm_status(it.get("status"))
            if st in {"done", "failed", "skipped", "stopped", "cancelled"}:
                continue
            step_order = int(idx) + 1
            break

    cur = None
    if step_order is not None and step_order >= 1:
        idx = int(step_order) - 1
        title = str(titles[idx]).strip() if 0 <= idx < len(titles) else ""
        item = items[idx] if 0 <= idx < len(items) and isinstance(items[idx], dict) else {}
        status = str(item.get("status") or "").strip() if isinstance(item, dict) else ""
        brief = str(item.get("brief") or "").strip() if isinstance(item, dict) else ""
        allow_list = allows[idx] if 0 <= idx < len(allows) and isinstance(allows[idx], list) else []

        # think: 可从 executor_assignments 里补齐 executor
        executor = None
        if isinstance(agent_state, dict) and isinstance(agent_state.get("executor_assignments"), list):
            for row in agent_state.get("executor_assignments") or []:
                if not isinstance(row, dict):
                    continue
                if _safe_int(row.get("step_order"), 0) == int(step_order):
                    executor = str(row.get("executor") or "").strip() or None
                    break

        cur = {
            "step_order": int(step_order),
            "title": title or None,
            "brief": brief or None,
            "status": status or None,
            "allow": list(allow_list or []),
            "executor": executor,
        }

    progress = (float(done) / float(total)) if total > 0 else 0.0
    return {
        "total": int(total),
        "done": int(done),
        "running": int(running),
        "waiting": int(waiting),
        "failed": int(failed),
        "skipped": int(skipped),
        "pending": int(pending),
        "by_status": by_status,
        "progress": round(progress, 4),
        "current_step": cur,
    }


def _compute_run_counters(*, run_id: int) -> dict:
    """
    为前端 UI 生成“运行计数器”（基于 DB 聚合）。

    注意：
    - 这是“可观测性”用途：不追求所有字段都精确覆盖；
    - 查询应尽量轻量，适配主面板 1s 级轮询。
    """
    rid = int(run_id)
    if rid <= 0:
        return {"ok": False, "error": "invalid_run_id"}

    with get_connection() as conn:
        # task_steps：按状态计数
        step_rows = conn.execute(
            "SELECT status, COUNT(*) AS c FROM task_steps WHERE run_id = ? GROUP BY status",
            (int(rid),),
        ).fetchall()
        steps_by_status: dict[str, int] = {}
        step_total = 0
        for row in step_rows or []:
            st = str(row["status"] or "").strip().lower() or "unknown"
            c = _safe_int(row["c"], 0)
            steps_by_status[st] = c
            step_total += c

        last_failed = conn.execute(
            "SELECT id, step_order, title, error, finished_at FROM task_steps "
            "WHERE run_id = ? AND status = 'failed' ORDER BY id DESC LIMIT 1",
            (int(rid),),
        ).fetchone()
        last_error = None
        if last_failed:
            last_error = {
                "step_id": _safe_int(last_failed["id"]),
                "step_order": _safe_int(last_failed["step_order"]),
                "title": str(last_failed["title"] or ""),
                "error": str(last_failed["error"] or ""),
                "finished_at": str(last_failed["finished_at"] or "") or None,
            }

        # llm_records：调用次数与 tokens_total
        llm_row = conn.execute(
            "SELECT COUNT(*) AS calls, COALESCE(SUM(tokens_total), 0) AS tokens_total FROM llm_records WHERE run_id = ?",
            (int(rid),),
        ).fetchone()
        llm_calls = _safe_int(llm_row["calls"], 0) if llm_row else 0
        tokens_total = _safe_int(llm_row["tokens_total"], 0) if llm_row else 0

        # tool_call_records：调用次数与复用质量
        tool_row = conn.execute(
            """
            SELECT
                COUNT(*) AS calls,
                COALESCE(SUM(reuse), 0) AS reuse_calls,
                COALESCE(SUM(CASE WHEN reuse_status = 'pass' THEN 1 ELSE 0 END), 0) AS pass_calls,
                COALESCE(SUM(CASE WHEN reuse_status = 'fail' THEN 1 ELSE 0 END), 0) AS fail_calls
            FROM tool_call_records
            WHERE run_id = ?
            """,
            (int(rid),),
        ).fetchone()
        tool_calls = _safe_int(tool_row["calls"], 0) if tool_row else 0
        reuse_calls = _safe_int(tool_row["reuse_calls"], 0) if tool_row else 0
        pass_calls = _safe_int(tool_row["pass_calls"], 0) if tool_row else 0
        fail_calls = _safe_int(tool_row["fail_calls"], 0) if tool_row else 0

    denom = pass_calls + fail_calls
    reuse_pass_rate = (float(pass_calls) / float(denom)) if denom else 0.0

    return {
        "ok": True,
        "task_steps": {
            "total": int(step_total),
            "by_status": steps_by_status,
        },
        "llm": {
            "calls": int(llm_calls),
            "tokens_total": int(tokens_total),
        },
        "tools": {
            "calls": int(tool_calls),
            "reuse_calls": int(reuse_calls),
            "pass_calls": int(pass_calls),
            "fail_calls": int(fail_calls),
            "reuse_pass_rate": round(reuse_pass_rate, 4),
        },
        "last_error": last_error,
    }


@router.get("/agent/runs/current")
def get_current_agent_run() -> dict:
    """
    获取“当前正在运行/等待输入”的 Agent run（若没有则回退到最近一次 Agent run）。

    说明：
    - 这是给主面板“Agent 大脑/实时进度”用的轻量接口
    - 只返回 run 元信息，不返回 plan/state 细节（细节用 /agent/runs/{run_id} 获取）
    """
    row = fetch_agent_run_with_task_title_by_statuses(
        statuses=[RUN_STATUS_RUNNING, RUN_STATUS_WAITING],
        limit=1,
    )
    if row:
        return {"run": _row_to_run_meta(row, str(row["task_title"] or ""), True)}

    latest = fetch_latest_agent_run_with_task_title()
    if latest:
        return {"run": _row_to_run_meta(latest, str(latest["task_title"] or ""), False)}

    return {"run": None}


@router.get("/agent/runs/{run_id}")
def get_agent_run_detail(run_id: int) -> dict:
    """
    获取某次 Agent run 的 plan/state 细节，用于主面板展示“计划/观测/暂停点”。
    """
    try:
        rid = int(run_id)
    except Exception:
        rid = 0
    if rid <= 0:
        return error_response(ERROR_CODE_INVALID_REQUEST, "run_id 不合法", HTTP_STATUS_BAD_REQUEST)

    row = get_task_run_with_task_title(run_id=int(rid))
    if not row:
        return error_response(ERROR_CODE_NOT_FOUND, ERROR_MESSAGE_RECORD_NOT_FOUND, HTTP_STATUS_NOT_FOUND)

    agent_plan = parse_json_value(row["agent_plan"]) or None
    agent_state = parse_json_value(row["agent_state"]) or None

    # ===== 运行快照（P3：可观测性）=====
    plan_snapshot = _compute_plan_snapshot(agent_plan=agent_plan, agent_state=agent_state)
    counters = _compute_run_counters(run_id=int(rid))

    # stage：优先以 run.status 收敛（waiting/done/failed/stopped），running 时再看 agent_state.stage。
    status_value = str(row["status"] or "").strip().lower()
    if status_value == "waiting":
        stage = "waiting_input"
    elif status_value == "failed":
        stage = "failed"
    elif status_value == "done":
        stage = "done"
    elif status_value == "stopped":
        stage = "stopped"
    else:
        stage = None
        if isinstance(agent_state, dict):
            stage = str(agent_state.get("stage") or "").strip() or None
        if not stage:
            if status_value == "running":
                stage = "execute" if isinstance(agent_plan, dict) else "planning"
            else:
                stage = status_value or "unknown"

    started_at = str(row["started_at"] or "") or None
    finished_at = str(row["finished_at"] or "") or None
    now_dt = datetime.now(timezone.utc)
    start_dt = _parse_iso_datetime(started_at) or _parse_iso_datetime(str(row["created_at"] or "")) or None
    end_dt = _parse_iso_datetime(finished_at)
    elapsed_ms = None
    if start_dt:
        end_value = end_dt or now_dt
        try:
            elapsed_ms = int((end_value - start_dt).total_seconds() * 1000)
        except Exception:
            elapsed_ms = None

    snapshot = {
        "mode": str(row["mode"] or "").strip() if "mode" in set(row.keys()) else None,
        "stage": stage,
        "plan": plan_snapshot,
        "counters": counters if isinstance(counters, dict) else {"ok": False},
        "elapsed_ms": elapsed_ms,
        "started_at": started_at,
        "finished_at": finished_at,
    }

    return {
        "run": _row_to_run_meta(row, str(row["task_title"] or ""), row["status"] in {RUN_STATUS_RUNNING, RUN_STATUS_WAITING}),
        "agent_plan": agent_plan,
        "agent_state": agent_state,
        "snapshot": snapshot,
    }
