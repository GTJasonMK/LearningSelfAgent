from typing import Optional

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

router = APIRouter()


def _row_to_run_meta(row, task_title: str, is_current: bool) -> dict:
    return {
        "run_id": int(row["id"]),
        "task_id": int(row["task_id"]),
        "task_title": task_title,
        "status": row["status"],
        "summary": row["summary"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "is_current": bool(is_current),
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

    return {
        "run": _row_to_run_meta(row, str(row["task_title"] or ""), row["status"] in {RUN_STATUS_RUNNING, RUN_STATUS_WAITING}),
        "agent_plan": agent_plan,
        "agent_state": agent_state,
    }
