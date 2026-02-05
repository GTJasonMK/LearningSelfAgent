from datetime import datetime, timedelta, timezone
from typing import List, Optional

from fastapi import APIRouter

from backend.src.api.schemas import TaskCreate, TaskUpdate
from backend.src.common.serializers import task_from_row
from backend.src.api.utils import ensure_write_permission, error_response, now_iso
from backend.src.constants import (
    ERROR_CODE_INVALID_REQUEST,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_INVALID_STATUS,
    ERROR_MESSAGE_TASK_NOT_FOUND,
    HEALTH_STATUS_OK,
    HTTP_STATUS_BAD_REQUEST,
    HTTP_STATUS_NOT_FOUND,
    RUN_STATUS_RUNNING,
    RUN_STATUS_WAITING,
    STATUS_CANCELLED,
    STATUS_DONE,
    STATUS_FAILED,
    STATUS_QUEUED,
    STATUS_RUNNING,
    STATUS_STOPPED,
    STATUS_WAITING,
)
from backend.src.repositories.tasks_repo import (
    count_tasks,
    create_task as create_task_record,
    fetch_current_task_title_by_run_statuses,
    get_task as get_task_repo,
    list_tasks as list_tasks_repo,
    update_task as update_task_repo,
)

router = APIRouter()


@router.get("/health")
def health() -> dict:
    return {"status": HEALTH_STATUS_OK}


@router.get("/tasks/summary")
def tasks_summary() -> dict:
    current = fetch_current_task_title_by_run_statuses(
        statuses=[RUN_STATUS_RUNNING, RUN_STATUS_WAITING],
        limit=1,
    )
    return {"count": count_tasks(), "current": current}


@router.post("/tasks")
def create_task(payload: TaskCreate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    task_id, created_at = create_task_record(
        title=payload.title,
        status=STATUS_QUEUED,
        expectation_id=payload.expectation_id,
        started_at=None,
        finished_at=None,
    )
    return {
        "task": {
            "id": task_id,
            "title": payload.title,
            "status": STATUS_QUEUED,
            "created_at": created_at,
            "expectation_id": payload.expectation_id,
            "started_at": None,
            "finished_at": None,
        }
    }


@router.get("/tasks")
def list_tasks(date: Optional[str] = None, days: Optional[int] = None) -> dict:
    """
    任务列表：
    - 默认按 created_at 倒序（最近优先），再按 id 倒序
    - 支持按日期区间筛选：date=YYYY-MM-DD & days=N（包含 date 当天，共 N 天）
    """
    where = []
    params: List = []
    if date:
        date_value = str(date).strip()
        try:
            start = datetime.strptime(date_value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except Exception:
            return error_response(
                ERROR_CODE_INVALID_REQUEST,
                "date 参数不合法，期望 YYYY-MM-DD",
                HTTP_STATUS_BAD_REQUEST,
            )
        try:
            days_value = int(days) if days is not None else 1
        except Exception:
            days_value = 1
        if days_value <= 0:
            days_value = 1
        if days_value > 31:
            # MVP：避免一次拉取过大范围导致 UI 卡顿
            days_value = 31
        end = start + timedelta(days=days_value)
        start_iso = start.isoformat().replace("+00:00", "Z")
        end_iso = end.isoformat().replace("+00:00", "Z")
        where.append("created_at >= ? AND created_at < ?")
        params.extend([start_iso, end_iso])

    start_iso = None
    end_iso = None
    if where:
        # where 只有一个区间条件：created_at >= start AND created_at < end
        start_iso = params[0] if len(params) > 0 else None
        end_iso = params[1] if len(params) > 1 else None
    rows = list_tasks_repo(start_created_at=start_iso, end_created_at=end_iso)
    return {"items": [task_from_row(row) for row in rows]}


@router.get("/tasks/{task_id}")
def get_task(task_id: int):
    row = get_task_repo(task_id=task_id)
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_TASK_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"task": task_from_row(row)}


@router.patch("/tasks/{task_id}")
def update_task(task_id: int, payload: TaskUpdate):
    permission = ensure_write_permission()
    if permission:
        return permission
    fields = []
    params: List = []
    if payload.status is not None:
        allowed_statuses = {
            STATUS_QUEUED,
            STATUS_RUNNING,
            STATUS_WAITING,
            STATUS_DONE,
            STATUS_CANCELLED,
            STATUS_FAILED,
            STATUS_STOPPED,
        }
        if payload.status not in allowed_statuses:
            return error_response(
                ERROR_CODE_INVALID_REQUEST,
                ERROR_MESSAGE_INVALID_STATUS,
                HTTP_STATUS_BAD_REQUEST,
            )
        fields.append("status = ?")
        params.append(payload.status)
    if payload.title is not None:
        fields.append("title = ?")
        params.append(payload.title)
    row = update_task_repo(
        task_id=task_id,
        status=payload.status,
        title=payload.title,
        updated_at=now_iso(),
    )
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_TASK_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"task": task_from_row(row)}
