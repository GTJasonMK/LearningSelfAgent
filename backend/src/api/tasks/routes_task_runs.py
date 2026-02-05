from fastapi import APIRouter

from backend.src.api.schemas import TaskRunCreate, TaskRunUpdate
from backend.src.common.serializers import task_run_from_row
from backend.src.api.utils import ensure_write_permission, error_response, now_iso
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    DEFAULT_PAGE_OFFSET,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_RECORD_NOT_FOUND,
    ERROR_MESSAGE_TASK_NOT_FOUND,
    HTTP_STATUS_NOT_FOUND,
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_PLANNED,
    RUN_STATUS_RUNNING,
    RUN_STATUS_STOPPED,
    RUN_STATUS_WAITING,
)
from backend.src.repositories.task_runs_repo import (
    create_task_run as create_task_run_record,
    get_task_run as repo_get_task_run,
    list_task_runs as list_task_runs_repo,
    update_task_run as update_task_run_repo,
)
from backend.src.repositories.tasks_repo import task_exists

router = APIRouter()


@router.post("/tasks/{task_id}/runs")
def create_task_run(task_id: int, payload: TaskRunCreate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    created_at = now_iso()
    updated_at = created_at
    status = payload.status or RUN_STATUS_PLANNED
    started_at = created_at if status == RUN_STATUS_RUNNING else None
    finished_at = created_at if status in {RUN_STATUS_DONE, RUN_STATUS_FAILED, RUN_STATUS_STOPPED} else None
    if not task_exists(task_id=task_id):
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_TASK_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    run_id, _created, _updated = create_task_run_record(
        task_id=task_id,
        status=status,
        summary=payload.summary,
        started_at=started_at,
        finished_at=finished_at,
        created_at=created_at,
        updated_at=updated_at,
    )
    row = repo_get_task_run(run_id=run_id)
    return {"run": task_run_from_row(row)}


@router.get("/tasks/{task_id}/runs")
def list_task_runs(
    task_id: int,
    offset: int = DEFAULT_PAGE_OFFSET,
    limit: int = DEFAULT_PAGE_LIMIT,
) -> dict:
    if not task_exists(task_id=task_id):
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_TASK_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    rows = list_task_runs_repo(task_id=task_id, offset=offset, limit=limit)
    return {"items": [task_run_from_row(row) for row in rows]}


@router.get("/tasks/runs/{run_id}")
def get_task_run(run_id: int):
    row = repo_get_task_run(run_id=run_id)
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_RECORD_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"run": task_run_from_row(row)}


@router.patch("/tasks/runs/{run_id}")
def update_task_run(run_id: int, payload: TaskRunUpdate):
    permission = ensure_write_permission()
    if permission:
        return permission
    updated_at = now_iso()
    row = update_task_run_repo(
        run_id=run_id,
        status=payload.status,
        summary=payload.summary,
        updated_at=updated_at,
    )
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_RECORD_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"run": task_run_from_row(row)}
