from fastapi import APIRouter

from backend.src.api.schemas import TaskRunCreate, TaskRunUpdate
from backend.src.common.serializers import task_run_from_row
from backend.src.api.utils import (
    clamp_non_negative_int,
    clamp_page_limit,
    now_iso,
    require_write_permission,
)
from backend.src.api.tasks.route_common import (
    ensure_task_exists_or_error,
    record_not_found_response,
)
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    DEFAULT_PAGE_OFFSET,
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_PLANNED,
    RUN_STATUS_RUNNING,
    RUN_STATUS_STOPPED,
)
from backend.src.services.tasks.task_queries import create_task_run as create_task_run_record
from backend.src.services.tasks.task_queries import get_task_run as repo_get_task_run
from backend.src.services.tasks.task_queries import list_task_runs as list_task_runs_repo
from backend.src.services.tasks.task_queries import update_task_run as update_task_run_repo

router = APIRouter()


@router.post("/tasks/{task_id}/runs")
@require_write_permission
def create_task_run(task_id: int, payload: TaskRunCreate) -> dict:
    created_at = now_iso()
    updated_at = created_at
    status = payload.status or RUN_STATUS_PLANNED
    started_at = created_at if status == RUN_STATUS_RUNNING else None
    finished_at = created_at if status in {RUN_STATUS_DONE, RUN_STATUS_FAILED, RUN_STATUS_STOPPED} else None
    task_exists_error = ensure_task_exists_or_error(task_id=task_id)
    if task_exists_error:
        return task_exists_error
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
    task_exists_error = ensure_task_exists_or_error(task_id=task_id)
    if task_exists_error:
        return task_exists_error
    safe_offset = clamp_non_negative_int(offset, default=DEFAULT_PAGE_OFFSET)
    safe_limit = clamp_page_limit(limit, default=DEFAULT_PAGE_LIMIT)
    rows = list_task_runs_repo(task_id=task_id, offset=safe_offset, limit=safe_limit)
    return {"items": [task_run_from_row(row) for row in rows]}


@router.get("/tasks/runs/{run_id}")
def get_task_run(run_id: int):
    row = repo_get_task_run(run_id=run_id)
    if not row:
        return record_not_found_response()
    return {"run": task_run_from_row(row)}


@router.patch("/tasks/runs/{run_id}")
@require_write_permission
def update_task_run(run_id: int, payload: TaskRunUpdate):
    updated_at = now_iso()
    row = update_task_run_repo(
        run_id=run_id,
        status=payload.status,
        summary=payload.summary,
        updated_at=updated_at,
    )
    if not row:
        return record_not_found_response()
    return {"run": task_run_from_row(row)}
