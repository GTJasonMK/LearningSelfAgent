from typing import Optional

from fastapi import APIRouter

from backend.src.api.schemas import TaskStepCreate, TaskStepUpdate
from backend.src.common.serializers import task_step_from_row
from backend.src.api.utils import (
    clamp_non_negative_int,
    clamp_page_limit,
    error_response,
    now_iso,
    require_write_permission,
)
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    DEFAULT_PAGE_OFFSET,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_MESSAGE_INVALID_STATUS,
    HTTP_STATUS_BAD_REQUEST,
    STEP_STATUS_PLANNED,
)
from backend.src.api.tasks.route_common import (
    ensure_task_exists_or_error,
    is_valid_task_step_status,
    task_step_not_found_response,
)
from backend.src.services.tasks.task_queries import TaskStepCreateParams
from backend.src.services.tasks.task_queries import create_task_step as create_task_step_repo
from backend.src.services.tasks.task_queries import get_task_step as get_task_step_repo
from backend.src.services.tasks.task_queries import list_task_steps as list_task_steps_repo
from backend.src.services.tasks.task_queries import update_task_step as update_task_step_repo

router = APIRouter()


@router.post("/tasks/{task_id}/steps")
@require_write_permission
def create_task_step(task_id: int, payload: TaskStepCreate) -> dict:
    created_at = now_iso()
    updated_at = created_at
    status = payload.status or STEP_STATUS_PLANNED
    task_exists_error = ensure_task_exists_or_error(task_id=task_id)
    if task_exists_error:
        return task_exists_error
    step_id, _created, _updated = create_task_step_repo(
        TaskStepCreateParams(
            task_id=task_id,
            run_id=payload.run_id,
            title=payload.title,
            status=status,
            detail=payload.detail,
            result=None,
            error=None,
            attempts=0,
            started_at=None,
            finished_at=None,
            step_order=payload.step_order,
            created_at=created_at,
            updated_at=updated_at,
        )
    )
    row = get_task_step_repo(step_id=step_id)
    return {"step": task_step_from_row(row)}


@router.get("/tasks/{task_id}/steps")
def list_task_steps(
    task_id: int,
    run_id: Optional[int] = None,
    offset: int = DEFAULT_PAGE_OFFSET,
    limit: int = DEFAULT_PAGE_LIMIT,
) -> dict:
    task_exists_error = ensure_task_exists_or_error(task_id=task_id)
    if task_exists_error:
        return task_exists_error
    safe_offset = clamp_non_negative_int(offset, default=DEFAULT_PAGE_OFFSET)
    safe_limit = clamp_page_limit(limit, default=DEFAULT_PAGE_LIMIT)
    rows = list_task_steps_repo(
        task_id=task_id,
        run_id=run_id,
        offset=safe_offset,
        limit=safe_limit,
    )
    return {"items": [task_step_from_row(row) for row in rows]}


@router.get("/tasks/steps/{step_id}")
def get_task_step(step_id: int):
    row = get_task_step_repo(step_id=step_id)
    if not row:
        return task_step_not_found_response()
    return {"step": task_step_from_row(row)}


@router.patch("/tasks/steps/{step_id}")
@require_write_permission
def update_task_step(step_id: int, payload: TaskStepUpdate):
    updated_at = now_iso()
    if payload.status is not None and not is_valid_task_step_status(payload.status):
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            ERROR_MESSAGE_INVALID_STATUS,
            HTTP_STATUS_BAD_REQUEST,
        )
    row = update_task_step_repo(
        step_id=step_id,
        title=payload.title,
        status=payload.status,
        detail=payload.detail,
        result=payload.result,
        error=payload.error,
        step_order=payload.step_order,
        run_id=payload.run_id,
        updated_at=updated_at,
    )
    if not row:
        return task_step_not_found_response()
    return {"step": task_step_from_row(row)}
