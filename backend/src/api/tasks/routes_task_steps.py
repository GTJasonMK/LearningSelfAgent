from typing import Optional

from fastapi import APIRouter

from backend.src.api.schemas import TaskStepCreate, TaskStepUpdate
from backend.src.common.serializers import task_step_from_row
from backend.src.api.utils import ensure_write_permission, error_response, now_iso
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    DEFAULT_PAGE_OFFSET,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_INVALID_STATUS,
    ERROR_MESSAGE_TASK_NOT_FOUND,
    ERROR_MESSAGE_TASK_STEP_NOT_FOUND,
    HTTP_STATUS_BAD_REQUEST,
    HTTP_STATUS_NOT_FOUND,
    STEP_STATUS_DONE,
    STEP_STATUS_FAILED,
    STEP_STATUS_PLANNED,
    STEP_STATUS_RUNNING,
    STEP_STATUS_SKIPPED,
    STEP_STATUS_WAITING,
)
from backend.src.repositories.task_steps_repo import (
    TaskStepCreateParams,
    create_task_step as create_task_step_repo,
    get_task_step as get_task_step_repo,
    list_task_steps as list_task_steps_repo,
    update_task_step as update_task_step_repo,
)
from backend.src.repositories.tasks_repo import task_exists

router = APIRouter()


@router.post("/tasks/{task_id}/steps")
def create_task_step(task_id: int, payload: TaskStepCreate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    created_at = now_iso()
    updated_at = created_at
    status = payload.status or STEP_STATUS_PLANNED
    if not task_exists(task_id=task_id):
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_TASK_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
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
    if not task_exists(task_id=task_id):
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_TASK_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    rows = list_task_steps_repo(task_id=task_id, run_id=run_id, offset=offset, limit=limit)
    return {"items": [task_step_from_row(row) for row in rows]}


@router.get("/tasks/steps/{step_id}")
def get_task_step(step_id: int):
    row = get_task_step_repo(step_id=step_id)
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_TASK_STEP_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"step": task_step_from_row(row)}


@router.patch("/tasks/steps/{step_id}")
def update_task_step(step_id: int, payload: TaskStepUpdate):
    permission = ensure_write_permission()
    if permission:
        return permission
    updated_at = now_iso()
    allowed_statuses = {
        STEP_STATUS_PLANNED,
        STEP_STATUS_RUNNING,
        STEP_STATUS_WAITING,
        STEP_STATUS_DONE,
        STEP_STATUS_FAILED,
        STEP_STATUS_SKIPPED,
    }
    if payload.status is not None and payload.status not in allowed_statuses:
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
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_TASK_STEP_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"step": task_step_from_row(row)}
