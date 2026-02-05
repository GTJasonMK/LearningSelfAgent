from typing import Optional

from fastapi import APIRouter

from backend.src.api.schemas import TaskOutputCreate
from backend.src.common.serializers import task_output_from_row
from backend.src.api.utils import ensure_write_permission, error_response, now_iso
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    DEFAULT_PAGE_OFFSET,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_RECORD_NOT_FOUND,
    ERROR_MESSAGE_TASK_NOT_FOUND,
    HTTP_STATUS_NOT_FOUND,
)
from backend.src.repositories.task_outputs_repo import (
    create_task_output as create_task_output_repo,
    get_task_output as get_task_output_repo,
    list_task_outputs as list_task_outputs_repo,
)
from backend.src.repositories.tasks_repo import task_exists

router = APIRouter()


@router.post("/tasks/{task_id}/outputs")
def create_task_output(task_id: int, payload: TaskOutputCreate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    created_at = now_iso()
    if not task_exists(task_id=task_id):
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_TASK_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    output_id, _created = create_task_output_repo(
        task_id=task_id,
        run_id=payload.run_id,
        output_type=payload.output_type,
        content=payload.content,
        created_at=created_at,
    )
    row = get_task_output_repo(output_id=output_id)
    return {"output": task_output_from_row(row)}


@router.get("/tasks/{task_id}/outputs")
def list_task_outputs(
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
    rows = list_task_outputs_repo(task_id=task_id, run_id=run_id, offset=offset, limit=limit)
    return {"items": [task_output_from_row(row) for row in rows]}


@router.get("/tasks/outputs/{output_id}")
def get_task_output(output_id: int):
    row = get_task_output_repo(output_id=output_id)
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_RECORD_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"output": task_output_from_row(row)}
