from typing import Optional

from fastapi import APIRouter

from backend.src.api.schemas import TaskOutputCreate
from backend.src.common.serializers import task_output_from_row
from backend.src.api.utils import (
    clamp_non_negative_int,
    clamp_page_limit,
    now_iso,
    require_write_permission,
)
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    DEFAULT_PAGE_OFFSET,
)
from backend.src.api.tasks.route_common import (
    ensure_task_exists_or_error,
    record_not_found_response,
)
from backend.src.services.tasks.task_queries import create_task_output as create_task_output_repo
from backend.src.services.tasks.task_queries import get_task_output as get_task_output_repo
from backend.src.services.tasks.task_queries import list_task_outputs as list_task_outputs_repo

router = APIRouter()


@router.post("/tasks/{task_id}/outputs")
@require_write_permission
def create_task_output(task_id: int, payload: TaskOutputCreate) -> dict:
    created_at = now_iso()
    task_exists_error = ensure_task_exists_or_error(task_id=task_id)
    if task_exists_error:
        return task_exists_error
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
    task_exists_error = ensure_task_exists_or_error(task_id=task_id)
    if task_exists_error:
        return task_exists_error
    safe_offset = clamp_non_negative_int(offset, default=DEFAULT_PAGE_OFFSET)
    safe_limit = clamp_page_limit(limit, default=DEFAULT_PAGE_LIMIT)
    rows = list_task_outputs_repo(
        task_id=task_id,
        run_id=run_id,
        offset=safe_offset,
        limit=safe_limit,
    )
    return {"items": [task_output_from_row(row) for row in rows]}


@router.get("/tasks/outputs/{output_id}")
def get_task_output(output_id: int):
    row = get_task_output_repo(output_id=output_id)
    if not row:
        return record_not_found_response()
    return {"output": task_output_from_row(row)}
