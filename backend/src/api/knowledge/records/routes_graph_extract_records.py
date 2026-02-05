from typing import Optional

from fastapi import APIRouter

from backend.src.api.utils import ensure_write_permission
from backend.src.common.utils import error_response
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_RECORD_NOT_FOUND,
    HTTP_STATUS_NOT_FOUND,
)
from backend.src.repositories.graph_extract_tasks_repo import (
    get_graph_extract_task,
    list_graph_extract_tasks,
)
from backend.src.services.graph.graph_extract import enqueue_existing_graph_extract

router = APIRouter()


@router.get("/records/graph-extracts")
def list_graph_extracts(
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
    status: Optional[str] = None,
    limit: int = DEFAULT_PAGE_LIMIT,
) -> dict:
    rows = list_graph_extract_tasks(
        task_id=task_id,
        run_id=run_id,
        status=status,
        limit=int(limit),
    )
    return {
        "items": [
            {
                "id": row["id"],
                "task_id": row["task_id"],
                "run_id": row["run_id"],
                "status": row["status"],
                "attempts": row["attempts"],
                "error": row["error"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "finished_at": row["finished_at"],
            }
            for row in rows
        ]
    }


@router.get("/records/graph-extracts/{extract_id}")
def get_graph_extract(extract_id: int) -> dict:
    row = get_graph_extract_task(extract_id=int(extract_id))
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_RECORD_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {
        "record": {
            "id": row["id"],
            "task_id": row["task_id"],
            "run_id": row["run_id"],
            "status": row["status"],
            "attempts": row["attempts"],
            "error": row["error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "finished_at": row["finished_at"],
        }
    }


@router.post("/records/graph-extracts/{extract_id}/retry")
def retry_graph_extract(extract_id: int) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    record = enqueue_existing_graph_extract(extract_id)
    if not record:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_RECORD_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"record": record}
