from typing import Optional

from fastapi import APIRouter

from backend.src.api.knowledge.records.route_common import record_not_found_response
from backend.src.api.utils import clamp_page_limit, require_write_permission
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
)
from backend.src.services.knowledge.knowledge_query import (
    get_graph_extract_task,
    list_graph_extract_tasks,
)
from backend.src.services.graph.graph_extract import enqueue_existing_graph_extract

router = APIRouter()


def _graph_extract_record_from_row(row) -> dict:
    return {
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


@router.get("/records/graph-extracts")
def list_graph_extracts(
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
    status: Optional[str] = None,
    limit: int = DEFAULT_PAGE_LIMIT,
) -> dict:
    safe_limit = clamp_page_limit(limit, default=DEFAULT_PAGE_LIMIT)
    rows = list_graph_extract_tasks(
        task_id=task_id,
        run_id=run_id,
        status=status,
        limit=int(safe_limit),
    )
    return {"items": [_graph_extract_record_from_row(row) for row in rows]}


@router.get("/records/graph-extracts/{extract_id}")
def get_graph_extract(extract_id: int) -> dict:
    row = get_graph_extract_task(extract_id=int(extract_id))
    if not row:
        return record_not_found_response()
    return {"record": _graph_extract_record_from_row(row)}


@router.post("/records/graph-extracts/{extract_id}/retry")
@require_write_permission
def retry_graph_extract(extract_id: int) -> dict:
    record = enqueue_existing_graph_extract(extract_id)
    if not record:
        return record_not_found_response()
    return {"record": record}
