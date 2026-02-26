from fastapi import APIRouter

from backend.src.api.schemas import SearchRecordCreate
from backend.src.api.knowledge.records.route_common import record_not_found_response
from backend.src.common.serializers import search_record_from_row
from backend.src.api.utils import require_write_permission
from backend.src.common.utils import now_iso
from backend.src.services.knowledge.knowledge_query import (
    create_search_record as create_search_record_repo,
    get_search_record as get_search_record_repo,
    list_search_records as list_search_records_repo,
)

router = APIRouter()


@router.post("/records/search")
@require_write_permission
def create_search_record(payload: SearchRecordCreate) -> dict:
    created_at = now_iso()
    record_id = create_search_record_repo(
        query=payload.query,
        sources=payload.sources,
        result_count=payload.result_count,
        task_id=payload.task_id,
        created_at=created_at,
    )
    row = get_search_record_repo(record_id=record_id)
    return {"record": search_record_from_row(row)}


@router.get("/records/search")
def list_search_records() -> dict:
    rows = list_search_records_repo()
    return {"items": [search_record_from_row(row) for row in rows]}


@router.get("/records/search/{record_id}")
def get_search_record(record_id: int):
    row = get_search_record_repo(record_id=record_id)
    if not row:
        return record_not_found_response()
    return {"record": search_record_from_row(row)}
