from fastapi import APIRouter

from backend.src.api.schemas import SearchRecordCreate
from backend.src.common.serializers import search_record_from_row
from backend.src.api.utils import ensure_write_permission
from backend.src.common.utils import error_response, now_iso
from backend.src.constants import (
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_RECORD_NOT_FOUND,
    HTTP_STATUS_NOT_FOUND,
)
from backend.src.repositories.search_records_repo import (
    create_search_record as create_search_record_repo,
    get_search_record as get_search_record_repo,
    list_search_records as list_search_records_repo,
)

router = APIRouter()


@router.post("/records/search")
def create_search_record(payload: SearchRecordCreate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
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
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_RECORD_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"record": search_record_from_row(row)}
