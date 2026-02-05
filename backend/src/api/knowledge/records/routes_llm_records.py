from typing import Optional

from fastapi import APIRouter

from backend.src.api.schemas import LLMRecordCreate
from backend.src.common.serializers import llm_record_from_row
from backend.src.api.utils import ensure_write_permission
from backend.src.common.utils import error_response, now_iso
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    DEFAULT_PAGE_OFFSET,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_RECORD_NOT_FOUND,
    HTTP_STATUS_NOT_FOUND,
    LLM_STATUS_SUCCESS,
)
from backend.src.repositories.llm_records_repo import (
    create_llm_record as create_llm_record_repo,
    get_llm_record as get_llm_record_repo,
    list_llm_records as list_llm_records_repo,
)

router = APIRouter()


@router.get("/records/llm")
def list_llm_records(
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
    offset: int = DEFAULT_PAGE_OFFSET,
    limit: int = DEFAULT_PAGE_LIMIT,
) -> dict:
    rows = list_llm_records_repo(task_id=task_id, run_id=run_id, offset=offset, limit=limit)
    return {"items": [llm_record_from_row(row) for row in rows]}


@router.get("/records/llm/{record_id}")
def get_llm_record(record_id: int):
    row = get_llm_record_repo(record_id=record_id)
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_RECORD_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"record": llm_record_from_row(row)}


@router.post("/records/llm")
def create_llm_record(payload: LLMRecordCreate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    created_at = now_iso()
    record_id = create_llm_record_repo(
        prompt=payload.prompt,
        response=payload.response,
        task_id=payload.task_id,
        run_id=payload.run_id,
        status=LLM_STATUS_SUCCESS,
        created_at=created_at,
        updated_at=created_at,
    )
    row = get_llm_record_repo(record_id=record_id)
    return {"record": llm_record_from_row(row)}
