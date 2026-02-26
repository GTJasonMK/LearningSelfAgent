from typing import Optional

from fastapi import APIRouter

from backend.src.api.schemas import LLMRecordCreate
from backend.src.common.serializers import llm_record_from_row
from backend.src.api.knowledge.records.route_common import record_not_found_response
from backend.src.api.utils import (
    clamp_non_negative_int,
    clamp_page_limit,
    require_write_permission,
)
from backend.src.common.utils import now_iso
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    DEFAULT_PAGE_OFFSET,
    LLM_STATUS_SUCCESS,
)
from backend.src.services.knowledge.knowledge_query import (
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
    safe_offset = clamp_non_negative_int(offset, default=DEFAULT_PAGE_OFFSET)
    safe_limit = clamp_page_limit(limit, default=DEFAULT_PAGE_LIMIT)
    rows = list_llm_records_repo(
        task_id=task_id,
        run_id=run_id,
        offset=safe_offset,
        limit=safe_limit,
    )
    return {"items": [llm_record_from_row(row) for row in rows]}


@router.get("/records/llm/{record_id}")
def get_llm_record(record_id: int):
    row = get_llm_record_repo(record_id=record_id)
    if not row:
        return record_not_found_response()
    return {"record": llm_record_from_row(row)}


@router.post("/records/llm")
@require_write_permission
def create_llm_record(payload: LLMRecordCreate) -> dict:
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
