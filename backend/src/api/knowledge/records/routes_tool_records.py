from typing import Optional

from fastapi import APIRouter

from backend.src.api.schemas import ToolCallCreate, ToolReuseValidation
from backend.src.common.serializers import tool_call_from_row
from backend.src.api.knowledge.records.route_common import record_not_found_response
from backend.src.api.utils import (
    clamp_non_negative_int,
    clamp_page_limit,
    require_write_permission,
)
from backend.src.common.utils import error_response
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    DEFAULT_PAGE_OFFSET,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_MESSAGE_INVALID_STATUS,
    HTTP_STATUS_BAD_REQUEST,
)
from backend.src.services.knowledge.knowledge_query import (
    list_tool_call_records as list_tool_call_records_repo,
    update_tool_call_record_validation as update_tool_call_record_validation_repo,
)
from backend.src.services.tools.tool_records import (
    create_tool_record as create_tool_record_service,
    is_valid_tool_reuse_status,
)

router = APIRouter()


@router.get("/records/tools")
def list_tool_records(
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
    tool_id: Optional[int] = None,
    reuse_status: Optional[str] = None,
    offset: int = DEFAULT_PAGE_OFFSET,
    limit: int = DEFAULT_PAGE_LIMIT,
) -> dict:
    safe_offset = clamp_non_negative_int(offset, default=DEFAULT_PAGE_OFFSET)
    safe_limit = clamp_page_limit(limit, default=DEFAULT_PAGE_LIMIT)
    rows = list_tool_call_records_repo(
        task_id=task_id,
        run_id=run_id,
        tool_id=tool_id,
        reuse_status=reuse_status,
        offset=safe_offset,
        limit=safe_limit,
    )
    return {"items": [tool_call_from_row(row) for row in rows]}


@router.post("/records/tools")
@require_write_permission
def create_tool_record(payload: ToolCallCreate) -> dict:
    return create_tool_record_service(payload)


@router.post("/records/tools/{record_id}/validate")
@require_write_permission
def validate_tool_record(record_id: int, payload: ToolReuseValidation) -> dict:
    if not is_valid_tool_reuse_status(payload.reuse_status, allow_none=False):
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            ERROR_MESSAGE_INVALID_STATUS,
            HTTP_STATUS_BAD_REQUEST,
        )
    row = update_tool_call_record_validation_repo(
        record_id=record_id,
        reuse_status=payload.reuse_status,
        reuse_notes=payload.reuse_notes,
    )
    if not row:
        return record_not_found_response()
    return {"record": tool_call_from_row(row)}
