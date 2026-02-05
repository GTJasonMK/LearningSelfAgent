from typing import Optional

from fastapi import APIRouter

from backend.src.api.schemas import ToolCallCreate, ToolReuseValidation
from backend.src.common.serializers import tool_call_from_row
from backend.src.api.utils import ensure_write_permission
from backend.src.common.utils import error_response
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    DEFAULT_PAGE_OFFSET,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_INVALID_STATUS,
    ERROR_MESSAGE_RECORD_NOT_FOUND,
    HTTP_STATUS_BAD_REQUEST,
    HTTP_STATUS_NOT_FOUND,
    TOOL_REUSE_STATUS_FAIL,
    TOOL_REUSE_STATUS_PASS,
    TOOL_REUSE_STATUS_UNKNOWN,
)
from backend.src.repositories.tool_call_records_repo import (
    list_tool_call_records as list_tool_call_records_repo,
    update_tool_call_record_validation as update_tool_call_record_validation_repo,
)
from backend.src.services.tools.tool_records import (
    create_tool_record as create_tool_record_service,
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
    rows = list_tool_call_records_repo(
        task_id=task_id,
        run_id=run_id,
        tool_id=tool_id,
        reuse_status=reuse_status,
        offset=offset,
        limit=limit,
    )
    return {"items": [tool_call_from_row(row) for row in rows]}


@router.post("/records/tools")
def create_tool_record(payload: ToolCallCreate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    return create_tool_record_service(payload)


@router.post("/records/tools/{record_id}/validate")
def validate_tool_record(record_id: int, payload: ToolReuseValidation) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    allowed_statuses = {
        TOOL_REUSE_STATUS_PASS,
        TOOL_REUSE_STATUS_FAIL,
        TOOL_REUSE_STATUS_UNKNOWN,
    }
    if payload.reuse_status not in allowed_statuses:
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
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_RECORD_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"record": tool_call_from_row(row)}
