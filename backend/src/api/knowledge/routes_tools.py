import asyncio

from fastapi import APIRouter

from backend.src.api.schemas import ToolCreate, ToolUpdate
from backend.src.common.serializers import tool_from_row
from backend.src.api.utils import (
    app_error_response,
    error_response,
    invalid_request_response_from_exception,
    now_iso,
    require_write_permission,
)
from backend.src.common.errors import AppError
from backend.src.constants import (
    ERROR_CODE_INVALID_REQUEST,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_TOOL_NOT_FOUND,
    HTTP_STATUS_BAD_REQUEST,
    HTTP_STATUS_NOT_FOUND,
)
from backend.src.services.tools.tools_delete import delete_tool_strong
from backend.src.services.tools.tools_query import ToolCreateParams
from backend.src.services.tools.tools_query import create_tool as create_tool_repo
from backend.src.services.tools.tools_query import get_tool as get_tool_repo
from backend.src.services.tools.tools_query import get_tool_reuse_stats
from backend.src.services.tools.tools_query import get_tool_reuse_stats_map
from backend.src.services.tools.tools_query import list_tool_versions as list_tool_versions_repo
from backend.src.services.tools.tools_query import list_tools as list_tools_repo
from backend.src.services.tools.tools_query import tool_exists
from backend.src.services.tools.tools_query import update_tool as update_tool_repo
from backend.src.services.tools.tools_store import publish_tool_file, sync_tools_from_files
from backend.src.storage import get_connection

router = APIRouter()


def _tool_not_found_response():
    return error_response(
        ERROR_CODE_NOT_FOUND,
        ERROR_MESSAGE_TOOL_NOT_FOUND,
        HTTP_STATUS_NOT_FOUND,
    )


def _build_reuse_stats(calls: int, reuse_calls: int) -> dict:
    calls_value = int(calls or 0)
    reuse_calls_value = int(reuse_calls or 0)
    return {
        "calls": calls_value,
        "reuse_calls": reuse_calls_value,
        "reuse_rate": (reuse_calls_value / calls_value) if calls_value else 0,
    }


def _publish_tool_file_or_raise(tool_id: int, *, conn):
    publish = publish_tool_file(int(tool_id), conn=conn)
    if not publish.get("ok"):
        raise AppError(
            code=ERROR_CODE_INVALID_REQUEST,
            message=str(publish.get("error") or "publish_tool_failed"),
            status_code=HTTP_STATUS_BAD_REQUEST,
        )
    return publish


@router.post("/tools")
@require_write_permission
def create_tool(payload: ToolCreate) -> dict:
    created_at = now_iso()
    updated_at = created_at
    with get_connection() as conn:
        tool_id = create_tool_repo(
            ToolCreateParams(
                name=payload.name,
                description=payload.description,
                version=payload.version,
                metadata=payload.metadata,
                created_at=created_at,
                updated_at=updated_at,
            ),
            conn=conn,
        )
        publish = _publish_tool_file_or_raise(int(tool_id), conn=conn)
        row = get_tool_repo(tool_id=tool_id, conn=conn)
    return {"tool": tool_from_row(row), "file": publish}


@router.get("/tools")
def list_tools() -> dict:
    items = [tool_from_row(row) for row in list_tools_repo()]
    tool_ids = [int(item["id"]) for item in items if item.get("id") is not None]
    stats_map = get_tool_reuse_stats_map(tool_ids=tool_ids)
    for item in items:
        stats = stats_map.get(int(item["id"]), {"calls": 0, "reuse_calls": 0})
        item["reuse_stats"] = _build_reuse_stats(
            calls=int(stats.get("calls") or 0),
            reuse_calls=int(stats.get("reuse_calls") or 0),
        )
    return {"items": items}


@router.get("/tools/{tool_id}")
def get_tool(tool_id: int):
    row = get_tool_repo(tool_id=tool_id)
    if not row:
        return _tool_not_found_response()
    tool = tool_from_row(row)
    calls, reuse_calls = get_tool_reuse_stats(tool_id=tool_id)
    tool["reuse_stats"] = _build_reuse_stats(calls=int(calls), reuse_calls=int(reuse_calls))
    return {"tool": tool}


@router.patch("/tools/{tool_id}")
@require_write_permission
def update_tool(tool_id: int, payload: ToolUpdate) -> dict:
    updated_at = now_iso()
    with get_connection() as conn:
        row = update_tool_repo(
            tool_id=tool_id,
            name=payload.name,
            description=payload.description,
            version=payload.version,
            metadata=payload.metadata,
            change_notes=payload.change_notes,
            updated_at=updated_at,
            conn=conn,
        )
        if not row:
            return _tool_not_found_response()
        publish = _publish_tool_file_or_raise(int(tool_id), conn=conn)
        latest = get_tool_repo(tool_id=int(tool_id), conn=conn)
    return {"tool": tool_from_row(latest or row), "file": publish}


@router.delete("/tools/{tool_id}")
@require_write_permission
def delete_tool(tool_id: int) -> dict:
    """
    强一致删除：同时删除 DB 与 backend/prompt/tools 下的文件存档。
    """
    try:
        result = delete_tool_strong(int(tool_id))
    except AppError as exc:
        return app_error_response(exc)
    except Exception as exc:
        return invalid_request_response_from_exception(exc)
    row = result.get("row")
    if not row:
        return _tool_not_found_response()
    return {"deleted": True, "tool": tool_from_row(row), "file": result.get("file")}


@router.post("/tools/sync")
@require_write_permission
async def sync_tools() -> dict:
    """
    将 backend/prompt/tools 下的工具文件同步到 SQLite（tools_items）。
    """
    result = await asyncio.to_thread(sync_tools_from_files, prune=True)
    return {"result": result}


@router.get("/tools/{tool_id}/versions")
def list_tool_versions(tool_id: int) -> dict:
    if not tool_exists(tool_id=tool_id):
        return _tool_not_found_response()
    rows = list_tool_versions_repo(tool_id=tool_id)
    return {
        "items": [
            {
                "id": row["id"],
                "tool_id": row["tool_id"],
                "previous_version": row["previous_version"],
                "next_version": row["next_version"],
                "change_notes": row["change_notes"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]
    }
