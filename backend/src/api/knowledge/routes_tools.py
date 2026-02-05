import asyncio

from fastapi import APIRouter

from backend.src.api.schemas import ToolCreate, ToolUpdate
from backend.src.common.serializers import tool_from_row
from backend.src.api.utils import ensure_write_permission, error_response, now_iso
from backend.src.common.errors import AppError
from backend.src.constants import (
    ERROR_CODE_INVALID_REQUEST,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_TOOL_NOT_FOUND,
    HTTP_STATUS_BAD_REQUEST,
    HTTP_STATUS_NOT_FOUND,
)
from backend.src.repositories.tool_call_records_repo import get_tool_reuse_stats, get_tool_reuse_stats_map
from backend.src.repositories.tools_repo import (
    ToolCreateParams,
    create_tool as create_tool_repo,
    get_tool as get_tool_repo,
    list_tool_versions as list_tool_versions_repo,
    list_tools as list_tools_repo,
    tool_exists,
    update_tool as update_tool_repo,
)
from backend.src.services.tools.tools_delete import delete_tool_strong
from backend.src.services.tools.tools_store import publish_tool_file, sync_tools_from_files
from backend.src.storage import get_connection

router = APIRouter()


@router.post("/tools")
def create_tool(payload: ToolCreate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
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
        publish = publish_tool_file(int(tool_id), conn=conn)
        if not publish.get("ok"):
            raise AppError(
                code=ERROR_CODE_INVALID_REQUEST,
                message=str(publish.get("error") or "publish_tool_failed"),
                status_code=HTTP_STATUS_BAD_REQUEST,
            )
        row = get_tool_repo(tool_id=tool_id, conn=conn)
    return {"tool": tool_from_row(row), "file": publish}


@router.get("/tools")
def list_tools() -> dict:
    items = [tool_from_row(row) for row in list_tools_repo()]
    tool_ids = [int(item["id"]) for item in items if item.get("id") is not None]
    stats_map = get_tool_reuse_stats_map(tool_ids=tool_ids)
    for item in items:
        stats = stats_map.get(int(item["id"]), {"calls": 0, "reuse_calls": 0})
        calls = int(stats.get("calls") or 0)
        reuse_calls = int(stats.get("reuse_calls") or 0)
        item["reuse_stats"] = {
            "calls": calls,
            "reuse_calls": reuse_calls,
            "reuse_rate": (reuse_calls / calls) if calls else 0,
        }
    return {"items": items}


@router.get("/tools/{tool_id}")
def get_tool(tool_id: int):
    row = get_tool_repo(tool_id=tool_id)
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND, ERROR_MESSAGE_TOOL_NOT_FOUND, HTTP_STATUS_NOT_FOUND
        )
    tool = tool_from_row(row)
    calls, reuse_calls = get_tool_reuse_stats(tool_id=tool_id)
    tool["reuse_stats"] = {
        "calls": calls,
        "reuse_calls": reuse_calls,
        "reuse_rate": (reuse_calls / calls) if calls else 0,
    }
    return {"tool": tool}


@router.patch("/tools/{tool_id}")
def update_tool(tool_id: int, payload: ToolUpdate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
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
            return error_response(
                ERROR_CODE_NOT_FOUND, ERROR_MESSAGE_TOOL_NOT_FOUND, HTTP_STATUS_NOT_FOUND
            )
        publish = publish_tool_file(int(tool_id), conn=conn)
        if not publish.get("ok"):
            raise AppError(
                code=ERROR_CODE_INVALID_REQUEST,
                message=str(publish.get("error") or "publish_tool_failed"),
                status_code=HTTP_STATUS_BAD_REQUEST,
            )
        latest = get_tool_repo(tool_id=int(tool_id), conn=conn)
    return {"tool": tool_from_row(latest or row), "file": publish}


@router.delete("/tools/{tool_id}")
def delete_tool(tool_id: int) -> dict:
    """
    强一致删除：同时删除 DB 与 backend/prompt/tools 下的文件存档。
    """
    permission = ensure_write_permission()
    if permission:
        return permission
    try:
        result = delete_tool_strong(int(tool_id))
    except AppError as exc:
        return error_response(exc.code, exc.message, exc.status_code)
    except Exception as exc:
        return error_response(ERROR_CODE_INVALID_REQUEST, f"{exc}", HTTP_STATUS_BAD_REQUEST)
    row = result.get("row")
    if not row:
        return error_response(ERROR_CODE_NOT_FOUND, ERROR_MESSAGE_TOOL_NOT_FOUND, HTTP_STATUS_NOT_FOUND)
    return {"deleted": True, "tool": tool_from_row(row), "file": result.get("file")}


@router.post("/tools/sync")
async def sync_tools() -> dict:
    """
    将 backend/prompt/tools 下的工具文件同步到 SQLite（tools_items）。
    """
    permission = ensure_write_permission()
    if permission:
        return permission
    result = await asyncio.to_thread(sync_tools_from_files, prune=True)
    return {"result": result}


@router.get("/tools/{tool_id}/versions")
def list_tool_versions(tool_id: int) -> dict:
    if not tool_exists(tool_id=tool_id):
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_TOOL_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
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
