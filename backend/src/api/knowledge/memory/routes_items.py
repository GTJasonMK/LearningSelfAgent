import asyncio

from fastapi import APIRouter

from backend.src.api.schemas import MemoryCreate, MemoryUpdate
from backend.src.common.serializers import memory_from_row
from backend.src.api.utils import ensure_write_permission, error_response
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    DEFAULT_PAGE_OFFSET,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_MEMORY_NOT_FOUND,
    HTTP_STATUS_NOT_FOUND,
)
from backend.src.repositories.memory_repo import (
    count_memory_items,
    fetch_latest_memory_content,
    get_memory_item as get_memory_item_repo,
    list_memory_items as list_memory_items_repo,
    search_memory_fts_or_like,
)
from backend.src.services.memory.memory_items import (
    create_memory_item as create_memory_item_service,
    delete_memory_item as delete_memory_item_service,
    update_memory_item as update_memory_item_service,
)
from backend.src.services.memory.memory_store import sync_memory_from_files

router = APIRouter()


@router.get("/memory/summary")
def memory_summary() -> dict:
    return {
        "items": count_memory_items(),
        "last_update": fetch_latest_memory_content(),
    }


@router.post("/memory/items")
def create_memory_item(payload: MemoryCreate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    return create_memory_item_service(payload)


@router.post("/memory/sync")
async def sync_memory() -> dict:
    """
    将 backend/prompt/memory 下的记忆文件同步到数据库（memory_items）。

    说明：
    - 支持用户手工编辑/删除文件后，重建 SQLite 的“快速查询索引”；
    - prune=True：文件不存在则删除 DB 记录（强一致删除）。
    """
    permission = ensure_write_permission()
    if permission:
        return permission
    result = await asyncio.to_thread(sync_memory_from_files, None, prune=True)
    return {"result": result}


@router.get("/memory/items")
def list_memory_items(
    offset: int = DEFAULT_PAGE_OFFSET, limit: int = DEFAULT_PAGE_LIMIT
) -> dict:
    rows = list_memory_items_repo(offset=offset, limit=limit)
    return {"items": [memory_from_row(row) for row in rows]}


@router.get("/memory/items/{item_id}")
def get_memory_item(item_id: int):
    row = get_memory_item_repo(item_id=item_id)
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_MEMORY_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"item": memory_from_row(row)}


@router.delete("/memory/items/{item_id}")
def delete_memory_item(item_id: int):
    permission = ensure_write_permission()
    if permission:
        return permission
    return delete_memory_item_service(item_id=int(item_id))


@router.patch("/memory/items/{item_id}")
def update_memory_item(item_id: int, payload: MemoryUpdate):
    permission = ensure_write_permission()
    if permission:
        return permission
    return update_memory_item_service(int(item_id), payload)


@router.get("/memory/search")
def search_memory(q: str) -> dict:
    rows = search_memory_fts_or_like(q=str(q or ""), limit=DEFAULT_PAGE_LIMIT)
    return {"items": [memory_from_row(row) for row in rows]}
