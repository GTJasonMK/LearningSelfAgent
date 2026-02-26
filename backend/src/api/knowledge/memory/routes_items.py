import asyncio

from fastapi import APIRouter

from backend.src.api.schemas import MemoryCreate, MemoryUpdate
from backend.src.common.serializers import memory_from_row
from backend.src.api.utils import (
    clamp_non_negative_int,
    clamp_page_limit,
    error_response,
    require_write_permission,
)
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    DEFAULT_PAGE_OFFSET,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_MEMORY_NOT_FOUND,
    HTTP_STATUS_NOT_FOUND,
)
from backend.src.services.knowledge.knowledge_query import (
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
@require_write_permission
def create_memory_item(payload: MemoryCreate) -> dict:
    return create_memory_item_service(payload)


@router.post("/memory/sync")
@require_write_permission
async def sync_memory() -> dict:
    """
    将 backend/prompt/memory 下的记忆文件同步到数据库（memory_items）。

    说明：
    - 支持用户手工编辑/删除文件后，重建 SQLite 的“快速查询索引”；
    - prune=True：文件不存在则删除 DB 记录（强一致删除）。
    """
    result = await asyncio.to_thread(sync_memory_from_files, None, prune=True)
    return {"result": result}


@router.get("/memory/items")
def list_memory_items(
    offset: int = DEFAULT_PAGE_OFFSET, limit: int = DEFAULT_PAGE_LIMIT
) -> dict:
    offset = clamp_non_negative_int(offset, default=DEFAULT_PAGE_OFFSET)
    limit = clamp_page_limit(limit, default=DEFAULT_PAGE_LIMIT)
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
@require_write_permission
def delete_memory_item(item_id: int):
    return delete_memory_item_service(item_id=int(item_id))


@router.patch("/memory/items/{item_id}")
@require_write_permission
def update_memory_item(item_id: int, payload: MemoryUpdate):
    return update_memory_item_service(int(item_id), payload)


@router.get("/memory/search")
def search_memory(q: str) -> dict:
    rows = search_memory_fts_or_like(q=str(q or ""), limit=DEFAULT_PAGE_LIMIT)
    return {"items": [memory_from_row(row) for row in rows]}
