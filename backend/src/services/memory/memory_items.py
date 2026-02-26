from typing import Any

from backend.src.common.app_error_utils import invalid_request_error, not_found_error
from backend.src.common.serializers import memory_from_row
from backend.src.common.utils import dump_model, now_iso
from backend.src.prompt.file_trash import finalize_staged_delete, restore_staged_file, stage_delete_file
from backend.src.prompt.paths import memory_prompt_dir
from backend.src.repositories.memory_repo import (
    create_memory_item as create_memory_item_repo,
    delete_memory_item as delete_memory_item_repo,
    get_memory_item as get_memory_item_repo,
    update_memory_item as update_memory_item_repo,
)
from backend.src.constants import (
    DEFAULT_MEMORY_TYPE,
    ERROR_MESSAGE_MEMORY_CONTENT_MISSING,
    ERROR_MESSAGE_MEMORY_NOT_FOUND,
)
from backend.src.services.common.coerce import to_int, to_non_empty_optional_text
from backend.src.services.memory.memory_store import memory_file_path, publish_memory_item_file
from backend.src.storage import get_connection

def create_memory_item(payload: Any) -> dict:
    """
    写入一条记忆到 memory_items（同步）。

    说明：
    - API 层的权限校验由路由负责；
    - Agent 执行链路也会复用该函数（避免直接调用 async 路由函数导致 coroutine 泄漏）。
    """
    data = dump_model(payload)
    content = data.get("content")
    if not isinstance(content, str) or not content.strip():
        raise invalid_request_error(ERROR_MESSAGE_MEMORY_CONTENT_MISSING)

    created_at = now_iso()
    memory_type = data.get("memory_type") or DEFAULT_MEMORY_TYPE
    tags = data.get("tags") or []
    if not isinstance(tags, list):
        tags = []
    task_id = data.get("task_id")
    with get_connection() as conn:
        item_id, _ = create_memory_item_repo(
            content=content,
            memory_type=memory_type,
            tags=tags,
            task_id=task_id,
            created_at=created_at,
            conn=conn,
        )
        publish = publish_memory_item_file(item_id=to_int(item_id), conn=conn)
        row = get_memory_item_repo(item_id=to_int(item_id), conn=conn)

    if not row:
        raise invalid_request_error("memory_item_create_failed")

    if not publish.get("ok"):
        # 原则：落盘失败视为失败（避免 DB 有、文件无，导致“灵魂存档”缺失）
        raise invalid_request_error(str(publish.get("error") or "publish_memory_failed"))

    return {"item": memory_from_row(row), "file": publish}


def update_memory_item(item_id: int, payload: Any) -> dict:
    """
    更新 memory_items 并同步落盘到 backend/prompt/memory。
    """
    data = dump_model(payload)
    with get_connection() as conn:
        row = update_memory_item_repo(
            item_id=to_int(item_id),
            content=data.get("content"),
            memory_type=data.get("memory_type"),
            tags=data.get("tags"),
            task_id=data.get("task_id"),
            conn=conn,
        )
        if not row:
            raise not_found_error(ERROR_MESSAGE_MEMORY_NOT_FOUND)
        publish = publish_memory_item_file(item_id=to_int(item_id), conn=conn)
        latest = get_memory_item_repo(item_id=to_int(item_id), conn=conn)

    if not latest:
        latest = row
    return {"item": memory_from_row(latest), "file": publish}


def delete_memory_item(item_id: int) -> dict:
    """
    强一致删除：同时删除 DB 记录与对应 uid 的文件。

    实现：先将文件移动到隐藏 .trash（便于 DB 失败时回滚），再删除 DB，最后再彻底删除暂存文件。
    """
    trash_path = None
    target_path = None
    publish_err = None

    try:
        with get_connection() as conn:
            existing = get_memory_item_repo(item_id=to_int(item_id), conn=conn)
            if not existing:
                raise not_found_error(ERROR_MESSAGE_MEMORY_NOT_FOUND)

            uid = to_non_empty_optional_text(existing["uid"])
            if uid:
                root = memory_prompt_dir().resolve()
                target_path = memory_file_path(uid).resolve()
                trash_path, publish_err = stage_delete_file(root_dir=root, target_path=target_path)
                if publish_err:
                    raise invalid_request_error(str(publish_err))

            row = delete_memory_item_repo(item_id=to_int(item_id), conn=conn)
            if not row:
                raise not_found_error(ERROR_MESSAGE_MEMORY_NOT_FOUND)
    except Exception:
        # DB 删除失败：尽量把文件恢复回去，避免“文件没了但 DB 还在”
        if trash_path and target_path:
            restore_staged_file(original_path=target_path, trash_path=trash_path)
        raise

    finalize_err = None
    if trash_path:
        finalize_err = finalize_staged_delete(trash_path=trash_path)

    file_info = {
        "uid": str(row["uid"] or "").strip() if row else None,
        "staged": bool(trash_path),
        "trash_path": str(trash_path) if trash_path else None,
        "finalize_error": finalize_err,
    }
    return {"deleted": True, "item": memory_from_row(row), "file": file_info}
