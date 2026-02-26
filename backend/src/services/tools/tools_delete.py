from __future__ import annotations

from typing import Any, Dict

from backend.src.common.app_error_utils import invalid_request_error, not_found_error
from backend.src.constants import (
    ERROR_MESSAGE_TOOL_NOT_FOUND,
)
from backend.src.prompt.file_trash import finalize_staged_delete, restore_staged_file
from backend.src.prompt.paths import tools_prompt_dir
from backend.src.repositories.tools_repo import get_tool as get_tool_repo
from backend.src.services.common.staged_delete_utils import (
    resolve_staged_paths,
    staged_delete_file_result,
)
from backend.src.services.tools.tools_store import stage_delete_tool_file_by_source_path, tool_file_path_from_source_path
from backend.src.storage import get_connection

def delete_tool_strong(tool_id: int) -> Dict[str, Any]:
    """
    强一致删除工具：DB + 文件系统同时删除（两阶段：先暂存到 .trash，再删 DB，再删除暂存文件）。
    """
    existing = get_tool_repo(tool_id=int(tool_id))
    if not existing:
        raise not_found_error(ERROR_MESSAGE_TOOL_NOT_FOUND)

    trash_rel, stage_err = stage_delete_tool_file_by_source_path(existing["source_path"])
    if stage_err:
        raise invalid_request_error(str(stage_err))

    root = tools_prompt_dir().resolve()
    target_path, trash_path = resolve_staged_paths(
        root=root,
        source_path=existing["source_path"],
        trash_rel=trash_rel,
        resolve_source_path=lambda source: tool_file_path_from_source_path(source),
    )

    try:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM tools_items WHERE id = ? LIMIT 1",
                (int(tool_id),),
            ).fetchone()
            if not row:
                raise not_found_error(ERROR_MESSAGE_TOOL_NOT_FOUND)
            # 删除版本记录（避免残留脏数据）
            conn.execute("DELETE FROM tool_version_records WHERE tool_id = ?", (int(tool_id),))
            conn.execute("DELETE FROM tools_items WHERE id = ?", (int(tool_id),))
    except Exception:
        # DB 删除失败：尽量恢复文件，避免“文件没了但 DB 还在”
        if target_path and trash_path:
            restore_staged_file(original_path=target_path, trash_path=trash_path)
        raise

    finalize_err = None
    if trash_path:
        finalize_err = finalize_staged_delete(trash_path=trash_path)

    return {
        "row": dict(existing),
        "file": staged_delete_file_result(
            source_path=existing["source_path"],
            trash_rel=trash_rel,
            finalize_error=finalize_err,
        ),
    }

