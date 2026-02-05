from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from backend.src.common.errors import AppError
from backend.src.constants import (
    ERROR_CODE_INVALID_REQUEST,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_SKILL_NOT_FOUND,
    HTTP_STATUS_BAD_REQUEST,
    HTTP_STATUS_NOT_FOUND,
)
from backend.src.prompt.file_trash import finalize_staged_delete, restore_staged_file
from backend.src.prompt.paths import skills_prompt_dir
from backend.src.repositories.skills_repo import delete_skill as delete_skill_repo
from backend.src.repositories.skills_repo import get_skill as get_skill_repo
from backend.src.services.skills.skills_publish import stage_delete_skill_file_by_source_path
from backend.src.storage import get_connection


def delete_skill_strong(skill_id: int) -> Dict[str, Any]:
    """
    强一致删除技能：DB + 文件系统同时删除（两阶段：先暂存到 .trash，再删 DB，再删除暂存文件）。
    """
    existing = get_skill_repo(skill_id=int(skill_id))
    if not existing:
        raise AppError(
            code=ERROR_CODE_NOT_FOUND,
            message=ERROR_MESSAGE_SKILL_NOT_FOUND,
            status_code=HTTP_STATUS_NOT_FOUND,
        )

    trash_rel, stage_err = stage_delete_skill_file_by_source_path(existing["source_path"])
    if stage_err:
        raise AppError(
            code=ERROR_CODE_INVALID_REQUEST,
            message=str(stage_err),
            status_code=HTTP_STATUS_BAD_REQUEST,
        )

    root = skills_prompt_dir().resolve()
    target_path = None
    trash_path = None
    try:
        source_path = str(existing["source_path"] or "").strip()
        if source_path:
            target_path = (root / source_path).resolve()
        if trash_rel:
            candidate = Path(trash_rel)
            trash_path = candidate if candidate.is_absolute() else (root / candidate).resolve()
    except Exception:
        target_path = None
        trash_path = None

    try:
        with get_connection() as conn:
            row = delete_skill_repo(skill_id=int(skill_id), conn=conn)
            if not row:
                raise AppError(
                    code=ERROR_CODE_NOT_FOUND,
                    message=ERROR_MESSAGE_SKILL_NOT_FOUND,
                    status_code=HTTP_STATUS_NOT_FOUND,
                )
    except Exception:
        # DB 删除失败：尽量恢复文件，避免“文件没了但 DB 还在”
        if target_path and trash_path:
            restore_staged_file(original_path=target_path, trash_path=trash_path)
        raise

    finalize_err = None
    if trash_path:
        finalize_err = finalize_staged_delete(trash_path=trash_path)

    return {
        "row": row,
        "file": {
            "removed": bool(trash_rel),
            "source_path": str(existing["source_path"] or "").strip() or None,
            "trash_path": trash_rel,
            "finalize_error": finalize_err,
        },
    }

