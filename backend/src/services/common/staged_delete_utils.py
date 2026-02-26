from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional, Tuple


def resolve_staged_paths(
    *,
    root: Path,
    source_path: Optional[str],
    trash_rel: Optional[str],
    resolve_source_path: Callable[[str], Path],
) -> Tuple[Optional[Path], Optional[Path]]:
    """
    解析强一致删除流程中的目标文件路径与 .trash 暂存路径。
    """
    target_path: Optional[Path] = None
    trash_path: Optional[Path] = None
    try:
        source_value = str(source_path or "").strip()
        if source_value:
            target_path = resolve_source_path(source_value).resolve()
        if trash_rel:
            candidate = Path(trash_rel)
            trash_path = candidate if candidate.is_absolute() else (root / candidate).resolve()
    except Exception:
        return None, None
    return target_path, trash_path


def staged_delete_file_result(
    *,
    source_path: Optional[str],
    trash_rel: Optional[str],
    finalize_error: Optional[str],
) -> dict:
    """
    统一强一致删除结果中的 file 字段结构。
    """
    return {
        "removed": bool(trash_rel),
        "source_path": str(source_path or "").strip() or None,
        "trash_path": trash_rel,
        "finalize_error": finalize_error,
    }

