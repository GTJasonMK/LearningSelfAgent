from __future__ import annotations

import os
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

from backend.src.common.path_utils import is_path_within_root


def _trash_timestamp() -> str:
    """
    生成可用于文件名的时间戳（Windows 兼容：不包含 ':' 等非法字符）。
    """
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def stage_delete_file(*, root_dir: Path, target_path: Path) -> Tuple[Optional[Path], Optional[str]]:
    """
    两阶段删除（第 1 阶段）：将文件从“活跃目录”移动到隐藏 .trash 下。

    返回：(trash_path, error)
    - trash_path=None, error=None 表示目标文件不存在（无需删除）
    - trash_path!=None 表示已暂存到 .trash，可在 DB 操作失败时恢复

    设计要点：
    - 只允许删除 root_dir 内文件，防止路径漂移误删仓库外文件；
    - 使用 os.replace 保证同分区原子重命名；
    - .trash 目录为隐藏目录（skills 同步扫描会跳过；memory 同步扫描也需要跳过）。
    """
    root = Path(root_dir).resolve()
    target = Path(target_path).resolve()
    if not is_path_within_root(target, root):
        return None, "invalid_path"

    if not target.exists():
        return None, None
    if target.is_dir():
        return None, "target_is_dir"

    rel = target.relative_to(root)
    trash_root = root / ".trash"
    # 防冲突：时间戳 + 随机后缀（避免同名文件重复删除产生覆盖）
    suffix = f".{_trash_timestamp()}.{secrets.token_hex(4)}.deleted"
    trash_path = trash_root / rel.parent / f"{rel.name}{suffix}"
    trash_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        os.replace(str(target), str(trash_path))
        return trash_path, None
    except Exception as exc:
        return None, f"stage_delete_failed: {exc}"


def restore_staged_file(*, original_path: Path, trash_path: Path) -> Optional[str]:
    """
    两阶段删除（回滚）：将 .trash 内文件恢复回原路径。
    """
    src = Path(trash_path).resolve()
    dst = Path(original_path).resolve()
    if not src.exists():
        return None
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        os.replace(str(src), str(dst))
        return None
    except Exception as exc:
        return f"restore_failed: {exc}"


def finalize_staged_delete(*, trash_path: Path) -> Optional[str]:
    """
    两阶段删除（第 2 阶段可选）：彻底删除 .trash 内暂存文件。
    """
    path = Path(trash_path).resolve()
    if not path.exists():
        return None
    try:
        path.unlink()
        return None
    except Exception as exc:
        return f"finalize_delete_failed: {exc}"

