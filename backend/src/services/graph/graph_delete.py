from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from backend.src.common.errors import AppError
from backend.src.constants import (
    ERROR_CODE_INVALID_REQUEST,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_EDGE_NOT_FOUND,
    ERROR_MESSAGE_NODE_NOT_FOUND,
    HTTP_STATUS_BAD_REQUEST,
    HTTP_STATUS_NOT_FOUND,
)
from backend.src.prompt.file_trash import finalize_staged_delete, restore_staged_file, stage_delete_file
from backend.src.prompt.paths import graph_prompt_dir
from backend.src.repositories.graph_repo import (
    delete_graph_edge as delete_graph_edge_repo,
    delete_graph_node as delete_graph_node_repo,
    get_graph_edge as get_graph_edge_repo,
    get_graph_node as get_graph_node_repo,
)
from backend.src.services.graph.graph_store import graph_edge_file_path, graph_node_file_path
from backend.src.storage import get_connection


def _stage_one(root: Path, target: Path) -> Tuple[Optional[Path], Optional[str]]:
    return stage_delete_file(root_dir=root, target_path=target)


def delete_graph_edge_strong(edge_id: int) -> Dict[str, Any]:
    """
    强一致删除边：DB + 文件系统同时删除（两阶段：先暂存到 .trash，再删 DB，再删除暂存文件）。
    """
    existing = get_graph_edge_repo(edge_id=int(edge_id))
    if not existing:
        raise AppError(
            code=ERROR_CODE_NOT_FOUND,
            message=ERROR_MESSAGE_EDGE_NOT_FOUND,
            status_code=HTTP_STATUS_NOT_FOUND,
        )

    root = graph_prompt_dir().resolve()
    target_path = graph_edge_file_path(int(edge_id)).resolve()
    trash_path, err = _stage_one(root, target_path)
    if err:
        raise AppError(
            code=ERROR_CODE_INVALID_REQUEST,
            message=str(err),
            status_code=HTTP_STATUS_BAD_REQUEST,
        )

    try:
        with get_connection() as conn:
            row = delete_graph_edge_repo(edge_id=int(edge_id), conn=conn)
            if not row:
                raise AppError(
                    code=ERROR_CODE_NOT_FOUND,
                    message=ERROR_MESSAGE_EDGE_NOT_FOUND,
                    status_code=HTTP_STATUS_NOT_FOUND,
                )
    except Exception:
        if trash_path:
            restore_staged_file(original_path=target_path, trash_path=trash_path)
        raise

    finalize_err = None
    if trash_path:
        finalize_err = finalize_staged_delete(trash_path=trash_path)

    return {
        "row": row,
        "file": {
            "removed": bool(trash_path),
            "trash_path": str(trash_path) if trash_path else None,
            "finalize_error": finalize_err,
        },
    }


def delete_graph_node_strong(node_id: int) -> Dict[str, Any]:
    """
    强一致删除节点：DB + 文件系统同时删除，并级联删除相关边的文件。
    """
    existing = get_graph_node_repo(node_id=int(node_id))
    if not existing:
        raise AppError(
            code=ERROR_CODE_NOT_FOUND,
            message=ERROR_MESSAGE_NODE_NOT_FOUND,
            status_code=HTTP_STATUS_NOT_FOUND,
        )

    root = graph_prompt_dir().resolve()

    # 先查出相关边 id（用于同步删除边文件）
    with get_connection() as conn:
        edge_rows = conn.execute(
            "SELECT id FROM graph_edges WHERE source = ? OR target = ? ORDER BY id ASC",
            (int(node_id), int(node_id)),
        ).fetchall()
    edge_ids = [int(r["id"]) for r in edge_rows]

    staged: List[Tuple[Path, Path]] = []

    def _stage(target: Path) -> None:
        trash_path, err = _stage_one(root, target)
        if err:
            raise AppError(ERROR_CODE_INVALID_REQUEST, str(err), HTTP_STATUS_BAD_REQUEST)
        if trash_path:
            staged.append((target, trash_path))

    try:
        # 1) 暂存节点文件 + 边文件
        _stage(graph_node_file_path(int(node_id)).resolve())
        for eid in edge_ids:
            _stage(graph_edge_file_path(int(eid)).resolve())

        # 2) 删除 DB（节点会级联删边）
        with get_connection() as conn:
            row = delete_graph_node_repo(node_id=int(node_id), conn=conn)
            if not row:
                raise AppError(
                    code=ERROR_CODE_NOT_FOUND,
                    message=ERROR_MESSAGE_NODE_NOT_FOUND,
                    status_code=HTTP_STATUS_NOT_FOUND,
                )
    except Exception:
        # 回滚：尽量恢复所有已暂存文件
        for original, trash in reversed(staged):
            restore_staged_file(original_path=original, trash_path=trash)
        raise

    finalize_errors: List[str] = []
    for _original, trash in staged:
        err = finalize_staged_delete(trash_path=trash)
        if err:
            finalize_errors.append(err)

    return {
        "row": row,
        "edges_deleted": len(edge_ids),
        "file": {
            "staged": len(staged),
            "finalize_errors": finalize_errors,
        },
    }

