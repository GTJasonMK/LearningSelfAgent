from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Tuple

from backend.src.common.utils import now_iso
from backend.src.repositories.repo_conn import provide_connection


@dataclass(frozen=True)
class ToolCreateParams:
    """
    tools_items 创建参数（避免长签名）。
    """

    name: str
    description: str
    version: str
    metadata: Optional[dict]
    source_path: Optional[str] = None
    last_used_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


def list_tools(*, conn: Optional[sqlite3.Connection] = None) -> List[sqlite3.Row]:
    sql = "SELECT * FROM tools_items ORDER BY id ASC"
    with provide_connection(conn) as inner:
        return list(inner.execute(sql).fetchall())


def get_tool(*, tool_id: int, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM tools_items WHERE id = ?"
    params = (int(tool_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def _exec_supports_input(metadata_text: Optional[str]) -> bool:
    if not metadata_text:
        return False
    try:
        meta = json.loads(metadata_text)
    except Exception:
        return False
    if not isinstance(meta, dict):
        return False
    exec_spec = meta.get("exec")
    if not isinstance(exec_spec, dict):
        return False
    command = exec_spec.get("command")
    if isinstance(command, str) and "{input}" in command:
        return True
    args = exec_spec.get("args")
    if isinstance(args, list):
        for item in args:
            try:
                if "{input}" in str(item):
                    return True
            except Exception:
                continue
    return False


def get_tool_by_name(*, name: str, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    """
    按 name 获取工具记录：
    - 优先选择 exec 支持 {input} 占位符的版本（避免旧记录忽略 input 导致工具不可用）
    - 若不存在可用版本，则回退到 id 最小的一条（保持兼容）
    """
    sql = "SELECT * FROM tools_items WHERE name = ? ORDER BY id ASC"
    params = (str(name or ""),)
    with provide_connection(conn) as inner:
        rows = list(inner.execute(sql, params).fetchall())
        if not rows:
            return None
        for row in rows:
            if _exec_supports_input(row["metadata"]):
                return row
        return rows[0]


def get_tool_metadata_by_id(*, tool_id: int, conn: Optional[sqlite3.Connection] = None) -> Optional[dict]:
    row = get_tool(tool_id=int(tool_id), conn=conn)
    if not row or not row["metadata"]:
        return None
    try:
        meta = json.loads(row["metadata"])
        return meta if isinstance(meta, dict) else None
    except Exception:
        return None


def get_tool_metadata_by_name(*, name: str, conn: Optional[sqlite3.Connection] = None) -> Optional[dict]:
    row = get_tool_by_name(name=str(name or ""), conn=conn)
    if not row or not row["metadata"]:
        return None
    try:
        meta = json.loads(row["metadata"])
        return meta if isinstance(meta, dict) else None
    except Exception:
        return None


def tool_exists(*, tool_id: int, conn: Optional[sqlite3.Connection] = None) -> bool:
    sql = "SELECT id FROM tools_items WHERE id = ?"
    params = (int(tool_id),)
    with provide_connection(conn) as inner:
        row = inner.execute(sql, params).fetchone()
        return bool(row and row["id"])


def create_tool(params: ToolCreateParams, *, conn: Optional[sqlite3.Connection] = None) -> int:
    created = params.created_at or now_iso()
    updated = params.updated_at or created
    last_used = params.last_used_at or created
    metadata_value = json.dumps(params.metadata, ensure_ascii=False) if params.metadata else None
    sql = (
        "INSERT INTO tools_items (name, description, version, created_at, updated_at, last_used_at, metadata, source_path) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    sql_params: Sequence = (
        params.name,
        params.description,
        params.version,
        created,
        updated,
        last_used,
        metadata_value,
        params.source_path,
    )
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, sql_params)
        return int(cursor.lastrowid)


def update_tool(
    *,
    tool_id: int,
    name: Optional[str],
    description: Optional[str],
    version: Optional[str],
    metadata: Optional[dict],
    source_path: Optional[str] = None,
    change_notes: Optional[str],
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    """
    更新 tools_items，并在版本变更时写入 tool_version_records。
    """
    updated = updated_at or now_iso()
    with provide_connection(conn) as inner:
        row = get_tool(tool_id=tool_id, conn=inner)
        if not row:
            return None

        fields: List[str] = []
        params: List[Any] = []
        if name is not None:
            fields.append("name = ?")
            params.append(name)
        if description is not None:
            fields.append("description = ?")
            params.append(description)
        if version is not None:
            fields.append("version = ?")
            params.append(version)
        if metadata is not None:
            fields.append("metadata = ?")
            params.append(json.dumps(metadata, ensure_ascii=False))
        if source_path is not None:
            fields.append("source_path = ?")
            params.append(source_path)

        previous_version = row["version"]
        next_version = version if version is not None else previous_version

        if fields:
            fields.append("updated_at = ?")
            params.append(updated)
            params.append(int(tool_id))
            inner.execute(
                f"UPDATE tools_items SET {', '.join(fields)} WHERE id = ?",
                params,
            )

        if version is not None and version != previous_version:
            try:
                snapshot = {k: row[k] for k in row.keys()}
                snapshot_text = json.dumps(snapshot, ensure_ascii=False)
            except Exception:
                snapshot_text = None
            inner.execute(
                "INSERT INTO tool_version_records (tool_id, previous_version, next_version, previous_snapshot, change_notes, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (int(tool_id), previous_version, next_version, snapshot_text, change_notes, updated),
            )

        return get_tool(tool_id=tool_id, conn=inner)


def list_tool_versions(
    *,
    tool_id: int,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    sql = "SELECT * FROM tool_version_records WHERE tool_id = ? ORDER BY id ASC"
    params = (int(tool_id),)
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def update_tool_last_used_at(
    *,
    tool_id: int,
    last_used_at: str,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    sql = "UPDATE tools_items SET last_used_at = ? WHERE id = ?"
    params = (str(last_used_at or ""), int(tool_id))
    with provide_connection(conn) as inner:
        inner.execute(sql, params)


def get_tool_by_source_path(*, source_path: str, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM tools_items WHERE source_path = ? ORDER BY id ASC LIMIT 1"
    params = (str(source_path or ""),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()
