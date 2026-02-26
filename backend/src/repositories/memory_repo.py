from __future__ import annotations

import sqlite3
from typing import Any, List, Optional, Sequence, Tuple

from backend.src.common.utils import dump_json_list, now_iso
from backend.src.services.search.fts_search import build_fts_or_query, fts_table_exists
from backend.src.repositories.repo_conn import provide_connection


def create_memory_item(
    *,
    content: str,
    memory_type: str,
    tags: Sequence[Any],
    task_id: Optional[int],
    uid: Optional[str] = None,
    created_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, str]:
    """
    创建 memory_items 记录并返回 (item_id, created_at)。
    """
    created = created_at or now_iso()
    tags_value = dump_json_list(tags)
    sql = "INSERT INTO memory_items (content, created_at, memory_type, tags, task_id, uid) VALUES (?, ?, ?, ?, ?, ?)"
    params = (content, created, memory_type, tags_value, task_id, uid)
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, params)
        item_id = int(cursor.lastrowid)
    return item_id, created


def get_memory_item_by_uid(*, uid: str, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM memory_items WHERE uid = ? ORDER BY id ASC LIMIT 1"
    params = (str(uid or ""),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def count_memory_items(*, conn: Optional[sqlite3.Connection] = None) -> int:
    sql = "SELECT COUNT(*) AS count FROM memory_items"
    with provide_connection(conn) as inner:
        row = inner.execute(sql).fetchone()
    return int(row["count"]) if row else 0


def find_memory_item_id_by_task_and_tag_like(
    *,
    task_id: int,
    tag_like: str,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[int]:
    """
    用于去重：判断某个 task 下是否已经存在包含指定 tag（字符串匹配）的记忆条目。
    """
    sql = "SELECT id FROM memory_items WHERE task_id = ? AND tags LIKE ? ORDER BY id ASC LIMIT 1"
    params = (int(task_id), str(tag_like or ""))
    with provide_connection(conn) as inner:
        row = inner.execute(sql, params).fetchone()
    return int(row["id"]) if row and row["id"] is not None else None


def fetch_latest_memory_content(*, conn: Optional[sqlite3.Connection] = None) -> Optional[str]:
    sql = "SELECT content FROM memory_items ORDER BY id DESC LIMIT 1"
    with provide_connection(conn) as inner:
        row = inner.execute(sql).fetchone()
    return row["content"] if row else None


def list_memory_items(
    *,
    offset: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    sql = "SELECT * FROM memory_items ORDER BY id ASC LIMIT ? OFFSET ?"
    params = (int(limit), int(offset))
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def search_memory_fts_or_like(
    *,
    q: str,
    limit: int = 10,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    """
    记忆检索：优先 FTS5，回退 LIKE。
    """
    with provide_connection(conn) as inner:
        fts_query = build_fts_or_query(q, limit=limit)
        if fts_query and fts_table_exists(inner, "memory_items_fts"):
            return list(
                inner.execute(
                    """
                    SELECT m.*
                    FROM memory_items_fts f
                    JOIN memory_items m ON m.id = f.rowid
                    WHERE memory_items_fts MATCH ?
                    ORDER BY bm25(memory_items_fts) ASC, m.id DESC
                    LIMIT ?
                    """,
                    (fts_query, int(limit)),
                ).fetchall()
            )

        pattern = f"%{q}%"
        return list(
            inner.execute(
                "SELECT * FROM memory_items WHERE content LIKE ? OR tags LIKE ? ORDER BY id ASC LIMIT ?",
                (pattern, pattern, int(limit)),
            ).fetchall()
        )


def get_memory_item(*, item_id: int, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM memory_items WHERE id = ?"
    params = (int(item_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def delete_memory_item(*, item_id: int, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    """
    删除 memory_items，并返回被删除的行（若不存在则返回 None）。
    """
    with provide_connection(conn) as inner:
        row = get_memory_item(item_id=item_id, conn=inner)
        if not row:
            return None
        inner.execute("DELETE FROM memory_items WHERE id = ?", (int(item_id),))
        return row


def update_memory_item(
    *,
    item_id: int,
    content: Optional[str] = None,
    memory_type: Optional[str] = None,
    tags: Optional[Sequence[Any]] = None,
    task_id: Optional[int] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    """
    更新 memory_items 指定字段，并返回更新后的行（若不存在则 None）。

    说明：
    - 传入 None 表示“不修改”；tags 传入空列表表示清空。
    """
    fields: List[str] = []
    params: List[Any] = []
    plain_updates = [
        ("content", content),
        ("memory_type", memory_type),
    ]
    for column, value in plain_updates:
        if value is None:
            continue
        fields.append(f"{column} = ?")
        params.append(value)
    if tags is not None:
        fields.append("tags = ?")
        params.append(dump_json_list(tags))
    if task_id is not None:
        fields.append("task_id = ?")
        params.append(int(task_id))

    with provide_connection(conn) as inner:
        existing = get_memory_item(item_id=item_id, conn=inner)
        if not existing:
            return None
        if fields:
            params.append(int(item_id))
            inner.execute(
                f"UPDATE memory_items SET {', '.join(fields)} WHERE id = ?",
                params,
            )
        return get_memory_item(item_id=item_id, conn=inner)
