from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import List, Optional, Sequence

from backend.src.common.utils import now_iso
from backend.src.repositories.repo_conn import provide_connection


@dataclass(frozen=True)
class ChatMessageCreateParams:
    """
    chat_messages 创建参数。
    """

    role: str
    content: str
    task_id: Optional[int]
    run_id: Optional[int]
    metadata: Optional[str]
    created_at: Optional[str] = None


def get_chat_message(*, message_id: int, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM chat_messages WHERE id = ?"
    params = (int(message_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def create_chat_message(
    params: ChatMessageCreateParams,
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> int:
    created = params.created_at or now_iso()
    sql = "INSERT INTO chat_messages (role, content, created_at, task_id, run_id, metadata) VALUES (?, ?, ?, ?, ?, ?)"
    sql_params: Sequence = (params.role, params.content, created, params.task_id, params.run_id, params.metadata)
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, sql_params)
        return int(cursor.lastrowid)


def list_chat_messages_latest(
    *,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    """
    返回最新 N 条（时间正序）。
    """
    sql = "SELECT * FROM chat_messages ORDER BY id DESC LIMIT ?"
    params = (int(limit),)
    with provide_connection(conn) as inner:
        rows = inner.execute(sql, params).fetchall()
    return list(reversed(list(rows)))


def list_chat_messages_before(
    *,
    before_id: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    """
    返回 id < before_id 的历史片段（时间正序）。
    """
    sql = "SELECT * FROM chat_messages WHERE id < ? ORDER BY id DESC LIMIT ?"
    params = (int(before_id), int(limit))
    with provide_connection(conn) as inner:
        rows = inner.execute(sql, params).fetchall()
    return list(reversed(list(rows)))


def list_chat_messages_after(
    *,
    after_id: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    """
    返回 id > after_id 的增量（时间正序）。
    """
    sql = "SELECT * FROM chat_messages WHERE id > ? ORDER BY id ASC LIMIT ?"
    params = (int(after_id), int(limit))
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def search_chat_messages_like(
    *,
    query: str,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    """
    按 content LIKE 检索（按时间倒序）。
    """
    like = f"%{query}%"
    sql = "SELECT * FROM chat_messages WHERE content LIKE ? ORDER BY id DESC LIMIT ?"
    params = (like, int(limit))
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())
