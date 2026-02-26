from __future__ import annotations

import sqlite3
from typing import Optional

from backend.src.repositories import memory_repo
from backend.src.services.common.coerce import to_int, to_int_or_default, to_text


def count_memory_items(*, conn: Optional[sqlite3.Connection] = None) -> int:
    return to_int_or_default(memory_repo.count_memory_items(conn=conn), default=0)


def fetch_latest_memory_content(*, conn: Optional[sqlite3.Connection] = None) -> Optional[str]:
    return memory_repo.fetch_latest_memory_content(conn=conn)


def get_memory_item(*, item_id: int, conn: Optional[sqlite3.Connection] = None):
    return memory_repo.get_memory_item(item_id=to_int(item_id), conn=conn)


def list_memory_items(
    *,
    offset: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return memory_repo.list_memory_items(
        offset=to_int(offset),
        limit=to_int(limit),
        conn=conn,
    )


def search_memory_fts_or_like(
    *,
    q: str,
    limit: int = 10,
    conn: Optional[sqlite3.Connection] = None,
):
    return memory_repo.search_memory_fts_or_like(
        q=to_text(q),
        limit=to_int(limit),
        conn=conn,
    )
