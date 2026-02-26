from __future__ import annotations

import sqlite3
from typing import Optional

from backend.src.repositories import chat_messages_repo
from backend.src.services.common.coerce import to_int, to_text

ChatMessageCreateParams = chat_messages_repo.ChatMessageCreateParams


def create_chat_message(
    params: ChatMessageCreateParams,
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> int:
    return to_int(chat_messages_repo.create_chat_message(params, conn=conn))


def get_chat_message(*, message_id: int, conn: Optional[sqlite3.Connection] = None):
    return chat_messages_repo.get_chat_message(message_id=to_int(message_id), conn=conn)


def list_chat_messages_latest(
    *,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return chat_messages_repo.list_chat_messages_latest(limit=to_int(limit), conn=conn)


def list_chat_messages_before(
    *,
    before_id: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return chat_messages_repo.list_chat_messages_before(
        before_id=to_int(before_id),
        limit=to_int(limit),
        conn=conn,
    )


def list_chat_messages_after(
    *,
    after_id: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return chat_messages_repo.list_chat_messages_after(
        after_id=to_int(after_id),
        limit=to_int(limit),
        conn=conn,
    )


def search_chat_messages_like(
    *,
    query: str,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return chat_messages_repo.search_chat_messages_like(
        query=to_text(query),
        limit=to_int(limit),
        conn=conn,
    )
