import json
from typing import Optional

from fastapi import APIRouter

from backend.src.api.schemas import ChatMessageCreate
from backend.src.common.serializers import chat_message_from_row
from backend.src.api.utils import clamp_page_limit, error_response, now_iso, require_write_permission
from backend.src.constants import (
    CHAT_MESSAGES_MAX_LIMIT,
    CHAT_ROLE_ASSISTANT,
    CHAT_ROLE_SYSTEM,
    CHAT_ROLE_USER,
    DEFAULT_PAGE_LIMIT,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_MESSAGE_CHAT_QUERY_MISSING,
    ERROR_MESSAGE_CHAT_ROLE_INVALID,
    ERROR_MESSAGE_CHAT_MESSAGE_MISSING,
    HTTP_STATUS_BAD_REQUEST,
)
from backend.src.services.knowledge.knowledge_query import (
    ChatMessageCreateParams,
    create_chat_message as create_chat_message_repo,
    get_chat_message as get_chat_message_repo,
    list_chat_messages_after,
    list_chat_messages_before,
    list_chat_messages_latest,
    search_chat_messages_like,
)

router = APIRouter()


@router.post("/chat/messages")
@require_write_permission
def create_chat_message(payload: ChatMessageCreate) -> dict:
    role = str(payload.role or "").strip()
    if role not in {CHAT_ROLE_SYSTEM, CHAT_ROLE_USER, CHAT_ROLE_ASSISTANT}:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            ERROR_MESSAGE_CHAT_ROLE_INVALID,
            HTTP_STATUS_BAD_REQUEST,
        )

    content = str(payload.content or "").strip()
    if not content:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            ERROR_MESSAGE_CHAT_MESSAGE_MISSING,
            HTTP_STATUS_BAD_REQUEST,
        )

    created_at = now_iso()
    metadata_text = json.dumps(payload.metadata or {}, ensure_ascii=False) if payload.metadata else None

    msg_id = create_chat_message_repo(
        ChatMessageCreateParams(
            role=role,
            content=content,
            task_id=payload.task_id,
            run_id=payload.run_id,
            metadata=metadata_text,
            created_at=created_at,
        )
    )
    row = get_chat_message_repo(message_id=msg_id)

    return {"message": chat_message_from_row(row)}


@router.get("/chat/messages")
def list_chat_messages(
    limit: int = DEFAULT_PAGE_LIMIT,
    before_id: Optional[int] = None,
    after_id: Optional[int] = None,
) -> dict:
    safe_limit = clamp_page_limit(
        limit,
        default=DEFAULT_PAGE_LIMIT,
        max_value=CHAT_MESSAGES_MAX_LIMIT,
    )

    if after_id is not None:
        rows = list_chat_messages_after(after_id=int(after_id), limit=safe_limit)
        return {"items": [chat_message_from_row(row) for row in rows]}

    if before_id is not None:
        rows = list_chat_messages_before(before_id=int(before_id), limit=safe_limit)
        return {"items": [chat_message_from_row(row) for row in rows]}

    rows = list_chat_messages_latest(limit=safe_limit)
    return {"items": [chat_message_from_row(row) for row in rows]}


@router.get("/chat/search")
def search_chat_messages(q: str, limit: int = DEFAULT_PAGE_LIMIT) -> dict:
    query = str(q or "").strip()
    if not query:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            ERROR_MESSAGE_CHAT_QUERY_MISSING,
            HTTP_STATUS_BAD_REQUEST,
        )

    safe_limit = clamp_page_limit(
        limit,
        default=DEFAULT_PAGE_LIMIT,
        max_value=CHAT_MESSAGES_MAX_LIMIT,
    )

    rows = search_chat_messages_like(query=query, limit=safe_limit)
    return {"items": [chat_message_from_row(row) for row in rows]}
