import asyncio
import logging
from typing import List

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from backend.src.api.schemas import (
    LLMCallCreate,
    LLMChatStreamRequest,
    PromptTemplateCreate,
    PromptTemplateUpdate,
)
from backend.src.common.serializers import prompt_template_from_row
from backend.src.api.utils import (
    ensure_write_permission,
    error_response,
    now_iso,
)
from backend.src.common.errors import AppError
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    DEFAULT_PAGE_OFFSET,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_LLM_CHAT_MESSAGE_MISSING,
    ERROR_MESSAGE_LLM_SDK_MISSING,
    ERROR_MESSAGE_LLM_CALL_FAILED,
    ERROR_MESSAGE_PROMPT_NOT_FOUND,
    LLM_CHAT_OUTPUT_SYSTEM_PROMPT,
    HTTP_STATUS_BAD_REQUEST,
    HTTP_STATUS_NOT_FOUND,
)
from backend.src.repositories.prompt_templates_repo import (
    create_prompt_template as create_prompt_template_repo,
    delete_prompt_template as delete_prompt_template_repo,
    get_prompt_template as get_prompt_template_repo,
    list_prompt_templates as list_prompt_templates_repo,
    update_prompt_template as update_prompt_template_repo,
)
from backend.src.services.llm.llm_calls import create_llm_call as create_llm_call_service

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/prompts")
def create_prompt_template(payload: PromptTemplateCreate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    created_at = now_iso()
    updated_at = created_at
    template_id = create_prompt_template_repo(
        name=payload.name,
        template=payload.template,
        description=payload.description,
        created_at=created_at,
        updated_at=updated_at,
    )
    row = get_prompt_template_repo(template_id=template_id)
    return {"prompt": prompt_template_from_row(row)}


@router.get("/prompts")
def list_prompt_templates(
    offset: int = DEFAULT_PAGE_OFFSET, limit: int = DEFAULT_PAGE_LIMIT
) -> dict:
    rows = list_prompt_templates_repo(offset=offset, limit=limit)
    return {"items": [prompt_template_from_row(row) for row in rows]}


@router.get("/prompts/{template_id}")
def get_prompt_template(template_id: int):
    row = get_prompt_template_repo(template_id=template_id)
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_PROMPT_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"prompt": prompt_template_from_row(row)}


@router.patch("/prompts/{template_id}")
def update_prompt_template(template_id: int, payload: PromptTemplateUpdate):
    permission = ensure_write_permission()
    if permission:
        return permission
    updated_at = now_iso()
    row = update_prompt_template_repo(
        template_id=template_id,
        name=payload.name,
        template=payload.template,
        description=payload.description,
        updated_at=updated_at,
    )
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_PROMPT_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"prompt": prompt_template_from_row(row)}


@router.delete("/prompts/{template_id}")
def delete_prompt_template(template_id: int):
    permission = ensure_write_permission()
    if permission:
        return permission
    row = delete_prompt_template_repo(template_id=template_id)
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_PROMPT_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"deleted": True, "prompt": prompt_template_from_row(row)}


@router.post("/llm/calls")
def create_llm_call(payload: LLMCallCreate):
    permission = ensure_write_permission()
    if permission:
        return permission
    return create_llm_call_service(payload)


@router.post("/llm/chat/stream")
async def stream_llm_chat(payload: LLMChatStreamRequest):
    """
    桌宠对话专用：SSE 流式输出（text/event-stream）
    data: {"delta":"..."} 逐段输出；event: done 表示结束；event: error 表示失败。
    """

    def dump_model(obj) -> dict:
        return obj.model_dump() if hasattr(obj, "model_dump") else obj.dict()

    messages = None
    if payload.messages:
        messages = [dump_model(m) for m in payload.messages]
    elif payload.message:
        messages = [{"role": "user", "content": payload.message}]
    else:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            ERROR_MESSAGE_LLM_CHAT_MESSAGE_MISSING,
            HTTP_STATUS_BAD_REQUEST,
        )

    # 统一输出风格：强制 Markdown（前端会渲染 Markdown，提高可读性）
    messages = [{"role": "system", "content": LLM_CHAT_OUTPUT_SYSTEM_PROMPT}] + (messages or [])

    try:
        from backend.src.services.llm.llm_client import resolve_default_model
        model = payload.model or await asyncio.to_thread(resolve_default_model)
    except Exception:
        model = payload.model
    params = payload.parameters or {}

    # 统一复用服务层封装（仿照 llm_tool.py）
    try:
        from backend.src.services.llm.llm_client import LLMClient, sse_json
    except Exception:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            ERROR_MESSAGE_LLM_SDK_MISSING,
            HTTP_STATUS_BAD_REQUEST,
        )

    try:
        llm = await asyncio.to_thread(LLMClient, provider=payload.provider)
    except AppError as exc:
        return error_response(exc.code, exc.message, exc.status_code)

    async def gen():
        cancelled = False
        try:
            async for chunk in llm.stream_chat(
                messages=messages,
                model=model,
                parameters=params,
            ):
                delta = chunk.get("content") or ""
                if delta:
                    yield sse_json({"delta": delta})
        except (asyncio.CancelledError, GeneratorExit):
            # SSE 客户端断开/主动取消时：不要再尝试继续 yield，否则会触发
            # “async generator ignored GeneratorExit/CancelledError” 类错误。
            cancelled = True
            raise
        except Exception as exc:
            try:
                yield sse_json(
                    {"message": f"{ERROR_MESSAGE_LLM_CALL_FAILED}:{exc}"},
                    event="error",
                )
            except BaseException:
                cancelled = True
                return
        finally:
            try:
                await llm.aclose()
            except asyncio.CancelledError:
                cancelled = True
                raise
            except Exception as exc:
                logger.exception("llm.aclose failed: %s", exc)
            if not cancelled:
                try:
                    yield sse_json({"type": "done"}, event="done")
                except BaseException:
                    return

    headers = {
        # SSE 推荐：关闭缓存，尽量避免中间层缓冲导致“假流式”
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(gen(), media_type="text/event-stream", headers=headers)
