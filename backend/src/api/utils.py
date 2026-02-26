from __future__ import annotations

import inspect
from functools import wraps
from typing import Optional

from backend.src.common.app_error_utils import app_error_response
from backend.src.common.utils import (
    as_bool,
    error_response,
    now_iso,
    parse_positive_int,
    parse_json_list,
    parse_json_value,
    render_prompt,
)
from backend.src.constants import ERROR_CODE_INVALID_REQUEST, HTTP_STATUS_BAD_REQUEST
from backend.src.services.execution.shell_command import run_shell_command as _run_shell_command
from backend.src.services.permissions.permission_checks import ensure_exec_permission, ensure_write_permission
from backend.src.services.permissions.permissions_store import has_write_permission as _has_write_permission


def has_write_permission() -> bool:
    return _has_write_permission()


def require_write_permission(handler):
    """
    API 入口统一写权限守卫：
    - 无权限：直接返回 JSONResponse；
    - 有权限：继续执行原处理函数。
    """
    if inspect.iscoroutinefunction(handler):
        @wraps(handler)
        async def _wrapped_async(*args, **kwargs):
            permission = ensure_write_permission()
            if permission:
                return permission
            return await handler(*args, **kwargs)

        return _wrapped_async

    @wraps(handler)
    def _wrapped_sync(*args, **kwargs):
        permission = ensure_write_permission()
        if permission:
            return permission
        return handler(*args, **kwargs)

    return _wrapped_sync


def run_shell_command(payload: dict) -> tuple[Optional[dict], Optional[str]]:
    return _run_shell_command(payload)


def invalid_request_response_from_exception(exc: Exception):
    return error_response(
        ERROR_CODE_INVALID_REQUEST,
        str(exc),
        HTTP_STATUS_BAD_REQUEST,
    )


def clamp_non_negative_int(value, *, default: int = 0) -> int:
    try:
        parsed = int(value) if value is not None else int(default)
    except Exception:
        parsed = int(default)
    return parsed if parsed >= 0 else 0


def clamp_page_limit(limit, *, default: int, max_value: Optional[int] = None) -> int:
    try:
        parsed = int(limit) if limit is not None else int(default)
    except Exception:
        parsed = int(default)
    if parsed <= 0:
        parsed = int(default)
    if max_value is not None:
        try:
            max_limit = int(max_value)
        except Exception:
            max_limit = int(default)
        if parsed > max_limit:
            parsed = max_limit
    return parsed
