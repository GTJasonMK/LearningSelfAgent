from __future__ import annotations

from typing import Optional

from backend.src.common.utils import error_response
from backend.src.constants import (
    ERROR_CODE_FORBIDDEN,
    ERROR_MESSAGE_PERMISSION_DENIED,
    HTTP_STATUS_FORBIDDEN,
)
from backend.src.services.permissions.permissions_store import (
    has_exec_permission,
    has_write_permission,
)


def _permission_denied_response():
    return error_response(
        ERROR_CODE_FORBIDDEN,
        ERROR_MESSAGE_PERMISSION_DENIED,
        HTTP_STATUS_FORBIDDEN,
    )


def ensure_write_permission():
    """
    权限兜底（写入）。

    说明：
    - 返回 JSONResponse 以便 API/Agent runner 直接 short-circuit；
    - 该函数位于 services 层，避免业务链路反向依赖 api.utils。
    """
    if has_write_permission():
        return None
    return _permission_denied_response()


def ensure_exec_permission(workdir: Optional[str]):
    """
    权限兜底（执行）。
    """
    if has_exec_permission(workdir):
        return None
    return _permission_denied_response()

