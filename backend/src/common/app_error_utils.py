from __future__ import annotations

from backend.src.common.errors import AppError
from backend.src.common.utils import error_response
from backend.src.constants import (
    ERROR_CODE_INVALID_REQUEST,
    ERROR_CODE_NOT_FOUND,
    HTTP_STATUS_BAD_REQUEST,
    HTTP_STATUS_NOT_FOUND,
)


def invalid_request_error(message: str) -> AppError:
    return AppError(
        code=ERROR_CODE_INVALID_REQUEST,
        message=str(message),
        status_code=HTTP_STATUS_BAD_REQUEST,
    )


def not_found_error(message: str) -> AppError:
    return AppError(
        code=ERROR_CODE_NOT_FOUND,
        message=str(message),
        status_code=HTTP_STATUS_NOT_FOUND,
    )


def app_error_response(exc: AppError):
    return error_response(exc.code, exc.message, exc.status_code)
