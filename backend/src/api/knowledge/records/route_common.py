from backend.src.api.utils import error_response
from backend.src.constants import (
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_RECORD_NOT_FOUND,
    HTTP_STATUS_NOT_FOUND,
)


def record_not_found_response():
    return error_response(
        ERROR_CODE_NOT_FOUND,
        ERROR_MESSAGE_RECORD_NOT_FOUND,
        HTTP_STATUS_NOT_FOUND,
    )
