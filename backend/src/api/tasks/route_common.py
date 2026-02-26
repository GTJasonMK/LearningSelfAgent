from backend.src.api.utils import error_response
from backend.src.constants import (
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_RECORD_NOT_FOUND,
    ERROR_MESSAGE_TASK_NOT_FOUND,
    ERROR_MESSAGE_TASK_STEP_NOT_FOUND,
    HTTP_STATUS_NOT_FOUND,
    STATUS_CANCELLED,
    STATUS_DONE,
    STATUS_FAILED,
    STATUS_QUEUED,
    STATUS_RUNNING,
    STATUS_STOPPED,
    STATUS_WAITING,
    STEP_STATUS_DONE,
    STEP_STATUS_FAILED,
    STEP_STATUS_PLANNED,
    STEP_STATUS_RUNNING,
    STEP_STATUS_SKIPPED,
    STEP_STATUS_WAITING,
)
from backend.src.services.tasks.task_queries import task_exists

ALLOWED_TASK_STATUSES = frozenset(
    {
        STATUS_QUEUED,
        STATUS_RUNNING,
        STATUS_WAITING,
        STATUS_DONE,
        STATUS_CANCELLED,
        STATUS_FAILED,
        STATUS_STOPPED,
    }
)

ALLOWED_TASK_STEP_STATUSES = frozenset(
    {
        STEP_STATUS_PLANNED,
        STEP_STATUS_RUNNING,
        STEP_STATUS_WAITING,
        STEP_STATUS_DONE,
        STEP_STATUS_FAILED,
        STEP_STATUS_SKIPPED,
    }
)


def task_not_found_response():
    return error_response(
        ERROR_CODE_NOT_FOUND,
        ERROR_MESSAGE_TASK_NOT_FOUND,
        HTTP_STATUS_NOT_FOUND,
    )


def task_step_not_found_response():
    return error_response(
        ERROR_CODE_NOT_FOUND,
        ERROR_MESSAGE_TASK_STEP_NOT_FOUND,
        HTTP_STATUS_NOT_FOUND,
    )


def record_not_found_response():
    return error_response(
        ERROR_CODE_NOT_FOUND,
        ERROR_MESSAGE_RECORD_NOT_FOUND,
        HTTP_STATUS_NOT_FOUND,
    )


def ensure_task_exists_or_error(task_id: int):
    if task_exists(task_id=task_id):
        return None
    return task_not_found_response()


def is_valid_task_status(value) -> bool:
    return value in ALLOWED_TASK_STATUSES


def is_valid_task_step_status(value) -> bool:
    return value in ALLOWED_TASK_STEP_STATUSES
