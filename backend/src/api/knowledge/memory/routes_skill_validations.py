from fastapi import APIRouter

from backend.src.api.schemas import SkillValidationCreate
from backend.src.common.serializers import skill_validation_from_row
from backend.src.api.utils import ensure_write_permission, error_response, now_iso
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    DEFAULT_PAGE_OFFSET,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_INVALID_STATUS,
    ERROR_MESSAGE_SKILL_NOT_FOUND,
    HTTP_STATUS_BAD_REQUEST,
    HTTP_STATUS_NOT_FOUND,
    SKILL_VALIDATION_STATUS_FAIL,
    SKILL_VALIDATION_STATUS_PASS,
    SKILL_VALIDATION_STATUS_UNKNOWN,
)
from backend.src.repositories.skill_validations_repo import (
    create_skill_validation as create_skill_validation_repo,
    get_skill_validation as get_skill_validation_repo,
    list_skill_validations as list_skill_validations_repo,
)
from backend.src.repositories.skills_repo import skill_exists

router = APIRouter()


@router.post("/memory/skills/{skill_id}/validate")
def create_skill_validation(skill_id: int, payload: SkillValidationCreate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    created_at = now_iso()
    allowed_statuses = {
        SKILL_VALIDATION_STATUS_PASS,
        SKILL_VALIDATION_STATUS_FAIL,
        SKILL_VALIDATION_STATUS_UNKNOWN,
    }
    if payload.status not in allowed_statuses:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            ERROR_MESSAGE_INVALID_STATUS,
            HTTP_STATUS_BAD_REQUEST,
        )
    if not skill_exists(skill_id=skill_id):
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_SKILL_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    record_id, _ = create_skill_validation_repo(
        skill_id=skill_id,
        task_id=payload.task_id,
        run_id=payload.run_id,
        status=payload.status,
        notes=payload.notes,
        created_at=created_at,
    )
    row = get_skill_validation_repo(record_id=record_id)
    return {"record": skill_validation_from_row(row)}


@router.get("/memory/skills/{skill_id}/validations")
def list_skill_validations(
    skill_id: int,
    offset: int = DEFAULT_PAGE_OFFSET,
    limit: int = DEFAULT_PAGE_LIMIT,
) -> dict:
    if not skill_exists(skill_id=skill_id):
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_SKILL_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    rows = list_skill_validations_repo(skill_id=skill_id, offset=offset, limit=limit)
    return {"items": [skill_validation_from_row(row) for row in rows]}
