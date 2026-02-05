from fastapi import APIRouter

from backend.src.api.schemas import SkillCreate, SkillUpdate
from backend.src.common.serializers import skill_from_row
from backend.src.api.utils import ensure_write_permission, error_response, now_iso
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    DEFAULT_PAGE_OFFSET,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_SKILL_NOT_FOUND,
    HTTP_STATUS_BAD_REQUEST,
    HTTP_STATUS_NOT_FOUND,
)
from backend.src.repositories.skills_repo import (
    SkillCreateParams,
    create_skill as create_skill_repo,
    get_skill as get_skill_repo,
    list_skills as list_skills_repo,
    search_skills_fts_or_like as search_skills_fts_or_like_repo,
    update_skill as update_skill_repo,
)
from backend.src.services.skills.skills_delete import delete_skill_strong
from backend.src.services.skills.skills_publish import publish_skill_file

router = APIRouter()


@router.get("/memory/skills")
def memory_skills() -> dict:
    items = [skill_from_row(row) for row in list_skills_repo()]
    return {"count": len(items), "items": items}


@router.post("/memory/skills")
def create_skill(payload: SkillCreate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    item_id = create_skill_repo(
        SkillCreateParams(
            name=payload.name,
            description=payload.description,
            scope=payload.scope,
            category=payload.category,
            tags=payload.tags or [],
            triggers=payload.triggers or [],
            aliases=payload.aliases or [],
            source_path=payload.source_path,
            prerequisites=payload.prerequisites or [],
            inputs=payload.inputs or [],
            outputs=payload.outputs or [],
            steps=payload.steps or [],
            failure_modes=payload.failure_modes or [],
            validation=payload.validation or [],
            version=payload.version,
            task_id=payload.task_id,
            created_at=now_iso(),
        )
    )
    # DB + 文件落盘（保持 skills_items 与 backend/prompt/skills 同步）
    source_path, publish_err = publish_skill_file(int(item_id))
    row = get_skill_repo(skill_id=item_id)
    return {
        "item": skill_from_row(row),
        "publish": {"ok": publish_err is None, "source_path": source_path, "error": publish_err},
    }


@router.get("/memory/skills/{skill_id}")
def get_skill(skill_id: int):
    row = get_skill_repo(skill_id=skill_id)
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_SKILL_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"item": skill_from_row(row)}


@router.delete("/memory/skills/{skill_id}")
def delete_skill(skill_id: int):
    permission = ensure_write_permission()
    if permission:
        return permission
    try:
        result = delete_skill_strong(int(skill_id))
    except Exception as exc:
        # API 层保持统一 JSON 错误结构
        if hasattr(exc, "code") and hasattr(exc, "message") and hasattr(exc, "status_code"):
            return error_response(exc.code, exc.message, exc.status_code)  # type: ignore[attr-defined]
        return error_response(ERROR_CODE_INVALID_REQUEST, f"{exc}", HTTP_STATUS_BAD_REQUEST)

    row = result.get("row")
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_SKILL_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"deleted": True, "item": skill_from_row(row), "file": result.get("file")}


@router.patch("/memory/skills/{skill_id}")
def update_skill(skill_id: int, payload: SkillUpdate):
    permission = ensure_write_permission()
    if permission:
        return permission
    row = update_skill_repo(
        skill_id=skill_id,
        name=payload.name,
        description=payload.description,
        scope=payload.scope,
        category=payload.category,
        tags=payload.tags,
        triggers=payload.triggers,
        aliases=payload.aliases,
        source_path=payload.source_path,
        prerequisites=payload.prerequisites,
        inputs=payload.inputs,
        outputs=payload.outputs,
        steps=payload.steps,
        failure_modes=payload.failure_modes,
        validation=payload.validation,
        version=payload.version,
        task_id=payload.task_id,
    )
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_SKILL_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    source_path, publish_err = publish_skill_file(int(skill_id))
    latest = get_skill_repo(skill_id=skill_id)
    return {
        "item": skill_from_row(latest or row),
        "publish": {"ok": publish_err is None, "source_path": source_path, "error": publish_err},
    }


@router.get("/memory/skills/search")
def search_skills(q: str) -> dict:
    rows = search_skills_fts_or_like_repo(q=q, limit=10)
    return {"items": [skill_from_row(row) for row in rows]}
