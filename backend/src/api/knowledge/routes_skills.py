import asyncio
from typing import Dict, Optional

from fastapi import APIRouter

from backend.src.common.serializers import skill_from_row
from backend.src.api.utils import ensure_write_permission, parse_json_list
from backend.src.constants import DEFAULT_PAGE_LIMIT, DEFAULT_PAGE_OFFSET
from backend.src.repositories.skills_repo import (
    list_skill_catalog_source,
    search_skills_filtered_like,
    update_skill_status,
    list_skills_by_status,
    get_skill,
    VALID_SKILL_STATUSES,
)
from backend.src.services.skills.skills_sync import sync_skills_from_files

router = APIRouter()


@router.post("/skills/sync")
async def sync_skills() -> dict:
    """
    将 backend/prompt/skills 下的技能文件同步到数据库（skills_items）。
    """
    permission = ensure_write_permission()
    if permission:
        return permission
    result = await asyncio.to_thread(sync_skills_from_files)
    return {"result": result}


@router.get("/skills/search")
def search_skills(
    q: Optional[str] = None,
    category: Optional[str] = None,
    tag: Optional[str] = None,
    skill_type: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = DEFAULT_PAGE_LIMIT,
    offset: int = DEFAULT_PAGE_OFFSET,
) -> dict:
    """
    技能检索（面向 UI 与 Agent 的快速查询）。

    - q：关键字（name/description/scope/category/tags/triggers 模糊匹配）
    - category：类目（支持前缀匹配：tool 会匹配 tool.*）
    - tag：标签（JSON 数组内精确匹配）
    - skill_type：methodology / solution
    - status：draft / approved / deprecated / abandoned
    """
    total, rows = search_skills_filtered_like(
        q=q,
        category=category,
        tag=tag,
        skill_type=skill_type,
        status=status,
        limit=limit,
        offset=offset,
    )
    return {"total": total, "items": [skill_from_row(row) for row in rows]}


@router.get("/skills/catalog")
def skills_catalog(limit_tags: int = 30) -> dict:
    """
    返回技能类目与 tags 的聚合统计（便于快速筛选）。
    """
    rows = list_skill_catalog_source()

    categories_map: Dict[str, int] = {}
    tags_map: Dict[str, int] = {}
    types_map: Dict[str, int] = {}
    status_map: Dict[str, int] = {}

    for row in rows:
        category = (row["category"] or "").strip() or "misc"
        categories_map[category] = categories_map.get(category, 0) + 1

        tags = parse_json_list(row["tags"]) if row["tags"] else []
        for item in tags:
            key = str(item).strip()
            if not key:
                continue
            tags_map[key] = tags_map.get(key, 0) + 1

        stype = (row["skill_type"] or "").strip() or "methodology"
        types_map[stype] = types_map.get(stype, 0) + 1

        st = (row["status"] or "").strip().lower() or "approved"
        status_map[st] = status_map.get(st, 0) + 1

    categories = [
        {"category": k, "count": v}
        for k, v in sorted(categories_map.items(), key=lambda kv: (-kv[1], kv[0]))
    ]
    tags = [
        {"tag": k, "count": v}
        for k, v in sorted(tags_map.items(), key=lambda kv: (-kv[1], kv[0]))[:limit_tags]
    ]
    skill_types = [
        {"skill_type": k, "count": v}
        for k, v in sorted(types_map.items(), key=lambda kv: (-kv[1], kv[0]))
    ]
    statuses = [
        {"status": k, "count": v}
        for k, v in sorted(status_map.items(), key=lambda kv: (-kv[1], kv[0]))
    ]

    return {
        "count": len(rows),
        "categories": categories,
        "tags": tags,
        "skill_types": skill_types,
        "statuses": statuses,
    }


@router.get("/skills/by-status/{status}")
def list_skills_with_status(
    status: str,
    limit: int = DEFAULT_PAGE_LIMIT,
    offset: int = DEFAULT_PAGE_OFFSET,
) -> dict:
    """
    按状态列出技能（用于管理界面查看 draft/deprecated 技能）。

    - status: draft / approved / deprecated
    """
    status = status.strip().lower()
    if status not in VALID_SKILL_STATUSES:
        return {"error": f"无效的状态值，有效值为: {', '.join(VALID_SKILL_STATUSES)}", "total": 0, "items": []}
    total, rows = list_skills_by_status(status=status, limit=limit, offset=offset)
    return {"total": total, "items": [skill_from_row(row) for row in rows]}


@router.put("/skills/{skill_id}/status")
async def change_skill_status(skill_id: int, status: str) -> dict:
    """
    更新技能状态（draft → approved → deprecated 生命周期管理）。

    状态转换规则：
    - draft → approved：技能通过审核，可参与常规检索
    - approved → deprecated：技能已过时，不再参与检索
    - deprecated → approved：重新启用已过时的技能
    - draft → deprecated：直接废弃未审核的草稿

    参数：
    - skill_id: 技能 ID
    - status: 目标状态（draft/approved/deprecated）
    """
    permission = ensure_write_permission()
    if permission:
        return permission

    status = status.strip().lower()
    if status not in VALID_SKILL_STATUSES:
        return {"error": f"无效的状态值，有效值为: {', '.join(VALID_SKILL_STATUSES)}"}

    row = await asyncio.to_thread(update_skill_status, skill_id=skill_id, status=status)
    if row is None:
        return {"error": "技能不存在或更新失败"}

    return {"success": True, "skill": skill_from_row(row)}


@router.post("/skills/{skill_id}/approve")
async def approve_skill(skill_id: int) -> dict:
    """
    审核通过技能（draft → approved）。
    """
    permission = ensure_write_permission()
    if permission:
        return permission

    # 检查当前状态
    existing = await asyncio.to_thread(get_skill, skill_id=skill_id)
    if existing is None:
        return {"error": "技能不存在"}

    current_status = (existing["status"] or "approved").strip().lower()
    if current_status == "approved":
        return {"message": "技能已经是 approved 状态", "skill": skill_from_row(existing)}

    row = await asyncio.to_thread(update_skill_status, skill_id=skill_id, status="approved")
    if row is None:
        return {"error": "更新失败"}

    return {"success": True, "skill": skill_from_row(row)}


@router.post("/skills/{skill_id}/deprecate")
async def deprecate_skill(skill_id: int) -> dict:
    """
    废弃技能（approved/draft → deprecated）。
    """
    permission = ensure_write_permission()
    if permission:
        return permission

    existing = await asyncio.to_thread(get_skill, skill_id=skill_id)
    if existing is None:
        return {"error": "技能不存在"}

    current_status = (existing["status"] or "approved").strip().lower()
    if current_status == "deprecated":
        return {"message": "技能已经是 deprecated 状态", "skill": skill_from_row(existing)}

    row = await asyncio.to_thread(update_skill_status, skill_id=skill_id, status="deprecated")
    if row is None:
        return {"error": "更新失败"}

    return {"success": True, "skill": skill_from_row(row)}
