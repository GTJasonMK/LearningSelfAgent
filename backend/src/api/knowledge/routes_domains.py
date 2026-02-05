"""
领域系统 API 路由。

提供领域的 CRUD 操作接口。
"""
import json
from typing import Optional

from fastapi import APIRouter

from backend.src.api.schemas import DomainCreate, DomainUpdate
from backend.src.api.utils import ensure_write_permission
from backend.src.constants import (
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_NOT_FOUND,
    HTTP_STATUS_NOT_FOUND,
)
from backend.src.repositories.domains_repo import (
    DomainCreateParams,
    DomainUpdateParams,
    count_domains,
    create_domain as create_domain_repo,
    delete_domain as delete_domain_repo,
    get_domain as get_domain_repo,
    list_domains as list_domains_repo,
    list_top_level_domains,
    list_child_domains,
    search_domains_by_keyword,
    update_domain as update_domain_repo,
)
from backend.src.storage import get_connection

router = APIRouter()


def _domain_from_row(row) -> dict:
    """将数据库行转换为 API 响应格式。"""
    if not row:
        return None
    keywords = None
    if row["keywords"]:
        try:
            keywords = json.loads(row["keywords"])
        except json.JSONDecodeError:
            keywords = []
    return {
        "id": row["id"],
        "domain_id": row["domain_id"],
        "name": row["name"],
        "parent_id": row["parent_id"],
        "description": row["description"],
        "keywords": keywords,
        "skill_count": row["skill_count"],
        "status": row["status"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


@router.get("/domains")
def list_domains(parent_id: Optional[str] = None, status: Optional[str] = None) -> dict:
    """
    列出领域。

    参数：
    - parent_id: 父领域 ID（空字符串表示查询一级领域）
    - status: 状态筛选（active/deprecated）
    """
    with get_connection() as conn:
        rows = list_domains_repo(parent_id=parent_id, status=status, conn=conn)
    return {"items": [_domain_from_row(row) for row in rows], "total": len(rows)}


@router.get("/domains/tree")
def list_domains_tree() -> dict:
    """
    以树形结构列出领域。

    返回一级领域及其子领域。
    """
    with get_connection() as conn:
        top_level = list_top_level_domains(conn=conn)
        result = []
        for domain in top_level:
            domain_dict = _domain_from_row(domain)
            children = list_child_domains(parent_id=domain["domain_id"], conn=conn)
            domain_dict["children"] = [_domain_from_row(child) for child in children]
            result.append(domain_dict)
    return {"items": result, "total": len(result)}


@router.get("/domains/search")
def search_domains(keyword: str) -> dict:
    """按关键词搜索领域。"""
    with get_connection() as conn:
        rows = search_domains_by_keyword(keyword=keyword, conn=conn)
    return {"items": [_domain_from_row(row) for row in rows], "total": len(rows)}


@router.get("/domains/{domain_id}")
def get_domain(domain_id: str) -> dict:
    """获取单个领域详情。"""
    with get_connection() as conn:
        row = get_domain_repo(domain_id=domain_id, conn=conn)
    if not row:
        return {
            "error": {
                "code": ERROR_CODE_NOT_FOUND,
                "message": ERROR_MESSAGE_NOT_FOUND,
                "status": HTTP_STATUS_NOT_FOUND,
            }
        }
    return _domain_from_row(row)


@router.post("/domains")
def create_domain(payload: DomainCreate) -> dict:
    """创建新领域。"""
    permission = ensure_write_permission()
    if permission:
        return permission
    with get_connection() as conn:
        # 检查是否已存在
        existing = get_domain_repo(domain_id=payload.domain_id, conn=conn)
        if existing:
            return {
                "error": {
                    "code": "DOMAIN_EXISTS",
                    "message": f"Domain '{payload.domain_id}' already exists",
                    "status": 400,
                }
            }
        domain_id = create_domain_repo(
            DomainCreateParams(
                domain_id=payload.domain_id,
                name=payload.name,
                parent_id=payload.parent_id,
                description=payload.description,
                keywords=payload.keywords,
            ),
            conn=conn,
        )
        row = get_domain_repo(id=domain_id, conn=conn)
    return _domain_from_row(row)


@router.patch("/domains/{domain_id}")
def update_domain(domain_id: str, payload: DomainUpdate) -> dict:
    """更新领域。"""
    permission = ensure_write_permission()
    if permission:
        return permission
    with get_connection() as conn:
        existing = get_domain_repo(domain_id=domain_id, conn=conn)
        if not existing:
            return {
                "error": {
                    "code": ERROR_CODE_NOT_FOUND,
                    "message": ERROR_MESSAGE_NOT_FOUND,
                    "status": HTTP_STATUS_NOT_FOUND,
                }
            }
        update_domain_repo(
            domain_id=domain_id,
            params=DomainUpdateParams(
                name=payload.name,
                description=payload.description,
                keywords=payload.keywords,
                status=payload.status,
            ),
            conn=conn,
        )
        row = get_domain_repo(domain_id=domain_id, conn=conn)
    return _domain_from_row(row)


@router.delete("/domains/{domain_id}")
def delete_domain(domain_id: str) -> dict:
    """删除领域。"""
    permission = ensure_write_permission()
    if permission:
        return permission
    with get_connection() as conn:
        existing = get_domain_repo(domain_id=domain_id, conn=conn)
        if not existing:
            return {
                "error": {
                    "code": ERROR_CODE_NOT_FOUND,
                    "message": ERROR_MESSAGE_NOT_FOUND,
                    "status": HTTP_STATUS_NOT_FOUND,
                }
            }
        # 检查是否有子领域
        children = list_child_domains(parent_id=domain_id, conn=conn)
        if children:
            return {
                "error": {
                    "code": "DOMAIN_HAS_CHILDREN",
                    "message": f"Domain '{domain_id}' has {len(children)} child domains",
                    "status": 400,
                }
            }
        # 检查是否有关联技能
        if existing["skill_count"] > 0:
            return {
                "error": {
                    "code": "DOMAIN_HAS_SKILLS",
                    "message": f"Domain '{domain_id}' has {existing['skill_count']} associated skills",
                    "status": 400,
                }
            }
        delete_domain_repo(domain_id=domain_id, conn=conn)
    return {"ok": True, "deleted": domain_id}


@router.get("/domains/stats")
def domain_stats() -> dict:
    """获取领域统计信息。"""
    return {"total": count_domains()}
