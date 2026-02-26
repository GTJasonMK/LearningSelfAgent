"""
领域系统 Repository。

提供 domains 表的 CRUD 操作。
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import List, Optional

from backend.src.common.utils import now_iso
from backend.src.repositories.repo_conn import provide_connection


def _dump_keywords_json(keywords: Optional[List[str]]) -> Optional[str]:
    if keywords is None:
        return None
    return json.dumps(keywords, ensure_ascii=False)


@dataclass(frozen=True)
class DomainCreateParams:
    """domains 创建参数。"""

    domain_id: str
    name: str
    parent_id: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[List[str]] = None


@dataclass(frozen=True)
class DomainUpdateParams:
    """domains 更新参数。"""

    name: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[List[str]] = None
    status: Optional[str] = None


def count_domains(*, conn: Optional[sqlite3.Connection] = None) -> int:
    """获取领域总数。"""
    sql = "SELECT COUNT(*) AS count FROM domains"
    with provide_connection(conn) as inner:
        row = inner.execute(sql).fetchone()
    return int(row["count"]) if row else 0


def create_domain(params: DomainCreateParams, *, conn: Optional[sqlite3.Connection] = None) -> int:
    """创建领域，返回 id。"""
    now = now_iso()
    keywords_json = _dump_keywords_json(params.keywords)
    sql = """
        INSERT INTO domains (domain_id, name, parent_id, description, keywords, skill_count, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, 0, 'active', ?, ?)
    """
    sql_params = (params.domain_id, params.name, params.parent_id, params.description, keywords_json, now, now)
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, sql_params)
        return int(cursor.lastrowid)


def get_domain(*, domain_id: Optional[str] = None, id: Optional[int] = None, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    """按 domain_id 或 id 获取领域。"""
    if domain_id:
        sql = "SELECT * FROM domains WHERE domain_id = ?"
        params = (domain_id,)
    elif id:
        sql = "SELECT * FROM domains WHERE id = ?"
        params = (int(id),)
    else:
        return None
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def list_domains(
    *,
    parent_id: Optional[str] = None,
    status: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    """列出领域，支持按 parent_id 和 status 筛选。"""
    conditions = []
    params = []

    if parent_id is not None:
        if parent_id == "":
            # 空字符串表示查询一级领域（parent_id IS NULL）
            conditions.append("parent_id IS NULL")
        else:
            conditions.append("parent_id = ?")
            params.append(parent_id)
    if status:
        conditions.append("status = ?")
        params.append(status)

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    sql = f"SELECT * FROM domains WHERE {where_clause} ORDER BY domain_id ASC"

    with provide_connection(conn) as inner:
        return list(inner.execute(sql, tuple(params)).fetchall())


def list_top_level_domains(*, conn: Optional[sqlite3.Connection] = None) -> List[sqlite3.Row]:
    """列出一级领域（parent_id IS NULL）。"""
    sql = "SELECT * FROM domains WHERE parent_id IS NULL ORDER BY domain_id ASC"
    with provide_connection(conn) as inner:
        return list(inner.execute(sql).fetchall())


def list_child_domains(*, parent_id: str, conn: Optional[sqlite3.Connection] = None) -> List[sqlite3.Row]:
    """列出指定领域的子领域。"""
    sql = "SELECT * FROM domains WHERE parent_id = ? ORDER BY domain_id ASC"
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, (parent_id,)).fetchall())


def update_domain(
    *,
    domain_id: str,
    params: DomainUpdateParams,
    conn: Optional[sqlite3.Connection] = None,
) -> bool:
    """更新领域，返回是否成功。"""
    updates = []
    sql_params = []

    plain_updates = [
        ("name", params.name),
        ("description", params.description),
        ("status", params.status),
    ]
    for column, value in plain_updates:
        if value is None:
            continue
        updates.append(f"{column} = ?")
        sql_params.append(value)

    if params.keywords is not None:
        updates.append("keywords = ?")
        sql_params.append(_dump_keywords_json(params.keywords))

    if not updates:
        return False

    updates.append("updated_at = ?")
    sql_params.append(now_iso())
    sql_params.append(domain_id)

    sql = f"UPDATE domains SET {', '.join(updates)} WHERE domain_id = ?"
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, tuple(sql_params))
        return cursor.rowcount > 0


def delete_domain(*, domain_id: str, conn: Optional[sqlite3.Connection] = None) -> bool:
    """删除领域，返回是否成功。"""
    sql = "DELETE FROM domains WHERE domain_id = ?"
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, (domain_id,))
        return cursor.rowcount > 0


def increment_skill_count(*, domain_id: str, conn: Optional[sqlite3.Connection] = None) -> bool:
    """增加领域的技能计数。"""
    return _update_skill_count(domain_id=domain_id, expression="skill_count + 1", conn=conn)


def decrement_skill_count(*, domain_id: str, conn: Optional[sqlite3.Connection] = None) -> bool:
    """减少领域的技能计数（不会小于 0）。"""
    return _update_skill_count(domain_id=domain_id, expression="MAX(0, skill_count - 1)", conn=conn)


def _update_skill_count(
    *,
    domain_id: str,
    expression: str,
    conn: Optional[sqlite3.Connection] = None,
) -> bool:
    sql = f"UPDATE domains SET skill_count = {expression}, updated_at = ? WHERE domain_id = ?"
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, (now_iso(), domain_id))
        return cursor.rowcount > 0


def search_domains_by_keyword(
    *,
    keyword: str,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    """按关键词搜索领域（匹配 name, description, keywords）。"""
    pattern = f"%{keyword}%"
    sql = """
        SELECT * FROM domains
        WHERE name LIKE ? OR description LIKE ? OR keywords LIKE ?
        ORDER BY domain_id ASC
    """
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, (pattern, pattern, pattern)).fetchall())


def get_domain_with_children(
    *,
    domain_id: str,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    """获取领域及其所有子领域。"""
    # 使用 LIKE 匹配前缀，例如 "data" 匹配 "data", "data.collect", "data.clean" 等
    pattern = f"{domain_id}%"
    sql = "SELECT * FROM domains WHERE domain_id LIKE ? ORDER BY domain_id ASC"
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, (pattern,)).fetchall())
