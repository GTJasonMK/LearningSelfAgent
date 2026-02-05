from __future__ import annotations

import sqlite3
from typing import List, Optional, Sequence

from backend.src.common.utils import now_iso
from backend.src.repositories.repo_conn import provide_connection


def list_prompt_templates(
    *,
    offset: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    sql = "SELECT * FROM prompt_templates ORDER BY id ASC LIMIT ? OFFSET ?"
    params = (int(limit), int(offset))
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def get_prompt_template(
    *, template_id: int, conn: Optional[sqlite3.Connection] = None
) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM prompt_templates WHERE id = ?"
    params = (int(template_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def create_prompt_template(
    *,
    name: str,
    template: str,
    description: Optional[str],
    created_at: Optional[str] = None,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> int:
    created = created_at or now_iso()
    updated = updated_at or created
    sql = "INSERT INTO prompt_templates (name, template, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?)"
    params: Sequence = (name, template, description, created, updated)
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, params)
        return int(cursor.lastrowid)


def update_prompt_template(
    *,
    template_id: int,
    name: Optional[str] = None,
    template: Optional[str] = None,
    description: Optional[str] = None,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    fields: List[str] = []
    params: List = []
    if name is not None:
        fields.append("name = ?")
        params.append(name)
    if template is not None:
        fields.append("template = ?")
        params.append(template)
    if description is not None:
        fields.append("description = ?")
        params.append(description)

    updated = updated_at or now_iso()

    with provide_connection(conn) as inner:
        row = get_prompt_template(template_id=template_id, conn=inner)
        if not row:
            return None
        if fields:
            fields.append("updated_at = ?")
            params.append(updated)
            params.append(int(template_id))
            inner.execute(
                f"UPDATE prompt_templates SET {', '.join(fields)} WHERE id = ?",
                params,
            )
        return get_prompt_template(template_id=template_id, conn=inner)


def delete_prompt_template(
    *, template_id: int, conn: Optional[sqlite3.Connection] = None
) -> Optional[sqlite3.Row]:
    with provide_connection(conn) as inner:
        row = get_prompt_template(template_id=template_id, conn=inner)
        if not row:
            return None
        inner.execute("DELETE FROM prompt_templates WHERE id = ?", (int(template_id),))
        return row
