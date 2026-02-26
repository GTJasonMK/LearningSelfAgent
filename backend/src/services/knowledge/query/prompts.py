from __future__ import annotations

import sqlite3
from typing import Optional

from backend.src.repositories import prompt_templates_repo
from backend.src.services.common.coerce import to_int, to_text


def list_prompt_templates(
    *,
    offset: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return prompt_templates_repo.list_prompt_templates(
        offset=to_int(offset),
        limit=to_int(limit),
        conn=conn,
    )


def get_prompt_template(*, template_id: int, conn: Optional[sqlite3.Connection] = None):
    return prompt_templates_repo.get_prompt_template(template_id=to_int(template_id), conn=conn)


def create_prompt_template(
    *,
    name: str,
    template: str,
    description: Optional[str],
    created_at: Optional[str] = None,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> int:
    return to_int(
        prompt_templates_repo.create_prompt_template(
            name=to_text(name),
            template=to_text(template),
            description=description,
            created_at=created_at,
            updated_at=updated_at,
            conn=conn,
        )
    )


def update_prompt_template(
    *,
    template_id: int,
    name: Optional[str] = None,
    template: Optional[str] = None,
    description: Optional[str] = None,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
):
    return prompt_templates_repo.update_prompt_template(
        template_id=to_int(template_id),
        name=name,
        template=template,
        description=description,
        updated_at=updated_at,
        conn=conn,
    )


def delete_prompt_template(*, template_id: int, conn: Optional[sqlite3.Connection] = None):
    return prompt_templates_repo.delete_prompt_template(template_id=to_int(template_id), conn=conn)
