from __future__ import annotations

import sqlite3
from typing import Any, Optional, Sequence

from backend.src.repositories import skill_validations_repo, skills_repo
from backend.src.services.common.coerce import (
    to_int,
    to_optional_int,
    to_optional_text,
    to_text,
)

SkillCreateParams = skills_repo.SkillCreateParams
VALID_SKILL_STATUSES = skills_repo.VALID_SKILL_STATUSES


def create_skill_validation(
    *,
    skill_id: int,
    task_id: Optional[int],
    run_id: Optional[int],
    status: str,
    notes: Optional[str],
    created_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
):
    return skill_validations_repo.create_skill_validation(
        skill_id=to_int(skill_id),
        task_id=to_optional_int(task_id),
        run_id=to_optional_int(run_id),
        status=to_text(status),
        notes=notes,
        created_at=created_at,
        conn=conn,
    )


def list_skill_validations(
    *,
    skill_id: int,
    offset: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return skill_validations_repo.list_skill_validations(
        skill_id=to_int(skill_id),
        offset=to_int(offset),
        limit=to_int(limit),
        conn=conn,
    )


def get_skill_validation(*, record_id: int, conn: Optional[sqlite3.Connection] = None):
    return skill_validations_repo.get_skill_validation(record_id=to_int(record_id), conn=conn)


def skill_exists(*, skill_id: int, conn: Optional[sqlite3.Connection] = None) -> bool:
    return bool(skills_repo.skill_exists(skill_id=to_int(skill_id), conn=conn))


def create_skill(params: SkillCreateParams, *, conn: Optional[sqlite3.Connection] = None) -> int:
    return to_int(skills_repo.create_skill(params, conn=conn))


def get_skill(*, skill_id: int, conn: Optional[sqlite3.Connection] = None):
    return skills_repo.get_skill(skill_id=to_int(skill_id), conn=conn)


def list_skills(*, conn: Optional[sqlite3.Connection] = None):
    return skills_repo.list_skills(conn=conn)


def search_skills_fts_or_like(
    *,
    q: str,
    limit: int = 10,
    conn: Optional[sqlite3.Connection] = None,
):
    return skills_repo.search_skills_fts_or_like(
        q=to_text(q),
        limit=to_int(limit),
        conn=conn,
    )


def update_skill(
    *,
    skill_id: int,
    name: Optional[str] = None,
    description: Optional[str] = None,
    scope: Optional[str] = None,
    category: Optional[str] = None,
    tags: Optional[Sequence[Any]] = None,
    triggers: Optional[Sequence[Any]] = None,
    aliases: Optional[Sequence[Any]] = None,
    source_path: Optional[str] = None,
    prerequisites: Optional[Sequence[Any]] = None,
    inputs: Optional[Sequence[Any]] = None,
    outputs: Optional[Sequence[Any]] = None,
    steps: Optional[Sequence[Any]] = None,
    failure_modes: Optional[Sequence[Any]] = None,
    validation: Optional[Sequence[Any]] = None,
    version: Optional[str] = None,
    task_id: Optional[int] = None,
    domain_id: Optional[str] = None,
    skill_type: Optional[str] = None,
    status: Optional[str] = None,
    source_task_id: Optional[int] = None,
    source_run_id: Optional[int] = None,
    change_notes: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
):
    return skills_repo.update_skill(
        skill_id=to_int(skill_id),
        name=name,
        description=description,
        scope=scope,
        category=category,
        tags=tags,
        triggers=triggers,
        aliases=aliases,
        source_path=source_path,
        prerequisites=prerequisites,
        inputs=inputs,
        outputs=outputs,
        steps=steps,
        failure_modes=failure_modes,
        validation=validation,
        version=version,
        task_id=to_optional_int(task_id),
        domain_id=domain_id,
        skill_type=skill_type,
        status=status,
        source_task_id=to_optional_int(source_task_id),
        source_run_id=to_optional_int(source_run_id),
        change_notes=change_notes,
        conn=conn,
    )


def list_skill_catalog_source(*, conn: Optional[sqlite3.Connection] = None):
    return skills_repo.list_skill_catalog_source(conn=conn)


def search_skills_filtered_like(
    *,
    q: Optional[str],
    category: Optional[str],
    tag: Optional[str],
    skill_type: Optional[str],
    status: Optional[str],
    limit: int,
    offset: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return skills_repo.search_skills_filtered_like(
        q=q,
        category=category,
        tag=tag,
        skill_type=skill_type,
        status=status,
        limit=to_int(limit),
        offset=to_int(offset),
        conn=conn,
    )


def update_skill_status(
    *,
    skill_id: int,
    status: str,
    conn: Optional[sqlite3.Connection] = None,
):
    return skills_repo.update_skill_status(
        skill_id=to_int(skill_id),
        status=to_text(status),
        conn=conn,
    )


def list_skills_by_status(
    *,
    status: Optional[str],
    limit: int,
    offset: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return skills_repo.list_skills_by_status(
        status=to_optional_text(status),
        limit=to_int(limit),
        offset=to_int(offset),
        conn=conn,
    )
