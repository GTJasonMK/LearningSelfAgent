from __future__ import annotations

import sqlite3
from typing import Optional

from backend.src.repositories import domains_repo
from backend.src.services.common.coerce import (
    to_int,
    to_int_or_default,
    to_optional_int,
    to_optional_text,
    to_text,
)

DomainCreateParams = domains_repo.DomainCreateParams
DomainUpdateParams = domains_repo.DomainUpdateParams


def count_domains(*, conn: Optional[sqlite3.Connection] = None) -> int:
    return to_int_or_default(domains_repo.count_domains(conn=conn), default=0)


def create_domain(params: DomainCreateParams, *, conn: Optional[sqlite3.Connection] = None) -> int:
    return to_int(domains_repo.create_domain(params, conn=conn))


def get_domain(
    *,
    domain_id: Optional[str] = None,
    id: Optional[int] = None,
    conn: Optional[sqlite3.Connection] = None,
):
    return domains_repo.get_domain(
        domain_id=to_optional_text(domain_id),
        id=to_optional_int(id),
        conn=conn,
    )


def list_domains(
    *,
    parent_id: Optional[str] = None,
    status: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
):
    return domains_repo.list_domains(
        parent_id=to_optional_text(parent_id),
        status=to_optional_text(status),
        conn=conn,
    )


def list_top_level_domains(*, conn: Optional[sqlite3.Connection] = None):
    return domains_repo.list_top_level_domains(conn=conn)


def list_child_domains(*, parent_id: str, conn: Optional[sqlite3.Connection] = None):
    return domains_repo.list_child_domains(parent_id=to_text(parent_id), conn=conn)


def search_domains_by_keyword(*, keyword: str, conn: Optional[sqlite3.Connection] = None):
    return domains_repo.search_domains_by_keyword(keyword=to_text(keyword), conn=conn)


def update_domain(
    *,
    domain_id: str,
    params: DomainUpdateParams,
    conn: Optional[sqlite3.Connection] = None,
) -> bool:
    return bool(domains_repo.update_domain(domain_id=to_text(domain_id), params=params, conn=conn))


def delete_domain(*, domain_id: str, conn: Optional[sqlite3.Connection] = None) -> bool:
    return bool(domains_repo.delete_domain(domain_id=to_text(domain_id), conn=conn))
