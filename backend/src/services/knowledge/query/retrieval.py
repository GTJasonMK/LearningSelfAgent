from __future__ import annotations

import sqlite3
from typing import Optional

from backend.src.repositories import agent_retrieval_repo
from backend.src.services.common.coerce import (
    to_int,
    to_int_list,
    to_int_or_default,
    to_non_empty_optional_text,
    to_non_empty_texts,
)


def list_tool_hints(*, limit: int = 8, conn: Optional[sqlite3.Connection] = None):
    return agent_retrieval_repo.list_tool_hints(
        limit=to_int_or_default(limit, default=8),
        conn=conn,
    )


def list_tool_hints_by_names(
    *,
    names: list[str],
    limit: int = 8,
    conn: Optional[sqlite3.Connection] = None,
):
    return agent_retrieval_repo.list_tool_hints_by_names(
        names=to_non_empty_texts(names),
        limit=to_int_or_default(limit, default=8),
        conn=conn,
    )


def list_domain_candidates(*, limit: int = 20, conn: Optional[sqlite3.Connection] = None):
    return agent_retrieval_repo.list_domain_candidates(
        limit=to_int_or_default(limit, default=20),
        conn=conn,
    )


def list_skill_candidates(
    *,
    limit: int,
    query_text: Optional[str] = None,
    debug: Optional[dict] = None,
    include_draft: bool = False,
    skill_type: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
):
    return agent_retrieval_repo.list_skill_candidates(
        limit=to_int(limit),
        query_text=to_non_empty_optional_text(query_text),
        debug=debug if isinstance(debug, dict) else None,
        include_draft=bool(include_draft),
        skill_type=to_non_empty_optional_text(skill_type),
        conn=conn,
    )


def list_skill_candidates_by_domains(
    *,
    domain_ids: list[str],
    limit: int,
    query_text: Optional[str] = None,
    debug: Optional[dict] = None,
    include_draft: bool = False,
    skill_type: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
):
    return agent_retrieval_repo.list_skill_candidates_by_domains(
        domain_ids=to_non_empty_texts(domain_ids),
        limit=to_int(limit),
        query_text=to_non_empty_optional_text(query_text),
        debug=debug if isinstance(debug, dict) else None,
        include_draft=bool(include_draft),
        skill_type=to_non_empty_optional_text(skill_type),
        conn=conn,
    )


def load_skills_by_ids(skill_ids: list[int], *, conn: Optional[sqlite3.Connection] = None):
    return agent_retrieval_repo.load_skills_by_ids(
        to_int_list(skill_ids, ignore_errors=True),
        conn=conn,
    )


def list_solution_candidates_by_skill_tags(
    *,
    skill_tags: list[str],
    limit: int,
    domain_ids: Optional[list[str]] = None,
    debug: Optional[dict] = None,
    conn: Optional[sqlite3.Connection] = None,
):
    normalized_domain_ids = to_non_empty_texts(domain_ids or [])
    return agent_retrieval_repo.list_solution_candidates_by_skill_tags(
        skill_tags=to_non_empty_texts(skill_tags),
        limit=to_int(limit),
        domain_ids=normalized_domain_ids or None,
        debug=debug if isinstance(debug, dict) else None,
        conn=conn,
    )


def list_memory_candidates(
    *,
    limit: int,
    query_text: Optional[str] = None,
    debug: Optional[dict] = None,
    conn: Optional[sqlite3.Connection] = None,
):
    return agent_retrieval_repo.list_memory_candidates(
        limit=to_int(limit),
        query_text=to_non_empty_optional_text(query_text),
        debug=debug if isinstance(debug, dict) else None,
        conn=conn,
    )


def list_graph_candidates(
    *,
    terms: list[str],
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return agent_retrieval_repo.list_graph_candidates(
        terms=to_non_empty_texts(terms),
        limit=to_int(limit),
        conn=conn,
    )


def load_graph_nodes_by_ids(node_ids: list[int], *, conn: Optional[sqlite3.Connection] = None):
    return agent_retrieval_repo.load_graph_nodes_by_ids(
        to_int_list(node_ids, ignore_errors=True),
        conn=conn,
    )


def load_graph_edges_between(
    *,
    node_ids: list[int],
    limit: int = 24,
    conn: Optional[sqlite3.Connection] = None,
):
    return agent_retrieval_repo.load_graph_edges_between(
        node_ids=to_int_list(node_ids, ignore_errors=True),
        limit=to_int_or_default(limit, default=24),
        conn=conn,
    )
