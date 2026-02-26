from __future__ import annotations

import sqlite3
from typing import Optional, Sequence

from backend.src.repositories import (
    eval_repo,
    graph_extract_tasks_repo,
    llm_records_repo,
    search_records_repo,
    tool_call_records_repo,
)
from backend.src.services.common.coerce import (
    to_int,
    to_int_list,
    to_optional_int,
    to_optional_text,
    to_text,
)


def list_graph_extract_tasks(
    *,
    task_id: Optional[int],
    run_id: Optional[int],
    status: Optional[str],
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return graph_extract_tasks_repo.list_graph_extract_tasks(
        task_id=to_optional_int(task_id),
        run_id=to_optional_int(run_id),
        status=to_optional_text(status),
        limit=to_int(limit),
        conn=conn,
    )


def get_graph_extract_task(*, extract_id: int, conn: Optional[sqlite3.Connection] = None):
    return graph_extract_tasks_repo.get_graph_extract_task(
        extract_id=to_int(extract_id),
        conn=conn,
    )


def list_llm_records(
    *,
    task_id: Optional[int],
    run_id: Optional[int],
    offset: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return llm_records_repo.list_llm_records(
        task_id=to_optional_int(task_id),
        run_id=to_optional_int(run_id),
        offset=to_int(offset),
        limit=to_int(limit),
        conn=conn,
    )


def list_llm_records_for_task(*, task_id: int, conn: Optional[sqlite3.Connection] = None):
    return llm_records_repo.list_llm_records_for_task(task_id=to_int(task_id), conn=conn)


def get_llm_record(*, record_id: int, conn: Optional[sqlite3.Connection] = None):
    return llm_records_repo.get_llm_record(record_id=to_int(record_id), conn=conn)


def create_llm_record(
    *,
    prompt: str,
    response: str,
    task_id: Optional[int],
    run_id: Optional[int],
    status: str,
    created_at: Optional[str] = None,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> int:
    return to_int(
        llm_records_repo.create_llm_record(
            prompt=to_text(prompt),
            response=to_text(response),
            task_id=to_optional_int(task_id),
            run_id=to_optional_int(run_id),
            status=to_text(status),
            created_at=created_at,
            updated_at=updated_at,
            conn=conn,
        )
    )


def list_tool_call_records(
    *,
    task_id: Optional[int],
    run_id: Optional[int],
    tool_id: Optional[int],
    reuse_status: Optional[str],
    offset: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return tool_call_records_repo.list_tool_call_records(
        task_id=to_optional_int(task_id),
        run_id=to_optional_int(run_id),
        tool_id=to_optional_int(tool_id),
        reuse_status=to_optional_text(reuse_status),
        offset=to_int(offset),
        limit=to_int(limit),
        conn=conn,
    )


def list_tool_call_records_for_task(*, task_id: int, conn: Optional[sqlite3.Connection] = None):
    return tool_call_records_repo.list_tool_call_records_for_task(
        task_id=to_int(task_id),
        conn=conn,
    )


def summarize_tool_reuse(
    *,
    task_id: Optional[int],
    run_id: Optional[int],
    tool_id: Optional[int],
    reuse_status: Optional[str],
    unknown_status_value: str,
    reuse_true_value: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return tool_call_records_repo.summarize_tool_reuse(
        task_id=to_optional_int(task_id),
        run_id=to_optional_int(run_id),
        tool_id=to_optional_int(tool_id),
        reuse_status=to_optional_text(reuse_status),
        unknown_status_value=to_text(unknown_status_value),
        reuse_true_value=to_int(reuse_true_value),
        limit=to_int(limit),
        conn=conn,
    )


def summarize_skill_reuse(
    *,
    task_id: Optional[int],
    run_id: Optional[int],
    tool_id: Optional[int],
    reuse_status: Optional[str],
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return tool_call_records_repo.summarize_skill_reuse(
        task_id=to_optional_int(task_id),
        run_id=to_optional_int(run_id),
        tool_id=to_optional_int(tool_id),
        reuse_status=to_optional_text(reuse_status),
        limit=to_int(limit),
        conn=conn,
    )


def update_tool_call_record_validation(
    *,
    record_id: int,
    reuse_status: str,
    reuse_notes: Optional[str],
    conn: Optional[sqlite3.Connection] = None,
):
    return tool_call_records_repo.update_tool_call_record_validation(
        record_id=to_int(record_id),
        reuse_status=to_text(reuse_status),
        reuse_notes=reuse_notes,
        conn=conn,
    )


def list_search_records(*, conn: Optional[sqlite3.Connection] = None):
    return search_records_repo.list_search_records(conn=conn)


def get_search_record(*, record_id: int, conn: Optional[sqlite3.Connection] = None):
    return search_records_repo.get_search_record(record_id=to_int(record_id), conn=conn)


def create_search_record(
    *,
    query: str,
    sources: Sequence[str],
    result_count: int,
    task_id: Optional[int],
    created_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> int:
    return to_int(
        search_records_repo.create_search_record(
            query=to_text(query),
            sources=[to_text(value) for value in (sources or [])],
            result_count=to_int(result_count),
            task_id=to_optional_int(task_id),
            created_at=created_at,
            conn=conn,
        )
    )


def list_eval_records_by_task(*, task_id: int, conn: Optional[sqlite3.Connection] = None):
    return eval_repo.list_eval_records_by_task(task_id=to_int(task_id), conn=conn)


def list_eval_criteria_by_eval_ids(
    *,
    eval_ids: Sequence[int],
    conn: Optional[sqlite3.Connection] = None,
):
    return eval_repo.list_eval_criteria_by_eval_ids(
        eval_ids=to_int_list(eval_ids),
        conn=conn,
    )
