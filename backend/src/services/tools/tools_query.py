from __future__ import annotations

import sqlite3
from typing import Dict, List, Optional, Sequence, Tuple

from backend.src.repositories.tool_call_records_repo import (
    list_tool_calls_with_tool_name_by_run as list_tool_calls_with_tool_name_by_run_repo,
)
from backend.src.repositories.tool_call_records_repo import (
    get_tool_reuse_stats as get_tool_reuse_stats_repo,
)
from backend.src.repositories.tool_call_records_repo import (
    get_tool_reuse_stats_map as get_tool_reuse_stats_map_repo,
)
from backend.src.repositories.tools_repo import ToolCreateParams
from backend.src.repositories.tools_repo import create_tool as create_tool_repo
from backend.src.repositories.tools_repo import get_tool as get_tool_repo
from backend.src.repositories.tools_repo import list_tool_versions as list_tool_versions_repo
from backend.src.repositories.tools_repo import list_tools as list_tools_repo
from backend.src.repositories.tools_repo import tool_exists as tool_exists_repo
from backend.src.repositories.tools_repo import update_tool as update_tool_repo
from backend.src.services.common.coerce import (
    to_int,
    to_int_list,
)


def create_tool(params: ToolCreateParams, *, conn: Optional[sqlite3.Connection] = None) -> int:
    return to_int(create_tool_repo(params, conn=conn))


def list_tools(*, conn: Optional[sqlite3.Connection] = None):
    return list_tools_repo(conn=conn)


def get_tool(*, tool_id: int, conn: Optional[sqlite3.Connection] = None):
    return get_tool_repo(tool_id=to_int(tool_id), conn=conn)


def update_tool(
    *,
    tool_id: int,
    name: Optional[str],
    description: Optional[str],
    version: Optional[str],
    metadata: Optional[dict],
    change_notes: Optional[str],
    updated_at: Optional[str],
    conn: Optional[sqlite3.Connection] = None,
):
    return update_tool_repo(
        tool_id=to_int(tool_id),
        name=name,
        description=description,
        version=version,
        metadata=metadata,
        change_notes=change_notes,
        updated_at=updated_at,
        conn=conn,
    )


def tool_exists(*, tool_id: int, conn: Optional[sqlite3.Connection] = None) -> bool:
    return bool(tool_exists_repo(tool_id=to_int(tool_id), conn=conn))


def list_tool_versions(*, tool_id: int, conn: Optional[sqlite3.Connection] = None):
    return list_tool_versions_repo(tool_id=to_int(tool_id), conn=conn)


def get_tool_reuse_stats(*, tool_id: int, conn: Optional[sqlite3.Connection] = None) -> Tuple[int, int]:
    return get_tool_reuse_stats_repo(tool_id=to_int(tool_id), conn=conn)


def get_tool_reuse_stats_map(
    *,
    tool_ids: Sequence[int],
    conn: Optional[sqlite3.Connection] = None,
) -> Dict[int, Dict[str, int]]:
    normalized_ids: List[int] = to_int_list(tool_ids, ignore_errors=True)
    return get_tool_reuse_stats_map_repo(tool_ids=normalized_ids, conn=conn)


def list_tool_calls_with_tool_name_by_run(
    *,
    run_id: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return list_tool_calls_with_tool_name_by_run_repo(
        run_id=to_int(run_id),
        limit=to_int(limit),
        conn=conn,
    )
