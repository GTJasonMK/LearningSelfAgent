from typing import Optional

from fastapi import APIRouter

from backend.src.common.utils import error_response
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_MESSAGE_INVALID_STATUS,
    HTTP_STATUS_BAD_REQUEST,
    SQL_BOOL_TRUE,
    TOOL_REUSE_STATUS_FAIL,
    TOOL_REUSE_STATUS_PASS,
    TOOL_REUSE_STATUS_UNKNOWN,
)
from backend.src.repositories.tool_call_records_repo import (
    summarize_skill_reuse,
    summarize_tool_reuse,
)

router = APIRouter()


@router.get("/records/tools/reuse")
def tool_reuse_summary(
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
    tool_id: Optional[int] = None,
    reuse_status: Optional[str] = None,
    limit: int = DEFAULT_PAGE_LIMIT,
) -> dict:
    allowed_statuses = {
        TOOL_REUSE_STATUS_PASS,
        TOOL_REUSE_STATUS_FAIL,
        TOOL_REUSE_STATUS_UNKNOWN,
    }
    if reuse_status is not None and reuse_status not in allowed_statuses:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            ERROR_MESSAGE_INVALID_STATUS,
            HTTP_STATUS_BAD_REQUEST,
        )
    summary_row, status_rows, tool_rows, tool_status_rows = summarize_tool_reuse(
        task_id=task_id,
        run_id=run_id,
        tool_id=tool_id,
        reuse_status=reuse_status,
        unknown_status_value=TOOL_REUSE_STATUS_UNKNOWN,
        reuse_true_value=SQL_BOOL_TRUE,
        limit=int(limit),
    )
    total_calls = summary_row["calls"] if summary_row else 0
    total_reuse_calls = summary_row["reuse_calls"] if summary_row else 0
    summary_status = {
        TOOL_REUSE_STATUS_PASS: 0,
        TOOL_REUSE_STATUS_FAIL: 0,
        TOOL_REUSE_STATUS_UNKNOWN: 0,
    }
    for row in status_rows:
        summary_status[row["status"]] = row["calls"]
    tool_status_map = {}
    for row in tool_status_rows:
        tool_status_map.setdefault(
            row["tool_id"],
            {
                TOOL_REUSE_STATUS_PASS: 0,
                TOOL_REUSE_STATUS_FAIL: 0,
                TOOL_REUSE_STATUS_UNKNOWN: 0,
            },
        )[row["status"]] = row["calls"]
    by_tool = []
    for row in tool_rows:
        calls = row["calls"]
        reuse_calls = row["reuse_calls"]
        by_tool.append(
            {
                "tool_id": row["tool_id"],
                "calls": calls,
                "reuse_calls": reuse_calls,
                "reuse_rate": (reuse_calls / calls) if calls else 0,
                "reuse_status": tool_status_map.get(
                    row["tool_id"],
                    {
                        TOOL_REUSE_STATUS_PASS: 0,
                        TOOL_REUSE_STATUS_FAIL: 0,
                        TOOL_REUSE_STATUS_UNKNOWN: 0,
                    },
                ),
            }
        )
    return {
        "summary": {
            "total_calls": total_calls,
            "reuse_calls": total_reuse_calls,
            "reuse_rate": (total_reuse_calls / total_calls) if total_calls else 0,
            "reuse_status": summary_status,
        },
        "by_tool": by_tool,
    }


@router.get("/records/skills/reuse")
def skill_reuse_summary(
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
    tool_id: Optional[int] = None,
    reuse_status: Optional[str] = None,
    limit: int = DEFAULT_PAGE_LIMIT,
) -> dict:
    total_row, rows = summarize_skill_reuse(
        task_id=task_id,
        run_id=run_id,
        tool_id=tool_id,
        reuse_status=reuse_status,
        limit=int(limit),
    )
    total_calls = total_row["calls"]
    total_reuse_calls = total_row["reuse_calls"]
    summary = {
        "total_calls": total_calls,
        "reuse_calls": total_reuse_calls,
        "reuse_rate": (total_reuse_calls / total_calls) if total_calls else 0,
    }
    by_skill = [
        {
            "skill_id": row["skill_id"],
            "calls": row["calls"],
            "reuse_calls": row["reuse_calls"],
            "reuse_rate": (row["reuse_calls"] / row["calls"]) if row["calls"] else 0,
        }
        for row in rows
    ]
    return {"summary": summary, "by_skill": by_skill}
