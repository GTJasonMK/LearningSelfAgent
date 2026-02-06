from typing import Optional

from fastapi import APIRouter

from backend.src.api.utils import error_response, parse_json_value
from backend.src.constants import DEFAULT_PAGE_LIMIT, DEFAULT_PAGE_OFFSET, ERROR_CODE_INVALID_REQUEST, HTTP_STATUS_BAD_REQUEST
from backend.src.repositories.agent_reviews_repo import (
    get_agent_review as repo_get_agent_review,
    list_agent_reviews as repo_list_agent_reviews,
)

router = APIRouter()


@router.get("/agent/reviews")
def list_agent_reviews(
    offset: int = DEFAULT_PAGE_OFFSET,
    limit: int = DEFAULT_PAGE_LIMIT,
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
):
    """
    评估记录列表（Eval Agent 输出）。
    """
    if offset < 0:
        offset = 0
    if limit <= 0:
        limit = DEFAULT_PAGE_LIMIT

    rows = repo_list_agent_reviews(offset=int(offset), limit=int(limit), task_id=task_id, run_id=run_id)
    items = []
    for row in rows:
        items.append(
            {
                "id": int(row["id"]),
                "task_id": row["task_id"],
                "run_id": int(row["run_id"]),
                "status": row["status"],
                "distill_status": row["distill_status"],
                "summary": row["summary"],
                "created_at": row["created_at"],
            }
        )
    return {"items": items}


@router.get("/agent/reviews/{review_id}")
def get_agent_review(review_id: int):
    """
    评估记录详情。
    """
    try:
        rid = int(review_id)
    except Exception:
        rid = 0
    if rid <= 0:
        return error_response(ERROR_CODE_INVALID_REQUEST, "review_id 不合法", HTTP_STATUS_BAD_REQUEST)

    row = repo_get_agent_review(review_id=int(rid))
    if not row:
        return error_response(ERROR_CODE_INVALID_REQUEST, "review 不存在", HTTP_STATUS_BAD_REQUEST)

    distill_evidence_refs = parse_json_value(row["distill_evidence_refs"]) or []
    issues = parse_json_value(row["issues"]) or []
    next_actions = parse_json_value(row["next_actions"]) or []
    skills = parse_json_value(row["skills"]) or []
    return {
        "review": {
            "id": int(row["id"]),
            "task_id": row["task_id"],
            "run_id": int(row["run_id"]),
            "status": row["status"],
            "pass_score": row["pass_score"],
            "pass_threshold": row["pass_threshold"],
            "distill_status": row["distill_status"],
            "distill_score": row["distill_score"],
            "distill_threshold": row["distill_threshold"],
            "distill_notes": row["distill_notes"],
            "distill_evidence_refs": distill_evidence_refs if isinstance(distill_evidence_refs, list) else [],
            "summary": row["summary"],
            "issues": issues if isinstance(issues, list) else [],
            "next_actions": next_actions if isinstance(next_actions, list) else [],
            "skills": skills if isinstance(skills, list) else [],
            "created_at": row["created_at"],
        }
    }
