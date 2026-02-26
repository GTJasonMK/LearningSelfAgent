from fastapi import APIRouter

from backend.src.api.schemas import EvalCreate, ExpectationCreate
from backend.src.common.serializers import eval_criterion_from_row, eval_from_row, expectation_from_row
from backend.src.api.utils import error_response, now_iso, require_write_permission
from backend.src.constants import (
    EVAL_STATUS_UNKNOWN,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_EVAL_NOT_FOUND,
    ERROR_MESSAGE_EXPECTATION_NOT_FOUND,
    HTTP_STATUS_NOT_FOUND,
)
from backend.src.services.system.expectations_eval import create_eval_record
from backend.src.services.system.expectations_eval import create_expectation as create_expectation_repo
from backend.src.services.system.expectations_eval import get_eval_latest_summary
from backend.src.services.system.expectations_eval import get_eval_record
from backend.src.services.system.expectations_eval import get_expectation as get_expectation_repo
from backend.src.services.system.expectations_eval import list_eval_criteria_by_eval_id

router = APIRouter()


@router.post("/expectations")
@require_write_permission
def create_expectation(payload: ExpectationCreate) -> dict:
    created_at = now_iso()
    expectation_id, _ = create_expectation_repo(
        goal=payload.goal,
        criteria=payload.criteria,
        created_at=created_at,
    )
    row = get_expectation_repo(expectation_id=expectation_id)
    return {"expectation": expectation_from_row(row)}


@router.get("/expectations/{expectation_id}")
def get_expectation(expectation_id: int):
    row = get_expectation_repo(expectation_id=expectation_id)
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_EXPECTATION_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )
    return {"expectation": expectation_from_row(row)}


@router.post("/eval")
@require_write_permission
def create_eval(payload: EvalCreate) -> dict:
    created_at = now_iso()
    eval_id, _ = create_eval_record(
        status=payload.status,
        score=payload.score,
        notes=payload.notes,
        task_id=payload.task_id,
        expectation_id=payload.expectation_id,
        created_at=created_at,
    )
    row = get_eval_record(eval_id=eval_id)
    return {"eval": eval_from_row(row)}


@router.get("/eval/{eval_id}")
def get_eval(eval_id: int):
    row = get_eval_record(eval_id=eval_id)
    if not row:
        return error_response(
            ERROR_CODE_NOT_FOUND, ERROR_MESSAGE_EVAL_NOT_FOUND, HTTP_STATUS_NOT_FOUND
        )
    criteria_rows = list_eval_criteria_by_eval_id(eval_id=eval_id)
    return {
        "eval": eval_from_row(row),
        "criteria": [eval_criterion_from_row(item) for item in criteria_rows],
    }


@router.get("/eval/latest")
def eval_latest() -> dict:
    row = get_eval_latest_summary()
    if not row:
        return {"status": EVAL_STATUS_UNKNOWN, "score": None, "notes": None}
    return {"status": row["status"], "score": row["score"], "notes": row["notes"]}
