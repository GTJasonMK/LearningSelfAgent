from typing import Optional

from fastapi import APIRouter

from backend.src.common.serializers import (
    eval_criterion_from_row,
    eval_from_row,
    llm_record_from_row,
    task_from_row,
    task_output_from_row,
    task_run_from_row,
    task_step_from_row,
    tool_call_from_row,
)
from backend.src.common.utils import error_response
from backend.src.constants import (
    DEFAULT_RECORDS_EXPORT_LIMIT,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_TASK_NOT_FOUND,
    HTTP_STATUS_NOT_FOUND,
)
from backend.src.repositories.eval_repo import list_eval_criteria_by_eval_ids, list_eval_records_by_task
from backend.src.repositories.llm_records_repo import list_llm_records, list_llm_records_for_task
from backend.src.repositories.task_outputs_repo import (
    list_task_outputs_for_run,
    list_task_outputs_for_task,
)
from backend.src.repositories.task_runs_repo import (
    get_task_run,
    list_task_runs_for_task,
)
from backend.src.repositories.task_steps_repo import list_task_steps_for_run, list_task_steps_for_task
from backend.src.repositories.tasks_repo import get_task
from backend.src.repositories.tool_call_records_repo import (
    list_tool_call_records,
    list_tool_call_records_for_task,
)

router = APIRouter()


@router.get("/records/tasks/{task_id}")
def get_task_record(task_id: int):
    task_row = get_task(task_id=int(task_id))
    if not task_row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_TASK_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )

    eval_rows = list_eval_records_by_task(task_id=int(task_id))
    eval_ids = [row["id"] for row in (eval_rows or [])]
    criteria_rows = list_eval_criteria_by_eval_ids(eval_ids=eval_ids) if eval_ids else []
    step_rows = list_task_steps_for_task(task_id=int(task_id))
    output_rows = list_task_outputs_for_task(task_id=int(task_id))
    run_rows = list_task_runs_for_task(task_id=int(task_id))
    llm_rows = list_llm_records_for_task(task_id=int(task_id))
    tool_call_rows = list_tool_call_records_for_task(task_id=int(task_id))
    return {
        "task": task_from_row(task_row),
        "evals": [eval_from_row(row) for row in eval_rows],
        "eval_criteria": [eval_criterion_from_row(row) for row in criteria_rows],
        "steps": [task_step_from_row(row) for row in step_rows],
        "outputs": [task_output_from_row(row) for row in output_rows],
        "runs": [task_run_from_row(row) for row in run_rows],
        "llm_records": [llm_record_from_row(row) for row in llm_rows],
        "tool_calls": [tool_call_from_row(row) for row in tool_call_rows],
    }


@router.get("/records/tasks/{task_id}/timeline")
def get_task_timeline(task_id: int, run_id: Optional[int] = None) -> dict:
    task_row = get_task(task_id=int(task_id))
    if not task_row:
        return error_response(
            ERROR_CODE_NOT_FOUND,
            ERROR_MESSAGE_TASK_NOT_FOUND,
            HTTP_STATUS_NOT_FOUND,
        )

    run_rows = []
    if run_id is not None:
        run_row = get_task_run(run_id=int(run_id))
        if run_row and int(run_row["task_id"]) == int(task_id):
            run_rows = [run_row]
    else:
        run_rows = list_task_runs_for_task(task_id=int(task_id))

    if run_id is not None:
        step_rows = list_task_steps_for_run(task_id=int(task_id), run_id=int(run_id))
        output_rows = list_task_outputs_for_run(task_id=int(task_id), run_id=int(run_id), order="ASC")
        llm_rows = list_llm_records(
            task_id=int(task_id),
            run_id=int(run_id),
            offset=0,
            limit=DEFAULT_RECORDS_EXPORT_LIMIT,
        )
        tool_rows = list_tool_call_records(
            task_id=int(task_id),
            run_id=int(run_id),
            tool_id=None,
            reuse_status=None,
            offset=0,
            limit=DEFAULT_RECORDS_EXPORT_LIMIT,
        )
    else:
        step_rows = list_task_steps_for_task(task_id=int(task_id))
        output_rows = list_task_outputs_for_task(task_id=int(task_id))
        llm_rows = list_llm_records(
            task_id=int(task_id),
            run_id=None,
            offset=0,
            limit=DEFAULT_RECORDS_EXPORT_LIMIT,
        )
        tool_rows = list_tool_call_records(
            task_id=int(task_id),
            run_id=None,
            tool_id=None,
            reuse_status=None,
            offset=0,
            limit=DEFAULT_RECORDS_EXPORT_LIMIT,
        )
    timeline = []
    for row in run_rows:
        timeline.append(
            {
                "type": "run",
                "timestamp": row["started_at"] or row["created_at"],
                "data": task_run_from_row(row),
            }
        )
    for row in step_rows:
        timeline.append(
            {
                "type": "step",
                "timestamp": row["started_at"] or row["created_at"],
                "data": task_step_from_row(row),
            }
        )
    for row in llm_rows:
        timeline.append(
            {
                "type": "llm",
                "timestamp": row["started_at"] or row["created_at"],
                "data": llm_record_from_row(row),
            }
        )
    for row in tool_rows:
        timeline.append(
            {
                "type": "tool",
                "timestamp": row["created_at"],
                "data": tool_call_from_row(row),
            }
        )
    for row in output_rows:
        timeline.append(
            {
                "type": "output",
                "timestamp": row["created_at"],
                "data": task_output_from_row(row),
            }
        )
    timeline.sort(key=lambda item: item["timestamp"] or "")
    return {"task": task_from_row(task_row), "items": timeline}
