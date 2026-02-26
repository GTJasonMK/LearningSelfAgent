from typing import Optional

from fastapi import APIRouter

from backend.src.api.tasks.route_common import task_not_found_response
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
from backend.src.constants import (
    DEFAULT_RECORDS_EXPORT_LIMIT,
)
from backend.src.services.knowledge.knowledge_query import (
    list_eval_criteria_by_eval_ids,
    list_eval_records_by_task,
    list_llm_records,
    list_llm_records_for_task,
    list_tool_call_records,
    list_tool_call_records_for_task,
)
from backend.src.services.tasks.task_queries import (
    get_task,
    get_task_run,
    list_task_outputs_for_run,
    list_task_outputs_for_task,
    list_task_runs_for_task,
    list_task_steps_for_run,
    list_task_steps_for_task,
)

router = APIRouter()


def _load_task_or_error(task_id: int):
    task_row = get_task(task_id=int(task_id))
    if not task_row:
        return None, task_not_found_response()
    return task_row, None


def _append_timeline_items(
    timeline: list,
    rows: list,
    *,
    item_type: str,
    serializer,
    timestamp_fields: tuple[str, ...],
) -> None:
    for row in rows:
        timestamp = None
        for field in timestamp_fields:
            value = row[field]
            if value:
                timestamp = value
                break
        timeline.append(
            {
                "type": item_type,
                "timestamp": timestamp,
                "data": serializer(row),
            }
        )


def _load_timeline_rows(task_id: int, run_id: Optional[int] = None):
    if run_id is not None:
        run_row = get_task_run(run_id=int(run_id))
        run_rows = [run_row] if run_row and int(run_row["task_id"]) == int(task_id) else []
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
        return run_rows, step_rows, output_rows, llm_rows, tool_rows

    run_rows = list_task_runs_for_task(task_id=int(task_id))
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
    return run_rows, step_rows, output_rows, llm_rows, tool_rows


@router.get("/records/tasks/{task_id}")
def get_task_record(task_id: int):
    task_row, error = _load_task_or_error(task_id=int(task_id))
    if error:
        return error

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
    task_row, error = _load_task_or_error(task_id=int(task_id))
    if error:
        return error

    run_rows, step_rows, output_rows, llm_rows, tool_rows = _load_timeline_rows(
        task_id=int(task_id),
        run_id=run_id,
    )
    timeline = []
    _append_timeline_items(
        timeline,
        run_rows,
        item_type="run",
        serializer=task_run_from_row,
        timestamp_fields=("started_at", "created_at"),
    )
    _append_timeline_items(
        timeline,
        step_rows,
        item_type="step",
        serializer=task_step_from_row,
        timestamp_fields=("started_at", "created_at"),
    )
    _append_timeline_items(
        timeline,
        llm_rows,
        item_type="llm",
        serializer=llm_record_from_row,
        timestamp_fields=("started_at", "created_at"),
    )
    _append_timeline_items(
        timeline,
        tool_rows,
        item_type="tool",
        serializer=tool_call_from_row,
        timestamp_fields=("created_at",),
    )
    _append_timeline_items(
        timeline,
        output_rows,
        item_type="output",
        serializer=task_output_from_row,
        timestamp_fields=("created_at",),
    )
    timeline.sort(key=lambda item: item["timestamp"] or "")
    return {"task": task_from_row(task_row), "items": timeline}
