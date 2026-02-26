from __future__ import annotations

from typing import Optional

from backend.src.common.utils import now_iso, parse_json_list
from backend.src.constants import EVAL_PASS_RATE_THRESHOLD
from backend.src.repositories.eval_repo import create_eval_criteria_bulk, create_eval_record
from backend.src.repositories.expectations_repo import get_expectation as get_expectation_repo
from backend.src.repositories.task_outputs_repo import list_task_outputs_for_run
from backend.src.repositories.task_steps_repo import list_task_steps_for_run
from backend.src.storage import get_connection


def create_eval_response(
    *,
    task_row,
    task_id: int,
    run_id: int,
) -> Optional[dict]:
    """
    根据 expectation.criteria 对 run 结果做关键词命中评估并落库。
    """
    expectation_id = task_row["expectation_id"] if task_row else None
    expectation_row = None
    if expectation_id is not None:
        expectation_row = get_expectation_repo(expectation_id=int(expectation_id))
    if not expectation_row:
        return None

    criteria_list = parse_json_list(expectation_row["criteria"])
    eval_created_at = now_iso()
    evidence_texts = [task_row["title"] or ""]
    output_rows = list_task_outputs_for_run(task_id=int(task_id), run_id=int(run_id), order="ASC")
    step_rows = list_task_steps_for_run(task_id=int(task_id), run_id=int(run_id))

    for row in output_rows:
        if row["content"]:
            evidence_texts.append(str(row["content"]))
    for row in step_rows:
        if row["result"]:
            evidence_texts.append(str(row["result"]))

    evidence_text = " ".join(evidence_texts).lower()

    pass_count = 0
    eval_criteria_payload = []
    for criterion in criteria_list:
        normalized = str(criterion).strip()
        if not normalized:
            continue
        matched = normalized.lower() in evidence_text
        status = "pass" if matched else "fail"
        if matched:
            pass_count += 1
        eval_criteria_payload.append(
            {
                "criterion": normalized,
                "status": status,
                "notes": "命中关键词" if matched else "未匹配关键词",
            }
        )

    score = pass_count / len(criteria_list) if criteria_list else None
    if criteria_list:
        eval_status = "pass" if score is not None and score >= EVAL_PASS_RATE_THRESHOLD else "fail"
        eval_notes = f"自动评估：命中 {pass_count}/{len(criteria_list)}"
    else:
        eval_status = "unknown"
        eval_notes = "自动评估：未提供 criteria"

    with get_connection() as conn:
        eval_id, _ = create_eval_record(
            status=eval_status,
            score=score,
            notes=eval_notes,
            task_id=task_id,
            expectation_id=expectation_row["id"],
            created_at=eval_created_at,
            conn=conn,
        )
        create_eval_criteria_bulk(
            eval_id=int(eval_id),
            items=eval_criteria_payload,
            created_at=eval_created_at,
            conn=conn,
        )

    return {"eval_id": eval_id, "status": eval_status}
