import threading
from typing import List, Optional, Tuple

from backend.src.services.debug.safe_debug import safe_write_debug as _safe_write_debug
from backend.src.services.graph.graph_extract import extract_graph_updates
from backend.src.services.llm.llm_client import call_openai
from backend.src.services.tasks.postprocess.backfill import (
    backfill_missing_agent_reviews as backfill_missing_agent_reviews_core,
)
from backend.src.services.tasks.postprocess.backfill import (
    backfill_waiting_feedback_agent_reviews as backfill_waiting_feedback_agent_reviews_core,
)
from backend.src.services.tasks.postprocess.helpers import (
    allow_tool_approval_on_waiting_feedback as allow_tool_approval_on_waiting_feedback_core,
)
from backend.src.services.tasks.postprocess.helpers import (
    extract_tool_name_from_tool_call_step as extract_tool_name_from_tool_call_step_core,
)
from backend.src.services.tasks.postprocess.helpers import (
    find_unverified_text_output as find_unverified_text_output_core,
)
from backend.src.services.tasks.postprocess.helpers import (
    is_selftest_title as is_selftest_title_core,
)
from backend.src.services.tasks.postprocess.review import (
    ensure_agent_review_record_core,
)
from backend.src.services.tasks.postprocess.run_finalize import (
    postprocess_task_run_core,
)
from backend.src.services.tasks.task_memory import (
    write_task_result_memory_if_missing as write_task_result_memory_if_missing_core,
)

# 保护 ensure_agent_review_record 的 check+insert 原子性，防止并发线程重复创建评估记录
_REVIEW_RECORD_LOCK = threading.Lock()


def _is_selftest_title(title: str) -> bool:
    return is_selftest_title_core(title)


def _extract_tool_name_from_tool_call_step(title: str, payload_preview: object) -> str:
    return extract_tool_name_from_tool_call_step_core(title, payload_preview)


def _find_unverified_text_output(output_rows: List[dict]) -> Optional[dict]:
    return find_unverified_text_output_core(output_rows)


def _allow_tool_approval_on_waiting_feedback(run_row: Optional[dict]) -> bool:
    return allow_tool_approval_on_waiting_feedback_core(run_row)


def ensure_agent_review_record(
    *,
    task_id: int,
    run_id: int,
    skills: Optional[list] = None,
    force: bool = False,
) -> Optional[int]:
    return ensure_agent_review_record_core(
        task_id=task_id,
        run_id=run_id,
        skills=skills,
        force=force,
        review_record_lock=_REVIEW_RECORD_LOCK,
        allow_tool_approval_on_waiting_feedback_fn=_allow_tool_approval_on_waiting_feedback,
        is_selftest_title_fn=_is_selftest_title,
        extract_tool_name_from_tool_call_step_fn=_extract_tool_name_from_tool_call_step,
        find_unverified_text_output_fn=_find_unverified_text_output,
        call_openai_fn=call_openai,
        safe_write_debug_fn=_safe_write_debug,
    )


def backfill_missing_agent_reviews(*, limit: int = 10) -> dict:
    return backfill_missing_agent_reviews_core(
        ensure_agent_review_record_fn=ensure_agent_review_record,
        limit=limit,
    )


def backfill_waiting_feedback_agent_reviews(*, limit: int = 10) -> dict:
    return backfill_waiting_feedback_agent_reviews_core(
        ensure_agent_review_record_fn=ensure_agent_review_record,
        limit=limit,
    )


def write_task_result_memory_if_missing(
    *,
    task_id: int,
    run_id: int,
    title: str,
    output_rows: Optional[List[dict]] = None,
) -> Optional[dict]:
    return write_task_result_memory_if_missing_core(
        task_id=task_id,
        run_id=run_id,
        title=title,
        output_rows=output_rows,
    )


def _resolve_default_model() -> str:
    from backend.src.services.llm.llm_client import resolve_default_model

    return resolve_default_model()


def postprocess_task_run(
    task_row,
    task_id: int,
    run_id: int,
    run_status: str,
) -> Tuple[Optional[dict], Optional[dict], Optional[dict]]:
    return postprocess_task_run_core(
        task_row=task_row,
        task_id=task_id,
        run_id=run_id,
        run_status=run_status,
        ensure_agent_review_record_fn=ensure_agent_review_record,
        safe_write_debug_fn=_safe_write_debug,
        extract_graph_updates_fn=extract_graph_updates,
        write_task_result_memory_if_missing_fn=write_task_result_memory_if_missing,
        resolve_default_model_fn=_resolve_default_model,
    )
