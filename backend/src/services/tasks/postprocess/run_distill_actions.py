from __future__ import annotations

from typing import Callable, List, Optional

from backend.src.repositories.agent_reviews_repo import update_agent_review_record
from backend.src.repositories.task_outputs_repo import list_task_outputs_for_run
from backend.src.repositories.task_steps_repo import list_task_steps_for_run
from backend.src.storage import get_connection


def sync_draft_skill_status(
    *,
    allow_distill: bool,
    review_status: str,
    task_id: int,
    run_id: int,
    latest_review_id: Optional[int],
    safe_write_debug_fn: Callable[..., None],
) -> None:
    """
    基于 review 结果处理本次 run 产生的 draft 技能状态。
    """
    if allow_distill:
        try:
            from backend.src.repositories.skills_repo import update_skill_status
            from backend.src.services.skills.skills_publish import publish_skill_file

            with get_connection() as conn:
                rows = conn.execute(
                    "SELECT id FROM skills_items WHERE status = 'draft' AND source_run_id = ? AND (skill_type IS NULL OR skill_type != 'solution') ORDER BY id ASC",
                    (int(run_id),),
                ).fetchall()

            approved_skill_ids: List[int] = []
            for row in rows or []:
                try:
                    sid = int(row["id"])
                except Exception:
                    continue
                if sid <= 0:
                    continue
                try:
                    _ = update_skill_status(skill_id=int(sid), status="approved")
                    _source_path, _publish_err = publish_skill_file(int(sid))
                    if _publish_err:
                        continue
                    approved_skill_ids.append(int(sid))
                except Exception:
                    continue

            if approved_skill_ids:
                safe_write_debug_fn(
                    int(task_id),
                    int(run_id),
                    message="skill.draft_approved",
                    data={
                        "review_id": int(latest_review_id) if latest_review_id else None,
                        "review_status": str(review_status or ""),
                        "skill_ids": approved_skill_ids,
                    },
                    level="info",
                )
        except Exception as exc:
            safe_write_debug_fn(
                int(task_id),
                int(run_id),
                message="skill.draft_approve_failed",
                data={"error": str(exc)},
                level="warning",
            )
        return

    if str(review_status or "").strip().lower() == "fail":
        try:
            from backend.src.repositories.skills_repo import update_skill_status

            with get_connection() as conn:
                rows = conn.execute(
                    "SELECT id FROM skills_items WHERE status = 'draft' AND source_run_id = ? ORDER BY id ASC",
                    (int(run_id),),
                ).fetchall()

            abandoned_skill_ids: List[int] = []
            for row in rows or []:
                try:
                    sid = int(row["id"])
                except Exception:
                    continue
                if sid <= 0:
                    continue
                try:
                    updated = update_skill_status(skill_id=int(sid), status="abandoned")
                    if updated:
                        abandoned_skill_ids.append(int(sid))
                except Exception:
                    continue

            if abandoned_skill_ids:
                safe_write_debug_fn(
                    int(task_id),
                    int(run_id),
                    message="skill.draft_abandoned",
                    data={
                        "review_id": int(latest_review_id) if latest_review_id else None,
                        "review_status": str(review_status or ""),
                        "skill_ids": abandoned_skill_ids,
                    },
                    level="info",
                )
        except Exception as exc:
            safe_write_debug_fn(
                int(task_id),
                int(run_id),
                message="skill.draft_abandon_failed",
                data={"error": str(exc)},
                level="warning",
            )


def collect_graph_update_if_allowed(
    *,
    allow_distill: bool,
    task_id: int,
    run_id: int,
    extract_graph_updates_fn: Callable[[int, int, List[dict], List[dict]], Optional[dict]],
) -> Optional[dict]:
    if not allow_distill:
        return None
    try:
        step_rows = list_task_steps_for_run(task_id=int(task_id), run_id=int(run_id))
        output_rows = list_task_outputs_for_run(task_id=int(task_id), run_id=int(run_id), order="ASC")
        return extract_graph_updates_fn(task_id, run_id, step_rows, output_rows)
    except Exception:
        return None


def autogen_solution_if_allowed(
    *,
    allow_distill: bool,
    task_id: int,
    run_id: int,
    safe_write_debug_fn: Callable[..., None],
) -> None:
    try:
        if allow_distill:
            from backend.src.services.skills.run_solution_autogen import autogen_solution_from_run

            _ = autogen_solution_from_run(task_id=int(task_id), run_id=int(run_id), force=False)
    except Exception as exc:
        safe_write_debug_fn(
            int(task_id),
            int(run_id),
            message="solution.autogen_failed",
            data={"error": str(exc)},
            level="warning",
        )


def autogen_skills_response(
    *,
    allow_distill: bool,
    task_id: int,
    run_id: int,
    resolve_default_model_fn: Callable[[], str],
) -> dict:
    try:
        if allow_distill:
            from backend.src.services.skills.run_skill_autogen import autogen_skills_from_run

            return autogen_skills_from_run(
                task_id=int(task_id),
                run_id=int(run_id),
                model=resolve_default_model_fn(),
            )
        return {"ok": True, "status": "skipped_review_not_pass"}
    except Exception as exc:
        return {"ok": False, "error": f"{exc}"}


def sync_review_skills(
    *,
    latest_review_id: Optional[int],
    skill_response: Optional[dict],
    task_id: int,
    run_id: int,
    safe_write_debug_fn: Callable[..., None],
) -> None:
    try:
        skills = []
        if isinstance(skill_response, dict) and isinstance(skill_response.get("skills"), list):
            skills = skill_response.get("skills") or []
        if latest_review_id:
            update_agent_review_record(review_id=int(latest_review_id), skills=skills)
    except Exception as exc:
        safe_write_debug_fn(
            int(task_id),
            int(run_id),
            message="agent.review.ensure_failed",
            data={"error": str(exc)},
            level="warning",
        )
