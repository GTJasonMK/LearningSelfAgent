from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Set

from backend.src.agent.core.plan_structure import PlanStep, PlanStructure


class PlanCoordinator:
    """
    统一计划结构的合并与重建逻辑，避免在多个入口复制“手工对齐列表”的代码。
    """

    @staticmethod
    def merge_replan_with_history(
        *,
        current_plan: PlanStructure,
        done_count: int,
        replan_titles: Sequence[str],
        replan_allows: Sequence[Sequence[str]],
        replan_items: Sequence[dict],
        replan_artifacts: Sequence[str],
    ) -> PlanStructure:
        safe_done_count = max(0, min(int(done_count), len(current_plan.steps or [])))

        merged_steps: List[PlanStep] = []
        for idx in range(safe_done_count):
            step = current_plan.steps[idx]
            status = str(step.status or "pending")
            if status == "failed":
                status = "skipped"
            merged_steps.append(
                PlanStep(
                    id=len(merged_steps) + 1,
                    title=str(step.title),
                    brief=str(step.brief),
                    allow=list(step.allow or []),
                    status=status,
                )
            )

        for idx, raw_title in enumerate(replan_titles, start=1):
            title = str(raw_title or "").strip()
            if not title:
                continue
            raw_item = replan_items[idx - 1] if idx - 1 < len(replan_items) and isinstance(replan_items[idx - 1], dict) else {}
            brief = str(raw_item.get("brief") or "").strip()
            raw_allow = replan_allows[idx - 1] if idx - 1 < len(replan_allows) else raw_item.get("allow")
            allow = list(raw_allow or []) if isinstance(raw_allow, list) else [raw_allow] if raw_allow else []
            merged_steps.append(
                PlanStep(
                    id=len(merged_steps) + 1,
                    title=title,
                    brief=brief,
                    allow=allow,
                    status="pending",
                )
            )

        plan = PlanStructure(
            steps=merged_steps,
            artifacts=[str(value or "").strip() for value in (replan_artifacts or []) if str(value or "").strip()],
        )
        plan.validate()
        return plan

    @staticmethod
    def rebuild_items_after_reflection_insert(
        *,
        plan_titles: Sequence[str],
        plan_briefs: Sequence[str],
        plan_allows: Sequence[Sequence[str]],
        old_plan_items: Sequence[dict],
        done_step_indices: Set[int],
        failed_step_index: int,
        insert_pos: int,
        fix_count: int,
    ) -> List[dict]:
        new_items: List[dict] = []
        safe_fix_count = max(0, int(fix_count))

        for idx, title in enumerate(plan_titles):
            brief = str(plan_briefs[idx] or "").strip() if idx < len(plan_briefs) else ""
            allow = list(plan_allows[idx] or []) if idx < len(plan_allows) else []

            if idx in set(done_step_indices or set()):
                status = "done"
            elif idx == int(failed_step_index):
                status = "skipped"
            elif int(insert_pos) <= idx < int(insert_pos) + safe_fix_count:
                status = "pending"
            else:
                old_index = idx
                if idx >= int(insert_pos) + safe_fix_count:
                    old_index = idx - safe_fix_count
                status = "pending"
                if 0 <= old_index < len(old_plan_items):
                    old_item = old_plan_items[old_index]
                    if isinstance(old_item, dict):
                        raw_status = str(old_item.get("status") or "").strip() or "pending"
                        status = "pending" if raw_status in {"running", "waiting", "planned"} else raw_status

            new_items.append(
                {
                    "id": idx + 1,
                    "title": str(title or "").strip(),
                    "brief": brief,
                    "allow": allow,
                    "status": status,
                }
            )

        return new_items

    @staticmethod
    def build_executor_assignments_payload(
        *,
        plan_titles: Sequence[str],
        plan_allows: Sequence[Sequence[str]],
        infer_executor,
    ) -> List[dict]:
        rows: List[dict] = []
        for idx, title in enumerate(plan_titles or []):
            allow = list(plan_allows[idx] or []) if idx < len(plan_allows) else []
            role = str(infer_executor(allow, str(title or "")) or "").strip()
            rows.append(
                {
                    "step_order": idx + 1,
                    "executor": role,
                    "allow": allow,
                }
            )
        return rows
