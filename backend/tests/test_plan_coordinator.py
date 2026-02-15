import unittest

from backend.src.agent.core.plan_coordinator import PlanCoordinator
from backend.src.agent.core.plan_structure import PlanStructure


class TestPlanCoordinator(unittest.TestCase):
    def test_merge_replan_with_history_keeps_done_and_skips_failed(self):
        current = PlanStructure.from_legacy(
            plan_titles=["A", "B", "C"],
            plan_items=[
                {"brief": "A", "status": "done"},
                {"brief": "B", "status": "failed"},
                {"brief": "C", "status": "pending"},
            ],
            plan_allows=[["tool_call"], ["shell_command"], ["task_output"]],
            plan_artifacts=["old.csv"],
        )

        merged = PlanCoordinator.merge_replan_with_history(
            current_plan=current,
            done_count=2,
            replan_titles=["D"],
            replan_allows=[["file_write"]],
            replan_items=[{"brief": "D"}],
            replan_artifacts=["new.csv"],
        )

        titles, items, allows, artifacts = merged.to_legacy_lists()
        self.assertEqual(titles, ["A", "B", "D"])
        self.assertEqual(items[0].get("status"), "done")
        self.assertEqual(items[1].get("status"), "skipped")
        self.assertEqual(items[2].get("status"), "pending")
        self.assertEqual(allows[2], ["file_write"])
        self.assertEqual(artifacts, ["new.csv"])

    def test_rebuild_items_after_reflection_insert(self):
        rebuilt = PlanCoordinator.rebuild_items_after_reflection_insert(
            plan_titles=["A", "B", "fix1", "fix2", "C"],
            plan_briefs=["A", "B", "f1", "f2", "C"],
            plan_allows=[["tool_call"], ["shell_command"], ["file_write"], ["tool_call"], ["task_output"]],
            old_plan_items=[
                {"status": "done"},
                {"status": "failed"},
                {"status": "waiting"},
            ],
            done_step_indices={0},
            failed_step_index=1,
            insert_pos=2,
            fix_count=2,
        )

        statuses = [str(item.get("status") or "") for item in rebuilt]
        self.assertEqual(statuses[0], "done")
        self.assertEqual(statuses[1], "skipped")
        self.assertEqual(statuses[2], "pending")
        self.assertEqual(statuses[3], "pending")
        self.assertEqual(statuses[4], "pending")


if __name__ == "__main__":
    unittest.main()
