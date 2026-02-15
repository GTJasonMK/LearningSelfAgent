import json
import unittest

from backend.src.agent.core.plan_structure import PlanStep, PlanStructure


class TestPlanStructure(unittest.TestCase):
    def test_from_agent_plan_payload_normalizes_fields(self):
        plan = PlanStructure.from_agent_plan_payload(
            {
                "titles": ["tool_call:web_fetch 抓取", "file_write:out.csv 写入"],
                "items": [{"status": "running"}, {}],
                "allows": [["tool", "TOOL_CALL"], "file_write"],
                "artifacts": ["out.csv", "out.csv", "backend\\tmp\\a.csv"],
            }
        )

        titles, items, allows, artifacts = plan.to_legacy_lists()
        self.assertEqual(len(titles), 2)
        self.assertEqual(allows[0], ["tool_call"])
        self.assertEqual(allows[1], ["file_write"])
        self.assertEqual(items[0].get("status"), "running")
        self.assertTrue(str(items[0].get("brief") or "").strip())
        self.assertEqual(artifacts, ["out.csv", "backend/tmp/a.csv"])

    def test_from_legacy_handles_mismatch_lengths(self):
        plan = PlanStructure.from_legacy(
            plan_titles=["step1", "step2", "step3"],
            plan_items=[{"brief": "A", "status": "done"}],
            plan_allows=[["tool_call"]],
            plan_artifacts=["a.csv"],
        )
        _, items, allows, _ = plan.to_legacy_lists()
        self.assertEqual(len(items), 3)
        self.assertEqual(items[0].get("status"), "done")
        self.assertEqual(items[1].get("status"), "pending")
        self.assertEqual(allows[0], ["tool_call"])
        self.assertEqual(allows[1], [])


class TestPlanStructureMutationAPI(unittest.TestCase):
    """PlanStructure 变更 API 测试。"""

    def _make_plan(self, count: int = 3) -> PlanStructure:
        steps = [
            PlanStep(id=i + 1, title=f"step{i + 1}", brief=f"brief{i + 1}", allow=["tool_call"], status="pending")
            for i in range(count)
        ]
        return PlanStructure(steps=steps, artifacts=["out.csv"])

    def test_step_count(self):
        plan = self._make_plan(3)
        self.assertEqual(plan.step_count, 3)
        self.assertEqual(PlanStructure(steps=[], artifacts=[]).step_count, 0)

    def test_get_step_valid_index(self):
        plan = self._make_plan(3)
        step = plan.get_step(0)
        self.assertIsNotNone(step)
        self.assertEqual(step.title, "step1")

    def test_get_step_out_of_bounds(self):
        plan = self._make_plan(3)
        self.assertIsNone(plan.get_step(-1))
        self.assertIsNone(plan.get_step(3))

    def test_set_step_status(self):
        plan = self._make_plan(3)
        plan.set_step_status(0, "running")
        self.assertEqual(plan.steps[0].status, "running")
        plan.set_step_status(1, "failed")
        self.assertEqual(plan.steps[1].status, "failed")

    def test_set_step_status_out_of_bounds_is_noop(self):
        plan = self._make_plan(2)
        plan.set_step_status(5, "done")
        # 不应抛异常，也不应改变任何步骤
        self.assertEqual(plan.steps[0].status, "pending")
        self.assertEqual(plan.steps[1].status, "pending")

    def test_mark_running_as_done(self):
        plan = self._make_plan(3)
        plan.set_step_status(0, "done")
        plan.set_step_status(1, "running")
        plan.set_step_status(2, "running")
        plan.mark_running_as_done()
        self.assertEqual(plan.steps[0].status, "done")
        self.assertEqual(plan.steps[1].status, "done")
        self.assertEqual(plan.steps[2].status, "done")

    def test_insert_steps(self):
        plan = self._make_plan(2)
        new_steps = [
            PlanStep(id=0, title="inserted1", brief="ins1", allow=["file_write"], status="pending"),
            PlanStep(id=0, title="inserted2", brief="ins2", allow=["shell_command"], status="pending"),
        ]
        plan.insert_steps(1, new_steps)
        self.assertEqual(plan.step_count, 4)
        self.assertEqual(plan.steps[0].title, "step1")
        self.assertEqual(plan.steps[1].title, "inserted1")
        self.assertEqual(plan.steps[2].title, "inserted2")
        self.assertEqual(plan.steps[3].title, "step2")
        # 验证重编号
        for i, step in enumerate(plan.steps, start=1):
            self.assertEqual(step.id, i)

    def test_replace_from(self):
        plan = self._make_plan(2)
        replacement = self._make_plan(4)
        replacement.artifacts = ["new.txt"]
        plan.replace_from(replacement)
        self.assertEqual(plan.step_count, 4)
        self.assertEqual(plan.artifacts, ["new.txt"])
        for i, step in enumerate(plan.steps, start=1):
            self.assertEqual(step.id, i)

    def test_get_titles_json(self):
        plan = self._make_plan(2)
        result = plan.get_titles_json()
        parsed = json.loads(result)
        self.assertEqual(parsed, ["step1", "step2"])

    def test_get_items_payload(self):
        plan = self._make_plan(2)
        plan.set_step_status(0, "done")
        items = plan.get_items_payload()
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["status"], "done")
        self.assertEqual(items[0]["title"], "step1")
        self.assertIn("allow", items[0])

    def test_get_titles(self):
        plan = self._make_plan(3)
        self.assertEqual(plan.get_titles(), ["step1", "step2", "step3"])


if __name__ == "__main__":
    unittest.main()
