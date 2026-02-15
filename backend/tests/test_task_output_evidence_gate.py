import unittest

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.react_artifacts_gate import _has_prior_http_success_step


class TestTaskOutputEvidenceGate(unittest.TestCase):
    def test_has_prior_http_requirement_but_not_done(self):
        plan_struct = PlanStructure.from_legacy(
            plan_titles=[
                "http_request:抓取原始数据",
                "json_parse:清洗数据",
                "task_output:输出结果",
            ],
            plan_items=[
                {"status": "failed"},
                {"status": "done"},
                {"status": "pending"},
            ],
            plan_allows=[
                ["http_request"],
                ["json_parse"],
                ["task_output"],
            ],
            plan_artifacts=[],
        )

        has_requirement, has_success = _has_prior_http_success_step(
            current_idx=2,
            plan_struct=plan_struct,
        )

        self.assertTrue(has_requirement)
        self.assertFalse(has_success)

    def test_has_prior_http_requirement_and_done(self):
        plan_struct = PlanStructure.from_legacy(
            plan_titles=[
                "http_request:抓取原始数据",
                "file_write:写文件",
                "task_output:输出结果",
            ],
            plan_items=[
                {"status": "done"},
                {"status": "done"},
                {"status": "pending"},
            ],
            plan_allows=[
                ["http_request"],
                ["file_write"],
                ["task_output"],
            ],
            plan_artifacts=[],
        )

        has_requirement, has_success = _has_prior_http_success_step(
            current_idx=2,
            plan_struct=plan_struct,
        )

        self.assertTrue(has_requirement)
        self.assertTrue(has_success)

    def test_no_prior_http_requirement(self):
        plan_struct = PlanStructure.from_legacy(
            plan_titles=[
                "file_write:写文件",
                "shell_command:验证",
                "task_output:输出结果",
            ],
            plan_items=[
                {"status": "done"},
                {"status": "done"},
                {"status": "pending"},
            ],
            plan_allows=[
                ["file_write"],
                ["shell_command"],
                ["task_output"],
            ],
            plan_artifacts=[],
        )

        has_requirement, has_success = _has_prior_http_success_step(
            current_idx=2,
            plan_struct=plan_struct,
        )

        self.assertFalse(has_requirement)
        self.assertFalse(has_success)


if __name__ == "__main__":
    unittest.main()
