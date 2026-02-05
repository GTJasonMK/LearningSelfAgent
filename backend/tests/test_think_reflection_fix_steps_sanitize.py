import unittest

from backend.src.agent.think.think_reflection import merge_fix_steps_into_plan


class TestThinkReflectionFixStepsSanitize(unittest.TestCase):
    def test_merge_fix_steps_sanitizes_allow_string(self):
        plan_titles = ["shell_command:step1", "task_output 输出结果"]
        plan_briefs = ["step1", "输出"]
        plan_allows = [["shell_command"], ["task_output"]]

        fix_steps = [
            {"title": "file_write:main.py 修复代码", "brief": "修复", "allow": "file_write"},
        ]

        new_titles, new_briefs, new_allows = merge_fix_steps_into_plan(
            current_step_index=0,
            plan_titles=plan_titles,
            plan_briefs=plan_briefs,
            plan_allows=plan_allows,
            fix_steps=fix_steps,
        )

        self.assertEqual(new_titles[1], "file_write:main.py 修复代码")
        self.assertEqual(new_briefs[1], "修复")
        self.assertEqual(new_allows[1], ["file_write"])

    def test_merge_fix_steps_infers_allow_from_title_when_missing(self):
        plan_titles = ["file_write:main.py 写代码", "task_output 输出结果"]
        plan_briefs = ["写代码", "输出"]
        plan_allows = [["file_write"], ["task_output"]]

        fix_steps = [{"title": "shell_command:pytest -q 运行测试", "brief": "验证"}]

        _, _, new_allows = merge_fix_steps_into_plan(
            current_step_index=0,
            plan_titles=plan_titles,
            plan_briefs=plan_briefs,
            plan_allows=plan_allows,
            fix_steps=fix_steps,
        )

        self.assertEqual(new_allows[1], ["shell_command"])

    def test_merge_fix_steps_filters_invalid_allow_and_falls_back_to_infer(self):
        plan_titles = ["file_write:main.py 写代码", "task_output 输出结果"]
        plan_briefs = ["写代码", "输出"]
        plan_allows = [["file_write"], ["task_output"]]

        fix_steps = [
            {"title": "file_write:main.py 修复代码", "brief": "修复", "allow": ["task_output"]},
        ]

        _, _, new_allows = merge_fix_steps_into_plan(
            current_step_index=0,
            plan_titles=plan_titles,
            plan_briefs=plan_briefs,
            plan_allows=plan_allows,
            fix_steps=fix_steps,
        )

        self.assertEqual(new_allows[1], ["file_write"])


if __name__ == "__main__":
    unittest.main()

