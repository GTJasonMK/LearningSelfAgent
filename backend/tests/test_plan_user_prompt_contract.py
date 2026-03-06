import unittest


class TestPlanUserPromptContract(unittest.TestCase):
    def test_normalize_plan_titles_rejects_regular_user_prompt(self):
        from backend.src.agent.plan_utils import _normalize_plan_titles

        titles, briefs, allows, artifacts, err = _normalize_plan_titles(
            {
                "plan": [
                    {
                        "title": "user_prompt:确认价格口径频率与输出路径",
                        "brief": "确认口径",
                        "allow": ["user_prompt"],
                    },
                    {
                        "title": "task_output:输出结果",
                        "brief": "输出结果",
                        "allow": ["task_output"],
                    },
                ],
                "artifacts": [],
            },
            max_steps=8,
        )

        self.assertIsNone(titles)
        self.assertIsNone(briefs)
        self.assertIsNone(allows)
        self.assertIsNone(artifacts)
        self.assertIn("不允许 user_prompt", str(err or ""))

    def test_apply_next_step_patch_rejects_insert_user_prompt(self):
        from backend.src.agent.support import apply_next_step_patch

        plan_titles = ["步骤1", "task_output:输出结果"]
        plan_items = [
            {"id": 1, "brief": "一", "status": "pending"},
            {"id": 2, "brief": "输出", "status": "pending"},
        ]
        plan_allows = [["tool_call"], ["task_output"]]
        plan_artifacts = []

        err = apply_next_step_patch(
            current_step_index=1,
            patch_obj={
                "step_index": 2,
                "insert_steps": [
                    {
                        "title": "user_prompt:确认数据源与USD/CNY汇率",
                        "brief": "确认来源",
                        "allow": ["user_prompt"],
                    }
                ],
            },
            plan_titles=plan_titles,
            plan_items=plan_items,
            plan_allows=plan_allows,
            plan_artifacts=plan_artifacts,
        )

        self.assertIsInstance(err, str)
        self.assertIn("不允许 user_prompt", err)

    def test_normalize_plan_titles_strips_illustrative_examples(self):
        from backend.src.agent.plan_utils import _normalize_plan_titles

        titles, briefs, allows, artifacts, err = _normalize_plan_titles(
            {
                "plan": [
                    {
                        "title": "tool_call:web_fetch 抓取黄金价格数据源（例如黄金期货GC=F页面）",
                        "brief": "抓取数据源",
                        "allow": ["tool_call"],
                    },
                    {
                        "title": "task_output:输出结果",
                        "brief": "输出结果",
                        "allow": ["task_output"],
                    },
                ],
                "artifacts": [],
            },
            max_steps=8,
        )

        self.assertIsNone(err)
        self.assertEqual(titles[0], "tool_call:web_fetch 抓取黄金价格数据源")
        self.assertNotIn("GC=F", titles[0])
        self.assertEqual(allows[0], ["tool_call"])



if __name__ == "__main__":
    unittest.main()
