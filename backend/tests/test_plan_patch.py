import unittest


class TestPlanPatch(unittest.TestCase):
    def test_apply_next_step_patch_updates_only_next_step(self):
        from backend.src.agent.support import apply_next_step_patch

        plan_titles = ["步骤1", "步骤2", "步骤3"]
        plan_items = [
            {"id": 1, "brief": "一", "status": "pending"},
            {"id": 2, "brief": "二", "status": "pending"},
            {"id": 3, "brief": "三", "status": "pending"},
        ]
        plan_allows = [["tool_call"], ["llm_call"], ["task_output"]]
        plan_artifacts = []

        err = apply_next_step_patch(
            current_step_index=1,
            patch_obj={
                "step_index": 2,
                "title": "tool_call:web_fetch 抓取数据",
                "allow": ["tool_call"],
                "artifacts_add": ["out/result.json"],
            },
            plan_titles=plan_titles,
            plan_items=plan_items,
            plan_allows=plan_allows,
            plan_artifacts=plan_artifacts,
        )
        self.assertIsNone(err)

        # 只改下一步（第2步）
        self.assertEqual(plan_titles[0], "步骤1")
        self.assertEqual(plan_titles[1], "tool_call:web_fetch 抓取数据")
        # artifacts 声明后会自动补齐 file_write 步骤，避免在 task_output 前被 artifacts 校验拦截
        self.assertTrue(str(plan_titles[2]).startswith("file_write:out/result.json"))
        self.assertEqual(plan_titles[3], "步骤3")
        self.assertEqual(plan_allows[1], ["tool_call"])
        self.assertIn("out/result.json", plan_artifacts)
        self.assertEqual(len(plan_items), 4)
        self.assertEqual([item.get("id") for item in plan_items], [1, 2, 3, 4])
        self.assertTrue(str(plan_items[1].get("brief") or "").strip())

    def test_apply_next_step_patch_rejects_cross_step_change(self):
        from backend.src.agent.support import apply_next_step_patch

        plan_titles = ["a", "b"]
        plan_items = [{"id": 1, "brief": "a", "status": "pending"}, {"id": 2, "brief": "b", "status": "pending"}]
        plan_allows = [["tool_call"], ["task_output"]]
        plan_artifacts = []

        err = apply_next_step_patch(
            current_step_index=1,
            patch_obj={"step_index": 3, "title": "x"},
            plan_titles=plan_titles,
            plan_items=plan_items,
            plan_allows=plan_allows,
            plan_artifacts=plan_artifacts,
        )
        self.assertIsInstance(err, str)
        self.assertIn("step_index", err)

    def test_apply_next_step_patch_appends_on_last_step(self):
        from backend.src.agent.support import apply_next_step_patch

        plan_titles = ["a"]
        plan_items = [{"id": 1, "brief": "a", "status": "pending"}]
        plan_allows = [["task_output"]]
        plan_artifacts = []

        err = apply_next_step_patch(
            current_step_index=1,
            patch_obj={"step_index": 2, "title": "b", "brief": "追加", "allow": ["task_output"]},
            plan_titles=plan_titles,
            plan_items=plan_items,
            plan_allows=plan_allows,
            plan_artifacts=plan_artifacts,
        )
        self.assertIsNone(err)
        self.assertEqual(plan_titles, ["a", "b"])
        self.assertEqual(plan_allows, [["task_output"], ["task_output"]])
        self.assertEqual(len(plan_items), 2)
        self.assertEqual(plan_items[0].get("id"), 1)
        self.assertEqual(plan_items[1].get("id"), 2)

    def test_apply_next_step_patch_insert_steps_inserts_at_next_step_position(self):
        from backend.src.agent.support import apply_next_step_patch

        plan_titles = ["步骤1", "步骤2", "步骤3"]
        plan_items = [
            {"id": 1, "brief": "一", "status": "pending"},
            {"id": 2, "brief": "二", "status": "pending"},
            {"id": 3, "brief": "三", "status": "pending"},
        ]
        plan_allows = [["tool_call"], ["llm_call"], ["task_output"]]
        plan_artifacts = []

        err = apply_next_step_patch(
            current_step_index=1,
            patch_obj={
                "step_index": 2,
                "insert_steps": [
                    {"title": "tool_call:web_fetch 抓取备用来源", "brief": "重试抓取", "allow": ["tool_call"]},
                    {"title": "输出结果", "brief": "输出", "allow": ["task_output"]},
                ],
            },
            plan_titles=plan_titles,
            plan_items=plan_items,
            plan_allows=plan_allows,
            plan_artifacts=plan_artifacts,
        )
        self.assertIsNone(err)
        self.assertEqual(
            plan_titles,
            ["步骤1", "tool_call:web_fetch 抓取备用来源", "输出结果", "步骤2", "步骤3"],
        )
        self.assertEqual(plan_allows[1], ["tool_call"])
        self.assertEqual(plan_allows[2], ["task_output"])
        self.assertEqual(len(plan_items), 5)
        self.assertEqual([item.get("id") for item in plan_items], [1, 2, 3, 4, 5])

    def test_apply_next_step_patch_accepts_http_request_and_json_parse(self):
        """
        回归：plan_patch.insert_steps 允许插入系统级 action 类型（http_request/json_parse 等）。

        之前 apply_next_step_patch 内部对 allow 类型做白名单过滤不全，会把 http_request/json_parse 过滤成空，
        导致报错 "plan_patch.insert_steps[i].allow 不能为空"。
        """
        from backend.src.agent.support import apply_next_step_patch

        plan_titles = ["步骤1", "task_output 输出最终结果"]
        plan_items = [
            {"id": 1, "brief": "一", "status": "pending"},
            {"id": 2, "brief": "输出", "status": "pending"},
        ]
        plan_allows = [["llm_call"], ["task_output"]]
        plan_artifacts = []

        err = apply_next_step_patch(
            current_step_index=1,
            patch_obj={
                "step_index": 2,
                "insert_steps": [
                    {"title": "http_request:https://example.com 获取数据", "brief": "抓取", "allow": ["http_request"]},
                    {"title": "json_parse 解析响应 JSON", "brief": "解析", "allow": ["json_parse"]},
                ],
            },
            plan_titles=plan_titles,
            plan_items=plan_items,
            plan_allows=plan_allows,
            plan_artifacts=plan_artifacts,
        )
        self.assertIsNone(err)

        self.assertEqual(
            plan_titles,
            [
                "步骤1",
                "http_request:https://example.com 获取数据",
                "json_parse 解析响应 JSON",
                "task_output 输出最终结果",
            ],
        )
        self.assertEqual(plan_allows[1], ["http_request"])
        self.assertEqual(plan_allows[2], ["json_parse"])
        self.assertEqual(plan_allows[3], ["task_output"])
        self.assertEqual([it.get("id") for it in plan_items], [1, 2, 3, 4])


if __name__ == "__main__":
    unittest.main()
