import unittest


class TestPlanArtifactsRepair(unittest.TestCase):
    def test_repair_inserts_missing_file_write_steps_before_task_output(self):
        from backend.src.agent.plan_utils import repair_plan_artifacts_with_file_write_steps

        titles = ["准备数据", "写入代码文件", "输出结果"]
        briefs = ["准备", "写文件", "输出"]
        allows = [["tool_call"], ["file_write"], ["task_output"]]
        artifacts = ["a.txt", "b.txt"]

        new_titles, new_briefs, new_allows, new_artifacts, err, patched = (
            repair_plan_artifacts_with_file_write_steps(
                titles=titles,
                briefs=briefs,
                allows=allows,
                artifacts=artifacts,
                max_steps=6,
            )
        )
        self.assertIsNone(err)
        self.assertEqual(patched, 1)
        self.assertEqual(new_artifacts, ["a.txt", "b.txt"])

        # 原有 file_write 步骤会被绑定到第一个 artifact
        self.assertTrue(new_titles[1].startswith("file_write:a.txt"))
        self.assertIn("写入代码文件", new_titles[1])

        # 新增的 file_write 步骤应插入到 task_output 之前
        self.assertEqual(new_titles[2], "file_write:b.txt 写入文件")
        self.assertEqual(new_briefs[2], "写文件")
        self.assertEqual(new_allows[2], ["file_write"])
        self.assertEqual(new_allows[3], ["task_output"])

    def test_repair_noop_when_file_write_steps_already_cover_artifacts(self):
        from backend.src.agent.plan_utils import repair_plan_artifacts_with_file_write_steps

        titles = ["file_write:a.txt 写入", "file_write:b.txt 写入", "输出"]
        briefs = ["写a", "写b", "输出"]
        allows = [["file_write"], ["file_write"], ["task_output"]]
        artifacts = ["a.txt", "b.txt"]

        new_titles, _, new_allows, _, err, patched = repair_plan_artifacts_with_file_write_steps(
            titles=titles,
            briefs=briefs,
            allows=allows,
            artifacts=artifacts,
            max_steps=6,
        )
        self.assertIsNone(err)
        self.assertEqual(patched, 0)
        self.assertEqual(new_titles, titles)
        self.assertEqual(new_allows, allows)

    def test_repair_fails_when_exceeding_max_steps(self):
        from backend.src.agent.plan_utils import repair_plan_artifacts_with_file_write_steps

        titles = ["a", "b", "c"]
        briefs = ["a", "b", "c"]
        allows = [["file_write"], ["task_output"], ["tool_call"]]
        artifacts = ["x.txt", "y.txt"]

        _, _, _, _, err, patched = repair_plan_artifacts_with_file_write_steps(
            titles=titles,
            briefs=briefs,
            allows=allows,
            artifacts=artifacts,
            max_steps=3,
        )
        self.assertIsInstance(err, str)
        self.assertEqual(patched, 0)


if __name__ == "__main__":
    unittest.main()
