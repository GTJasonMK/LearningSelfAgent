import unittest


class TestPlanScriptFileWriteReorder(unittest.TestCase):
    def test_reorder_moves_script_file_write_before_first_exec_step(self):
        from backend.src.agent.plan_utils import reorder_script_file_writes_before_exec_steps

        titles = [
            "tool_call:创建工具",
            "http_request:抓取数据",
            "file_write:backend/.agent/workspace/data.csv 写入",
            "file_write:backend/.agent/workspace/fetch.py 写入",
            "shell_command:运行脚本",
            "task_output:输出结果",
        ]
        briefs = ["a", "b", "c", "d", "e", "f"]
        allows = [
            ["tool_call"],
            ["http_request"],
            ["file_write"],
            ["file_write"],
            ["shell_command"],
            ["task_output"],
        ]

        new_titles, new_briefs, new_allows, moved = reorder_script_file_writes_before_exec_steps(
            titles=titles,
            briefs=briefs,
            allows=allows,
        )
        self.assertEqual(moved, 1)
        self.assertTrue(new_titles[0].startswith("file_write:backend/.agent/workspace/fetch.py"))
        self.assertEqual(new_titles[1], "tool_call:创建工具")
        self.assertEqual(new_titles[2], "http_request:抓取数据")
        self.assertTrue(new_titles[3].startswith("file_write:backend/.agent/workspace/data.csv"))
        self.assertEqual(new_titles[-1], "task_output:输出结果")
        self.assertEqual(new_briefs[0], "d")
        self.assertEqual(new_allows[0], ["file_write"])

    def test_reorder_moves_script_file_write_before_shell_command_when_no_tool_call(self):
        from backend.src.agent.plan_utils import reorder_script_file_writes_before_exec_steps

        titles = [
            "llm_call:分析任务",
            "shell_command:运行 a.py",
            "file_write:backend/.agent/workspace/a.py 写入",
            "task_output:输出",
        ]
        briefs = ["分析", "运行", "写a", "输出"]
        allows = [["llm_call"], ["shell_command"], ["file_write"], ["task_output"]]

        new_titles, new_briefs, new_allows, moved = reorder_script_file_writes_before_exec_steps(
            titles=titles,
            briefs=briefs,
            allows=allows,
        )
        self.assertEqual(moved, 1)
        self.assertEqual(new_titles[0], "llm_call:分析任务")
        self.assertTrue(new_titles[1].startswith("file_write:backend/.agent/workspace/a.py"))
        self.assertEqual(new_titles[2], "shell_command:运行 a.py")
        self.assertEqual(new_briefs[1], "写a")
        self.assertEqual(new_allows[1], ["file_write"])


if __name__ == "__main__":
    unittest.main()

