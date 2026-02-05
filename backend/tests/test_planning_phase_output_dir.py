import unittest
import os


class TestPlanningPhaseOutputDir(unittest.TestCase):
    def test_extract_output_dir_from_message_windows_path(self):
        from backend.src.agent.planning_phase import _extract_output_dir_from_message

        message = '请你获取最近三个月的黄金价格并保存在"E:\\\\code\\\\LearningSelfAgent\\\\test"目录中'
        workdir = "E:\\\\code\\\\LearningSelfAgent"
        self.assertEqual(
            _extract_output_dir_from_message(message=message, workdir=workdir),
            "test",
        )

    def test_extract_output_dir_ignores_file_like_path(self):
        from backend.src.agent.planning_phase import _extract_output_dir_from_message

        message = '把结果保存在"E:\\\\code\\\\LearningSelfAgent\\\\test\\\\gold_price.csv"中'
        workdir = "E:\\\\code\\\\LearningSelfAgent"
        self.assertIsNone(_extract_output_dir_from_message(message=message, workdir=workdir))

    def test_extract_output_dir_from_message_windows_path_under_wsl_workdir(self):
        from backend.src.agent.planning_phase import _extract_output_dir_from_message

        # 兼容 WSL：workdir 为 /mnt/<drive>/...，但用户输入仍可能是 Windows 盘符路径。
        message = '请你获取最近三个月的黄金价格并保存在"E:\\\\code\\\\LearningSelfAgent\\\\test"目录中'
        workdir = "/mnt/e/code/LearningSelfAgent"
        self.assertEqual(
            _extract_output_dir_from_message(message=message, workdir=workdir),
            "test",
        )

    def test_apply_output_dir_hint_to_plan_prefixes_bare_filenames(self):
        from backend.src.agent.planning_phase import _apply_output_dir_hint_to_plan

        titles = [
            "tool_call:web_fetch 抓取数据",
            "file_write:gold_price.csv 写入价格数据",
            "task_output 输出结果",
        ]
        artifacts = ["gold_price.csv"]
        new_titles, new_artifacts, changed = _apply_output_dir_hint_to_plan(
            output_dir_rel="test",
            titles=titles,
            artifacts=artifacts,
        )
        self.assertTrue(changed)
        self.assertIn("test/gold_price.csv", new_artifacts)
        self.assertIn("file_write:test/gold_price.csv", new_titles[1])


class TestPathUtilsWindowsToWsl(unittest.TestCase):
    def test_normalize_windows_abs_path_on_posix(self):
        from backend.src.common.path_utils import normalize_windows_abs_path_on_posix

        if os.name == "nt":
            self.assertEqual(
                normalize_windows_abs_path_on_posix(r"E:\\code\\a.txt"),
                r"E:\\code\\a.txt",
            )
            return

        self.assertEqual(
            normalize_windows_abs_path_on_posix(r"E:\\code\\LearningSelfAgent\\docs\\agent"),
            "/mnt/e/code/LearningSelfAgent/docs/agent",
        )
        self.assertEqual(
            normalize_windows_abs_path_on_posix("E:/code/LearningSelfAgent/docs/agent"),
            "/mnt/e/code/LearningSelfAgent/docs/agent",
        )
        self.assertEqual(normalize_windows_abs_path_on_posix("docs/agent"), "docs/agent")


if __name__ == "__main__":
    unittest.main()
