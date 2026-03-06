import unittest

from backend.src.agent.plan_utils import _normalize_plan_titles, extract_file_write_declared_paths


class TestPlanFileWriteContract(unittest.TestCase):
    def test_extract_declared_paths_keeps_multiple_targets(self):
        title = "file_write:data/output.csv backend/.agent/workspace/build_csv.py 写入解析脚本"
        paths = extract_file_write_declared_paths(title)
        self.assertEqual(paths, ["data/output.csv", "backend/.agent/workspace/build_csv.py"])

    def test_normalize_plan_titles_rejects_ambiguous_file_write_target(self):
        plan = {
            "plan": [
                {
                    "title": "file_write:data/output.csv backend/.agent/workspace/build_csv.py 写入解析脚本",
                    "allow": ["file_write"],
                },
                {
                    "title": "task_output:输出结果",
                    "allow": ["task_output"],
                },
            ]
        }

        titles, briefs, allows, artifacts, error = _normalize_plan_titles(plan, max_steps=6)

        self.assertIsNone(titles)
        self.assertIsNone(briefs)
        self.assertIsNone(allows)
        self.assertIsNone(artifacts)
        self.assertIn("file_write 标题必须且只能声明一个目标路径", str(error or ""))


    def test_normalize_plan_titles_rejects_file_read_without_explicit_path(self):
        plan = {
            "plan": [
                {
                    "title": "file_read:读取http_request响应内容",
                    "allow": ["file_read"],
                },
                {
                    "title": "task_output:输出结果",
                    "allow": ["task_output"],
                },
            ]
        }

        titles, briefs, allows, artifacts, error = _normalize_plan_titles(plan, max_steps=6)

        self.assertIsNone(titles)
        self.assertIsNone(briefs)
        self.assertIsNone(allows)
        self.assertIsNone(artifacts)
        self.assertIn("file_read 标题必须显式声明目标路径", str(error or ""))


if __name__ == "__main__":
    unittest.main()
