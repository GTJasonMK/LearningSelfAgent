import unittest


class TestNeedInputChoices(unittest.TestCase):
    def test_normalize_need_input_choices(self):
        from backend.src.agent.runner.need_input_choices import normalize_need_input_choices

        raw = [
            "  方案A ",
            {"label": "方案B", "value": "B"},
            {"label": "方案B", "value": "B"},
            {"label": "方案C"},
            "",
            {"label": "  "},
        ]
        got = normalize_need_input_choices(raw, limit=3)
        self.assertEqual(
            got,
            [
                {"label": "方案A", "value": "方案A"},
                {"label": "方案B", "value": "B"},
                {"label": "方案C", "value": "方案C"},
            ],
        )

    def test_resolve_need_input_choices_prefers_explicit(self):
        from backend.src.agent.runner.need_input_choices import resolve_need_input_choices

        got = resolve_need_input_choices(
            raw_choices=[{"label": "快速", "value": "fast"}],
            question="请选择模式",
            kind="knowledge_sufficiency",
        )
        self.assertEqual(got, [{"label": "快速", "value": "fast"}])

    def test_resolve_need_input_choices_knowledge_default(self):
        from backend.src.agent.runner.need_input_choices import resolve_need_input_choices

        got = resolve_need_input_choices(
            raw_choices=None,
            question="为了继续规划，请补充关键约束",
            kind="knowledge_sufficiency",
        )
        self.assertGreaterEqual(len(got), 2)
        labels = [str(item.get("label") or "") for item in got]
        self.assertIn("按当前信息继续", labels)
        self.assertIn("先给我澄清问题", labels)

    def test_resolve_need_input_choices_yes_no_infer(self):
        from backend.src.agent.runner.need_input_choices import resolve_need_input_choices

        got = resolve_need_input_choices(
            raw_choices=[],
            question="请确认是否继续执行？",
            kind=None,
        )
        self.assertEqual(got, [{"label": "是", "value": "是"}, {"label": "否", "value": "否"}])

    def test_resolve_need_input_choices_open_question_no_default(self):
        from backend.src.agent.runner.need_input_choices import resolve_need_input_choices

        got = resolve_need_input_choices(
            raw_choices=[],
            question="请补充目标文件路径和输出格式",
            kind=None,
        )
        self.assertEqual(got, [])


if __name__ == "__main__":
    unittest.main()
