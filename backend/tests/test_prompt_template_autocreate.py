import os
import tempfile
import unittest


class TestPromptTemplateAutoCreate(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_test.db")

        # 覆盖 DB 路径并重建 schema，避免污染真实数据文件。
        os.environ["AGENT_DB_PATH"] = self._db_path
        os.environ["AGENT_PROMPT_ROOT"] = os.path.join(self._tmpdir.name, "prompt")
        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
        except Exception:
            pass
        self._tmpdir.cleanup()

    def test_missing_numeric_template_id_is_not_auto_recovered(self):
        from backend.src.services.llm.prompt_templates import ensure_llm_call_template
        from backend.src.constants import PROMPT_TEMPLATE_AUTO_RECOVER_PREFIX
        from backend.src.storage import get_connection

        payload = {
            "template_id": 9999,
            "prompt": "请把下面代码格式化。\n输出格式化后的代码。",
        }
        ensure_llm_call_template(payload, step_title="整合与优化代码")

        # 不做自动恢复：保持原值，由后续 llm_call 执行阶段报“提示词不存在”。
        self.assertEqual(payload.get("template_id"), 9999)
        with get_connection() as conn:
            recovered_name = f"{PROMPT_TEMPLATE_AUTO_RECOVER_PREFIX}9999"
            tpl = conn.execute(
                "SELECT * FROM prompt_templates WHERE name = ? ORDER BY id DESC LIMIT 1",
                (recovered_name,),
            ).fetchone()
            self.assertIsNone(tpl)

    def test_missing_named_template_is_not_auto_created(self):
        from backend.src.services.llm.prompt_templates import ensure_llm_call_template
        from backend.src.storage import get_connection

        payload = {
            "template_id": "my_missing_template",
            "prompt": "请把这段文字总结成 3 条要点。",
        }
        ensure_llm_call_template(payload, step_title="生成摘要")

        # 不做自动创建：保持字符串（可在后续阶段显式创建模板）。
        self.assertEqual(payload.get("template_id"), "my_missing_template")

        with get_connection() as conn:
            tpl = conn.execute(
                "SELECT * FROM prompt_templates WHERE name = ? ORDER BY id DESC LIMIT 1",
                ("my_missing_template",),
            ).fetchone()
            self.assertIsNone(tpl)

    def test_existing_named_template_is_resolved_to_id(self):
        from backend.src.services.llm.prompt_templates import ensure_llm_call_template
        from backend.src.repositories.prompt_templates_repo import create_prompt_template

        template_id = create_prompt_template(
            name="my_existing_template",
            template="你好，{name}",
            description="test",
        )
        self.assertIsInstance(template_id, int)

        payload = {"template_id": "my_existing_template"}
        ensure_llm_call_template(payload, step_title="使用模板")
        self.assertEqual(payload.get("template_id"), template_id)


if __name__ == "__main__":
    unittest.main()
