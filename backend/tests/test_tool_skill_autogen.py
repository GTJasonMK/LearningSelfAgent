import json
import os
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timezone


class TestToolSkillAutogen(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_test.db")

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

    def test_autogen_tool_skill_creates_record_and_is_idempotent(self):
        from backend.src.storage import get_connection
        from backend.src.services.skills.tool_skill_autogen import autogen_tool_skill_from_call, find_tool_skill_id
        from backend.src.constants import SKILL_SCOPE_TOOL_PREFIX

        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        tool_meta = {
            "exec": {"type": "shell", "args": ["echo", "{input}"], "timeout_ms": 1000}
        }
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tools_items (name, description, version, created_at, updated_at, last_used_at, metadata) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    "my_echo_tool",
                    "回显输入（测试用）",
                    "0.1.0",
                    created_at,
                    created_at,
                    created_at,
                    json.dumps(tool_meta, ensure_ascii=False),
                ),
            )
            tool_id = int(cursor.lastrowid)

        fake_skill_json = json.dumps(
            {
                "name": "回显输入工具",
                "description": "用于把输入原样回显，便于调试管道。",
                "steps": ["tool_call 调用 my_echo_tool，input 填要回显的文本", "读取 tool_call.output"],
                "validation": ["output 应等于 input"],
                "failure_modes": ["工具未配置 exec", "命令执行失败"],
                "tags": ["debug", "echo"],
                "triggers": ["回显", "echo", "输出输入"],
                "aliases": ["stable"],
            },
            ensure_ascii=False,
        )

        with patch(
            "backend.src.services.skills.tool_skill_autogen.call_openai",
            return_value=(fake_skill_json, None, None),
        ), patch(
            "backend.src.services.skills.tool_skill_autogen.classify_and_publish_skill",
            return_value={"ok": True},
        ), patch(
            "backend.src.services.skills.tool_skill_autogen.resolve_default_model",
            return_value="gpt-4o-mini",
        ):
            result = autogen_tool_skill_from_call(
                tool_id=tool_id,
                tool_input="hello",
                tool_output="hello",
                task_id=1,
                run_id=1,
            )
            self.assertTrue(result.get("ok"))
            self.assertEqual(result.get("status"), "created")
            skill_id = int(result.get("skill_id"))

            # 再次调用应命中 exists（幂等）
            result2 = autogen_tool_skill_from_call(
                tool_id=tool_id,
                tool_input="hello",
                tool_output="hello",
                task_id=1,
                run_id=1,
            )
            self.assertTrue(result2.get("ok"))
            self.assertEqual(result2.get("status"), "exists")
            self.assertEqual(int(result2.get("skill_id")), skill_id)

        # DB 校验
        with get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM skills_items WHERE id = ?",
                (skill_id,),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["scope"], f"{SKILL_SCOPE_TOOL_PREFIX}{tool_id}")
        self.assertEqual(row["name"], "回显输入工具")
        self.assertIn("回显", str(row["description"] or ""))

        # find_tool_skill_id 可查到
        self.assertEqual(find_tool_skill_id(tool_id), skill_id)


if __name__ == "__main__":
    unittest.main()
