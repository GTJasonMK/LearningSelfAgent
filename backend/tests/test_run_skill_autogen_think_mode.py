import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch


class TestRunSkillAutogenThinkMode(unittest.TestCase):
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

    def _now_iso(self):
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def test_autogen_skills_from_run_think_mode_uses_evaluator_model_and_allows_3(self):
        from backend.src.storage import get_connection
        from backend.src.services.skills.run_skill_autogen import autogen_skills_from_run

        now = self._now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at) VALUES (?, ?, ?)",
                ("think 技能沉淀测试", "done", now),
            )
            task_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_state, agent_plan) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "done",
                    "agent_command_think",
                    now,
                    now,
                    now,
                    now,
                    json.dumps(
                        {
                            "mode": "think",
                            "model": "base-model",
                            "think_config": {"agents": {"evaluator": "eval-model"}},
                        },
                        ensure_ascii=False,
                    ),
                    json.dumps({"titles": ["tool_call:web_fetch 抓取"], "artifacts": []}, ensure_ascii=False),
                ),
            )
            run_id = int(cursor.lastrowid)

            # 至少包含一次 tool_call，触发“可迁移动作”判断
            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "tool_call:web_fetch 抓取页面",
                    "done",
                    json.dumps({"type": "tool_call", "payload": {"tool_name": "web_fetch", "input": "https://example.com"}}, ensure_ascii=False),
                    None,
                    None,
                    1,
                    now,
                    now,
                    1,
                    now,
                    now,
                ),
            )

            tool_row = conn.execute(
                "SELECT id FROM tools_items WHERE name = ? ORDER BY id ASC LIMIT 1",
                ("web_fetch",),
            ).fetchone()
            self.assertIsNotNone(tool_row)
            tool_id = int(tool_row["id"])
            conn.execute(
                "INSERT INTO tool_call_records (tool_id, task_id, skill_id, run_id, reuse, input, output, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (tool_id, task_id, None, run_id, 0, "https://example.com", "<html/>", now),
            )

        fake_skill_json = json.dumps(
            {
                "skills": [
                    {
                        "mode": "create",
                        "name": "Skill A",
                        "description": "A",
                        "scope": "",
                        "category": "agent.workflow",
                        "tags": [],
                        "triggers": [],
                        "aliases": [],
                        "prerequisites": [],
                        "inputs": [],
                        "outputs": [],
                        "steps": ["step-a1", "step-a2"],
                        "failure_modes": [],
                        "validation": [],
                        "version": "0.1.0",
                    },
                    {
                        "mode": "create",
                        "name": "Skill B",
                        "description": "B",
                        "scope": "",
                        "category": "agent.workflow",
                        "tags": [],
                        "triggers": [],
                        "aliases": [],
                        "prerequisites": [],
                        "inputs": [],
                        "outputs": [],
                        "steps": ["step-b1"],
                        "failure_modes": [],
                        "validation": [],
                        "version": "0.1.0",
                    },
                    {
                        "mode": "create",
                        "name": "Skill C",
                        "description": "C",
                        "scope": "",
                        "category": "agent.workflow",
                        "tags": [],
                        "triggers": [],
                        "aliases": [],
                        "prerequisites": [],
                        "inputs": [],
                        "outputs": [],
                        "steps": ["step-c1"],
                        "failure_modes": [],
                        "validation": [],
                        "version": "0.1.0",
                    },
                ]
            },
            ensure_ascii=False,
        )

        called = {"model": None, "prompt": None}

        def _fake_call_openai(prompt, model, parameters):
            called["model"] = model
            called["prompt"] = prompt
            return fake_skill_json, None, None

        with patch(
            "backend.src.services.skills.run_skill_autogen.call_openai",
            side_effect=_fake_call_openai,
        ), patch(
            "backend.src.services.skills.run_skill_autogen.publish_skill_file",
            return_value=("skills/agent.workflow/skill.md", None),
        ):
            result = autogen_skills_from_run(task_id=task_id, run_id=run_id, model="base-model")

        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("status"), "applied")
        self.assertEqual(result.get("model"), "eval-model")
        self.assertEqual(called["model"], "eval-model")
        self.assertIn("最多输出 3", str(called["prompt"] or ""))
        self.assertEqual(len(result.get("skills") or []), 3)

        from backend.src.storage import get_connection as get_conn2

        with get_conn2() as conn:
            count = conn.execute("SELECT COUNT(*) AS c FROM skills_items").fetchone()["c"]
        self.assertEqual(int(count), 3)

    def test_autogen_skills_from_run_think_mode_defaults_to_base_model_when_evaluator_missing(self):
        """
        验证：think_config 未配置 evaluator 时，技能沉淀默认使用 run 的 base model（state.model）。
        """
        from backend.src.storage import get_connection
        from backend.src.services.skills.run_skill_autogen import autogen_skills_from_run

        now = self._now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at) VALUES (?, ?, ?)",
                ("think 技能沉淀兜底测试", "done", now),
            )
            task_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_state, agent_plan) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "done",
                    "agent_command_think",
                    now,
                    now,
                    now,
                    now,
                    json.dumps(
                        {
                            "mode": "think",
                            "model": "base-model",
                            "think_config": {"agents": {"planner_a": "planner-model"}},
                        },
                        ensure_ascii=False,
                    ),
                    json.dumps({"titles": ["tool_call:web_fetch 抓取"], "artifacts": []}, ensure_ascii=False),
                ),
            )
            run_id = int(cursor.lastrowid)

            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "tool_call:web_fetch 抓取页面",
                    "done",
                    json.dumps({"type": "tool_call", "payload": {"tool_name": "web_fetch", "input": "https://example.com"}}, ensure_ascii=False),
                    None,
                    None,
                    1,
                    now,
                    now,
                    1,
                    now,
                    now,
                ),
            )

            tool_row = conn.execute(
                "SELECT id FROM tools_items WHERE name = ? ORDER BY id ASC LIMIT 1",
                ("web_fetch",),
            ).fetchone()
            self.assertIsNotNone(tool_row)
            tool_id = int(tool_row["id"])
            conn.execute(
                "INSERT INTO tool_call_records (tool_id, task_id, skill_id, run_id, reuse, input, output, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (tool_id, task_id, None, run_id, 0, "https://example.com", "<html/>", now),
            )

        fake_skill_json = json.dumps(
            {
                "skills": [
                    {
                        "mode": "create",
                        "name": "Skill A",
                        "description": "A",
                        "scope": "",
                        "category": "agent.workflow",
                        "tags": [],
                        "triggers": [],
                        "aliases": [],
                        "prerequisites": [],
                        "inputs": [],
                        "outputs": [],
                        "steps": ["step-a1"],
                        "failure_modes": [],
                        "validation": [],
                        "version": "0.1.0",
                    }
                ]
            },
            ensure_ascii=False,
        )

        called = {"model": None}

        def _fake_call_openai(prompt, model, parameters):
            called["model"] = model
            return fake_skill_json, None, None

        with patch(
            "backend.src.services.skills.run_skill_autogen.call_openai",
            side_effect=_fake_call_openai,
        ), patch(
            "backend.src.services.skills.run_skill_autogen.publish_skill_file",
            return_value=("skills/agent.workflow/skill.md", None),
        ):
            result = autogen_skills_from_run(task_id=task_id, run_id=run_id, model="base-model")

        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("model"), "base-model")
        self.assertEqual(called["model"], "base-model")


if __name__ == "__main__":
    unittest.main()
