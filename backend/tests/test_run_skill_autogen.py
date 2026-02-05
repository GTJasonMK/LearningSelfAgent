import json
import os
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timezone


class TestRunSkillAutogen(unittest.TestCase):
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

    def test_autogen_skills_from_run_creates_and_is_idempotent(self):
        from backend.src.storage import get_connection
        from backend.src.services.skills.run_skill_autogen import autogen_skills_from_run

        now = self._now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at) VALUES (?, ?, ?)",
                ("查看四川天气", "done", now),
            )
            task_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (task_id, "done", None, now, now, now, now),
            )
            run_id = int(cursor.lastrowid)
            conn.execute(
                "UPDATE task_runs SET agent_plan = ? WHERE id = ?",
                (json.dumps({"titles": ["获取天气", "总结输出"]}, ensure_ascii=False), run_id),
            )

            # 至少包含一次 tool_call，触发“可迁移动作”判断
            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "tool_call:web_fetch 抓取天气",
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
            conn.execute(
                "INSERT INTO task_outputs (task_id, run_id, output_type, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (task_id, run_id, "result", "天气：晴", now),
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
                        "name": "外部信息获取并总结（web_fetch+llm_call）",
                        "description": "当任务需要实时/外部信息时，先抓取再总结输出。",
                        "scope": "agent.workflow:web_fetch_summarize",
                        "category": "agent.workflow",
                        "tags": ["web", "workflow"],
                        "triggers": ["天气", "新闻", "价格", "最新"],
                        "aliases": [],
                        "prerequisites": ["已配置 LLM 与网络/代理"],
                        "inputs": ["用户问题", "URL 或站点", "期望输出格式"],
                        "outputs": ["简短结论", "关键字段/数据点"],
                        "steps": [
                            "先用 tool_call:web_fetch 获取页面/接口原始内容",
                            "必要时做一次 llm_call 抽取关键信息/结构化 JSON",
                            "用 task_output 输出最终结果（不要直接说无法访问）",
                        ],
                        "failure_modes": ["URL 访问失败/超时", "页面结构变化导致抽取失败"],
                        "validation": ["检查输出是否包含用户关心的关键字段", "必要时复现抓取命令验证数据来源"],
                        "version": "0.1.0",
                    }
                ]
            },
            ensure_ascii=False,
        )

        with patch(
            "backend.src.services.skills.run_skill_autogen.call_openai",
            return_value=(fake_skill_json, None, None),
        ), patch(
            "backend.src.services.skills.run_skill_autogen.publish_skill_file",
            return_value=("skills/agent.workflow/web_fetch.md", None),
        ):
            result = autogen_skills_from_run(task_id=task_id, run_id=run_id, model="gpt-4o-mini")
            self.assertTrue(result.get("ok"))
            self.assertEqual(result.get("status"), "applied")
            self.assertEqual(len(result.get("skills") or []), 1)

            # 再跑一次应更新同一个技能（不产生重复记录）
            result2 = autogen_skills_from_run(task_id=task_id, run_id=run_id, model="gpt-4o-mini")
            self.assertTrue(result2.get("ok"))

        with get_connection() as conn:
            rows = conn.execute(
                "SELECT id, name, category, steps FROM skills_items ORDER BY id ASC"
            ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["name"], "外部信息获取并总结（web_fetch+llm_call）")
        self.assertEqual(rows[0]["category"], "agent.workflow")
        steps = json.loads(rows[0]["steps"])
        self.assertTrue(any("web_fetch" in str(s) for s in steps))

    def test_autogen_skills_from_run_skips_when_no_transferable_actions(self):
        from backend.src.storage import get_connection
        from backend.src.services.skills.run_skill_autogen import autogen_skills_from_run

        now = self._now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at) VALUES (?, ?, ?)",
                ("纯聊天", "done", now),
            )
            task_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (task_id, "done", None, now, now, now, now),
            )
            run_id = int(cursor.lastrowid)
            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "llm_call 回答问题",
                    "done",
                    json.dumps({"type": "llm_call", "payload": {"prompt": "hi"}}, ensure_ascii=False),
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

        with patch("backend.src.services.skills.run_skill_autogen.call_openai") as call_mock:
            result = autogen_skills_from_run(task_id=task_id, run_id=run_id, model="gpt-4o-mini")
            self.assertTrue(result.get("ok"))
            self.assertEqual(result.get("status"), "skipped_no_transferable_actions")
            call_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
