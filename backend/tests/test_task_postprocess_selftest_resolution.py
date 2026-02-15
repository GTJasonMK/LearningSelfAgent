import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


class TestTaskPostprocessSelftestResolution(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp.name) / "agent_test.db"
        os.environ["AGENT_DB_PATH"] = str(db_path)
        os.environ["AGENT_PROMPT_ROOT"] = str(Path(self._tmp.name) / "prompt")

        import backend.src.storage as storage

        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
            self._tmp.cleanup()
        except Exception:
            pass

    def test_failed_selftest_is_not_auto_fail_when_later_tool_call_succeeds(self):
        """
        回归：历史上出现过一次“工具自测失败”不应直接硬判 needs_changes。

        典型流程是：先自测失败 -> 修复/重试 -> 自测通过/工具调用成功。
        评估应进入 LLM 审查阶段，而不是被 auto_fail 一票否决。
        """
        from backend.src.services.tasks.task_postprocess import ensure_agent_review_record
        from backend.src.storage import get_connection

        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("自测失败已修复", "done", created_at, None, created_at, created_at),
            )
            task_id = int(cursor.lastrowid)

            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_plan, agent_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "done",
                    "agent_command_react",
                    created_at,
                    created_at,
                    created_at,
                    created_at,
                    json.dumps({"titles": ["x"], "allows": [["tool_call"]], "artifacts": []}, ensure_ascii=False),
                    json.dumps({"mode": "do", "workdir": os.getcwd()}, ensure_ascii=False),
                ),
            )
            run_id = int(cursor.lastrowid)

            # 先失败的自测
            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "tool_call:web_fetch 自测工具",
                    "failed",
                    json.dumps(
                        {"type": "tool_call", "payload": {"tool_name": "web_fetch", "input": "https://example.com", "output": ""}},
                        ensure_ascii=False,
                    ),
                    None,
                    "工具执行失败: 56",
                    1,
                    created_at,
                    created_at,
                    1,
                    created_at,
                    created_at,
                ),
            )

            # 后续成功的 tool_call（可视为修复后的自测/调用）
            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "tool_call:web_fetch 自测工具",
                    "done",
                    json.dumps(
                        {"type": "tool_call", "payload": {"tool_name": "web_fetch", "input": "https://example.com", "output": ""}},
                        ensure_ascii=False,
                    ),
                    json.dumps({"output": "ok"}, ensure_ascii=False),
                    None,
                    2,
                    created_at,
                    created_at,
                    2,
                    created_at,
                    created_at,
                ),
            )

            conn.execute(
                "INSERT INTO task_outputs (task_id, run_id, output_type, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (task_id, run_id, "text", "done", created_at),
            )

        fake_eval_json = json.dumps(
            {
                "status": "pass",
                "summary": "ok",
                "issues": [],
                "next_actions": [],
                "skills": [],
                "pass_score": 95,
                "distill_status": "deny",
                "distill_score": 50,
                "distill_notes": "not needed",
                "distill_evidence_refs": [],
            },
            ensure_ascii=False,
        )

        called = {"count": 0}

        def _fake_call_openai(prompt, model, parameters):
            called["count"] += 1
            _ = prompt, model, parameters
            return fake_eval_json, None, None

        with patch(
            "backend.src.services.tasks.task_postprocess.call_openai",
            side_effect=_fake_call_openai,
        ):
            review_id = ensure_agent_review_record(task_id=task_id, run_id=run_id, skills=[], force=True)

        self.assertIsNotNone(review_id)
        self.assertGreaterEqual(called["count"], 1)


if __name__ == "__main__":
    unittest.main()

