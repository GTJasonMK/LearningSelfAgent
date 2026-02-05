import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


class TestTaskPostprocessThinkEvaluatorModel(unittest.TestCase):
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

    def test_ensure_agent_review_record_think_mode_uses_evaluator_model(self):
        from backend.src.services.tasks.task_postprocess import ensure_agent_review_record
        from backend.src.storage import get_connection

        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("think 评估模型测试", "done", created_at, None, created_at, created_at),
            )
            task_id = int(cursor.lastrowid)

            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_plan, agent_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "done",
                    "agent_command_think",
                    created_at,
                    created_at,
                    created_at,
                    created_at,
                    json.dumps({"titles": ["task_output 输出"], "allows": [["task_output"]], "artifacts": []}, ensure_ascii=False),
                    json.dumps(
                        {
                            "mode": "think",
                            "model": "base-model",
                            "think_config": {"agents": {"evaluator": "eval-model"}},
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
            run_id = int(cursor.lastrowid)

            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "输出结果",
                    "done",
                    json.dumps({"type": "task_output", "payload": {"output_type": "text", "content": "hi"}}, ensure_ascii=False),
                    json.dumps({"content": "hi"}, ensure_ascii=False),
                    None,
                    1,
                    created_at,
                    created_at,
                    1,
                    created_at,
                    created_at,
                ),
            )
            conn.execute(
                "INSERT INTO task_outputs (task_id, run_id, output_type, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (task_id, run_id, "text", "hi", created_at),
            )

        fake_eval_json = json.dumps(
            {
                "status": "pass",
                "summary": "ok",
                "issues": [],
                "next_actions": [],
                "skills": [],
            },
            ensure_ascii=False,
        )

        called = {"model": None, "prompt": None}

        def _fake_call_openai(prompt, model, parameters):
            called["model"] = model
            called["prompt"] = prompt
            return fake_eval_json, None, None

        with patch(
            "backend.src.services.tasks.task_postprocess.call_openai",
            side_effect=_fake_call_openai,
        ):
            review_id = ensure_agent_review_record(task_id=task_id, run_id=run_id, skills=[], force=True)

        self.assertIsNotNone(review_id)
        self.assertEqual(called["model"], "eval-model")
        self.assertIn('"mode": "think"', str(called["prompt"]))

    def test_ensure_agent_review_record_think_mode_defaults_to_base_model_when_evaluator_missing(self):
        """
        验证：think_config 未配置 evaluator 时，评估默认使用 run 的 base model（state.model）。
        """
        from backend.src.services.tasks.task_postprocess import ensure_agent_review_record
        from backend.src.storage import get_connection

        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("think 评估模型兜底测试", "done", created_at, None, created_at, created_at),
            )
            task_id = int(cursor.lastrowid)

            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_plan, agent_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "done",
                    "agent_command_think",
                    created_at,
                    created_at,
                    created_at,
                    created_at,
                    json.dumps({"titles": ["task_output 输出"], "allows": [["task_output"]], "artifacts": []}, ensure_ascii=False),
                    json.dumps(
                        {
                            "mode": "think",
                            "model": "base-model",
                            "think_config": {"agents": {"planner_a": "planner-model"}},
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
            run_id = int(cursor.lastrowid)

            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "输出结果",
                    "done",
                    json.dumps({"type": "task_output", "payload": {"output_type": "text", "content": "hi"}}, ensure_ascii=False),
                    json.dumps({"content": "hi"}, ensure_ascii=False),
                    None,
                    1,
                    created_at,
                    created_at,
                    1,
                    created_at,
                    created_at,
                ),
            )
            conn.execute(
                "INSERT INTO task_outputs (task_id, run_id, output_type, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (task_id, run_id, "text", "hi", created_at),
            )

        fake_eval_json = json.dumps(
            {
                "status": "pass",
                "summary": "ok",
                "issues": [],
                "next_actions": [],
                "skills": [],
            },
            ensure_ascii=False,
        )

        called = {"model": None}

        def _fake_call_openai(prompt, model, parameters):
            called["model"] = model
            return fake_eval_json, None, None

        with patch(
            "backend.src.services.tasks.task_postprocess.call_openai",
            side_effect=_fake_call_openai,
        ):
            review_id = ensure_agent_review_record(task_id=task_id, run_id=run_id, skills=[], force=True)

        self.assertIsNotNone(review_id)
        self.assertEqual(called["model"], "base-model")


if __name__ == "__main__":
    unittest.main()
