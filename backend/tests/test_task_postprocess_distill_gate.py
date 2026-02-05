import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


class TestTaskPostprocessDistillGate(unittest.TestCase):
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

    def test_postprocess_skips_distill_when_review_not_pass(self):
        from backend.src.constants import RUN_STATUS_DONE
        from backend.src.services.tasks.task_postprocess import postprocess_task_run
        from backend.src.storage import get_connection

        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("后处理门控测试", "done", created_at, None, created_at, created_at),
            )
            task_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_plan, agent_state) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "done",
                    "agent_command_react",
                    created_at,
                    created_at,
                    created_at,
                    created_at,
                    json.dumps({"titles": ["task_output 输出"], "allows": [["task_output"]], "artifacts": []}, ensure_ascii=False),
                    json.dumps({"mode": "do", "message": "后处理门控测试"}, ensure_ascii=False),
                ),
            )
            run_id = int(cursor.lastrowid)

            conn.execute(
                "INSERT INTO task_outputs (task_id, run_id, output_type, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (task_id, run_id, "text", "最终结果：ok", created_at),
            )

            task_row = conn.execute("SELECT * FROM tasks WHERE id = ?", (int(task_id),)).fetchone()

        fake_eval_json = json.dumps(
            {
                "status": "needs_changes",
                "summary": "not ok",
                "issues": [{"title": "bad", "severity": "high", "details": "x"}],
                "next_actions": [{"title": "fix", "details": "y"}],
                "skills": [],
            },
            ensure_ascii=False,
        )

        called = {"solution": False, "skills": False, "graph": False}

        def _fake_call_openai(prompt, model, parameters):
            return fake_eval_json, None, None

        def _mark_solution(*args, **kwargs):
            called["solution"] = True
            return {"ok": True, "status": "should_not_call"}

        def _mark_skills(*args, **kwargs):
            called["skills"] = True
            return {"ok": True, "status": "should_not_call"}

        def _mark_graph(*args, **kwargs):
            called["graph"] = True
            return {"nodes_created": 0, "edges_created": 0}

        with patch(
            "backend.src.services.tasks.task_postprocess.call_openai",
            side_effect=_fake_call_openai,
        ), patch(
            "backend.src.services.skills.run_solution_autogen.autogen_solution_from_run",
            side_effect=_mark_solution,
        ), patch(
            "backend.src.services.skills.run_skill_autogen.autogen_skills_from_run",
            side_effect=_mark_skills,
        ), patch(
            "backend.src.services.tasks.task_postprocess.extract_graph_updates",
            side_effect=_mark_graph,
        ):
            postprocess_task_run(task_row=task_row, task_id=int(task_id), run_id=int(run_id), run_status=RUN_STATUS_DONE)

        self.assertFalse(called["solution"])
        self.assertFalse(called["skills"])
        self.assertFalse(called["graph"])


if __name__ == "__main__":
    unittest.main()

