import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


class TestTaskPostprocessUnverifiedOutputGate(unittest.TestCase):
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

    def test_postprocess_unverified_output_can_pass_but_cannot_distill(self):
        from backend.src.constants import RUN_STATUS_DONE
        from backend.src.services.tasks.task_postprocess import postprocess_task_run
        from backend.src.storage import get_connection

        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("unverified output gate", "done", created_at, None, created_at, created_at),
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
                    json.dumps({"titles": ["task_output:输出结果"], "allows": [["task_output"]], "artifacts": []}, ensure_ascii=False),
                    json.dumps({"mode": "do", "message": "生成结果"}, ensure_ascii=False),
                ),
            )
            run_id = int(cursor.lastrowid)

            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, created_at, updated_at, step_order) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "task_output:输出结果",
                    "done",
                    json.dumps({"type": "task_output", "payload": {"output_type": "text", "content": "hello"}}, ensure_ascii=False),
                    json.dumps({"output_type": "text"}, ensure_ascii=False),
                    created_at,
                    created_at,
                    1,
                ),
            )
            cursor = conn.execute(
                "INSERT INTO task_outputs (task_id, run_id, output_type, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "text",
                    "【未验证草稿】仅示例输出\n\n[证据引用]\n- 无（当前仅为未验证草稿，需补齐 step/tool/artifact 证据）",
                    created_at,
                ),
            )
            output_id = int(cursor.lastrowid)

            task_row = conn.execute("SELECT * FROM tasks WHERE id = ?", (int(task_id),)).fetchone()

        fake_review_json = json.dumps(
            {
                "status": "pass",
                "pass_score": 95,
                "pass_threshold": 80,
                "distill": {
                    "status": "allow",
                    "score": 95,
                    "threshold": 90,
                    "reason": "看起来可以沉淀",
                    "evidence_refs": [{"kind": "output", "output_id": int(output_id)}],
                },
                "summary": "任务完成",
                "issues": [],
                "next_actions": [],
                "skills": [],
            },
            ensure_ascii=False,
        )

        called = {"solution": False, "skills": False, "graph": False}

        def _fake_call_openai(_prompt, _model, _parameters):
            return fake_review_json, None, None

        def _mark_solution(*_args, **_kwargs):
            called["solution"] = True
            return {"ok": True}

        def _mark_skills(*_args, **_kwargs):
            called["skills"] = True
            return {"ok": True}

        def _mark_graph(*_args, **_kwargs):
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
            postprocess_task_run(
                task_row=task_row,
                task_id=int(task_id),
                run_id=int(run_id),
                run_status=RUN_STATUS_DONE,
            )

        self.assertFalse(called["solution"])
        self.assertFalse(called["skills"])
        self.assertFalse(called["graph"])

        with get_connection() as conn:
            review = conn.execute(
                "SELECT * FROM agent_review_records WHERE run_id = ? ORDER BY id DESC LIMIT 1",
                (int(run_id),),
            ).fetchone()

        self.assertIsNotNone(review)
        self.assertEqual(str(review["status"] or ""), "pass")
        self.assertIn("缺少可验证证据", str(review["summary"] or ""))
        self.assertNotEqual(str(review["distill_status"] or ""), "allow")


if __name__ == "__main__":
    unittest.main()
