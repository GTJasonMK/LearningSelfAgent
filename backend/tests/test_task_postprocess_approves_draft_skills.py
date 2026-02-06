import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


class TestTaskPostprocessApprovesDraftSkills(unittest.TestCase):
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

    def test_postprocess_pass_promotes_draft_skill_and_publishes_file(self):
        from backend.src.constants import RUN_STATUS_DONE
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill
        from backend.src.services.tasks.task_postprocess import postprocess_task_run
        from backend.src.storage import get_connection

        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("草稿技能升级测试", "done", created_at, None, created_at, created_at),
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
                    json.dumps({"titles": [], "allows": [], "artifacts": []}, ensure_ascii=False),
                    json.dumps({"mode": "do", "message": "草稿技能升级测试"}, ensure_ascii=False),
                ),
            )
            run_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_outputs (task_id, run_id, output_type, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (task_id, run_id, "text", "最终结果：ok", created_at),
            )
            output_id = int(cursor.lastrowid)
            task_row = conn.execute("SELECT * FROM tasks WHERE id = ?", (int(task_id),)).fetchone()

        draft_skill_id = create_skill(
            SkillCreateParams(
                name="测试草稿技能",
                description="draft",
                steps=["step1", "step2"],
                task_id=int(task_id),
                domain_id="misc",
                skill_type="methodology",
                status="draft",
                source_task_id=int(task_id),
                source_run_id=int(run_id),
                created_at=created_at,
            )
        )

        fake_eval_json = json.dumps(
            {
                "status": "pass",
                "summary": "ok",
                "distill": {
                    "status": "allow",
                    "score": 95,
                    "threshold": 90,
                    "reason": "可沉淀",
                    "evidence_refs": [{"kind": "output", "output_id": int(output_id)}],
                },
                "issues": [],
                "next_actions": [],
                "skills": [],
            },
            ensure_ascii=False,
        )

        def _fake_call_openai(prompt, model, parameters):
            return fake_eval_json, None, None

        with patch(
            "backend.src.services.tasks.task_postprocess.call_openai",
            side_effect=_fake_call_openai,
        ), patch(
            "backend.src.services.skills.run_solution_autogen.autogen_solution_from_run",
            return_value={"ok": True, "status": "skipped_test"},
        ), patch(
            "backend.src.services.skills.run_skill_autogen.autogen_skills_from_run",
            return_value={"ok": True, "status": "skipped_test"},
        ), patch(
            "backend.src.services.tasks.task_postprocess.extract_graph_updates",
            return_value={"nodes_created": 0, "edges_created": 0},
        ):
            postprocess_task_run(task_row=task_row, task_id=int(task_id), run_id=int(run_id), run_status=RUN_STATUS_DONE)

        with get_connection() as conn:
            row = conn.execute("SELECT * FROM skills_items WHERE id = ?", (int(draft_skill_id),)).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(str(row["status"] or ""), "approved")
        self.assertTrue(str(row["source_path"] or "").strip())

        source_path = str(row["source_path"] or "").strip()
        target = (Path(os.environ["AGENT_PROMPT_ROOT"]) / "skills" / Path(source_path)).resolve()
        self.assertTrue(target.exists())


if __name__ == "__main__":
    unittest.main()
