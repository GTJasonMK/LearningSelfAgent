import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


class TestTaskPostprocessDistillEvidenceRefsGate(unittest.TestCase):
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

    def test_postprocess_distill_allow_but_missing_evidence_refs_should_downgrade_to_manual(self):
        """
        验证：当评估输出 distill=allow 但没有可定位 evidence_refs 时，
        后处理应把 distill 降级为 manual，阻止知识沉淀（含草稿技能升级）。
        """
        from backend.src.constants import RUN_STATUS_DONE
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill
        from backend.src.services.tasks.task_postprocess import postprocess_task_run
        from backend.src.storage import get_connection

        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("distill evidence_refs 门槛测试", "done", created_at, None, created_at, created_at),
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
                    json.dumps({"mode": "do", "message": "distill evidence_refs 门槛测试"}, ensure_ascii=False),
                ),
            )
            run_id = int(cursor.lastrowid)
            task_row = conn.execute("SELECT * FROM tasks WHERE id = ?", (int(task_id),)).fetchone()

        draft_skill_id = create_skill(
            SkillCreateParams(
                name="不应被升级的草稿技能（缺证据）",
                description="draft",
                steps=["step1"],
                task_id=int(task_id),
                domain_id="misc",
                skill_type="methodology",
                status="draft",
                source_task_id=int(task_id),
                source_run_id=int(run_id),
                created_at=created_at,
            )
        )

        fake_review_json = json.dumps(
            {
                "status": "pass",
                "pass_score": 95,
                "pass_threshold": 80,
                "distill": {
                    "status": "allow",
                    "score": 95,
                    "threshold": 90,
                    "reason": "看似可沉淀，但缺少证据引用",
                    "evidence_refs": [],
                },
                "summary": "ok",
                "issues": [],
                "next_actions": [],
                "skills": [],
            },
            ensure_ascii=False,
        )

        called = {"solution": False, "skills": False, "graph": False}

        def _fake_call_openai(prompt, model, parameters):
            return fake_review_json, None, None

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
            row = conn.execute("SELECT * FROM skills_items WHERE id = ?", (int(draft_skill_id),)).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row["status"] or ""), "draft")

            review = conn.execute(
                "SELECT * FROM agent_review_records WHERE run_id = ? ORDER BY id DESC LIMIT 1",
                (int(run_id),),
            ).fetchone()
            self.assertIsNotNone(review)
            self.assertEqual(str(review["distill_status"] or ""), "manual")
            refs = json.loads(review["distill_evidence_refs"] or "[]")
            self.assertEqual(refs, [])
        self.assertIn("evidence_refs", str(review["distill_notes"] or ""))

    def test_postprocess_distill_allow_but_invalid_evidence_refs_should_downgrade_to_manual(self):
        """
        验证：distill=allow 但引用不存在的 step/output/tool_call 证据时，
        证据引用会被过滤为空，并自动降级为 manual（阻止沉淀）。
        """
        from backend.src.constants import RUN_STATUS_DONE
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill
        from backend.src.services.tasks.task_postprocess import postprocess_task_run
        from backend.src.storage import get_connection

        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("distill invalid evidence_refs 测试", "done", created_at, None, created_at, created_at),
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
                    json.dumps({"mode": "do", "message": "distill invalid evidence_refs 测试"}, ensure_ascii=False),
                ),
            )
            run_id = int(cursor.lastrowid)

            cursor = conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, created_at, updated_at, step_order) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (task_id, run_id, "输出结果", "done", created_at, created_at, 1),
            )
            step_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_outputs (task_id, run_id, output_type, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (task_id, run_id, "text", "最终结果：ok", created_at),
            )
            output_id = int(cursor.lastrowid)

            task_row = conn.execute("SELECT * FROM tasks WHERE id = ?", (int(task_id),)).fetchone()

        draft_skill_id = create_skill(
            SkillCreateParams(
                name="不应被升级的草稿技能（invalid refs）",
                description="draft",
                steps=["step1"],
                task_id=int(task_id),
                domain_id="misc",
                skill_type="methodology",
                status="draft",
                source_task_id=int(task_id),
                source_run_id=int(run_id),
                created_at=created_at,
            )
        )

        fake_review_json = json.dumps(
            {
                "status": "pass",
                "pass_score": 95,
                "pass_threshold": 80,
                "distill": {
                    "status": "allow",
                    "score": 95,
                    "threshold": 90,
                    "reason": "看似可沉淀，但证据 id 不存在",
                    "evidence_refs": [
                        {"kind": "step", "step_id": int(step_id) + 9999},
                        {"kind": "output", "output_id": int(output_id) + 9999},
                    ],
                },
                "summary": "ok",
                "issues": [],
                "next_actions": [],
                "skills": [],
            },
            ensure_ascii=False,
        )

        called = {"solution": False, "skills": False, "graph": False}

        def _fake_call_openai(prompt, model, parameters):
            return fake_review_json, None, None

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
            row = conn.execute("SELECT * FROM skills_items WHERE id = ?", (int(draft_skill_id),)).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row["status"] or ""), "draft")

            review = conn.execute(
                "SELECT * FROM agent_review_records WHERE run_id = ? ORDER BY id DESC LIMIT 1",
                (int(run_id),),
            ).fetchone()
            self.assertIsNotNone(review)
            self.assertEqual(str(review["distill_status"] or ""), "manual")
            refs = json.loads(review["distill_evidence_refs"] or "[]")
            self.assertEqual(refs, [])
            self.assertIn("evidence_refs", str(review["distill_notes"] or ""))


if __name__ == "__main__":
    unittest.main()
