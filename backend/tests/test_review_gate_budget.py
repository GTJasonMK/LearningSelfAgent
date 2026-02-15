import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


class TestReviewGateBudget(unittest.TestCase):
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

    def _create_task_and_run(self):
        from backend.src.constants import RUN_STATUS_RUNNING, STATUS_RUNNING
        from backend.src.storage import get_connection

        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("test", STATUS_RUNNING, created_at, None, created_at, None),
            )
            task_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (task_id, RUN_STATUS_RUNNING, "agent_command_react", created_at, None, created_at, created_at),
            )
            run_id = int(cursor.lastrowid)
        return task_id, run_id

    def test_review_gate_budget_exhausted_fallback_to_feedback(self):
        from backend.src.agent.core.plan_structure import PlanStructure
        from backend.src.agent.runner.react_loop import run_react_loop
        from backend.src.constants import AGENT_REACT_REPLAN_MAX_ATTEMPTS, AGENT_TASK_FEEDBACK_STEP_TITLE, RUN_STATUS_WAITING

        task_id, run_id = self._create_task_and_run()

        plan_titles = ["输出结果", AGENT_TASK_FEEDBACK_STEP_TITLE]
        plan_items = [
            {"id": 1, "brief": "输出", "status": "pending"},
            {"id": 2, "brief": "反馈", "status": "pending"},
        ]
        plan_allows = [["task_output"], ["user_prompt"]]

        plan_struct = PlanStructure.from_legacy(
            plan_titles=list(plan_titles),
            plan_items=list(plan_items),
            plan_allows=[list(a) for a in plan_allows],
            plan_artifacts=[],
        )

        llm_action = {
            "action": {
                "type": "task_output",
                "payload": {"output_type": "text", "content": "draft"},
            }
        }

        def _fake_get_review(review_id: int):
            _ = review_id
            return {
                "status": "needs_changes",
                "summary": "仍需修复",
                "issues": "[]",
                "next_actions": "[{\"title\":\"继续修复\"}]",
            }

        def _fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            detail = json.loads(_step_row.get("detail") or "{}")
            if detail.get("type") == "task_output":
                payload = detail.get("payload") or {}
                return {"content": str(payload.get("content") or "")}, None
            if detail.get("type") == "llm_call":
                return {"response": "ok"}, None
            return {"ok": True}, None

        agent_state = {"max_steps": 10, "review_gate_attempts": int(AGENT_REACT_REPLAN_MAX_ATTEMPTS)}

        with patch(
            "backend.src.services.tasks.task_postprocess.ensure_agent_review_record",
            return_value=31,
        ) as ensure_mock, patch(
            "backend.src.repositories.agent_reviews_repo.get_agent_review",
            side_effect=_fake_get_review,
        ), patch(
            "backend.src.agent.runner.review_repair.call_llm_for_text",
            return_value=("{}", None),
        ) as repair_llm_mock, patch(
            "backend.src.agent.runner.react_loop.create_llm_call",
            return_value={"record": {"status": "success", "response": json.dumps(llm_action, ensure_ascii=False)}},
        ), patch(
            "backend.src.agent.runner.react_loop._execute_step_action",
            side_effect=_fake_execute_step_action,
        ):
            gen = run_react_loop(
                task_id=task_id,
                run_id=run_id,
                message="m",
                workdir=os.getcwd(),
                model="gpt-4o-mini",
                parameters={"temperature": 0},
                plan_struct=plan_struct,
                tools_hint="(无)",
                skills_hint="(无)",
                memories_hint="(无)",
                graph_hint="(无)",
                agent_state=agent_state,
                context={"last_llm_response": None},
                observations=[],
                start_step_order=1,
                variables_source="test",
            )
            try:
                while True:
                    next(gen)
            except StopIteration as exc:
                result = exc.value

        self.assertEqual(result.run_status, RUN_STATUS_WAITING)
        self.assertEqual(ensure_mock.call_count, 1)
        self.assertEqual(repair_llm_mock.call_count, 0)

    def test_review_gate_records_attempt_after_repair(self):
        from backend.src.agent.core.plan_structure import PlanStructure
        from backend.src.agent.runner.react_loop import run_react_loop
        from backend.src.constants import AGENT_REACT_REPLAN_MAX_ATTEMPTS, AGENT_TASK_FEEDBACK_STEP_TITLE, RUN_STATUS_WAITING

        task_id, run_id = self._create_task_and_run()

        plan_titles = ["输出结果", AGENT_TASK_FEEDBACK_STEP_TITLE]
        plan_items = [
            {"id": 1, "brief": "输出", "status": "pending"},
            {"id": 2, "brief": "反馈", "status": "pending"},
        ]
        plan_allows = [["task_output"], ["user_prompt"]]

        plan_struct = PlanStructure.from_legacy(
            plan_titles=list(plan_titles),
            plan_items=list(plan_items),
            plan_allows=[list(a) for a in plan_allows],
            plan_artifacts=[],
        )

        llm_action = {
            "action": {
                "type": "task_output",
                "payload": {"output_type": "text", "content": "draft"},
            }
        }

        def _fake_get_review(review_id: int):
            _ = review_id
            return {
                "status": "needs_changes",
                "summary": "仍需修复",
                "issues": "[]",
                "next_actions": "[{\"title\":\"继续修复\"}]",
            }

        repair_text = json.dumps(
            {
                "decision": "repair",
                "reasons": ["需要补齐证据"],
                "evidence": ["评估未通过"],
                "insert_steps": [
                    {"title": "输出结果(修复)", "brief": "补齐", "allow": ["task_output"]}
                ],
            },
            ensure_ascii=False,
        )

        def _fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            detail = json.loads(_step_row.get("detail") or "{}")
            if detail.get("type") == "task_output":
                payload = detail.get("payload") or {}
                return {"content": str(payload.get("content") or "")}, None
            if detail.get("type") == "llm_call":
                return {"response": "ok"}, None
            return {"ok": True}, None

        agent_state = {"max_steps": 10, "review_gate_attempts": 0}

        with patch(
            "backend.src.services.tasks.task_postprocess.ensure_agent_review_record",
            side_effect=[41, 42, 43],
        ) as ensure_mock, patch(
            "backend.src.repositories.agent_reviews_repo.get_agent_review",
            side_effect=_fake_get_review,
        ), patch(
            "backend.src.agent.runner.review_repair.call_llm_for_text",
            return_value=(repair_text, None),
        ) as repair_llm_mock, patch(
            "backend.src.agent.runner.react_loop.create_llm_call",
            return_value={"record": {"status": "success", "response": json.dumps(llm_action, ensure_ascii=False)}},
        ) as react_llm_mock, patch(
            "backend.src.agent.runner.react_loop._execute_step_action",
            side_effect=_fake_execute_step_action,
        ):
            gen = run_react_loop(
                task_id=task_id,
                run_id=run_id,
                message="m",
                workdir=os.getcwd(),
                model="gpt-4o-mini",
                parameters={"temperature": 0},
                plan_struct=plan_struct,
                tools_hint="(无)",
                skills_hint="(无)",
                memories_hint="(无)",
                graph_hint="(无)",
                agent_state=agent_state,
                context={"last_llm_response": None},
                observations=[],
                start_step_order=1,
                variables_source="test",
            )
            try:
                while True:
                    next(gen)
            except StopIteration as exc:
                result = exc.value

        self.assertEqual(result.run_status, RUN_STATUS_WAITING)
        self.assertEqual(ensure_mock.call_count, int(AGENT_REACT_REPLAN_MAX_ATTEMPTS) + 1)
        self.assertEqual(repair_llm_mock.call_count, int(AGENT_REACT_REPLAN_MAX_ATTEMPTS))
        self.assertEqual(react_llm_mock.call_count, int(AGENT_REACT_REPLAN_MAX_ATTEMPTS))
        result_titles = [s.title for s in result.plan_struct.steps]
        self.assertEqual(result_titles[-1], AGENT_TASK_FEEDBACK_STEP_TITLE)
        self.assertEqual(int(agent_state.get("review_gate_attempts") or 0), int(AGENT_REACT_REPLAN_MAX_ATTEMPTS))


if __name__ == "__main__":
    unittest.main()
