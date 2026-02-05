import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


class TestReviewGateBeforeFeedback(unittest.TestCase):
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
        from backend.src.storage import get_connection
        from backend.src.constants import STATUS_RUNNING, RUN_STATUS_RUNNING

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

    def test_review_gate_inserts_steps_before_feedback_until_pass(self):
        """
        预期行为：
        - 到达“确认满意度”前，先触发 Eval Agent 检查是否完成；
        - 若评估未通过，则插入修复步骤并继续执行（不立刻进入 waiting 询问满意度）；
        - 直到评估通过，才进入 waiting 询问满意度。
        """
        from backend.src.agent.runner.react_loop import run_react_loop
        from backend.src.constants import AGENT_TASK_FEEDBACK_STEP_TITLE, RUN_STATUS_WAITING

        task_id, run_id = self._create_task_and_run()
        workdir = os.getcwd()

        plan_titles = ["输出结果", AGENT_TASK_FEEDBACK_STEP_TITLE]
        plan_items = [
            {"id": 1, "brief": "输出", "status": "pending"},
            {"id": 2, "brief": "反馈", "status": "pending"},
        ]
        plan_allows = [["task_output"], ["user_prompt"]]

        # step1（原计划）+ step2（插入的修复步骤）
        llm_actions = [
            {"action": {"type": "task_output", "payload": {"output_type": "text", "content": "draft"}}},
            {"action": {"type": "task_output", "payload": {"output_type": "text", "content": "fixed"}}},
        ]
        llm_side_effect = [
            {"record": {"status": "success", "response": json.dumps(action, ensure_ascii=False)}}
            for action in llm_actions
        ]

        # 评估门闩：第一次 needs_changes -> 触发修复；第二次 pass -> 进入 waiting 询问满意度
        ensure_review_side_effect = [1, 2]

        def _fake_get_review(review_id: int):
            if int(review_id) == 1:
                return {
                    "status": "needs_changes",
                    "summary": "未完成",
                    "issues": "[]",
                    "next_actions": "[{\"title\":\"补齐执行\",\"details\":\"需要真实执行并验证\"}]",
                }
            return {
                "status": "pass",
                "summary": "完成",
                "issues": "[]",
                "next_actions": "[]",
            }

        # 修复建议：插入 1 个 task_output 步骤（位于“确认满意度”之前）
        repair_text = json.dumps(
            {"insert_steps": [{"title": "输出结果(修复)", "brief": "补齐", "allow": ["task_output"]}]},
            ensure_ascii=False,
        )

        exec_calls = {"count": 0}

        def _fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            exec_calls["count"] += 1
            detail = json.loads(_step_row.get("detail") or "{}")
            if detail.get("type") == "task_output":
                payload = detail.get("payload") or {}
                return {"content": str(payload.get("content") or "")}, None
            if detail.get("type") == "llm_call":
                return {"response": "ok"}, None
            return {"ok": True}, None

        with patch(
            "backend.src.services.tasks.task_postprocess.ensure_agent_review_record",
            side_effect=ensure_review_side_effect,
        ) as ensure_mock, patch(
            "backend.src.repositories.agent_reviews_repo.get_agent_review",
            side_effect=_fake_get_review,
        ), patch(
            "backend.src.agent.runner.review_repair.call_llm_for_text",
            return_value=(repair_text, None),
        ) as repair_llm_mock, patch(
            "backend.src.agent.runner.react_loop.create_llm_call",
            side_effect=llm_side_effect,
        ) as react_llm_mock, patch(
            "backend.src.agent.runner.react_loop._execute_step_action",
            side_effect=_fake_execute_step_action,
        ):
            gen = run_react_loop(
                task_id=task_id,
                run_id=run_id,
                message="m",
                workdir=workdir,
                model="gpt-4o-mini",
                parameters={"temperature": 0},
                plan_titles=plan_titles,
                plan_items=plan_items,
                plan_allows=plan_allows,
                plan_artifacts=[],
                tools_hint="(无)",
                skills_hint="(无)",
                memories_hint="(无)",
                graph_hint="(无)",
                agent_state={"max_steps": 10},
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
        # 评估应至少跑两次：未通过 -> 修复；通过 -> 才进入 waiting
        self.assertEqual(ensure_mock.call_count, 2)
        self.assertEqual(repair_llm_mock.call_count, 1)
        self.assertEqual(react_llm_mock.call_count, 2)
        # 应执行 2 个 task_output：原计划输出 + 修复后输出（确认满意度不算执行 action）
        self.assertEqual(exec_calls["count"], 2)
        self.assertEqual(plan_titles, ["输出结果", "输出结果(修复)", AGENT_TASK_FEEDBACK_STEP_TITLE])


if __name__ == "__main__":
    unittest.main()

