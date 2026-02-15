import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


class TestReactLoopPlanSafety(unittest.TestCase):
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
                (
                    task_id,
                    RUN_STATUS_RUNNING,
                    "agent_command_react",
                    created_at,
                    None,
                    created_at,
                    created_at,
                ),
            )
            run_id = int(cursor.lastrowid)
        return task_id, run_id

    def test_pads_missing_plan_items(self):
        """plan_items 为空/缺失时，不应触发 IndexError，应自动补齐并完成执行。"""
        from backend.src.agent.core.plan_structure import PlanStructure
        from backend.src.agent.runner.react_loop import run_react_loop
        from backend.src.constants import RUN_STATUS_DONE

        task_id, run_id = self._create_task_and_run()

        plan_titles = ["输出结果"]
        plan_items = []  # 故意传空：验证 PlanStructure.from_legacy 安全垫片逻辑
        plan_allows = [["task_output"]]

        fake_action = {
            "action": {
                "type": "task_output",
                "payload": {"output_type": "text", "content": "ok"},
            }
        }

        plan_struct = PlanStructure.from_legacy(
            plan_titles=list(plan_titles),
            plan_items=list(plan_items),
            plan_allows=[list(a) for a in plan_allows],
            plan_artifacts=[],
        )

        with patch(
            "backend.src.agent.runner.react_loop.create_llm_call",
            return_value={"record": {"status": "success", "response": json.dumps(fake_action, ensure_ascii=False)}},
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
                agent_state={},
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

        self.assertEqual(result.run_status, RUN_STATUS_DONE)

    def test_empty_plan_fails_gracefully(self):
        """空计划应直接失败并返回明确结果（不应越界崩溃）。"""
        from backend.src.agent.core.plan_structure import PlanStructure
        from backend.src.agent.runner.react_loop import run_react_loop
        from backend.src.constants import RUN_STATUS_FAILED

        task_id, run_id = self._create_task_and_run()

        plan_struct = PlanStructure.from_legacy(
            plan_titles=[],
            plan_items=[],
            plan_allows=[],
            plan_artifacts=[],
        )

        # 若错误地调用了 LLM，这里会直接让测试失败
        with patch("backend.src.agent.runner.react_loop.create_llm_call", side_effect=AssertionError("should not call llm")):
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
                agent_state={},
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

        self.assertEqual(result.run_status, RUN_STATUS_FAILED)
        self.assertEqual(int(result.last_step_order), 0)


if __name__ == "__main__":
    unittest.main()

