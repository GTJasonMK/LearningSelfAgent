import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


class TestReactLoopPlanPatchInsertStepsExecutes(unittest.TestCase):
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

    def test_insert_steps_expands_plan_and_executes_shifted_steps(self):
        """
        回归测试：
        - 旧实现用 for range(len(plan_titles))，执行中插入步骤会导致“后移的原计划步骤”永远不会被执行；
        - 新实现用 while 动态读取 plan 长度，必须能把后移步骤也跑完。
        """
        from backend.src.agent.runner.react_loop import run_react_loop
        from backend.src.constants import RUN_STATUS_DONE

        task_id, run_id = self._create_task_and_run()
        workdir = os.getcwd()

        plan_titles = ["步骤1", "步骤2"]
        plan_items = [
            {"id": 1, "brief": "一", "status": "pending"},
            {"id": 2, "brief": "二", "status": "pending"},
        ]
        plan_allows = [["llm_call"], ["task_output"]]

        llm_actions = [
            # step1：执行 llm_call，同时在下一步位置插入一个新的 llm_call 步骤（原“步骤2”后移）
            {
                "action": {"type": "llm_call", "payload": {"prompt": "hi"}},
                "plan_patch": {
                    "step_index": 2,
                    "insert_steps": [
                        {"title": "llm_call 中间总结", "brief": "总结", "allow": ["llm_call"]},
                    ],
                },
            },
            # 插入的新 step2
            {"action": {"type": "llm_call", "payload": {"prompt": "mid"}}},
            # 原“步骤2”被后移成 step3
            {"action": {"type": "task_output", "payload": {"output_type": "text", "content": "final"}}},
        ]

        llm_side_effect = [
            {"record": {"status": "success", "response": json.dumps(action, ensure_ascii=False)}}
            for action in llm_actions
        ]

        exec_calls = {"count": 0}

        def _fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            exec_calls["count"] += 1
            # llm_call/task_output 的执行结果只要是 dict 即可
            detail = json.loads(_step_row.get("detail") or "{}")
            if detail.get("type") == "task_output":
                return {"content": "final"}, None
            if detail.get("type") == "llm_call":
                return {"response": "ok"}, None
            return {"ok": True}, None

        with patch(
            "backend.src.agent.runner.react_loop.create_llm_call",
            side_effect=llm_side_effect,
        ), patch(
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

        self.assertEqual(result.run_status, RUN_STATUS_DONE)
        # 应执行 3 步：step1 + 插入的新 step2 + 后移后的原 step2（step3）
        self.assertEqual(exec_calls["count"], 3)


if __name__ == "__main__":
    unittest.main()

