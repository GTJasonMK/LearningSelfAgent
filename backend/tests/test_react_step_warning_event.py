import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


class TestReactStepWarningEvent(unittest.TestCase):
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

    def test_run_react_loop_emits_step_warning_for_tool_call_warnings(self):
        from backend.src.agent.core.plan_structure import PlanStructure
        from backend.src.agent.runner.react_loop import run_react_loop
        from backend.src.constants import RUN_STATUS_DONE

        task_id, run_id = self._create_task_and_run()
        workdir = self._tmp.name
        plan_struct = PlanStructure.from_legacy(
            plan_titles=["tool_call:web_fetch 抓取数据"],
            plan_items=[{"id": 1, "brief": "抓取", "status": "pending"}],
            plan_allows=[["tool_call"]],
            plan_artifacts=[],
        )

        llm_response = {
            "record": {
                "status": "success",
                "response": json.dumps(
                    {
                        "action": {
                            "type": "tool_call",
                            "payload": {
                                "tool_name": "web_fetch",
                                "input": "gold price",
                                "output": "",
                            },
                        }
                    },
                    ensure_ascii=False,
                ),
            }
        }

        def _fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            return (
                {
                    "tool_id": 1,
                    "tool_name": "web_fetch",
                    "input": "gold price",
                    "output": '{"ok":true}',
                    "warnings": ["web_fetch 已自动切换到备用源"],
                    "attempts": [
                        {"host": "bad.example", "status": "failed", "error_code": "http_403", "reason": "forbidden"},
                        {"host": "good.example", "status": "ok", "error_code": "", "reason": ""},
                    ],
                    "protocol": {"source": "fallback"},
                    "result_contract": {"version": 1, "action_type": "tool_call", "required_result": True, "status": "warn"},
                },
                None,
            )

        chunks = []
        with patch(
            "backend.src.agent.runner.react_loop.create_llm_call",
            side_effect=[llm_response],
        ), patch(
            "backend.src.agent.runner.react_loop._execute_step_action",
            side_effect=_fake_execute_step_action,
        ):
            gen = run_react_loop(
                task_id=task_id,
                run_id=run_id,
                message="m",
                workdir=workdir,
                model="gpt-5.2",
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
                    chunks.append(next(gen))
            except StopIteration as exc:
                result = exc.value

        self.assertEqual(result.run_status, RUN_STATUS_DONE)
        payloads = []
        for chunk in chunks:
            if not isinstance(chunk, str):
                continue
            for line in chunk.splitlines():
                if not line.startswith("data: "):
                    continue
                try:
                    payloads.append(json.loads(line[len("data: ") :]))
                except Exception:
                    pass
        warnings = [item for item in payloads if str(item.get("type") or "") == "step_warning"]
        self.assertEqual(len(warnings), 1)
        payload = warnings[0]
        self.assertEqual(int(payload.get("step_order") or 0), 1)
        self.assertEqual(str(payload.get("action_type") or ""), "tool_call")
        self.assertEqual(str(payload.get("primary_warning") or ""), "web_fetch 已自动切换到备用源")
        self.assertTrue(bool(payload.get("fallback_used")))
        self.assertEqual(int(payload.get("failed_attempt_count") or 0), 1)
        self.assertEqual(int(payload.get("successful_attempt_count") or 0), 1)
        self.assertEqual(str(payload.get("protocol_source") or ""), "fallback")


if __name__ == "__main__":
    unittest.main()
