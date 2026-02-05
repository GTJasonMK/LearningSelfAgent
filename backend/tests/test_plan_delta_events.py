import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


def _parse_sse_json(msg: str) -> dict | None:
    if not isinstance(msg, str) or not msg:
        return None
    data_lines = []
    for line in msg.splitlines():
        if line.startswith("data:"):
            data_lines.append(line[len("data:") :].lstrip())
    if not data_lines:
        return None
    try:
        return json.loads("\n".join(data_lines))
    except Exception:
        return None


def _apply_plan_delta(items: list[dict], changes: list[dict]) -> None:
    """
    前端合并逻辑的最小等价实现（仅用于后端回归测试）：
    - 优先按 id 匹配，找不到则按 step_order-1 作为索引更新。
    """
    for ch in changes or []:
        if not isinstance(ch, dict):
            continue
        raw_id = ch.get("id")
        raw_order = ch.get("step_order")
        idx = -1
        try:
            cid = int(raw_id)
        except Exception:
            cid = 0
        if cid > 0:
            for i, it in enumerate(items):
                try:
                    if int(it.get("id")) == cid:
                        idx = i
                        break
                except Exception:
                    continue
        if idx == -1:
            try:
                order = int(raw_order)
            except Exception:
                order = 0
            if order > 0:
                idx = order - 1
        if idx < 0 or idx >= len(items):
            continue

        if ch.get("status") is not None:
            items[idx]["status"] = ch.get("status")
        if ch.get("brief") is not None:
            items[idx]["brief"] = ch.get("brief")
        if ch.get("title") is not None:
            items[idx]["title"] = ch.get("title")


class TestPlanDeltaEvents(unittest.TestCase):
    """
    回归：ReAct/do 执行过程中应使用 plan_delta 做增量更新。
    - 该测试不依赖前端，直接验证后端 SSE 输出包含 plan_delta，并且可合并得到最终状态。
    """

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

    def _create_task_and_run(self) -> tuple[int, int]:
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

    def test_react_loop_emits_plan_delta_and_reaches_done(self):
        from backend.src.agent.runner.react_loop import run_react_loop
        from backend.src.constants import RUN_STATUS_DONE

        task_id, run_id = self._create_task_and_run()

        plan_titles = ["shell_command:echo ok", "task_output 输出结果"]
        plan_items = [
            {"id": 1, "brief": "执行", "status": "pending"},
            {"id": 2, "brief": "输出", "status": "pending"},
        ]
        plan_allows = [["shell_command"], ["task_output"]]

        # 两步收敛：shell_command -> task_output
        llm_actions = [
            {"action": {"type": "shell_command", "payload": {"command": "echo ok"}}},
            {"action": {"type": "task_output", "payload": {"output_type": "text", "content": "ok"}}},
        ]
        llm_calls: list[dict] = []

        def _fake_create_llm_call(payload: dict):
            llm_calls.append(dict(payload))
            resp = json.dumps(llm_actions[len(llm_calls) - 1], ensure_ascii=False)
            return {"record": {"status": "success", "response": resp}}

        def _fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            return {"ok": True}, None

        observed = [dict(it) for it in plan_items]
        saw_delta = False

        with patch(
            "backend.src.agent.runner.react_loop.create_llm_call",
            side_effect=_fake_create_llm_call,
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
                plan_titles=list(plan_titles),
                plan_items=plan_items,
                plan_allows=[list(a) for a in plan_allows],
                plan_artifacts=[],
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
                    msg = next(gen)
                    obj = _parse_sse_json(msg)
                    if not obj:
                        continue
                    if obj.get("type") == "plan_delta":
                        saw_delta = True
                        changes = obj.get("changes")
                        if isinstance(changes, list):
                            _apply_plan_delta(observed, changes)
            except StopIteration as exc:
                result = exc.value

        self.assertTrue(saw_delta, "应在执行过程中产生 plan_delta 事件")
        self.assertEqual(result.run_status, RUN_STATUS_DONE)
        self.assertEqual([it.get("status") for it in observed], ["done", "done"])


if __name__ == "__main__":
    unittest.main()

