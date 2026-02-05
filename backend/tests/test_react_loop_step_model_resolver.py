import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


class TestReactLoopStepModelResolver(unittest.TestCase):
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

    def test_step_llm_config_resolver_overrides_model_per_step(self):
        from backend.src.agent.runner.react_loop import run_react_loop
        from backend.src.constants import RUN_STATUS_DONE

        task_id, run_id = self._create_task_and_run()
        workdir = os.getcwd()

        plan_titles = [
            "file_write:README.md 写入文档",
            "file_write:main.py 写入代码",
        ]
        plan_items = [
            {"id": 1, "brief": "doc", "status": "pending"},
            {"id": 2, "brief": "code", "status": "pending"},
        ]
        plan_allows = [
            ["file_write"],
            ["file_write"],
        ]
        plan_artifacts = []

        models: list[str] = []
        llm_actions = [
            {"action": {"type": "file_write", "payload": {"path": "README.md", "content": "doc"}}},
            {"action": {"type": "file_write", "payload": {"path": "main.py", "content": "code"}}},
        ]

        def _fake_llm_call(payload):
            models.append(str(payload.get("model") or ""))
            resp = json.dumps(llm_actions[len(models) - 1], ensure_ascii=False)
            return {"record": {"status": "success", "response": resp}}

        def _fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            return {"ok": True}, None

        def _resolver(step_order: int, title: str, allow: list[str]):
            if step_order == 1:
                return "doc-model", {"temperature": 0}
            if step_order == 2:
                return "code-model", {"temperature": 0}
            return None, None

        with patch(
            "backend.src.agent.runner.react_loop.create_llm_call",
            side_effect=_fake_llm_call,
        ), patch(
            "backend.src.agent.runner.react_loop._execute_step_action",
            side_effect=_fake_execute_step_action,
        ):
            gen = run_react_loop(
                task_id=task_id,
                run_id=run_id,
                message="m",
                workdir=workdir,
                model="base-model",
                parameters={"temperature": 0.2},
                plan_titles=plan_titles,
                plan_items=plan_items,
                plan_allows=plan_allows,
                plan_artifacts=plan_artifacts,
                tools_hint="(无)",
                skills_hint="(无)",
                memories_hint="(无)",
                graph_hint="(无)",
                agent_state={},
                context={"last_llm_response": None},
                observations=[],
                start_step_order=1,
                variables_source="test",
                step_llm_config_resolver=_resolver,
            )
            try:
                while True:
                    next(gen)
            except StopIteration as exc:
                result = exc.value

        self.assertEqual(result.run_status, RUN_STATUS_DONE)
        self.assertEqual(models, ["doc-model", "code-model"])

    def test_llm_call_action_uses_resolved_model_and_overrides(self):
        from backend.src.agent.runner.react_loop import run_react_loop
        from backend.src.constants import RUN_STATUS_DONE

        task_id, run_id = self._create_task_and_run()
        workdir = os.getcwd()

        plan_titles = ["llm_call:验证 生成文本"]
        plan_items = [{"id": 1, "brief": "verify", "status": "pending"}]
        plan_allows = [["llm_call"]]
        plan_artifacts = []

        executed_payloads: list[dict] = []

        def _fake_llm_call(_payload):
            # 返回 llm_call action（payload 中故意带一个“错误模型”和自定义参数）
            # 预期：执行阶段应使用 resolver 解析出的 model/参数覆盖，而不是信任 action payload。
            action = {
                "action": {
                    "type": "llm_call",
                    "payload": {
                        "prompt": "hello",
                        "model": "wrong-model",
                        "parameters": {"temperature": 0.9},
                    },
                }
            }
            resp = json.dumps(action, ensure_ascii=False)
            return {"record": {"status": "success", "response": resp}}

        def _fake_execute_step_action(_task_id, _run_id, step_row, context=None):
            detail = json.loads(step_row.get("detail") or "{}")
            payload = detail.get("payload") if isinstance(detail, dict) else None
            executed_payloads.append(payload if isinstance(payload, dict) else {})
            return {"response": "ok"}, None

        def _resolver(_step_order: int, _title: str, _allow: list[str]):
            return "resolved-model", {"temperature": 0.1, "max_tokens": 123}

        with patch(
            "backend.src.agent.runner.react_loop.create_llm_call",
            side_effect=_fake_llm_call,
        ), patch(
            "backend.src.agent.runner.react_loop._execute_step_action",
            side_effect=_fake_execute_step_action,
        ):
            gen = run_react_loop(
                task_id=task_id,
                run_id=run_id,
                message="m",
                workdir=workdir,
                model="base-model",
                parameters={"temperature": 0.2},
                plan_titles=plan_titles,
                plan_items=plan_items,
                plan_allows=plan_allows,
                plan_artifacts=plan_artifacts,
                tools_hint="(无)",
                skills_hint="(无)",
                memories_hint="(无)",
                graph_hint="(无)",
                agent_state={},
                context={"last_llm_response": None},
                observations=[],
                start_step_order=1,
                variables_source="test",
                step_llm_config_resolver=_resolver,
            )
            try:
                while True:
                    next(gen)
            except StopIteration as exc:
                result = exc.value

        self.assertEqual(result.run_status, RUN_STATUS_DONE)
        self.assertEqual(len(executed_payloads), 1)
        self.assertEqual(executed_payloads[0].get("model"), "resolved-model")
        params = executed_payloads[0].get("parameters") if isinstance(executed_payloads[0].get("parameters"), dict) else {}
        self.assertEqual(params.get("temperature"), 0.1)
        self.assertEqual(params.get("max_tokens"), 123)


if __name__ == "__main__":
    unittest.main()
