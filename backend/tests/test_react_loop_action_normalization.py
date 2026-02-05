import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


class TestReactLoopActionNormalization(unittest.TestCase):
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

    def _run_react_loop_once(self, *, plan_title: str, allow: list[str], llm_actions: list[dict]):
        from backend.src.agent.runner.react_loop import run_react_loop
        from backend.src.constants import RUN_STATUS_DONE

        task_id, run_id = self._create_task_and_run()
        workdir = os.getcwd()

        plan_titles = [plan_title]
        plan_items = [{"id": 1, "brief": "t", "status": "pending"}]
        plan_allows = [allow]
        plan_artifacts = []

        # create_llm_call 会被调用 1..N 次（allow_mismatch/强制补齐等）
        llm_side_effect = []
        for action in llm_actions:
            llm_side_effect.append(
                {"record": {"status": "success", "response": json.dumps(action, ensure_ascii=False)}}
            )

        captured = {"detail": None}

        def _fake_execute_step_action(_task_id, _run_id, step_row, context=None):
            captured["detail"] = step_row.get("detail")
            # 返回一个最小成功结果，避免真实执行命令/写文件
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
            )
            try:
                while True:
                    next(gen)
            except StopIteration as exc:
                result = exc.value

        self.assertEqual(result.run_status, RUN_STATUS_DONE)
        self.assertIsInstance(captured["detail"], str)
        return captured["detail"], workdir

    def test_shell_command_missing_workdir_is_auto_filled_before_validation(self):
        detail, workdir = self._run_react_loop_once(
            plan_title="shell_command 列目录",
            allow=["shell_command"],
            llm_actions=[
                {
                    "action": {
                        "type": "shell_command",
                        "payload": {"command": "echo hi"},
                    }
                }
            ],
        )
        obj = json.loads(detail)
        self.assertEqual(obj["type"], "shell_command")
        self.assertEqual(obj["payload"]["workdir"], workdir)
        self.assertIn("timeout_ms", obj["payload"])

    def test_shell_command_alias_cmd_missing_workdir_is_auto_filled_before_validation(self):
        """
        回归：模型输出 action.type=cmd（alias）时，也应在校验前补齐 workdir/timeout_ms。
        """
        detail, workdir = self._run_react_loop_once(
            plan_title="shell_command 列目录",
            allow=["shell_command"],
            llm_actions=[
                {
                    "action": {
                        "type": "cmd",
                        "payload": {"command": "echo hi"},
                    }
                }
            ],
        )
        obj = json.loads(detail)
        self.assertEqual(obj["type"], "shell_command")
        self.assertEqual(obj["payload"]["workdir"], workdir)
        self.assertIn("timeout_ms", obj["payload"])

    def test_file_write_missing_path_is_coerced_from_title_before_validation(self):
        detail, _ = self._run_react_loop_once(
            plan_title="file_write:test/out.txt 写入文件",
            allow=["file_write"],
            llm_actions=[
                {
                    "action": {
                        "type": "file_write",
                        "payload": {"content": "hello"},
                    }
                }
            ],
        )
        obj = json.loads(detail)
        self.assertEqual(obj["type"], "file_write")
        self.assertEqual(obj["payload"]["path"], "test/out.txt")
        self.assertEqual(obj["payload"]["content"], "hello")

    def test_file_write_alias_file_missing_path_is_coerced_from_title_before_validation(self):
        """
        回归：模型输出 action.type=file（alias）时，也应在校验前从 title 兜底 path。
        """
        detail, _ = self._run_react_loop_once(
            plan_title="file_write:test/out.txt 写入文件",
            allow=["file_write"],
            llm_actions=[
                {
                    "action": {
                        "type": "file",
                        "payload": {"content": "hello"},
                    }
                }
            ],
        )
        obj = json.loads(detail)
        self.assertEqual(obj["type"], "file_write")
        self.assertEqual(obj["payload"]["path"], "test/out.txt")
        self.assertEqual(obj["payload"]["content"], "hello")

    def test_file_read_alias_read_file_missing_path_is_coerced_from_title_before_validation(self):
        """
        回归：模型输出 action.type=read_file（alias）且 payload.path 漏填时，
        允许从 title 的 file_read: 前缀兜底补齐路径。
        """
        detail, _ = self._run_react_loop_once(
            plan_title="file_read:README.md 读取文件",
            allow=["file_read"],
            llm_actions=[
                {
                    "action": {
                        "type": "read_file",
                        "payload": {},
                    }
                }
            ],
        )
        obj = json.loads(detail)
        self.assertEqual(obj["type"], "file_read")
        self.assertEqual(obj["payload"]["path"], "README.md")

    def test_file_list_missing_path_is_coerced_from_title_before_validation(self):
        """
        回归：file_list 常见目标是“无扩展名目录名”（如 backend/src 或 backend）。
        当模型漏填 payload.path 时，应允许从 title 的 file_list: 前缀兜底补齐。
        """
        detail, _ = self._run_react_loop_once(
            plan_title="file_list:backend 列出目录",
            allow=["file_list"],
            llm_actions=[
                {
                    "action": {
                        "type": "file_list",
                        "payload": {},
                    }
                }
            ],
        )
        obj = json.loads(detail)
        self.assertEqual(obj["type"], "file_list")
        self.assertEqual(obj["payload"]["path"], "backend")

    def test_http_request_missing_url_is_coerced_from_title_before_validation(self):
        """
        回归：模型输出 http_request 且 payload.url 漏填时，允许从 title 的 http_request: 前缀兜底补齐 URL。
        """
        detail, _ = self._run_react_loop_once(
            plan_title="http_request:https://example.com 获取数据",
            allow=["http_request"],
            llm_actions=[
                {
                    "action": {
                        "type": "http_request",
                        "payload": {},
                    }
                }
            ],
        )
        obj = json.loads(detail)
        self.assertEqual(obj["type"], "http_request")
        self.assertEqual(obj["payload"]["url"], "https://example.com")

    def test_task_output_empty_content_without_last_llm_triggers_reprompt(self):
        # 第一次给空 content；第二次补齐 content
        detail, _ = self._run_react_loop_once(
            plan_title="输出结果",
            allow=["task_output"],
            llm_actions=[
                {
                    "action": {
                        "type": "task_output",
                        "payload": {"output_type": "text", "content": ""},
                    }
                },
                {
                    "action": {
                        "type": "task_output",
                        "payload": {"output_type": "text", "content": "final"},
                    }
                },
            ],
        )
        obj = json.loads(detail)
        self.assertEqual(obj["type"], "task_output")
        self.assertEqual(obj["payload"]["content"], "final")

    def test_llm_call_alias_chat_strips_provider_model(self):
        """
        回归：模型输出 action.type=chat（alias）时，也应被归一化为 llm_call，
        且 provider/model 字段必须被剔除，避免模型写错导致不可用。
        """
        detail, _ = self._run_react_loop_once(
            plan_title="llm_call 生成文本",
            allow=["llm_call"],
            llm_actions=[
                {
                    "action": {
                        "type": "chat",
                        "payload": {"prompt": "hi", "provider": "bad", "model": "bad"},
                    }
                }
            ],
        )
        obj = json.loads(detail)
        self.assertEqual(obj["type"], "llm_call")
        self.assertNotIn("provider", obj["payload"])
        self.assertNotIn("model", obj["payload"])

    def test_tool_call_exec_missing_workdir_is_auto_filled_before_validation(self):
        detail, workdir = self._run_react_loop_once(
            plan_title="tool_call:demo",
            allow=["tool_call"],
            llm_actions=[
                {
                    "action": {
                        "type": "tool_call",
                        "payload": {
                            "tool_name": "demo",
                            "input": "hi",
                            "output": "",
                            "tool_metadata": {"exec": {"command": "echo {input}"}},
                        },
                    }
                }
            ],
        )
        obj = json.loads(detail)
        self.assertEqual(obj["type"], "tool_call")
        exec_spec = obj["payload"]["tool_metadata"]["exec"]
        self.assertEqual(exec_spec["workdir"], workdir)
        self.assertIn("timeout_ms", exec_spec)

    def test_tool_call_alias_tool_exec_missing_workdir_is_auto_filled_before_validation(self):
        """
        回归：模型输出 action.type=tool（alias）时，也应补齐 tool_metadata.exec.workdir/timeout_ms。
        """
        detail, workdir = self._run_react_loop_once(
            plan_title="tool_call:demo",
            allow=["tool_call"],
            llm_actions=[
                {
                    "action": {
                        "type": "tool",
                        "payload": {
                            "tool_name": "demo",
                            "input": "hi",
                            "output": "",
                            "tool_metadata": {"exec": {"command": "echo {input}"}},
                        },
                    }
                }
            ],
        )
        obj = json.loads(detail)
        self.assertEqual(obj["type"], "tool_call")
        exec_spec = obj["payload"]["tool_metadata"]["exec"]
        self.assertEqual(exec_spec["workdir"], workdir)
        self.assertIn("timeout_ms", exec_spec)


if __name__ == "__main__":
    unittest.main()
