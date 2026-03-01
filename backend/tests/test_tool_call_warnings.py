import os
import unittest
from unittest.mock import patch


class TestToolCallWarnings(unittest.TestCase):
    def test_tool_call_empty_output_returns_warning_not_error(self):
        from backend.src.actions.handlers.tool_call import execute_tool_call

        step_row = {"id": 1, "title": "tool_call:demo"}
        payload = {
            "tool_name": "demo_tool",
            "tool_description": "demo",
            "tool_version": "0.1.0",
            "tool_metadata": {
                "exec": {"type": "shell", "command": "echo ok", "workdir": "/tmp"},
            },
            "input": "ping",
            "output": "",
        }

        with patch("backend.src.actions.handlers.tool_call.is_tool_enabled", return_value=True), patch(
            "backend.src.actions.handlers.tool_call.get_tool_by_name", return_value=None
        ), patch(
            "backend.src.actions.handlers.tool_call._enforce_tool_exec_script_dependency", return_value=None
        ), patch(
            "backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec",
            return_value=("", None),
        ), patch(
            "backend.src.actions.handlers.tool_call._create_tool_record",
            return_value={"record": {"tool_id": 1, "tool_name": "demo_tool", "input": "ping", "output": ""}},
        ):
            record, error = execute_tool_call(task_id=1, run_id=1, step_row=step_row, payload=payload)

        self.assertIsNone(error)
        self.assertIsInstance(record, dict)
        self.assertIn("warnings", record)
        self.assertIn("输出为空", str(record.get("warnings")))

    def test_web_fetch_block_marker_returns_error(self):
        from backend.src.actions.handlers.tool_call import execute_tool_call

        step_row = {"id": 1, "title": "tool_call:web_fetch 抓取页面"}
        payload = {
            "tool_name": "web_fetch",
            "tool_metadata": {
                "exec": {"type": "shell", "command": "echo ok", "workdir": "/tmp"},
            },
            "input": "https://example.com",
            "output": "",
        }

        with patch("backend.src.actions.handlers.tool_call.is_tool_enabled", return_value=True), patch(
            "backend.src.actions.handlers.tool_call.get_tool_by_name",
            return_value={"id": 1, "name": "web_fetch", "metadata": "{}"},
        ), patch(
            "backend.src.actions.handlers.tool_call._enforce_tool_exec_script_dependency", return_value=None
        ), patch(
            "backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec",
            return_value=("Edge: Too Many Requests", None),
        ), patch(
            "backend.src.actions.handlers.tool_call._create_tool_record",
            return_value={"record": {"tool_id": 1, "tool_name": "web_fetch", "input": "https://example.com", "output": "Edge: Too Many Requests"}},
        ):
            record, error = execute_tool_call(task_id=1, run_id=1, step_row=step_row, payload=payload)

        self.assertIsInstance(record, dict)
        self.assertIsNotNone(error)
        self.assertIn("web_fetch", str(error))
        self.assertIn("rate_limited", str(error))
        self.assertIn("too_many_requests", str(error))

    def test_web_fetch_semantic_error_success_false_returns_error(self):
        from backend.src.actions.handlers.tool_call import execute_tool_call

        step_row = {"id": 1, "title": "tool_call:web_fetch 抓取页面"}
        payload = {
            "tool_name": "web_fetch",
            "tool_metadata": {
                "exec": {"type": "shell", "command": "echo ok", "workdir": "/tmp"},
            },
            "input": "https://api.exchangerate.host/timeseries",
            "output": "",
        }
        semantic_error_output = (
            '{'
            '"success": false, '
            '"error": {"type": "missing_access_key", "info": "access key required"}'
            '}'
        )

        with patch("backend.src.actions.handlers.tool_call.is_tool_enabled", return_value=True), patch(
            "backend.src.actions.handlers.tool_call.get_tool_by_name",
            return_value={"id": 1, "name": "web_fetch", "metadata": "{}"},
        ), patch(
            "backend.src.actions.handlers.tool_call._enforce_tool_exec_script_dependency", return_value=None
        ), patch(
            "backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec",
            return_value=(semantic_error_output, None),
        ), patch(
            "backend.src.actions.handlers.tool_call._create_tool_record",
            return_value={
                "record": {
                    "tool_id": 1,
                    "tool_name": "web_fetch",
                    "input": "https://api.exchangerate.host/timeseries",
                    "output": semantic_error_output,
                }
            },
        ):
            record, error = execute_tool_call(task_id=1, run_id=1, step_row=step_row, payload=payload)

        self.assertIsInstance(record, dict)
        self.assertIsNotNone(error)
        self.assertIn("web_fetch", str(error))
        self.assertIn("missing_api_key", str(error))
        self.assertIn("missing_access_key", str(error))

    def test_web_fetch_daily_hits_limit_is_treated_as_rate_limited(self):
        from backend.src.actions.handlers.tool_call import execute_tool_call

        step_row = {"id": 1, "title": "tool_call:web_fetch 抓取页面"}
        payload = {
            "tool_name": "web_fetch",
            "tool_metadata": {
                "exec": {"type": "shell", "command": "echo ok", "workdir": "/tmp"},
            },
            "input": "https://stooq.com/q/d/l/?s=xauusd&i=d",
            "output": "",
        }

        with patch("backend.src.actions.handlers.tool_call.is_tool_enabled", return_value=True), patch(
            "backend.src.actions.handlers.tool_call.get_tool_by_name",
            return_value={"id": 1, "name": "web_fetch", "metadata": "{}"},
        ), patch(
            "backend.src.actions.handlers.tool_call._enforce_tool_exec_script_dependency", return_value=None
        ), patch(
            "backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec",
            return_value=("Exceeded the daily hits limit", None),
        ), patch(
            "backend.src.actions.handlers.tool_call._create_tool_record",
            return_value={
                "record": {
                    "tool_id": 1,
                    "tool_name": "web_fetch",
                    "input": payload["input"],
                    "output": "Exceeded the daily hits limit",
                }
            },
        ):
            _record, error = execute_tool_call(task_id=1, run_id=1, step_row=step_row, payload=payload)

        self.assertIsNotNone(error)
        self.assertIn("rate_limited", str(error))
        self.assertIn("daily hits limit", str(error))

    def test_web_fetch_block_marker_can_be_extended_by_env(self):
        from backend.src.actions.handlers import tool_call as tool_call_module
        from backend.src.actions.handlers.tool_call import execute_tool_call

        step_row = {"id": 1, "title": "tool_call:web_fetch 抓取页面"}
        payload = {
            "tool_name": "web_fetch",
            "tool_metadata": {
                "exec": {"type": "shell", "command": "echo ok", "workdir": "/tmp"},
            },
            "input": "https://demo.example.org",
            "output": "",
        }

        with patch.dict(
            os.environ,
            {"AGENT_WEB_FETCH_BLOCK_MARKERS_JSON": '[["upstream waf triggered", "request_blocked"]]'},
            clear=False,
        ):
            tool_call_module._get_web_fetch_block_markers.cache_clear()
            with patch("backend.src.actions.handlers.tool_call.is_tool_enabled", return_value=True), patch(
                "backend.src.actions.handlers.tool_call.get_tool_by_name",
                return_value={"id": 1, "name": "web_fetch", "metadata": "{}"},
            ), patch(
                "backend.src.actions.handlers.tool_call._enforce_tool_exec_script_dependency", return_value=None
            ), patch(
                "backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec",
                return_value=("upstream waf triggered", None),
            ), patch(
                "backend.src.actions.handlers.tool_call._create_tool_record",
                return_value={
                    "record": {
                        "tool_id": 1,
                        "tool_name": "web_fetch",
                        "input": payload["input"],
                        "output": "upstream waf triggered",
                    }
                },
            ):
                _record, error = execute_tool_call(task_id=1, run_id=1, step_row=step_row, payload=payload)

        tool_call_module._get_web_fetch_block_markers.cache_clear()
        self.assertIsNotNone(error)
        self.assertIn("web_fetch_blocked", str(error))
        self.assertIn("request_blocked", str(error))

    def test_web_fetch_status_line_429_is_treated_as_rate_limited(self):
        from backend.src.actions.handlers import tool_call as tool_call_module
        from backend.src.actions.handlers.tool_call import execute_tool_call

        step_row = {"id": 1, "title": "tool_call:web_fetch 抓取页面"}
        payload = {
            "tool_name": "web_fetch",
            "tool_metadata": {
                "exec": {"type": "shell", "command": "echo ok", "workdir": "/tmp"},
            },
            "input": "https://rate.example.org",
            "output": "",
        }

        tool_call_module._get_web_fetch_block_markers.cache_clear()
        with patch("backend.src.actions.handlers.tool_call.is_tool_enabled", return_value=True), patch(
            "backend.src.actions.handlers.tool_call.get_tool_by_name",
            return_value={"id": 1, "name": "web_fetch", "metadata": "{}"},
        ), patch(
            "backend.src.actions.handlers.tool_call._enforce_tool_exec_script_dependency", return_value=None
        ), patch(
            "backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec",
            return_value=("HTTP/1.1 429", None),
        ), patch(
            "backend.src.actions.handlers.tool_call._create_tool_record",
            return_value={
                "record": {
                    "tool_id": 1,
                    "tool_name": "web_fetch",
                    "input": payload["input"],
                    "output": "HTTP/1.1 429",
                }
            },
        ):
            _record, error = execute_tool_call(task_id=1, run_id=1, step_row=step_row, payload=payload)

        self.assertIsNotNone(error)
        self.assertIn("rate_limited", str(error))
        self.assertIn("too_many_requests", str(error))

    def test_tool_structured_failed_status_returns_error(self):
        from backend.src.actions.handlers.tool_call import execute_tool_call

        step_row = {"id": 1, "title": "tool_call:script_analyze_optimize 执行"}
        structured_output = (
            '{'
            '"status":"failed",'
            '"error_code":"optimizer_llm_failed",'
            '"summary":"LLM 返回非 JSON",'
            '"errors":[{"code":"llm_failed","message":"invalid_json"}]'
            '}'
        )
        payload = {
            "tool_name": "script_analyze_optimize",
            "tool_metadata": {
                "exec": {"type": "shell", "command": "python scripts/script_optimizer.py --input-stdin", "workdir": "/tmp"},
            },
            "input": '{"target_paths":["scripts/demo.py"],"mode":"analyze"}',
            "output": "",
        }

        with patch("backend.src.actions.handlers.tool_call.is_tool_enabled", return_value=True), patch(
            "backend.src.actions.handlers.tool_call.get_tool_by_name",
            return_value={"id": 10, "name": "script_analyze_optimize", "metadata": "{}"},
        ), patch(
            "backend.src.actions.handlers.tool_call._enforce_tool_exec_script_dependency", return_value=None
        ), patch(
            "backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec",
            return_value=(structured_output, None),
        ), patch(
            "backend.src.actions.handlers.tool_call._create_tool_record",
            return_value={
                "record": {
                    "tool_id": 10,
                    "tool_name": "script_analyze_optimize",
                    "input": payload["input"],
                    "output": structured_output,
                }
            },
        ):
            _record, error = execute_tool_call(task_id=1, run_id=1, step_row=step_row, payload=payload)

        self.assertIsNotNone(error)
        self.assertIn("optimizer_llm_failed", str(error))
        self.assertIn("返回失败状态", str(error))

    def test_tool_structured_partial_without_applied_returns_error(self):
        from backend.src.actions.handlers.tool_call import execute_tool_call

        step_row = {"id": 1, "title": "tool_call:script_analyze_optimize 执行"}
        structured_output = (
            '{'
            '"status":"partial",'
            '"summary":"2 个文件失败",'
            '"errors":[{"code":"file_not_found","message":"x.py"}],'
            '"applied":[]'
            '}'
        )
        payload = {
            "tool_name": "script_analyze_optimize",
            "tool_metadata": {
                "exec": {"type": "shell", "command": "python scripts/script_optimizer.py --input-stdin", "workdir": "/tmp"},
            },
            "input": '{"target_paths":["x.py"],"mode":"apply_patch"}',
            "output": "",
        }

        with patch("backend.src.actions.handlers.tool_call.is_tool_enabled", return_value=True), patch(
            "backend.src.actions.handlers.tool_call.get_tool_by_name",
            return_value={"id": 10, "name": "script_analyze_optimize", "metadata": "{}"},
        ), patch(
            "backend.src.actions.handlers.tool_call._enforce_tool_exec_script_dependency", return_value=None
        ), patch(
            "backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec",
            return_value=(structured_output, None),
        ), patch(
            "backend.src.actions.handlers.tool_call._create_tool_record",
            return_value={
                "record": {
                    "tool_id": 10,
                    "tool_name": "script_analyze_optimize",
                    "input": payload["input"],
                    "output": structured_output,
                }
            },
        ):
            _record, error = execute_tool_call(task_id=1, run_id=1, step_row=step_row, payload=payload)

        self.assertIsNotNone(error)
        self.assertIn("tool_partial_failed", str(error))
        self.assertIn("partial", str(error))

    def test_web_fetch_missing_api_key_auto_switches_source(self):
        from backend.src.actions.handlers.tool_call import execute_tool_call

        step_row = {"id": 1, "title": "tool_call:web_fetch 抓取页面"}
        payload = {
            "tool_name": "web_fetch",
            "tool_metadata": {
                "exec": {"type": "shell", "command": "echo ok", "workdir": "/tmp"},
            },
            "input": (
                "https://api.exchangerate.host/timeseries"
                "?start_date=2026-01-01&end_date=2026-01-03&base=USD&symbols=CNY"
            ),
            "output": "",
        }
        missing_key_output = (
            '{'
            '"success": false, '
            '"error": {"type": "missing_access_key", "info": "access key required"}'
            '}'
        )
        frankfurter_output = '{"base":"USD","rates":{"2026-01-01":{"CNY":7.1}}}'

        def fake_exec(_exec_spec, tool_input):
            url = str(tool_input or "")
            if "exchangerate.host" in url:
                return missing_key_output, None
            if "api.frankfurter.app" in url:
                return frankfurter_output, None
            return "", "工具执行失败: unexpected source"

        def fake_create_tool_record(current_payload):
            return {
                "record": {
                    "tool_id": 1,
                    "tool_name": "web_fetch",
                    "input": str(current_payload.get("input") or ""),
                    "output": str(current_payload.get("output") or ""),
                }
            }

        with patch("backend.src.actions.handlers.tool_call.is_tool_enabled", return_value=True), patch(
            "backend.src.actions.handlers.tool_call.get_tool_by_name",
            return_value={"id": 1, "name": "web_fetch", "metadata": "{}"},
        ), patch(
            "backend.src.actions.handlers.tool_call._enforce_tool_exec_script_dependency", return_value=None
        ), patch(
            "backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec",
            side_effect=fake_exec,
        ) as mocked_exec, patch(
            "backend.src.actions.handlers.tool_call._create_tool_record",
            side_effect=fake_create_tool_record,
        ):
            record, error = execute_tool_call(task_id=1, run_id=1, step_row=step_row, payload=payload)

        self.assertIsNone(error)
        self.assertIsInstance(record, dict)
        self.assertIn("rates", str(record.get("output")))
        self.assertIn("自动切换到备用源", str(record.get("warnings")))
        called_urls = [str(call.args[1] or "") for call in mocked_exec.call_args_list]
        self.assertTrue(any("api.frankfurter.app" in item for item in called_urls))

    def test_web_fetch_skips_same_host_after_access_denied(self):
        from backend.src.actions.handlers.tool_call import execute_tool_call

        step_row = {"id": 1, "title": "tool_call:web_fetch 抓取页面"}
        payload = {
            "tool_name": "web_fetch",
            "tool_metadata": {
                "exec": {"type": "shell", "command": "echo ok", "workdir": "/tmp"},
            },
            "input": "https://blocked.example.com/data",
            "output": "",
        }

        def fake_exec(_exec_spec, tool_input):
            url = str(tool_input or "")
            if url == "https://blocked.example.com/data":
                return "", "工具执行失败: HTTP 403 Forbidden"
            if url.startswith("https://r.jina.ai/"):
                return "", "工具执行失败: HTTP 429"
            if url == "https://blocked.example.com/fallback":
                return "should-not-run", None
            if url == "https://mirror.example.net/ok":
                return "ok-from-mirror", None
            return "", "工具执行失败: unknown"

        def fake_create_tool_record(current_payload):
            return {
                "record": {
                    "tool_id": 1,
                    "tool_name": "web_fetch",
                    "input": str(current_payload.get("input") or ""),
                    "output": str(current_payload.get("output") or ""),
                }
            }

        with patch.dict(
            os.environ,
            {
                "AGENT_WEB_FETCH_FALLBACK_URL_TEMPLATES_JSON": (
                    '["https://blocked.example.com/fallback","https://mirror.example.net/ok"]'
                )
            },
            clear=False,
        ), patch("backend.src.actions.handlers.tool_call.is_tool_enabled", return_value=True), patch(
            "backend.src.actions.handlers.tool_call.get_tool_by_name",
            return_value={"id": 1, "name": "web_fetch", "metadata": "{}"},
        ), patch(
            "backend.src.actions.handlers.tool_call._enforce_tool_exec_script_dependency", return_value=None
        ), patch(
            "backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec",
            side_effect=fake_exec,
        ) as mocked_exec, patch(
            "backend.src.actions.handlers.tool_call._create_tool_record",
            side_effect=fake_create_tool_record,
        ):
            record, error = execute_tool_call(task_id=1, run_id=1, step_row=step_row, payload=payload)

        self.assertIsNone(error)
        self.assertIsInstance(record, dict)
        self.assertEqual(str(record.get("output")), "ok-from-mirror")
        called_urls = [str(call.args[1] or "") for call in mocked_exec.call_args_list]
        self.assertIn("https://mirror.example.net/ok", called_urls)
        self.assertNotIn("https://blocked.example.com/fallback", called_urls)
        attempts = record.get("attempts") or []
        self.assertTrue(any(str(item.get("status") or "") == "skipped" for item in attempts if isinstance(item, dict)))


if __name__ == "__main__":
    unittest.main()
