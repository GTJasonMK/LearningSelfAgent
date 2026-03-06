# -*- coding: utf-8 -*-
"""CLI 命令集成测试（使用 mock ApiClient）。"""

import unittest
from unittest.mock import patch

from click.testing import CliRunner

from backend.src.cli.client import ApiClient
from backend.src.cli.main import cli
from backend.src.cli.sse import SseEvent


class _MockClient:
    """模拟 ApiClient，按路径返回预设响应。"""

    def __init__(self, responses: dict | None = None):
        self._responses = responses or {}
        self.last_stream_path = None
        self.last_stream_json = None

    def _lookup(self, path: str) -> dict:
        for key, val in self._responses.items():
            if key in path:
                return val
        return {}

    def get(self, path, params=None):
        return self._lookup(path)

    def post(self, path, json_data=None):
        return self._lookup(path)

    def patch(self, path, json_data=None):
        return self._lookup(path)

    def delete(self, path):
        return self._lookup(path)

    def stream_post(self, path, json_data=None):
        self.last_stream_path = path
        self.last_stream_json = json_data
        stream = self._lookup(path)
        if isinstance(stream, list):
            for event in stream:
                yield event


def _invoke(args: list, responses: dict | None = None, catch_exceptions: bool = False):
    """辅助函数：使用 CliRunner 调用 CLI 并注入 mock client。"""
    result, _ = _invoke_with_client(args, responses=responses, catch_exceptions=catch_exceptions)
    return result


def _invoke_with_client(args: list, responses: dict | None = None, catch_exceptions: bool = False):
    """辅助函数：返回 CLI 结果和 mock client，便于断言请求参数。"""
    runner = CliRunner()
    mock_client = _MockClient(responses)

    # 注入 mock client 到 click context
    with patch.object(ApiClient, "__init__", lambda self, **kw: None):
        with patch.object(ApiClient, "get", mock_client.get):
            with patch.object(ApiClient, "post", mock_client.post):
                with patch.object(ApiClient, "patch", mock_client.patch):
                    with patch.object(ApiClient, "delete", mock_client.delete):
                        with patch.object(ApiClient, "stream_post", mock_client.stream_post):
                            result = runner.invoke(cli, args, catch_exceptions=catch_exceptions)
    return result, mock_client


class TestHealthCommand(unittest.TestCase):
    """health 命令测试。"""

    def test_health_ok(self):
        result = _invoke(["health"], responses={"/health": {"status": "ok"}})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("正常", result.output)

    def test_health_json(self):
        result = _invoke(["--json", "health"], responses={"/health": {"status": "ok"}})
        self.assertEqual(result.exit_code, 0)


class TestMemoryCommands(unittest.TestCase):
    """memory 命令组测试。"""

    def test_memory_list(self):
        items = [
            {"id": 1, "content": "测试记忆", "memory_type": "lesson", "tags": ["test"], "created_at": "2026-01-01T00:00:00"},
        ]
        result = _invoke(["memory", "list"], responses={"/memory/items": {"items": items}})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("测试记忆", result.output)

    def test_memory_list_empty(self):
        result = _invoke(["memory", "list"], responses={"/memory/items": {"items": []}})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("暂无", result.output)

    def test_memory_create(self):
        result = _invoke(
            ["memory", "create", "新记忆", "--type", "lesson", "--tags", "a,b"],
            responses={"/memory/items": {"id": 42}},
        )
        self.assertEqual(result.exit_code, 0)
        self.assertIn("42", result.output)

    def test_memory_get(self):
        item = {"id": 1, "content": "详情内容", "memory_type": "insight", "tags": [], "created_at": "2026-01-01T00:00:00"}
        result = _invoke(["memory", "get", "1"], responses={"/memory/items/1": {"item": item}})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("详情内容", result.output)

    def test_memory_update(self):
        result = _invoke(
            ["memory", "update", "1", "--content", "更新后"],
            responses={"/memory/items/1": {"id": 1}},
        )
        self.assertEqual(result.exit_code, 0)
        self.assertIn("已更新", result.output)

    def test_memory_update_no_fields(self):
        result = _invoke(["memory", "update", "1"])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("未指定", result.output)

    def test_memory_delete_with_yes(self):
        result = _invoke(
            ["memory", "delete", "1", "--yes"],
            responses={"/memory/items/1": {"deleted": True}},
        )
        self.assertEqual(result.exit_code, 0)
        self.assertIn("已删除", result.output)

    def test_memory_search(self):
        items = [
            {"id": 1, "content": "搜索结果", "memory_type": "lesson", "tags": [], "created_at": "2026-01-01T00:00:00"},
        ]
        result = _invoke(["memory", "search", "测试"], responses={"/memory/search": {"items": items}})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("搜索结果", result.output)

    def test_memory_summary(self):
        result = _invoke(
            ["memory", "summary"],
            responses={"/memory/summary": {"items": 10, "last_update": "2026-01-01"}},
        )
        self.assertEqual(result.exit_code, 0)


class TestTaskCommands(unittest.TestCase):
    """task 命令组测试。"""

    def test_task_list(self):
        tasks = [
            {"id": 1, "title": "测试任务", "status": "queued", "created_at": "2026-01-01T00:00:00"},
        ]
        result = _invoke(["task", "list"], responses={"/tasks": {"items": tasks}})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("测试任务", result.output)

    def test_task_create(self):
        result = _invoke(["task", "create", "新任务"], responses={"/tasks": {"task": {"id": 5, "title": "新任务"}}})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("5", result.output)

    def test_task_get(self):
        task = {"id": 2, "title": "详情任务", "status": "queued", "created_at": "2026-01-01T00:00:00"}
        result = _invoke(["task", "get", "2"], responses={"/tasks/2": {"task": task}})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("详情任务", result.output)

    def test_task_summary(self):
        result = _invoke(
            ["task", "summary"],
            responses={"/tasks/summary": {"count": 10, "current": "doing"}},
        )
        self.assertEqual(result.exit_code, 0)

    def test_task_list_invalid_shape_fails(self):
        result = _invoke(["task", "list"], responses={"/tasks": {"tasks": []}})
        self.assertEqual(result.exit_code, 1)
        self.assertIn("缺少 items 列表", result.output)

    def test_task_create_invalid_shape_fails(self):
        result = _invoke(["task", "create", "新任务"], responses={"/tasks": {"id": 1}})
        self.assertEqual(result.exit_code, 1)
        self.assertIn("缺少 task 对象", result.output)

    def test_task_get_invalid_shape_fails(self):
        result = _invoke(["task", "get", "1"], responses={"/tasks/1": {"id": 1}})
        self.assertEqual(result.exit_code, 1)
        self.assertIn("缺少 task 对象", result.output)


class TestSkillCommands(unittest.TestCase):
    """skill 命令组测试。"""

    def test_skill_list(self):
        items = [{"id": 1, "name": "技能A", "category": "tool.web", "status": "approved"}]
        result = _invoke(["skill", "list"], responses={"/memory/skills": {"items": items}})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("技能A", result.output)


class TestGraphCommands(unittest.TestCase):
    """graph 命令组测试。"""

    def test_graph_summary(self):
        result = _invoke(
            ["graph", "summary"],
            responses={"/memory/graph": {"nodes": 5, "edges": 3}},
        )
        self.assertEqual(result.exit_code, 0)

    def test_graph_nodes(self):
        nodes = [{"id": 1, "label": "Python", "node_type": "concept"}]
        result = _invoke(["graph", "nodes"], responses={"/memory/graph/nodes": {"nodes": nodes}})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Python", result.output)


class TestSearchCommand(unittest.TestCase):
    """search 命令测试。"""

    def test_search(self):
        results = {
            "memory": [{"id": 1, "content": "Python 记忆"}],
            "skills": [],
            "graph": {"nodes": [], "edges": []},
        }
        result = _invoke(["search", "Python"], responses={"/search": results})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Python", result.output)

    def test_search_no_results(self):
        results = {"memory": [], "skills": [], "graph": {"nodes": [], "edges": []}}
        result = _invoke(["search", "不存在"], responses={"/search": results})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("未找到", result.output)


class TestStreamCommands(unittest.TestCase):
    def test_ask_stream_renders_plan_delta_and_need_input(self):
        events = [
            SseEvent(
                event="message",
                data='{"type":"plan_delta","changes":[{"id":1,"step_order":1,"status":"running","title":"步骤A"}]}',
                json_data={
                    "type": "plan_delta",
                    "changes": [{"id": 1, "step_order": 1, "status": "running", "title": "步骤A"}],
                },
            ),
            SseEvent(
                event="message",
                data='{"type":"need_input","question":"请选择方案","choices":[{"label":"默认方案","value":"default"}]}',
                json_data={
                    "type": "need_input",
                    "question": "请选择方案",
                    "choices": [{"label": "默认方案", "value": "default"}],
                    "prompt_token": "tok_1",
                    "session_key": "sess_1",
                },
            ),
            SseEvent(event="done", data='{"type":"done"}', json_data={"type": "done"}),
        ]
        result = _invoke(["ask", "测试"], responses={"/agent/command/stream": events})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("步骤A [running]", result.output)
        self.assertIn("请选择方案", result.output)
        self.assertIn("默认方案", result.output)
        self.assertIn("prompt_token: tok_1", result.output)

    def test_ask_stream_renders_step_warning(self):
        events = [
            SseEvent(
                event="message",
                data='{"type":"step_warning","run_id":2,"task_id":1,"step_order":1,"tool":"web_fetch","primary_warning":"已自动切换到备用源","attempt_count":2,"failed_attempt_count":1,"successful_attempt_count":1,"fallback_used":true,"protocol_source":"fallback"}',
                json_data={
                    "type": "step_warning",
                    "run_id": 2,
                    "task_id": 1,
                    "step_order": 1,
                    "tool": "web_fetch",
                    "primary_warning": "已自动切换到备用源",
                    "attempt_count": 2,
                    "failed_attempt_count": 1,
                    "successful_attempt_count": 1,
                    "fallback_used": True,
                    "protocol_source": "fallback",
                },
            ),
            SseEvent(event="done", data='{"type":"stream_end","run_status":"done"}', json_data={"type": "stream_end", "run_status": "done"}),
        ]
        result = _invoke(["ask", "测试"], responses={"/agent/command/stream": events})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("步骤告警", result.output)
        self.assertIn("web_fetch", result.output)
        self.assertIn("已自动切换到备用源", result.output)
        self.assertIn("fallback=yes", result.output)

    def test_ask_stream_without_done_fails(self):
        events = [
            SseEvent(event="message", data='{"delta":"still running"}', json_data={"delta": "still running"}),
        ]
        result = _invoke(["ask", "测试"], responses={"/agent/command/stream": events})
        self.assertEqual(result.exit_code, 1)
        self.assertIn("未收到 done/stream_end 事件", result.output)

    def test_ask_stream_json_stream_end_is_done(self):
        events = [
            SseEvent(event="done", data='{"type":"stream_end"}', json_data={"type": "stream_end"}),
        ]
        result = _invoke(["--json", "ask", "测试"], responses={"/agent/command/stream": events})
        self.assertEqual(result.exit_code, 0)

    def test_resume_stream_command_exists(self):
        events = [
            SseEvent(event="message", data='{"delta":"running..."}', json_data={"delta": "running..."}),
            SseEvent(event="done", data='{"type":"done"}', json_data={"type": "done"}),
        ]
        result = _invoke(
            ["resume", "12", "继续执行", "--prompt-token", "tok", "--session-key", "sess"],
            responses={"/agent/command/resume/stream": events},
        )
        self.assertEqual(result.exit_code, 0)
        self.assertIn("恢复执行完毕", result.output)

    def test_ask_stream_failed_status_exits_nonzero(self):
        events = [
            SseEvent(
                event="done",
                data='{"type":"stream_end","task_id":1,"run_id":2,"run_status":"failed"}',
                json_data={"type": "stream_end", "task_id": 1, "run_id": 2, "run_status": "failed"},
            ),
        ]
        result = _invoke(["ask", "测试"], responses={"/agent/command/stream": events})
        self.assertEqual(result.exit_code, 1)
        self.assertIn("执行失败", result.output)
        self.assertNotIn("执行完毕", result.output)

    def test_ask_stream_waiting_status_returns_zero_and_not_done_message(self):
        events = [
            SseEvent(
                event="done",
                data='{"type":"stream_end","task_id":1,"run_id":2,"run_status":"waiting"}',
                json_data={"type": "stream_end", "task_id": 1, "run_id": 2, "run_status": "waiting"},
            ),
        ]
        result = _invoke(["ask", "测试"], responses={"/agent/command/stream": events})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("等待输入", result.output)
        self.assertNotIn("执行完毕", result.output)

    def test_resume_stream_failed_status_exits_nonzero(self):
        events = [
            SseEvent(
                event="done",
                data='{"type":"stream_end","task_id":1,"run_id":2,"run_status":"failed"}',
                json_data={"type": "stream_end", "task_id": 1, "run_id": 2, "run_status": "failed"},
            ),
        ]
        result = _invoke(
            ["resume", "12", "继续执行", "--prompt-token", "tok", "--session-key", "sess"],
            responses={"/agent/command/resume/stream": events},
        )
        self.assertEqual(result.exit_code, 1)
        self.assertIn("执行失败", result.output)
        self.assertNotIn("恢复执行完毕", result.output)

    def test_ask_stream_payload_supports_parameters_and_think_config(self):
        events = [SseEvent(event="done", data='{"type":"done"}', json_data={"type": "done"})]
        result, mock_client = _invoke_with_client(
            [
                "ask",
                "测试",
                "--parameters-json",
                '{"temperature":0.2}',
                "--think-config-json",
                '{"parallel":2}',
            ],
            responses={"/agent/command/stream": events},
        )
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(mock_client.last_stream_path, "/agent/command/stream")
        self.assertEqual(mock_client.last_stream_json.get("parameters"), {"temperature": 0.2})
        self.assertEqual(mock_client.last_stream_json.get("think_config"), {"parallel": 2})

    def test_task_execute_stream_payload_supports_options(self):
        events = [SseEvent(event="done", data='{"type":"done"}', json_data={"type": "done"})]
        result, mock_client = _invoke_with_client(
            ["task", "execute", "9", "--run-summary", "ok", "--max-retries", "2", "--on-failure", "continue"],
            responses={"/tasks/9/execute/stream": events},
        )
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(mock_client.last_stream_path, "/tasks/9/execute/stream")
        self.assertEqual(
            mock_client.last_stream_json,
            {"run_summary": "ok", "max_retries": 2, "on_failure": "continue"},
        )

    def test_ask_stream_replay_events_can_recover_done(self):
        stream_events = [
            SseEvent(
                event="message",
                data='{"type":"run_created","task_id":1,"run_id":7}',
                json_data={"type": "run_created", "task_id": 1, "run_id": 7},
            ),
        ]
        replay_payload = {
            "items": [
                {
                    "event_id": "sess_x:7:2:run_status",
                    "payload": {
                        "type": "run_status",
                        "task_id": 1,
                        "run_id": 7,
                        "status": "done",
                        "event_id": "sess_x:7:2:run_status",
                    },
                },
                {
                    "event_id": "sess_x:7:3:stream_end",
                    "payload": {
                        "type": "stream_end",
                        "task_id": 1,
                        "run_id": 7,
                        "run_status": "done",
                        "event_id": "sess_x:7:3:stream_end",
                    },
                },
            ]
        }
        result = _invoke(
            ["ask", "测试"],
            responses={
                "/agent/command/stream": stream_events,
                "/agent/runs/7/events": replay_payload,
            },
        )
        self.assertEqual(result.exit_code, 0)
        self.assertIn("补齐 2 条事件", result.output)

    def test_ask_stream_done_without_business_state_triggers_replay(self):
        stream_events = [
            SseEvent(
                event="done",
                data='{"type":"stream_end","task_id":1,"run_id":7}',
                json_data={"type": "stream_end", "task_id": 1, "run_id": 7},
            ),
        ]
        replay_payload = {
            "items": [
                {
                    "event_id": "sess_x:7:2:run_status",
                    "payload": {
                        "type": "run_status",
                        "task_id": 1,
                        "run_id": 7,
                        "status": "done",
                        "event_id": "sess_x:7:2:run_status",
                    },
                },
            ]
        }
        result = _invoke(
            ["ask", "测试"],
            responses={
                "/agent/command/stream": stream_events,
                "/agent/runs/7/events": replay_payload,
            },
        )
        self.assertEqual(result.exit_code, 0)
        self.assertIn("补齐 1 条事件", result.output)

    def test_task_execute_stream_replay_events_can_recover_done(self):
        stream_events = [
            SseEvent(
                event="message",
                data='{"type":"run_created","task_id":9,"run_id":7}',
                json_data={"type": "run_created", "task_id": 9, "run_id": 7},
            ),
        ]
        replay_payload = {
            "items": [
                {
                    "event_id": "task_exec:9:7:2:run_status",
                    "payload": {
                        "type": "run_status",
                        "task_id": 9,
                        "run_id": 7,
                        "status": "done",
                        "event_id": "task_exec:9:7:2:run_status",
                    },
                },
                {
                    "event_id": "task_exec:9:7:3:stream_end",
                    "payload": {
                        "type": "stream_end",
                        "task_id": 9,
                        "run_id": 7,
                        "run_status": "done",
                        "event_id": "task_exec:9:7:3:stream_end",
                    },
                },
            ]
        }
        result = _invoke(
            ["task", "execute", "9"],
            responses={
                "/tasks/9/execute/stream": stream_events,
                "/agent/runs/7/events": replay_payload,
            },
        )
        self.assertEqual(result.exit_code, 0)
        self.assertIn("补齐 2 条事件", result.output)

    def test_task_execute_stream_without_done_fails(self):
        events = [SseEvent(event="message", data='{"delta":"running"}', json_data={"delta": "running"})]
        result = _invoke(["task", "execute", "9"], responses={"/tasks/9/execute/stream": events})
        self.assertEqual(result.exit_code, 1)
        self.assertIn("未收到 done/stream_end 事件", result.output)

    def test_ask_stream_renders_search_events(self):
        events = [
            SseEvent(
                event="message",
                data='{"type":"search_progress","stage":"search_query","query":"黄金 价格 元/克"}',
                json_data={"type": "search_progress", "stage": "search_query", "query": "黄金 价格 元/克"},
            ),
            SseEvent(
                event="message",
                data='{"type":"search_candidates","total_candidates":2,"candidates":[{"host":"data.example.com","url":"https://data.example.com/gold.csv","initial_score":8}]}',
                json_data={
                    "type": "search_candidates",
                    "total_candidates": 2,
                    "candidates": [{"host": "data.example.com", "url": "https://data.example.com/gold.csv", "initial_score": 8}],
                },
            ),
            SseEvent(
                event="message",
                data='{"type":"search_rejected","total_rejected":1,"rejected":[{"host":"forum.example.com","reason":"low_relevance","detail":"缺少日期信号"}]}',
                json_data={
                    "type": "search_rejected",
                    "total_rejected": 1,
                    "rejected": [{"host": "forum.example.com", "reason": "low_relevance", "detail": "缺少日期信号"}],
                },
            ),
            SseEvent(
                event="message",
                data='{"type":"search_selected","selected":{"host":"data.example.com","url":"https://data.example.com/gold.csv","score":28,"evidence":["required_fields=2","units=1"]}}',
                json_data={
                    "type": "search_selected",
                    "selected": {
                        "host": "data.example.com",
                        "url": "https://data.example.com/gold.csv",
                        "score": 28,
                        "evidence": ["required_fields=2", "units=1"],
                    },
                },
            ),
            SseEvent(event="done", data='{"type":"stream_end","run_status":"done"}', json_data={"type": "stream_end", "run_status": "done"}),
        ]
        result = _invoke(["ask", "测试"], responses={"/agent/command/stream": events})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("搜索", result.output)
        self.assertIn("候选", result.output)
        self.assertIn("拒绝", result.output)
        self.assertIn("命中", result.output)
        self.assertIn("data.example.com", result.output)

    def test_ask_stream_renders_search_events_with_markup_like_detail(self):
        events = [
            SseEvent(
                event="message",
                data='{"type":"search_rejected","total_rejected":1,"rejected":[{"host":"unsafe.example.com","reason":"cloudflare","detail":"[blocked] <script>alert(1)</script> [retry]"}]}',
                json_data={
                    "type": "search_rejected",
                    "total_rejected": 1,
                    "rejected": [
                        {
                            "host": "unsafe.example.com",
                            "reason": "cloudflare",
                            "detail": "[blocked] <script>alert(1)</script> [retry]",
                        }
                    ],
                },
            ),
            SseEvent(event="done", data='{"type":"stream_end","run_status":"done"}', json_data={"type": "stream_end", "run_status": "done"}),
        ]
        result = _invoke(["ask", "测试"], responses={"/agent/command/stream": events})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("unsafe.example.com", result.output)
        self.assertIn("[blocked]", result.output)


class TestConfigCommands(unittest.TestCase):
    """config 命令组测试。"""

    def test_config_show(self):
        result = _invoke(
            ["config", "show"],
            responses={"/config": {"tray_enabled": True, "pet_enabled": True}},
        )
        self.assertEqual(result.exit_code, 0)

    def test_config_llm(self):
        result = _invoke(
            ["config", "llm"],
            responses={"/config/llm": {"provider": "openai", "model": "gpt-4o"}},
        )
        self.assertEqual(result.exit_code, 0)


if __name__ == "__main__":
    unittest.main()
