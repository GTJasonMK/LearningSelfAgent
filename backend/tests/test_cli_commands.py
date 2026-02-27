# -*- coding: utf-8 -*-
"""CLI 命令集成测试（使用 mock ApiClient）。"""

import json
import unittest
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from backend.src.cli.client import ApiClient, CliError
from backend.src.cli.main import cli


class _MockClient:
    """模拟 ApiClient，按路径返回预设响应。"""

    def __init__(self, responses: dict | None = None):
        self._responses = responses or {}

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


def _invoke(args: list, responses: dict | None = None, catch_exceptions: bool = False):
    """辅助函数：使用 CliRunner 调用 CLI 并注入 mock client。"""
    runner = CliRunner()
    mock_client = _MockClient(responses)

    def inject_client(ctx):
        ctx.ensure_object(dict)
        ctx.obj["client"] = mock_client
        ctx.obj["output_json"] = "--json" in args

    # 注入 mock client 到 click context
    with patch.object(ApiClient, "__init__", lambda self, **kw: None):
        with patch.object(ApiClient, "get", mock_client.get):
            with patch.object(ApiClient, "post", mock_client.post):
                with patch.object(ApiClient, "patch", mock_client.patch):
                    with patch.object(ApiClient, "delete", mock_client.delete):
                        result = runner.invoke(cli, args, catch_exceptions=catch_exceptions)
    return result


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
        result = _invoke(["task", "list"], responses={"/tasks": {"tasks": tasks}})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("测试任务", result.output)

    def test_task_create(self):
        result = _invoke(["task", "create", "新任务"], responses={"/tasks": {"id": 5}})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("5", result.output)

    def test_task_summary(self):
        result = _invoke(
            ["task", "summary"],
            responses={"/tasks/summary": {"total": 10, "done": 5}},
        )
        self.assertEqual(result.exit_code, 0)


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
            "memories": [{"id": 1, "content": "Python 记忆"}],
            "skills": [],
            "graph_nodes": [],
        }
        result = _invoke(["search", "Python"], responses={"/search": results})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Python", result.output)

    def test_search_no_results(self):
        results = {"memories": [], "skills": [], "graph_nodes": []}
        result = _invoke(["search", "不存在"], responses={"/search": results})
        self.assertEqual(result.exit_code, 0)
        self.assertIn("未找到", result.output)


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
