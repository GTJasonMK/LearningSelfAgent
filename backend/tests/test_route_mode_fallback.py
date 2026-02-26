import unittest
from unittest.mock import patch


class _RoutePayload:
    def __init__(self, message: str, model: str = "", parameters: dict | None = None):
        self.message = message
        self.model = model
        self.parameters = parameters or {}


class TestRouteModeFallback(unittest.TestCase):
    def test_route_fallback_prefers_do_for_external_fetch_when_llm_fails(self):
        from backend.src.agent.runner.route_mode import route_agent_mode

        payload = _RoutePayload(message="请抓取 https://example.com 的最新价格并整理为表格")
        with patch("backend.src.agent.runner.route_mode.call_openai", return_value=("", None, "provider_unavailable")):
            result = route_agent_mode(payload)

        self.assertEqual(result.get("mode"), "do")
        self.assertIn("heuristic:url_or_external_task", str(result.get("reason") or ""))
        self.assertIn("llm=provider_unavailable", str(result.get("reason") or ""))

    def test_route_fallback_prefers_think_for_analysis_task(self):
        from backend.src.agent.runner.route_mode import route_agent_mode

        payload = _RoutePayload(message="请先分析系统架构方案并给出权衡与取舍")
        with patch("backend.src.agent.runner.route_mode.call_openai", return_value=("not-json", None, None)):
            result = route_agent_mode(payload)

        self.assertEqual(result.get("mode"), "think")
        self.assertIn("heuristic:analysis_task", str(result.get("reason") or ""))

    def test_route_fallback_defaults_chat_for_small_talk(self):
        from backend.src.agent.runner.route_mode import route_agent_mode

        payload = _RoutePayload(message="你好呀，今天心情怎么样")
        with patch("backend.src.agent.runner.route_mode.call_openai", return_value=("", None, "timeout")):
            result = route_agent_mode(payload)

        self.assertEqual(result.get("mode"), "chat")
        self.assertIn("heuristic:default_chat", str(result.get("reason") or ""))


if __name__ == "__main__":
    unittest.main()
