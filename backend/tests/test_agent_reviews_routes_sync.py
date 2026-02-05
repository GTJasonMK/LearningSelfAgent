import inspect
import unittest


class TestAgentReviewsRoutesSync(unittest.TestCase):
    def test_agent_reviews_routes_are_sync(self):
        """
        回归：/agent/reviews 相关接口主要做 SQLite 读取（同步），保持为 sync def
        以避免 async 路由在事件循环里直接执行阻塞 IO。
        """
        from backend.src.api.agent.routes_agent_reviews import get_agent_review, list_agent_reviews

        self.assertFalse(inspect.iscoroutinefunction(list_agent_reviews))
        self.assertFalse(inspect.iscoroutinefunction(get_agent_review))


if __name__ == "__main__":
    unittest.main()

