import unittest


class TestRoutesConfigActionsSchema(unittest.TestCase):
    def test_get_actions_schema_contains_one_of(self):
        try:
            from backend.src.api.system.routes_config import get_actions_schema
        except ModuleNotFoundError as exc:
            if str(exc).find("fastapi") >= 0:
                self.skipTest("fastapi 未安装，跳过路由导入测试")
            raise

        result = get_actions_schema()
        schema = result.get("schema") if isinstance(result, dict) else None
        self.assertTrue(isinstance(schema, dict))
        one_of = schema.get("oneOf") if isinstance(schema, dict) else None
        self.assertTrue(isinstance(one_of, list))
        self.assertGreater(len(one_of), 0)


if __name__ == "__main__":
    unittest.main()
