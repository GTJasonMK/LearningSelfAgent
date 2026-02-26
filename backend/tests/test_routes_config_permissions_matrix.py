import unittest
from unittest.mock import patch


class TestRoutesConfigPermissionsMatrix(unittest.TestCase):
    def test_get_permissions_matrix_returns_compiled_matrix(self):
        try:
            from backend.src.api.system.routes_config import get_permissions_matrix
        except ModuleNotFoundError as exc:
            if "fastapi" in str(exc):
                self.skipTest("fastapi 未安装，跳过路由导入测试")
            raise

        fake_matrix = {
            "version": 1,
            "ops": {"write": True, "execute": False},
            "allowed_paths": ["/tmp/workspace"],
            "disabled_actions": ["file_delete"],
            "disabled_tools": ["shell_command"],
        }
        with patch(
            "backend.src.api.system.routes_config.get_permission_policy_matrix",
            return_value=fake_matrix,
        ) as mocked:
            result = get_permissions_matrix()

        mocked.assert_called_once()
        self.assertEqual({"matrix": fake_matrix}, result)


if __name__ == "__main__":
    unittest.main()
