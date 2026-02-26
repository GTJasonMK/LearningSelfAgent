import unittest


class TestPermissionPolicyMatrix(unittest.TestCase):
    def test_compile_permission_policy_matrix_normalizes_and_sorts(self):
        from backend.src.services.permissions.permissions_store import compile_permission_policy_matrix

        matrix = compile_permission_policy_matrix(
            allowed_ops=["write", "execute", "", "write"],
            allowed_paths=[" /tmp/a ", "", "/tmp/b"],
            disabled_actions=[" task_output ", "file_write", "task_output"],
            disabled_tools=[" shell_command", "", "http_request", "shell_command"],
        )

        self.assertEqual(1, matrix.get("version"))
        self.assertTrue(isinstance(matrix.get("compiled_at"), str))
        self.assertEqual(True, matrix.get("ops", {}).get("write"))
        self.assertEqual(True, matrix.get("ops", {}).get("execute"))
        self.assertEqual(["/tmp/a", "/tmp/b"], matrix.get("allowed_paths"))
        self.assertEqual(["file_write", "task_output"], matrix.get("disabled_actions"))
        self.assertEqual(["http_request", "shell_command"], matrix.get("disabled_tools"))

    def test_compile_permission_policy_matrix_handles_missing_ops(self):
        from backend.src.services.permissions.permissions_store import compile_permission_policy_matrix

        matrix = compile_permission_policy_matrix(
            allowed_ops=["write"],
            allowed_paths=[],
            disabled_actions=[],
            disabled_tools=[],
        )

        self.assertEqual(True, matrix.get("ops", {}).get("write"))
        self.assertEqual(False, matrix.get("ops", {}).get("execute"))


if __name__ == "__main__":
    unittest.main()
