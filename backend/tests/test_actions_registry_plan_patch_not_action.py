import unittest


class TestActionRegistryPlanPatchNotAction(unittest.TestCase):
    def test_plan_patch_action_type_returns_structured_error(self):
        from backend.src.actions.registry import validate_action_object
        from backend.src.common.task_error_codes import extract_task_error_code

        err = validate_action_object({"action": {"type": "plan_patch", "payload": {"step_index": 2}}})
        self.assertIsInstance(err, str)
        self.assertEqual(extract_task_error_code(err), "plan_patch_not_action")


if __name__ == "__main__":
    unittest.main()

