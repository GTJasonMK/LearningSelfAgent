import importlib.util
import sys
import types
import unittest
from unittest.mock import patch

HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None


class _RowLikeNoGet:
    """模拟 sqlite3.Row：支持下标访问，不提供 dict.get。"""

    def __init__(self, data: dict):
        self._data = dict(data or {})

    def __getitem__(self, key):
        return self._data[key]


def _ensure_fastapi_stub_for_tests() -> None:
    if HAS_FASTAPI:
        return
    if "fastapi" in sys.modules and "fastapi.responses" in sys.modules:
        return

    fastapi_mod = types.ModuleType("fastapi")

    class _APIRouter:
        def post(self, *_args, **_kwargs):
            def _decorator(func):
                return func

            return _decorator

    fastapi_mod.APIRouter = _APIRouter  # type: ignore[attr-defined]

    responses_mod = types.ModuleType("fastapi.responses")

    class _StreamingResponse:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    responses_mod.StreamingResponse = _StreamingResponse  # type: ignore[attr-defined]

    sys.modules.setdefault("fastapi", fastapi_mod)
    sys.modules.setdefault("fastapi.responses", responses_mod)

    if "pydantic" not in sys.modules:
        pydantic_mod = types.ModuleType("pydantic")

        class _BaseModel:
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)

        def _field(*_args, **kwargs):
            return kwargs.get("default", None)

        pydantic_mod.BaseModel = _BaseModel  # type: ignore[attr-defined]
        pydantic_mod.Field = _field  # type: ignore[attr-defined]
        sys.modules.setdefault("pydantic", pydantic_mod)


class TaskExecuteWaitingStateTests(unittest.TestCase):
    def test_build_step_error_event_contains_structured_alias_fields(self):
        _ensure_fastapi_stub_for_tests()
        from backend.src.api.tasks import routes_task_execute as mod

        payload = mod._build_step_error_event(
            task_id=1,
            run_id=2,
            step_row={"id": 3, "step_order": 4},
            action_type="shell_command",
            message="执行失败",
            recoverable=False,
        )

        self.assertEqual(str(payload.get("type") or ""), "step_error")
        self.assertEqual(str(payload.get("code") or ""), "task_step_failed")
        self.assertEqual(str(payload.get("error_code") or ""), "task_step_failed")
        self.assertEqual(str(payload.get("message") or ""), "执行失败")
        self.assertEqual(str(payload.get("error_message") or ""), "执行失败")
        self.assertEqual(str(payload.get("phase") or ""), "task_execute_step")
        self.assertFalse(bool(payload.get("recoverable")))
        self.assertFalse(bool(payload.get("retryable")))

    def test_execute_emits_run_created_and_done_status_events(self):
        _ensure_fastapi_stub_for_tests()
        from backend.src.api.tasks import routes_task_execute as mod

        with patch.object(mod, "get_task_repo", return_value={"id": 1, "title": "t"}), \
                patch.object(mod, "create_task_run_record", return_value=(201, None, None)), \
                patch.object(mod, "list_task_steps_for_task", return_value=[]), \
                patch.object(mod, "update_task_run_repo") as mock_update_run, \
                patch.object(mod, "update_task_repo") as mock_update_task, \
                patch.object(mod, "get_task_run", return_value={"id": 201, "status": "done"}), \
                patch.object(mod, "task_run_from_row", side_effect=lambda row: row), \
                patch("backend.src.services.tasks.task_postprocess.postprocess_task_run", return_value=(None, None, None)):
            gen = mod._execute_task_with_messages(task_id=1, payload=None)
            events = []
            try:
                while True:
                    events.append(next(gen))
            except StopIteration as stop:
                result = stop.value

        self.assertTrue(
            any(
                isinstance(msg, dict)
                and str(msg.get("type") or "") == "run_created"
                and int(msg.get("run_id") or 0) == 201
                for msg in events
            )
        )
        self.assertTrue(
            any(
                isinstance(msg, dict)
                and str(msg.get("type") or "") == "run_status"
                and str(msg.get("status") or "") == "running"
                for msg in events
            )
        )
        self.assertTrue(
            any(
                isinstance(msg, dict)
                and str(msg.get("type") or "") == "run_status"
                and str(msg.get("status") or "") == "done"
                for msg in events
            )
        )
        mock_update_run.assert_called_with(run_id=201, status="done", updated_at=unittest.mock.ANY)
        mock_update_task.assert_called_with(task_id=1, status="done", updated_at=unittest.mock.ANY)
        self.assertIsNone(result.get("eval"))
        self.assertIsNone(result.get("skill"))
        self.assertIsNone(result.get("graph_update"))

    def test_execute_keeps_waiting_status_when_waiting_steps_exist(self):
        _ensure_fastapi_stub_for_tests()
        from backend.src.api.tasks import routes_task_execute as mod

        waiting_step = {
            "id": 11,
            "status": "waiting",
            "title": "user_prompt: 请补充信息",
            "detail": "{}",
        }

        with patch.object(mod, "get_task_repo", return_value={"id": 1, "title": "t"}), \
                patch.object(mod, "create_task_run_record", return_value=(101, None, None)), \
                patch.object(mod, "list_task_steps_for_task", return_value=[waiting_step]), \
                patch.object(mod, "update_task_run_repo") as mock_update_run, \
                patch.object(mod, "update_task_repo") as mock_update_task, \
                patch.object(mod, "get_task_run", return_value={"id": 101, "status": "waiting"}), \
                patch.object(mod, "task_run_from_row", side_effect=lambda row: row), \
                patch("backend.src.services.tasks.task_postprocess.postprocess_task_run") as mock_postprocess:
            gen = mod._execute_task_with_messages(task_id=1, payload=None)
            events = []
            try:
                while True:
                    events.append(next(gen))
            except StopIteration as stop:
                result = stop.value

        self.assertTrue(any("waiting 步骤" in str(msg) for msg in events))
        self.assertTrue(
            any(isinstance(msg, dict) and str(msg.get("type") or "") == "run_created" for msg in events)
        )
        self.assertTrue(
            any(
                isinstance(msg, dict)
                and str(msg.get("type") or "") == "run_status"
                and str(msg.get("status") or "") == "waiting"
                for msg in events
            )
        )
        mock_update_run.assert_called_with(run_id=101, status="waiting", updated_at=unittest.mock.ANY)
        mock_update_task.assert_called_with(task_id=1, status="waiting", updated_at=unittest.mock.ANY)
        mock_postprocess.assert_not_called()
        self.assertIsNone(result.get("eval"))
        self.assertIsNone(result.get("skill"))
        self.assertIsNone(result.get("graph_update"))

    def test_execute_waiting_status_supports_row_like_without_get(self):
        _ensure_fastapi_stub_for_tests()
        from backend.src.api.tasks import routes_task_execute as mod

        waiting_step = _RowLikeNoGet(
            {
                "id": 11,
                "status": "waiting",
                "title": "user_prompt: 请补充信息",
                "detail": "{}",
            }
        )

        with patch.object(mod, "get_task_repo", return_value={"id": 1, "title": "t"}), \
                patch.object(mod, "create_task_run_record", return_value=(102, None, None)), \
                patch.object(mod, "list_task_steps_for_task", return_value=[waiting_step]), \
                patch.object(mod, "update_task_run_repo") as mock_update_run, \
                patch.object(mod, "update_task_repo") as mock_update_task, \
                patch.object(mod, "get_task_run", return_value={"id": 102, "status": "waiting"}), \
                patch.object(mod, "task_run_from_row", side_effect=lambda row: row), \
                patch("backend.src.services.tasks.task_postprocess.postprocess_task_run") as mock_postprocess:
            gen = mod._execute_task_with_messages(task_id=1, payload=None)
            events = []
            try:
                while True:
                    events.append(next(gen))
            except StopIteration:
                pass

        self.assertTrue(any("waiting 步骤" in str(msg) for msg in events))
        self.assertTrue(
            any(
                isinstance(msg, dict)
                and str(msg.get("type") or "") == "run_status"
                and str(msg.get("status") or "") == "waiting"
                for msg in events
            )
        )
        mock_update_run.assert_called_with(run_id=102, status="waiting", updated_at=unittest.mock.ANY)
        mock_update_task.assert_called_with(task_id=1, status="waiting", updated_at=unittest.mock.ANY)
        mock_postprocess.assert_not_called()


if __name__ == "__main__":
    unittest.main()
