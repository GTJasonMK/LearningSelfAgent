import sqlite3
import unittest
from unittest.mock import AsyncMock, patch


class TestExecutionPipelineFailedOutput(unittest.IsolatedAsyncioTestCase):
    async def test_ensure_failed_task_output_injects_when_text_missing(self):
        from backend.src.agent.runner.execution_pipeline import RUN_STATUS_FAILED, ensure_failed_task_output

        emitted: list[str] = []

        def _emit(message: str) -> None:
            emitted.append(str(message))

        async def _fake_to_thread(func, *args, **kwargs):
            return func(*args, **kwargs)

        with patch(
            "backend.src.agent.runner.execution_pipeline._has_text_task_output",
            return_value=False,
        ), patch(
            "backend.src.agent.runner.execution_pipeline._build_failed_task_output_content",
            return_value="【失败总结】\n- 结论：失败",
        ), patch(
            "backend.src.agent.runner.execution_pipeline.create_task_output",
            return_value=(123, "2026-02-10T00:00:00+00:00"),
        ) as mocked_create, patch(
            "backend.src.agent.runner.execution_pipeline.safe_write_debug",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.execution_pipeline.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            await ensure_failed_task_output(1, 2, RUN_STATUS_FAILED, _emit)

        mocked_create.assert_called_once()
        kwargs = mocked_create.call_args.kwargs
        self.assertEqual(int(kwargs.get("task_id")), 1)
        self.assertEqual(int(kwargs.get("run_id")), 2)
        self.assertEqual(str(kwargs.get("output_type") or ""), "text")
        self.assertTrue(any("失败总结" in line for line in emitted))

    async def test_ensure_failed_task_output_skips_when_text_exists(self):
        from backend.src.agent.runner.execution_pipeline import RUN_STATUS_FAILED, ensure_failed_task_output

        with patch(
            "backend.src.agent.runner.execution_pipeline._has_text_task_output",
            return_value=True,
        ), patch(
            "backend.src.agent.runner.execution_pipeline.create_task_output",
        ) as mocked_create:
            await ensure_failed_task_output(1, 2, RUN_STATUS_FAILED, lambda _msg: None)

        mocked_create.assert_not_called()

    async def test_handle_execution_exception_calls_failed_output_injection(self):
        from backend.src.agent.runner.execution_pipeline import RUN_STATUS_FAILED, handle_execution_exception

        async def _fake_to_thread(func, *args, **kwargs):
            return func(*args, **kwargs)

        ensure_mock = AsyncMock(return_value=None)
        with patch(
            "backend.src.agent.runner.execution_pipeline.mark_run_failed",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.execution_pipeline.ensure_failed_task_output",
            ensure_mock,
        ), patch(
            "backend.src.agent.runner.execution_pipeline.enqueue_postprocess_thread",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.execution_pipeline.safe_write_debug",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.execution_pipeline.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            await handle_execution_exception(RuntimeError("boom"), 10, 20, lambda _msg: None)

        ensure_mock.assert_awaited_once()
        args = ensure_mock.await_args.args
        self.assertEqual(int(args[0]), 10)
        self.assertEqual(int(args[1]), 20)
        self.assertEqual(str(args[2]), str(RUN_STATUS_FAILED))


    def test_build_failed_step_lines_supports_sqlite_row(self):
        from backend.src.agent.runner.execution_pipeline import _build_failed_step_lines

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE steps (step_order INTEGER, title TEXT, status TEXT, error TEXT, result TEXT)"
        )
        cur.execute(
            "INSERT INTO steps (step_order, title, status, error, result) VALUES (?, ?, ?, ?, ?)",
            (5, "json_parse:解析", "failed", "boom", ""),
        )
        row = cur.execute("SELECT step_order, title, status, error, result FROM steps").fetchone()

        with patch(
            "backend.src.agent.runner.execution_pipeline.list_task_steps_for_run",
            return_value=[row],
        ):
            lines = _build_failed_step_lines(1, 2)

        conn.close()

        self.assertEqual(len(lines), 1)
        self.assertIn("step#5", lines[0])
        self.assertIn("boom", lines[0])


if __name__ == "__main__":
    unittest.main()
