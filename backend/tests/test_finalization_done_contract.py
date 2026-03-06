import unittest
from unittest.mock import AsyncMock, patch

from backend.src.constants import RUN_STATUS_DONE, RUN_STATUS_FAILED


class TestFinalizationDoneContract(unittest.TestCase):
    def test_enforce_done_visible_output_contract_downgrades_missing_text_output(self):
        from backend.src.agent.runner.finalization_pipeline import enforce_done_visible_output_contract

        async def _run_case():
            emitted = []
            with patch(
                "backend.src.agent.runner.finalization_pipeline._has_text_task_output",
                return_value=False,
            ), patch(
                "backend.src.agent.runner.finalization_pipeline.safe_write_debug",
                return_value=None,
            ), patch(
                "backend.src.agent.runner.finalization_pipeline.asyncio.to_thread",
                new=AsyncMock(return_value=False),
            ):
                status = await enforce_done_visible_output_contract(
                    run_status=RUN_STATUS_DONE,
                    task_id=1,
                    run_id=2,
                    yield_func=lambda chunk: emitted.append(str(chunk)),
                )
            self.assertEqual(status, RUN_STATUS_FAILED)
            self.assertTrue(any("missing_visible_task_output" in chunk for chunk in emitted))

        import asyncio

        asyncio.run(_run_case())

    def test_check_and_report_missing_artifacts_downgrades_done(self):
        from backend.src.agent.runner.finalization_pipeline import check_and_report_missing_artifacts

        async def _run_case():
            emitted = []
            with patch(
                "backend.src.agent.runner.finalization_pipeline.check_missing_artifacts",
                return_value=["out.csv"],
            ), patch(
                "backend.src.agent.runner.finalization_pipeline.safe_write_debug",
                return_value=None,
            ):
                status = await check_and_report_missing_artifacts(
                    run_status=RUN_STATUS_DONE,
                    plan_artifacts=["out.csv"],
                    workdir=".",
                    task_id=1,
                    run_id=2,
                    yield_func=lambda chunk: emitted.append(str(chunk)),
                )
            self.assertEqual(status, RUN_STATUS_FAILED)
            self.assertTrue(any("missing_artifacts" in chunk for chunk in emitted))

        import asyncio

        asyncio.run(_run_case())


if __name__ == "__main__":
    unittest.main()
