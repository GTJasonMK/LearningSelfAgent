import unittest
from unittest.mock import patch

from backend.src.actions.handlers.task_output import execute_task_output


class TestTaskOutputAuthenticityGuard(unittest.TestCase):
    def test_keep_missing_url_claim_without_evidence(self):
        rows = [
            {
                "id": 1,
                "status": "done",
                "title": "shell_command:验证脚本",
                "detail": '{"type":"shell_command","payload":{"command":"python web_fetch.py"}}',
                "result": '{"stdout":"ok","stderr":"","returncode":0,"ok":true}',
            }
        ]

        content = "脚本验证完成。\n无参数调用时脚本按预期输出错误信息。"
        with patch("backend.src.actions.handlers.task_output.list_task_steps_for_run", return_value=rows), patch(
            "backend.src.actions.handlers.task_output._create_task_output_record",
            side_effect=lambda task_id, payload: ({"content": payload.get("content")}, None),
        ):
            output, error_message = execute_task_output(
                task_id=1,
                run_id=1,
                payload={"output_type": "text", "content": content},
                context={
                    "enforce_task_output_evidence": True,
                    "enforce_csv_artifact_quality": False,
                },
                step_row={"id": 99},
            )

        self.assertIsNone(error_message)
        final_text = str(output.get("content") or "")
        self.assertIn("无参数调用时脚本按预期输出错误信息", final_text)
        self.assertNotIn("[真实性校验]", final_text)

    def test_keep_missing_url_claim_when_auto_retry_evidence_exists(self):
        rows = [
            {
                "id": 1,
                "status": "done",
                "title": "shell_command:验证web_fetch脚本（无参数）",
                "detail": '{"type":"shell_command","payload":{"command":"python web_fetch.py"}}',
                "result": (
                    '{"stdout":"<html>ok</html>","stderr":"","returncode":0,"ok":true,'
                    '"auto_retry":{"trigger":"missing_url","fallback_url":"https://example.com",'
                    '"initial_stderr":"ERROR: No URL provided","initial_stdout":"","initial_returncode":1}}'
                ),
            }
        ]

        content = "脚本验证完成。\n无参数调用时脚本按预期输出错误信息。"
        with patch("backend.src.actions.handlers.task_output.list_task_steps_for_run", return_value=rows), patch(
            "backend.src.actions.handlers.task_output._create_task_output_record",
            side_effect=lambda task_id, payload: ({"content": payload.get("content")}, None),
        ):
            output, error_message = execute_task_output(
                task_id=1,
                run_id=1,
                payload={"output_type": "text", "content": content},
                context={
                    "enforce_task_output_evidence": True,
                    "enforce_csv_artifact_quality": False,
                },
                step_row={"id": 99},
            )

        self.assertIsNone(error_message)
        final_text = str(output.get("content") or "")
        self.assertIn("无参数调用时脚本按预期输出错误信息", final_text)
        self.assertNotIn("[真实性校验]", final_text)
        self.assertIn("shell auto_retry=missing_url", final_text)


if __name__ == "__main__":
    unittest.main()
