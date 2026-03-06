import json
import unittest
from unittest.mock import patch


class TestReplanFeedbackPrompt(unittest.TestCase):
    def test_run_replan_phase_includes_feedback_and_retry_requirements(self):
        from backend.src.agent.planning_phase import run_replan_phase

        captured = {}

        def _fake_create_llm_call(payload):
            captured["prompt"] = str(payload.get("prompt") or "")
            return {
                "record": {
                    "id": 1,
                    "status": "success",
                    "response": json.dumps(
                        {
                            "plan": [
                                {
                                    "title": "tool_call:web_fetch 抓取备用来源",
                                    "brief": "换源抓取",
                                    "allow": ["tool_call"],
                                },
                                {
                                    "title": "task_output: 输出结果",
                                    "brief": "输出结果",
                                    "allow": ["task_output"],
                                },
                            ],
                            "artifacts": [],
                        },
                        ensure_ascii=False,
                    ),
                }
            }

        failure_signatures = {
            "step_feedback_history": [
                {
                    "step_order": 1,
                    "title": "tool_call:web_fetch 搜索黄金价格",
                    "action_type": "tool_call",
                    "failure_class": "source_unavailable",
                    "summary_for_llm": "status=failed；goal_progress=none；must_change=source_selection,query_strategy",
                }
            ],
            "pending_retry_requirements": {
                "active": True,
                "failure_class": "source_unavailable",
                "must_change": ["source_selection", "query_strategy"],
                "retry_constraints": ["禁止继续使用同一外部源。"],
                "primary_error": "source_http_timeout",
            },
        }

        with patch("backend.src.agent.planning_phase.create_llm_call", side_effect=_fake_create_llm_call):
            gen = run_replan_phase(
                task_id=1,
                run_id=1,
                message="请你帮我收集最近三个月的黄金价格数据，单位元/克，并保存为csv文件",
                workdir=".",
                model="gpt-5.2",
                parameters={"temperature": 0},
                max_steps=4,
                tools_hint="(无)",
                skills_hint="(无)",
                solutions_hint="(无)",
                memories_hint="(无)",
                graph_hint="(无)",
                plan_titles=["tool_call:web_fetch 搜索黄金价格", "task_output: 输出结果"],
                plan_artifacts=[],
                done_steps=[],
                error="source_http_timeout",
                observations=["tool_call:web_fetch 搜索黄金价格: FAIL source_http_timeout"],
                failure_signatures=failure_signatures,
            )
            try:
                while True:
                    next(gen)
            except StopIteration as exc:
                result = exc.value

        self.assertIsNotNone(result)
        prompt = str(captured.get("prompt") or "")
        self.assertIn("最近步骤反馈", prompt)
        self.assertIn("source_unavailable", prompt)
        self.assertIn("当前重试约束", prompt)
        self.assertIn("失败修复策略提示", prompt)
        self.assertIn("真实样本状态", prompt)
        self.assertIn("sample_available=no", prompt)
        self.assertIn("禁止继续使用同一外部源", prompt)


if __name__ == "__main__":
    unittest.main()
