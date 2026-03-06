import json
import unittest
from unittest.mock import patch


class TestReplanGroundingContract(unittest.TestCase):
    def test_replan_retries_when_plan_writes_multiple_scripts_before_first_sample(self):
        from backend.src.agent.planning_phase import run_replan_phase

        prompts = []
        invalid_plan = json.dumps(
            {
                "plan": [
                    {
                        "title": "file_write:backend/.agent/workspace/fetch_gold.py 写抓取脚本",
                        "brief": "写抓取",
                        "allow": ["file_write"],
                    },
                    {
                        "title": "file_write:backend/.agent/workspace/parse_gold.py 写解析脚本",
                        "brief": "写解析",
                        "allow": ["file_write"],
                    },
                    {
                        "title": "shell_command:执行抓取脚本获取样本",
                        "brief": "取样本",
                        "allow": ["shell_command"],
                    },
                    {
                        "title": "shell_command:执行解析脚本生成CSV",
                        "brief": "生成CSV",
                        "allow": ["shell_command"],
                    },
                    {
                        "title": "file_write:gold_prices.csv 写入CSV",
                        "brief": "写CSV",
                        "allow": ["file_write"],
                    },
                    {
                        "title": "task_output:输出结果",
                        "brief": "输出结果",
                        "allow": ["task_output"],
                    },
                ],
                "artifacts": ["gold_prices.csv"],
            },
            ensure_ascii=False,
        )
        valid_plan = json.dumps(
            {
                "plan": [
                    {
                        "title": "file_write:backend/.agent/workspace/fetch_gold.py 写抓取脚本",
                        "brief": "写抓取",
                        "allow": ["file_write"],
                    },
                    {
                        "title": "shell_command:执行抓取脚本获取样本",
                        "brief": "取样本",
                        "allow": ["shell_command"],
                    },
                    {
                        "title": "file_write:backend/.agent/workspace/parse_gold.py 写解析脚本",
                        "brief": "写解析",
                        "allow": ["file_write"],
                    },
                    {
                        "title": "shell_command:执行解析脚本生成CSV",
                        "brief": "生成CSV",
                        "allow": ["shell_command"],
                    },
                    {
                        "title": "file_write:gold_prices.csv 写入CSV",
                        "brief": "写CSV",
                        "allow": ["file_write"],
                    },
                    {
                        "title": "task_output:输出结果",
                        "brief": "输出结果",
                        "allow": ["task_output"],
                    },
                ],
                "artifacts": ["gold_prices.csv"],
            },
            ensure_ascii=False,
        )

        def _fake_create_llm_call(payload):
            prompts.append(str(payload.get("prompt") or ""))
            response = invalid_plan if len(prompts) == 1 else valid_plan
            return {
                "record": {
                    "id": len(prompts),
                    "status": "success",
                    "response": response,
                }
            }

        with patch("backend.src.agent.planning_phase.create_llm_call", side_effect=_fake_create_llm_call):
            gen = run_replan_phase(
                task_id=1,
                run_id=1,
                message="请你帮我收集最近三个月的黄金价格数据，单位元/克，并保存为csv文件",
                workdir=".",
                model="deepseek-chat",
                parameters={"temperature": 0},
                max_steps=8,
                tools_hint="(无)",
                skills_hint="(无)",
                solutions_hint="(无)",
                memories_hint="(无)",
                graph_hint="(无)",
                plan_titles=["tool_call:web_fetch 抓取黄金价格数据源", "task_output:输出结果"],
                plan_artifacts=["gold_prices.csv"],
                done_steps=[],
                error="[code=low_relevance_candidates] 候选页弱相关",
                observations=["tool_call:web_fetch 抓取黄金价格数据源: FAIL [code=low_relevance_candidates] 候选页弱相关"],
                context={},
            )
            try:
                while True:
                    next(gen)
            except StopIteration as exc:
                result = exc.value

        self.assertEqual(len(prompts), 2)
        self.assertIn("真实样本状态", prompts[0])
        self.assertIn("sample_available=no", prompts[0])
        self.assertIn("剩余计划不合法", prompts[1])
        self.assertIn("最多只允许一个抓取/读取脚本 file_write", prompts[1])
        self.assertEqual(result.plan_titles[0], "file_write:backend/.agent/workspace/fetch_gold.py 写抓取脚本")
        self.assertEqual(result.plan_titles[1], "shell_command:执行抓取脚本获取样本")
        self.assertIn("file_write:gold_prices.csv 写入CSV", result.plan_titles)

    def test_replan_retries_when_source_failure_without_grounded_url_starts_with_http_request(self):
        from backend.src.agent.planning_phase import run_replan_phase

        prompts = []
        invalid_plan = json.dumps(
            {
                "plan": [
                    {
                        "title": "http_request:查询黄金价格API",
                        "brief": "抓公开API",
                        "allow": ["http_request"],
                    },
                    {
                        "title": "task_output:输出结果",
                        "brief": "输出结果",
                        "allow": ["task_output"],
                    },
                ],
                "artifacts": [],
            },
            ensure_ascii=False,
        )
        valid_plan = json.dumps(
            {
                "plan": [
                    {
                        "title": "tool_call:web_fetch 继续发现来源",
                        "brief": "继续找源",
                        "allow": ["tool_call"],
                    },
                    {
                        "title": "http_request:抓取已发现来源",
                        "brief": "抓取来源",
                        "allow": ["http_request"],
                    },
                    {
                        "title": "task_output:输出结果",
                        "brief": "输出结果",
                        "allow": ["task_output"],
                    },
                ],
                "artifacts": [],
            },
            ensure_ascii=False,
        )

        def _fake_create_llm_call(payload):
            prompts.append(str(payload.get("prompt") or ""))
            response = invalid_plan if len(prompts) == 1 else valid_plan
            return {
                "record": {
                    "id": len(prompts),
                    "status": "success",
                    "response": response,
                }
            }

        with patch("backend.src.agent.planning_phase.create_llm_call", side_effect=_fake_create_llm_call):
            gen = run_replan_phase(
                task_id=1,
                run_id=1,
                message="请你帮我收集最近三个月的黄金价格数据，单位元/克，并保存为csv文件",
                workdir=".",
                model="deepseek-chat",
                parameters={"temperature": 0},
                max_steps=8,
                tools_hint="(无)",
                skills_hint="(无)",
                solutions_hint="(无)",
                memories_hint="(无)",
                graph_hint="(无)",
                plan_titles=["tool_call:web_fetch 抓取黄金价格数据源", "task_output:输出结果"],
                plan_artifacts=[],
                done_steps=[],
                error="[code=low_relevance_candidates] 候选页弱相关",
                observations=["tool_call:web_fetch 抓取黄金价格数据源: FAIL [code=low_relevance_candidates] 候选页弱相关"],
                context={},
            )
            try:
                while True:
                    next(gen)
            except StopIteration as exc:
                result = exc.value

        self.assertEqual(len(prompts), 2)
        self.assertIn("首个抓取步骤不能直接是 http_request", prompts[1])
        self.assertEqual(result.plan_titles[0], "tool_call:web_fetch 继续发现来源")
        self.assertEqual(result.plan_titles[1], "http_request:抓取已发现来源")


    def test_replan_retries_when_parser_script_appears_before_first_sample(self):
        from backend.src.agent.planning_phase import run_replan_phase

        prompts = []
        invalid_plan = json.dumps(
            {
                "plan": [
                    {
                        "title": "file_write:backend/.agent/workspace/parse_gold.py 写解析脚本",
                        "brief": "写解析",
                        "allow": ["file_write"],
                    },
                    {
                        "title": "tool_call:web_fetch 继续发现来源",
                        "brief": "继续找源",
                        "allow": ["tool_call"],
                    },
                    {
                        "title": "task_output:输出结果",
                        "brief": "输出结果",
                        "allow": ["task_output"],
                    },
                ],
                "artifacts": [],
            },
            ensure_ascii=False,
        )
        valid_plan = json.dumps(
            {
                "plan": [
                    {
                        "title": "tool_call:web_fetch 继续发现来源",
                        "brief": "继续找源",
                        "allow": ["tool_call"],
                    },
                    {
                        "title": "file_write:backend/.agent/workspace/parse_gold.py 写解析脚本",
                        "brief": "写解析",
                        "allow": ["file_write"],
                    },
                    {
                        "title": "task_output:输出结果",
                        "brief": "输出结果",
                        "allow": ["task_output"],
                    },
                ],
                "artifacts": [],
            },
            ensure_ascii=False,
        )

        def _fake_create_llm_call(payload):
            prompts.append(str(payload.get("prompt") or ""))
            response = invalid_plan if len(prompts) == 1 else valid_plan
            return {
                "record": {
                    "id": len(prompts),
                    "status": "success",
                    "response": response,
                }
            }

        with patch("backend.src.agent.planning_phase.create_llm_call", side_effect=_fake_create_llm_call):
            gen = run_replan_phase(
                task_id=1,
                run_id=1,
                message="请你帮我收集最近三个月的黄金价格数据，单位元/克，并保存为csv文件",
                workdir=".",
                model="deepseek-chat",
                parameters={"temperature": 0},
                max_steps=8,
                tools_hint="(无)",
                skills_hint="(无)",
                solutions_hint="(无)",
                memories_hint="(无)",
                graph_hint="(无)",
                plan_titles=["tool_call:web_fetch 抓取黄金价格数据源", "task_output:输出结果"],
                plan_artifacts=[],
                done_steps=[],
                error="[code=low_relevance_candidates] 候选页弱相关",
                observations=["tool_call:web_fetch 抓取黄金价格数据源: FAIL [code=low_relevance_candidates] 候选页弱相关"],
                context={},
            )
            try:
                while True:
                    next(gen)
            except StopIteration as exc:
                result = exc.value

        self.assertEqual(len(prompts), 2)
        self.assertIn("只允许抓取/读取类脚本", prompts[1])
        self.assertEqual(result.plan_titles[0], "tool_call:web_fetch 继续发现来源")
        self.assertEqual(result.plan_titles[1], "file_write:backend/.agent/workspace/parse_gold.py 写解析脚本")


    def test_replan_retries_when_plan_switches_unit_family(self):
        from backend.src.agent.planning_phase import run_replan_phase

        prompts = []
        invalid_plan = json.dumps(
            {
                "plan": [
                    {
                        "title": "tool_call:web_fetch 搜索 gold price historical data USD/oz",
                        "brief": "搜美元金价",
                        "allow": ["tool_call"],
                    },
                    {
                        "title": "task_output:输出结果",
                        "brief": "输出结果",
                        "allow": ["task_output"],
                    },
                ],
                "artifacts": [],
            },
            ensure_ascii=False,
        )
        valid_plan = json.dumps(
            {
                "plan": [
                    {
                        "title": "tool_call:web_fetch 搜索黄金价格 元/克 最近三个月 CSV",
                        "brief": "继续找源",
                        "allow": ["tool_call"],
                    },
                    {
                        "title": "task_output:输出结果",
                        "brief": "输出结果",
                        "allow": ["task_output"],
                    },
                ],
                "artifacts": [],
            },
            ensure_ascii=False,
        )

        def _fake_create_llm_call(payload):
            prompts.append(str(payload.get("prompt") or ""))
            response = invalid_plan if len(prompts) == 1 else valid_plan
            return {
                "record": {
                    "id": len(prompts),
                    "status": "success",
                    "response": response,
                }
            }

        with patch("backend.src.agent.planning_phase.create_llm_call", side_effect=_fake_create_llm_call):
            gen = run_replan_phase(
                task_id=1,
                run_id=1,
                message="请你帮我收集最近三个月的黄金价格数据，单位元/克，并保存为csv文件",
                workdir=".",
                model="deepseek-chat",
                parameters={"temperature": 0},
                max_steps=8,
                tools_hint="(无)",
                skills_hint="(无)",
                solutions_hint="(无)",
                memories_hint="(无)",
                graph_hint="(无)",
                plan_titles=["tool_call:web_fetch 抓取黄金价格数据源", "task_output:输出结果"],
                plan_artifacts=[],
                done_steps=[],
                error="[code=low_relevance_candidates] 候选页弱相关",
                observations=["tool_call:web_fetch 抓取黄金价格数据源: FAIL [code=low_relevance_candidates] 候选页弱相关"],
                context={},
            )
            try:
                while True:
                    next(gen)
            except StopIteration as exc:
                result = exc.value

        self.assertEqual(len(prompts), 2)
        self.assertIn("原任务不可变约束", prompts[0])
        self.assertIn("unit=元/克", prompts[0])
        self.assertIn("剩余计划改写了任务单位口径", prompts[1])
        self.assertIn("不得改写原任务的单位", prompts[1])
        self.assertEqual(result.plan_titles[0], "tool_call:web_fetch 搜索黄金价格 元/克 最近三个月 CSV")



if __name__ == "__main__":
    unittest.main()
