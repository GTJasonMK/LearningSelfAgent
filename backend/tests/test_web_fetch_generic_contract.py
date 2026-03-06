import json
import unittest
from unittest.mock import patch


class TestWebFetchGenericContract(unittest.TestCase):
    def test_normalize_web_fetch_protocol_keeps_non_gold_task_generic(self):
        from backend.src.actions.handlers.tool_call import _normalize_web_fetch_protocol

        protocol = _normalize_web_fetch_protocol(
            {
                "search_queries": ["最近30天 BTC 价格 USD 历史 数据 CSV"],
            },
            fallback_query="最近30天 BTC 价格 USD 历史 数据 CSV",
            source="llm",
        )

        merged_queries = " ".join(protocol.get("search_queries") or [])
        merged_signals = " ".join(protocol.get("target_signals") or [])
        self.assertIn("BTC", merged_queries)
        self.assertNotIn("黄金", merged_queries)
        self.assertNotIn("gold", merged_signals.lower())
        self.assertIn("date", protocol.get("required_fields") or [])
        self.assertTrue(any(item in (protocol.get("required_fields") or []) for item in ["price", "value"]))

    def test_analyze_web_fetch_candidate_content_does_not_force_unit_when_protocol_has_no_unit(self):
        from backend.src.actions.handlers.tool_call import _analyze_web_fetch_candidate_content

        protocol = {
            "required_fields": ["date", "value"],
            "target_signals": ["BTC", "价格"],
            "time_hints": ["最近30天", "日度"],
            "unit_hints": [],
            "negative_terms": ["论坛", "help", "schema"],
            "require_structured": True,
        }
        sample = "date,value\n2026-02-01,97000\n2026-02-02,97250\nBTC 历史价格"

        result = _analyze_web_fetch_candidate_content(
            url="https://data.example.com/btc/history.csv",
            context_text="BTC 历史数据 CSV",
            output_text=sample,
            protocol=protocol,
            query_keywords=["BTC", "历史", "CSV"],
        )

        self.assertTrue(bool(result.get("acceptable")))
        self.assertNotIn("缺少单位信号", list(result.get("rejections") or []))

    def test_search_result_fallback_keeps_context_and_filters_irrelevant_ads(self):
        from backend.src.actions.handlers.tool_call import _extract_web_fetch_link_records_from_text

        html = """
        <html><body>
          <div class="result">
            <a href="https://www.sge.com.cn/data/history">上海黄金交易所历史行情</a>
            <p>SGE 官方历史价格 JSON API 数据下载</p>
          </div>
          <div class="ad">
            <a href="https://www.zillow.com/homedetails/123">Houston Home</a>
            <p>Buy home in Texas today</p>
          </div>
        </body></html>
        """

        records = _extract_web_fetch_link_records_from_text(
            html,
            exclude_hosts=set(),
            exclude_host_families={"bing.com"},
            query="上海黄金交易所 SGE 官方价格 历史数据 JSON API",
            force_search_result_page=True,
        )

        urls = [str(item.get("url") or "") for item in records]
        self.assertEqual(urls, ["https://www.sge.com.cn/data/history"])
        self.assertIn("SGE", str(records[0].get("context_text") or ""))
        self.assertGreater(int(records[0].get("keyword_hits") or 0), 0)

    def test_react_step_prompt_forbids_guessing_api_without_observation(self):
        from backend.src.agent.runner.react_helpers import build_react_step_prompt

        prompt = build_react_step_prompt(
            workdir=".",
            message="抓取最近三个月黄金价格并保存为 CSV",
            plan='["http_request:抓取", "task_output:输出"]',
            step_index=1,
            step_title="http_request:抓取",
            allowed_actions="http_request",
            observations="- web_fetch: FAIL [code=low_relevance_candidates]",
            recent_source_failures="- code=low_relevance_candidates recent_count=1",
            graph="(无)",
            tools="(无)",
            skills="(无)",
            memories="(无)",
            now_utc="2026-03-07T00:00:00Z",
        )

        self.assertIn("不要凭记忆猜 API", prompt)

    def test_replan_prompt_forbids_inventing_new_api_url_before_source_discovery(self):
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
                    ),
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
            except StopIteration:
                pass

        prompt = str(captured.get("prompt") or "")
        self.assertIn("不要直接编造新的 API URL 或站点路径", prompt)
        self.assertIn("不要把猜测 URL 放进首个抓取步骤", prompt)

    def test_react_step_prompt_includes_task_grounding_constraints(self):
        from backend.src.agent.runner.react_helpers import build_react_step_prompt

        prompt = build_react_step_prompt(
            workdir=".",
            message="请你帮我收集最近三个月的黄金价格数据，单位元/克，并保存为csv文件",
            plan='["tool_call:web_fetch 搜索黄金价格", "task_output:输出结果"]',
            step_index=1,
            step_title="tool_call:web_fetch 搜索黄金价格",
            allowed_actions="tool_call",
            observations="- web_fetch: FAIL [code=low_relevance_candidates]",
            recent_source_failures="- code=low_relevance_candidates recent_count=1",
            graph="(无)",
            tools="(无)",
            skills="(无)",
            memories="(无)",
            now_utc="2026-03-07T00:00:00Z",
        )

        self.assertIn("原任务不可变约束", prompt)
        self.assertIn("unit=元/克", prompt)
        self.assertIn("time_range=最近三个月", prompt)
        self.assertIn("output_file_type=csv", prompt)
        self.assertIn("不得改写用户要求的目标对象、单位、时间范围和最终产物格式", prompt)



if __name__ == "__main__":
    unittest.main()
