import json
import unittest
from urllib.parse import parse_qs, unquote_plus, urlparse
from unittest.mock import patch


class TestToolCallWebFetchProtocol(unittest.TestCase):
    def test_web_fetch_generates_protocol_first_and_caches_to_context(self):
        from backend.src.actions.handlers.tool_call import (
            _WEB_FETCH_PROTOCOL_CONTEXT_KEY,
            execute_tool_call,
        )

        step_row = {"id": 1, "title": "tool_call:web_fetch 抓取黄金数据"}
        payload = {
            "tool_name": "web_fetch",
            "tool_metadata": {
                "exec": {"type": "shell", "command": "echo ok", "workdir": "/tmp"},
            },
            "input": "最近三个月 黄金 元/克",
            "output": "",
        }
        context = {"message": "请收集最近三个月黄金元/克并保存为 CSV"}

        with patch("backend.src.actions.handlers.tool_call.is_tool_enabled", return_value=True), patch(
            "backend.src.actions.handlers.tool_call.get_tool_by_name",
            return_value={"id": 1, "name": "web_fetch", "metadata": "{}"},
        ), patch(
            "backend.src.actions.handlers.tool_call._enforce_tool_exec_script_dependency",
            return_value=None,
        ), patch(
            "backend.src.actions.handlers.tool_call.create_llm_call",
            return_value={
                "record": {
                    "status": "success",
                    "response": (
                        '{"objective":"收集最近三个月黄金价格（元/克）","search_queries":["最近三个月 黄金 价格 元/克 日线 JSON API"],'
                        '"required_columns":["date","price_cny_per_gram"],"deny_domains":["youtube.com"],"require_structured":true}'
                    ),
                }
            },
        ), patch(
            "backend.src.actions.handlers.tool_call._execute_web_fetch_with_fallback",
            return_value={
                "ok": True,
                "output_text": '{"rows":[{"date":"2026-03-01","price_cny_per_gram":681.2}]}',
                "warnings": [],
                "attempts": [],
                "error_code": "",
                "error_message": "",
            },
        ) as mock_fetch, patch(
            "backend.src.actions.handlers.tool_call._create_tool_record",
            return_value={
                "record": {
                    "tool_id": 1,
                    "tool_name": "web_fetch",
                    "input": payload["input"],
                    "output": '{"rows":[{"date":"2026-03-01","price_cny_per_gram":681.2}]}',
                }
            },
        ):
            record, error = execute_tool_call(
                task_id=1,
                run_id=1,
                step_row=step_row,
                payload=payload,
                context=context,
            )

        self.assertIsNone(error)
        self.assertIsInstance(record, dict)
        self.assertIn(_WEB_FETCH_PROTOCOL_CONTEXT_KEY, context)
        cached = context.get(_WEB_FETCH_PROTOCOL_CONTEXT_KEY) or {}
        self.assertEqual("llm", str(cached.get("source") or ""))
        self.assertGreater(len(cached.get("search_queries") or []), 0)
        self.assertIn("protocol", record)
        self.assertEqual("llm", str((record.get("protocol") or {}).get("source") or ""))
        self.assertEqual(1, mock_fetch.call_count)
        called_protocol = mock_fetch.call_args.kwargs.get("protocol")
        self.assertIsInstance(called_protocol, dict)
        self.assertEqual("llm", str(called_protocol.get("source") or ""))

    def test_web_fetch_protocol_cache_reuses_same_intent(self):
        from backend.src.actions.handlers.tool_call import _ensure_web_fetch_protocol

        context = {"message": "请收集最近三个月黄金元/克并保存为 CSV"}
        step_row = {"id": 1, "title": "tool_call:web_fetch 抓取黄金数据"}

        with patch(
            "backend.src.actions.handlers.tool_call.create_llm_call",
            return_value={
                "record": {
                    "status": "success",
                    "response": (
                        '{"objective":"收集最近三个月黄金价格（元/克）","search_queries":["最近三个月 黄金 价格 元/克 日线 JSON API"],'
                        '"required_columns":["date","price_cny_per_gram"],"require_structured":true}'
                    ),
                }
            },
        ) as mock_llm:
            first, warnings1 = _ensure_web_fetch_protocol(
                task_id=1,
                run_id=1,
                step_row=step_row,
                tool_input="最近三个月 黄金 元/克",
                context=context,
            )
            second, warnings2 = _ensure_web_fetch_protocol(
                task_id=1,
                run_id=1,
                step_row=step_row,
                tool_input="最近三个月 黄金 元/克",
                context=context,
            )

        self.assertEqual(mock_llm.call_count, 1)
        self.assertEqual(warnings1, [])
        self.assertEqual(warnings2, [])
        self.assertEqual(str(first.get("intent_key") or ""), str(second.get("intent_key") or ""))

    def test_web_fetch_protocol_cache_invalidates_when_intent_changes(self):
        from backend.src.actions.handlers.tool_call import (
            _WEB_FETCH_PROTOCOL_CONTEXT_KEY,
            _ensure_web_fetch_protocol,
        )

        context = {"message": "请收集最近三个月黄金元/克并保存为 CSV"}
        generic_step = {"id": 1, "title": "tool_call:web_fetch 抓取黄金数据"}
        sge_step = {"id": 2, "title": "tool_call:web_fetch 抓取上海黄金交易所数据"}

        with patch(
            "backend.src.actions.handlers.tool_call.create_llm_call",
            side_effect=[
                {
                    "record": {
                        "status": "success",
                        "response": (
                            '{"objective":"收集最近三个月黄金价格（元/克）","search_queries":["最近三个月 黄金 价格 元/克 日线 JSON API"],'
                            '"required_columns":["date","price_cny_per_gram"],"require_structured":true}'
                        ),
                    }
                },
                {
                    "record": {
                        "status": "success",
                        "response": (
                            '{"objective":"优先查找上海黄金交易所历史数据","search_queries":["上海黄金交易所 Au99.99 历史数据 CSV"],'
                            '"required_columns":["date","price_cny_per_gram"],"require_structured":true}'
                        ),
                    }
                },
            ],
        ) as mock_llm:
            first, warnings1 = _ensure_web_fetch_protocol(
                task_id=1,
                run_id=1,
                step_row=generic_step,
                tool_input="最近三个月 黄金 元/克",
                context=context,
            )
            second, warnings2 = _ensure_web_fetch_protocol(
                task_id=1,
                run_id=1,
                step_row=sge_step,
                tool_input="上海黄金交易所 Au99.99 价格 历史数据 CSV",
                context=context,
            )

        self.assertEqual(mock_llm.call_count, 2)
        self.assertEqual(warnings1, [])
        self.assertEqual(warnings2, [])
        self.assertNotEqual(str(first.get("intent_key") or ""), str(second.get("intent_key") or ""))
        self.assertIn("上海黄金交易所", " ".join(second.get("search_queries") or []))
        cached = context.get(_WEB_FETCH_PROTOCOL_CONTEXT_KEY) or {}
        self.assertEqual(str(cached.get("intent_key") or ""), str(second.get("intent_key") or ""))

    def test_web_fetch_protocol_fallback_when_llm_failed(self):
        from backend.src.actions.handlers.tool_call import (
            _WEB_FETCH_PROTOCOL_CONTEXT_KEY,
            execute_tool_call,
        )

        step_row = {"id": 1, "title": "tool_call:web_fetch 抓取黄金数据"}
        payload = {
            "tool_name": "web_fetch",
            "tool_metadata": {
                "exec": {"type": "shell", "command": "echo ok", "workdir": "/tmp"},
            },
            "input": "最近三个月 黄金 元/克",
            "output": "",
        }
        context = {"message": "请收集最近三个月黄金元/克并保存为 CSV"}

        with patch("backend.src.actions.handlers.tool_call.is_tool_enabled", return_value=True), patch(
            "backend.src.actions.handlers.tool_call.get_tool_by_name",
            return_value={"id": 1, "name": "web_fetch", "metadata": "{}"},
        ), patch(
            "backend.src.actions.handlers.tool_call._enforce_tool_exec_script_dependency",
            return_value=None,
        ), patch(
            "backend.src.actions.handlers.tool_call.create_llm_call",
            side_effect=RuntimeError("llm unavailable"),
        ), patch(
            "backend.src.actions.handlers.tool_call._execute_web_fetch_with_fallback",
            return_value={
                "ok": True,
                "output_text": '{"rows":[]}',
                "warnings": [],
                "attempts": [],
                "error_code": "",
                "error_message": "",
            },
        ), patch(
            "backend.src.actions.handlers.tool_call._create_tool_record",
            return_value={
                "record": {
                    "tool_id": 1,
                    "tool_name": "web_fetch",
                    "input": payload["input"],
                    "output": '{"rows":[]}',
                }
            },
        ):
            record, error = execute_tool_call(
                task_id=1,
                run_id=1,
                step_row=step_row,
                payload=payload,
                context=context,
            )

        self.assertIsNone(error)
        self.assertIsInstance(record, dict)
        self.assertIn("warnings", record)
        self.assertTrue(any("协议生成失败" in str(item) for item in (record.get("warnings") or [])))
        cached = context.get(_WEB_FETCH_PROTOCOL_CONTEXT_KEY) or {}
        self.assertEqual("fallback", str(cached.get("source") or ""))

    def test_web_fetch_protocol_denied_domain_is_skipped(self):
        from backend.src.actions.handlers.tool_call import _execute_web_fetch_with_fallback

        protocol = {
            "version": 1,
            "source": "llm",
            "objective": "x",
            "search_queries": ["x"],
            "required_columns": ["date", "price_cny_per_gram"],
            "deny_domains": ["forbidden.example.com"],
            "require_structured": True,
        }

        with patch(
            "backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec"
        , return_value=("", "network blocked")) as mock_exec:
            result = _execute_web_fetch_with_fallback(
                {"command": "echo ok", "workdir": "/tmp"},
                "https://forbidden.example.com/path",
                protocol=protocol,
            )

        self.assertFalse(bool(result.get("ok")))
        self.assertGreaterEqual(mock_exec.call_count, 0)
        attempts = list(result.get("attempts") or [])
        self.assertGreater(len(attempts), 0)
        self.assertTrue(
            any(str(item.get("error_code") or "") == "protocol_domain_denied" for item in attempts)
        )

    def test_web_fetch_mixed_url_and_keywords_falls_back_to_keyword_search(self):
        from backend.src.actions.handlers.tool_call import _execute_web_fetch_with_fallback

        protocol = {
            "version": 1,
            "source": "llm",
            "objective": "x",
            "search_queries": ["最近三个月 黄金 价格 元/克 数据源"],
            "required_columns": ["date", "price_cny_per_gram"],
            "deny_domains": [],
            "require_structured": True,
        }

        with patch(
            "backend.src.actions.handlers.tool_call._execute_web_fetch_keyword_search",
            return_value={
                "ok": True,
                "output_text": '{"rows":[{"date":"2026-03-01","price_cny_per_gram":681.2}]}',
                "warnings": [],
                "attempts": [],
                "error_code": "",
                "error_message": "",
            },
        ) as mock_keyword_search:
            result = _execute_web_fetch_with_fallback(
                {"command": "echo ok", "workdir": "/tmp"},
                "https://dataexchangerate-api.com/ CNY USD exchange rate API free JSON",
                protocol=protocol,
            )

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(1, mock_keyword_search.call_count)

    def test_web_fetch_resolve_queries_prioritizes_direct_tool_input(self):
        from backend.src.actions.handlers.tool_call import _resolve_protocol_search_queries

        protocol = {
            "search_queries": [
                "黄金 价格 元/克 人民币/克 日度 daily",
                "gold price cny gram historical daily table csv",
            ]
        }

        queries = _resolve_protocol_search_queries(
            "上海黄金交易所 Au99.99 价格 历史数据 CSV",
            protocol,
        )

        self.assertGreater(len(queries), 0)
        self.assertEqual(queries[0], "上海黄金交易所 Au99.99 价格 历史数据 CSV")

    def test_web_fetch_normalize_url_candidate_rejects_invalid_host(self):
        from backend.src.actions.handlers.tool_call import _normalize_web_fetch_url_candidate

        self.assertEqual("", _normalize_web_fetch_url_candidate('https://","http://'))
        self.assertEqual("", _normalize_web_fetch_url_candidate("https://"))
        self.assertEqual("https://www.baidu.com", _normalize_web_fetch_url_candidate("https://www.baidu.com"))

    def test_web_fetch_protocol_query_sanitizes_url_token(self):
        from backend.src.actions.handlers.tool_call import _normalize_web_fetch_protocol

        protocol = _normalize_web_fetch_protocol(
            {
                "search_queries": [
                    "https://www.sge.com.cn/sjzx/mrhqsj 中国黄金协会 上海金基准价 Au99.99 日度 数据 元/克 CSV 下载"
                ],
                "required_columns": ["date", "price_cny_per_gram"],
            },
            fallback_query="最近三个月 黄金 价格 元/克",
            source="llm",
        )
        queries = list(protocol.get("search_queries") or [])
        self.assertGreater(len(queries), 0)
        self.assertNotIn("https://www.sge.com.cn/sjzx/mrhqsj", " ".join(queries))
        self.assertTrue(any("中国黄金协会" in str(item) for item in queries))

    def test_web_fetch_url_candidates_failed_then_retry_keyword_search(self):
        from backend.src.actions.handlers.tool_call import _execute_web_fetch_with_fallback

        protocol = {
            "version": 1,
            "source": "llm",
            "objective": "x",
            "search_queries": ["最近三个月 黄金 价格 元/克 结构化 数据"],
            "required_columns": ["date", "price_cny_per_gram"],
            "deny_domains": [],
            "require_structured": True,
        }

        with patch(
            "backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec",
            return_value=("", "403 forbidden"),
        ), patch(
            "backend.src.actions.handlers.tool_call._execute_web_fetch_keyword_search",
            return_value={
                "ok": True,
                "output_text": '{"rows":[{"date":"2026-03-01","price_cny_per_gram":681.2}]}',
                "warnings": ["keyword_search_ok"],
                "attempts": [{"stage": "search", "status": "ok", "host": "www.bing.com"}],
                "error_code": "",
                "error_message": "",
            },
        ) as mock_keyword_search:
            result = _execute_web_fetch_with_fallback(
                {"command": "echo ok", "workdir": "/tmp"},
                "https://open.er-api.com/v6/timeseries?start_date=2025-12-04&end_date=2026-03-03&base=USD&symbols=CNY",
                protocol=protocol,
            )

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(1, mock_keyword_search.call_count)
        self.assertTrue(any("关键词检索" in str(item) for item in (result.get("warnings") or [])))
        attempts = list(result.get("attempts") or [])
        self.assertGreater(len(attempts), 1)

    def test_web_fetch_protocol_adds_extended_search_fields(self):
        from backend.src.actions.handlers.tool_call import _normalize_web_fetch_protocol

        protocol = _normalize_web_fetch_protocol(
            {
                "search_queries": ["最近三个月 黄金 价格 元/克 JSON API"],
                "required_fields": ["date", "price_cny_per_gram"],
                "target_signals": ["黄金", "价格", "元/克"],
            },
            fallback_query="最近三个月 黄金 价格 元/克",
            source="llm",
        )

        self.assertIn("required_fields", protocol)
        self.assertIn("target_signals", protocol)
        self.assertIn("negative_terms", protocol)
        self.assertIn("unit_hints", protocol)
        self.assertGreaterEqual(len(protocol.get("search_queries") or []), 1)

    def test_rerank_web_fetch_candidates_with_llm_prefers_content_page(self):
        from backend.src.actions.handlers.tool_call import _rerank_web_fetch_candidates_with_llm

        candidates = [
            {
                "url": "https://tv.example.com/search?q=gold",
                "host": "tv.example.com",
                "query": "黄金价格 元/克",
                "context_text": "视频搜索结果页",
                "initial_score": 8,
                "signals": {"search_like": True, "keyword_hits": 2},
            },
            {
                "url": "https://data.example.com/gold/history.csv",
                "host": "data.example.com",
                "query": "黄金价格 元/克",
                "context_text": "黄金历史价格 CSV，含日期和元/克字段",
                "initial_score": 6,
                "signals": {"search_like": False, "keyword_hits": 3},
            },
            {
                "url": "https://news.example.com/gold",
                "host": "news.example.com",
                "query": "黄金价格 元/克",
                "context_text": "新闻解读文章",
                "initial_score": 5,
                "signals": {"search_like": False, "keyword_hits": 1},
            },
            {
                "url": "https://forum.example.com/thread/1",
                "host": "forum.example.com",
                "query": "黄金价格 元/克",
                "context_text": "论坛讨论帖",
                "initial_score": 4,
                "signals": {"search_like": False, "keyword_hits": 1},
            },
        ]

        with patch(
            "backend.src.actions.handlers.tool_call.create_llm_call",
            return_value={
                "record": {
                    "status": "success",
                    "response": json.dumps(
                        {
                            "selected_urls": [
                                "https://data.example.com/gold/history.csv",
                                "https://news.example.com/gold",
                            ],
                            "notes": ["优先真实数据页，搜索页靠后"],
                        },
                        ensure_ascii=False,
                    ),
                }
            },
        ):
            reordered, notes = _rerank_web_fetch_candidates_with_llm(
                tool_input="黄金价格 元/克",
                protocol={"objective": "抓取最近三个月黄金价格"},
                candidates=candidates,
                context={"task_id": 1, "run_id": 1},
            )

        self.assertEqual("https://data.example.com/gold/history.csv", str(reordered[0].get("url") or ""))
        self.assertTrue(bool(reordered[0].get("llm_selected")))
        self.assertEqual(1, int(reordered[0].get("llm_rank") or 0))
        self.assertIn("优先真实数据页", " ".join(notes))

    def test_web_fetch_keyword_search_selects_candidate_with_content_signals(self):
        from backend.src.actions.handlers.tool_call import _execute_web_fetch_keyword_search

        protocol = {
            "version": 1,
            "source": "llm",
            "search_queries": ["最近三个月 黄金 价格 元/克 数据"],
            "required_fields": ["date", "price_cny_per_gram"],
            "required_columns": ["date", "price_cny_per_gram"],
            "target_signals": ["黄金", "价格", "元/克"],
            "unit_hints": ["元/克", "人民币/克"],
            "negative_terms": ["论坛", "help", "schema"],
            "deny_domains": [],
            "require_structured": True,
        }
        search_page = (
            "https://news.example.com/forum/gold-talk\n"
            "https://data.example.com/gold/history.csv\n"
        )
        strong_candidate = 'date,price_cny_per_gram\n2026-01-01,620.5\n2026-01-02,621.8\n单位: 元/克'
        weak_candidate = '<html><body>黄金论坛讨论帖，只有观点没有数据</body></html>'
        calls = []

        def fake_exec(_exec_spec, url):
            calls.append(url)
            if "search" in url or "q=" in url or "wd=" in url:
                return search_page, None
            if "history.csv" in url:
                return strong_candidate, None
            if "forum" in url:
                return weak_candidate, None
            return "", "404"

        emitted = []
        context = {"event_sink": lambda payload: emitted.append(payload)}
        with patch("backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec", side_effect=fake_exec):
            result = _execute_web_fetch_keyword_search(
                {"command": "echo ok", "workdir": "/tmp"},
                "最近三个月 黄金 元/克",
                protocol=protocol,
                context=context,
            )

        self.assertTrue(bool(result.get("ok")))
        self.assertIn("history.csv", str((result.get("selected_candidate") or {}).get("url") or ""))
        self.assertTrue(any(str(item.get("type") or "") == "search_candidates" for item in emitted))
        self.assertTrue(any(str(item.get("type") or "") == "search_selected" for item in emitted))
        self.assertGreaterEqual(len(result.get("candidate_rankings") or []), 1)
        self.assertGreaterEqual(len(calls), 2)

    def test_extract_web_fetch_links_prefers_result_anchors_over_page_chrome_urls(self):
        from backend.src.actions.handlers.tool_call import _extract_web_fetch_links_from_text

        html = (
            '<html><body>'
            '<a href="https://login.example.com/signin">Sign in</a>'
            '<a href="https://data.example.com/gold/history.csv">最近三个月黄金价格数据 CSV</a>'
            '<a href="https://docs.example.com/schema">Schema</a>'
            '<script>var img="https://storage.example.com/users/0x1/myprofile/expressionprofile/profilephoto";</script>'
            '</body></html>'
        )

        links = _extract_web_fetch_links_from_text(
            html,
            exclude_hosts=set(),
            exclude_host_families=set(),
            query='最近三个月 黄金 价格 元/克 数据',
        )

        self.assertIn('https://data.example.com/gold/history.csv', links)
        self.assertNotIn('https://login.example.com/signin', links)
        self.assertNotIn('https://storage.example.com/users/0x1/myprofile/expressionprofile/profilephoto', links)

    def test_web_fetch_keyword_search_returns_low_relevance_when_candidates_weak(self):
        from backend.src.actions.handlers.tool_call import _execute_web_fetch_keyword_search

        protocol = {
            "version": 1,
            "source": "llm",
            "search_queries": ["最近三个月 黄金 价格 元/克 数据"],
            "required_fields": ["date", "price_cny_per_gram"],
            "required_columns": ["date", "price_cny_per_gram"],
            "target_signals": ["黄金", "价格", "元/克"],
            "unit_hints": ["元/克"],
            "negative_terms": ["论坛", "help", "schema"],
            "deny_domains": [],
            "require_structured": True,
        }
        search_page = "https://community.example.com/help/gold\nhttps://docs.example.com/schema/gold"
        emitted = []

        def fake_exec(_exec_spec, url):
            if "search" in url or "q=" in url or "wd=" in url:
                return search_page, None
            return "<html><body>论坛帮助文档，没有日期和价格数据</body></html>", None

        with patch("backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec", side_effect=fake_exec):
            result = _execute_web_fetch_keyword_search(
                {"command": "echo ok", "workdir": "/tmp"},
                "最近三个月 黄金 元/克",
                protocol=protocol,
                context={"event_sink": lambda payload: emitted.append(payload)},
            )

        self.assertFalse(bool(result.get("ok")))
        self.assertEqual("low_relevance_candidates", str(result.get("error_code") or ""))
        self.assertGreaterEqual(len(result.get("candidate_rejections") or []), 1)
        self.assertTrue(any(str(item.get("type") or "") == "search_rejected" for item in emitted))


    def test_web_fetch_candidate_analysis_rejects_account_like_page_even_with_dates(self):
        from backend.src.actions.handlers.tool_call import _analyze_web_fetch_candidate_content

        protocol = {
            "version": 1,
            "source": "llm",
            "required_fields": ["date", "price_cny_per_gram"],
            "required_columns": ["date", "price_cny_per_gram"],
            "target_signals": ["黄金", "价格", "元/克"],
            "unit_hints": ["元/克"],
            "negative_terms": ["help", "schema"],
            "deny_domains": [],
            "require_structured": True,
        }
        fake_account_page = (
            '<html><body>Sign in to your account. Privacy notice. Cookies. '
            'Updated: 2026-03-01. {"date":"2026-03-01","items":[]} '
            'profilephoto avatar mailbox all rights reserved.</body></html>'
        )

        analysis = _analyze_web_fetch_candidate_content(
            url='https://storage.example.com/users/0x1/myprofile/expressionprofile/profilephoto',
            context_text='最近三个月 黄金 价格 元/克 数据',
            output_text=fake_account_page,
            protocol=protocol,
            query_keywords=['黄金', '价格', '元/克', 'gold', 'price'],
        )

        self.assertFalse(bool(analysis.get('acceptable')))
        self.assertGreater(int(analysis.get('page_noise_hits') or 0), 0)
        self.assertTrue(any('账号/门户噪声信号' in str(item) for item in (analysis.get('rejections') or [])))

    def test_web_fetch_keyword_search_prefers_real_dataset_over_account_like_candidate(self):
        from backend.src.actions.handlers.tool_call import _execute_web_fetch_keyword_search

        protocol = {
            "version": 1,
            "source": "llm",
            "search_queries": ["最近三个月 黄金 价格 元/克 数据"],
            "required_fields": ["date", "price_cny_per_gram"],
            "required_columns": ["date", "price_cny_per_gram"],
            "target_signals": ["黄金", "价格", "元/克"],
            "unit_hints": ["元/克", "人民币/克"],
            "negative_terms": ["论坛", "help", "schema"],
            "deny_domains": [],
            "require_structured": True,
        }
        search_page = (
            'https://storage.example.com/users/0x1/myprofile/expressionprofile/profilephoto\n'
            'https://data.example.com/gold/history.csv\n'
        )
        bad_candidate = (
            '<html><body>Sign in to your account. Privacy notice. Cookies. '
            'Updated: 2026-03-01. {"date":"2026-03-01","items":[]} '
            'profilephoto avatar mailbox all rights reserved.</body></html>'
        )
        good_candidate = 'date,price_cny_per_gram\n2026-01-01,620.5\n2026-01-02,621.8\n单位: 元/克'

        def fake_exec(_exec_spec, url):
            if 'search' in url or 'q=' in url or 'wd=' in url:
                return search_page, None
            if 'history.csv' in url:
                return good_candidate, None
            if 'profilephoto' in url:
                return bad_candidate, None
            return '', '404'

        with patch('backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec', side_effect=fake_exec):
            result = _execute_web_fetch_keyword_search(
                {"command": "echo ok", "workdir": "/tmp"},
                "最近三个月 黄金 元/克",
                protocol=protocol,
                context={"event_sink": lambda _payload: None},
            )

        self.assertTrue(bool(result.get('ok')))
        self.assertIn('history.csv', str((result.get('selected_candidate') or {}).get('url') or ''))
        self.assertFalse(any('profilephoto' in str(item.get('url') or '') for item in (result.get('candidate_rankings') or [])))

    def test_web_fetch_candidate_analysis_accepts_semantic_table_without_internal_field_name(self):
        from backend.src.actions.handlers.tool_call import _analyze_web_fetch_candidate_content

        protocol = {
            "version": 1,
            "source": "llm",
            "required_fields": ["date", "price_cny_per_gram"],
            "required_columns": ["date", "price_cny_per_gram"],
            "target_signals": ["黄金", "价格", "元/克"],
            "unit_hints": ["元/克", "人民币/克", "CNY/g"],
            "negative_terms": ["论坛", "help", "schema"],
            "deny_domains": [],
            "require_structured": True,
        }
        html = (
            '<table><thead><tr><th>日期</th><th>黄金价格(元/克)</th></tr></thead>'
            '<tbody><tr><td>2026-02-01</td><td>684.12</td></tr>'
            '<tr><td>2026-02-02</td><td>685.08</td></tr></tbody></table>'
        )

        analysis = _analyze_web_fetch_candidate_content(
            url='https://data.example.com/gold/daily',
            context_text='最近三个月 黄金 价格 元/克 数据',
            output_text=html,
            protocol=protocol,
            query_keywords=['黄金', '价格', '元/克', 'gold', 'price'],
        )

        self.assertTrue(bool(analysis.get('acceptable')))
        self.assertGreaterEqual(int(analysis.get('required_hits') or 0), 2)
        field_evidence = analysis.get('required_field_evidence') or {}
        self.assertIn('date', field_evidence)
        self.assertIn('price_cny_per_gram', field_evidence)

    def test_web_fetch_keyword_search_selects_semantic_html_table_candidate(self):
        from backend.src.actions.handlers.tool_call import _execute_web_fetch_keyword_search

        protocol = {
            "version": 1,
            "source": "llm",
            "search_queries": ["最近三个月 黄金 价格 元/克 历史 数据 表格"],
            "required_fields": ["date", "price_cny_per_gram"],
            "required_columns": ["date", "price_cny_per_gram"],
            "target_signals": ["黄金", "价格", "元/克"],
            "unit_hints": ["元/克", "人民币/克", "CNY/g"],
            "negative_terms": ["论坛", "help", "schema"],
            "deny_domains": [],
            "require_structured": True,
        }
        search_page = 'https://data.example.com/gold/daily-table'
        candidate_page = (
            '<html><body><h1>最近三个月黄金价格</h1>'
            '<table><tr><th>日期</th><th>价格(元/克)</th></tr>'
            '<tr><td>2026-02-01</td><td>684.12</td></tr>'
            '<tr><td>2026-02-02</td><td>685.08</td></tr></table>'
            '</body></html>'
        )

        def fake_exec(_exec_spec, url):
            if 'search' in url or 'q=' in url or 'wd=' in url:
                return search_page, None
            if 'daily-table' in url:
                return candidate_page, None
            return '', '404'

        with patch('backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec', side_effect=fake_exec):
            result = _execute_web_fetch_keyword_search(
                {"command": "echo ok", "workdir": "/tmp"},
                "最近三个月 黄金 元/克",
                protocol=protocol,
                context={"event_sink": lambda _payload: None},
            )

        self.assertTrue(bool(result.get('ok')))
        self.assertIn('daily-table', str((result.get('selected_candidate') or {}).get('url') or ''))


    def test_extract_web_fetch_links_from_bing_blocks_ignores_footer_noise(self):
        from backend.src.actions.handlers.tool_call import _extract_web_fetch_links_from_text

        html = (
            '<div id="b_content"><ol id="b_results">'
            '<li class="b_algo"><h2><a href="https://www.bing.com/ck/a?u=a1aHR0cHM6Ly9kYXRhLmV4YW1wbGUuY29tL2dvbGQvaGlzdG9yeS5jc3Y">黄金历史价格数据</a></h2>'
            '<div class="b_caption"><p>最近三个月 黄金 价格 元/克 历史数据表格</p></div></li>'
            '</ol></div>'
            '<footer id="b_footer">'
            '<a href="https://beian.miit.gov.cn">京ICP备</a>'
            '<a href="https://beian.mps.gov.cn/#/query/webSearch?code=1">公网安备</a>'
            '</footer>'
        )

        links = _extract_web_fetch_links_from_text(
            html,
            exclude_hosts=set(),
            exclude_host_families=set(),
            query='黄金 价格 元/克 历史 数据 表格',
        )

        self.assertIn('https://data.example.com/gold/history.csv', links)
        self.assertNotIn('https://beian.miit.gov.cn', links)
        self.assertNotIn('https://beian.mps.gov.cn/#/query/webSearch?code=1', links)

    def test_extract_web_fetch_links_from_360_blocks_prefers_data_mdurl(self):
        from backend.src.actions.handlers.tool_call import _extract_web_fetch_links_from_text

        html = (
            '<ul class="result">'
            '<li class="res-list">'
            '<h3 class="res-title"><a href="https://www.so.com/link?m=wrapped" data-mdurl="https://data.example.com/gold/table">黄金历史价格一览表</a></h3>'
            '<div class="res-rich"><span class="res-list-summary">黄金 历史 价格 元/克 数据 表格</span></div>'
            '</li>'
            '</ul>'
        )

        links = _extract_web_fetch_links_from_text(
            html,
            exclude_hosts=set(),
            exclude_host_families=set(),
            query='黄金 价格 元/克 历史 数据 表格',
        )

        self.assertIn('https://data.example.com/gold/table', links)
        self.assertNotIn('https://www.so.com/link?m=wrapped', links)


    def test_web_fetch_candidate_analysis_rejects_article_like_single_date_page_for_historical_task(self):
        from backend.src.actions.handlers.tool_call import _analyze_web_fetch_candidate_content

        protocol = {
            "version": 1,
            "source": "fallback",
            "objective": "收集最近三个月黄金价格",
            "required_columns": ["date", "price_cny_per_gram"],
            "required_fields": ["date", "price_cny_per_gram"],
            "target_signals": ["黄金", "价格", "元/克"],
            "unit_hints": ["元/克"],
            "time_hints": ["最近三个月", "日度"],
            "require_structured": True,
        }
        article_like = (
            '<html><head><title>黄金价格新闻</title></head>'
            '<body><div>发布时间 2024-10-25 10:15:30</div>'
            '<p>今日黄金价格 806 元/克，市场分析如下。</p>'
            '<p>更多资讯请关注财经频道。</p></body></html>'
        )

        analysis = _analyze_web_fetch_candidate_content(
            url="https://example.com/news/gold-article",
            context_text="黄金价格 元/克 最近三个月",
            output_text=article_like,
            protocol=protocol,
            query_keywords=["黄金", "价格", "元/克", "最近三个月", "daily"],
        )

        self.assertFalse(bool(analysis.get("acceptable")))
        self.assertIn("历史结构化任务缺少多日期证据", list(analysis.get("rejections") or []))

    def test_web_fetch_candidate_analysis_accepts_multi_date_structured_csv(self):
        from backend.src.actions.handlers.tool_call import _analyze_web_fetch_candidate_content

        protocol = {
            "version": 1,
            "source": "fallback",
            "objective": "收集最近三个月黄金价格",
            "required_columns": ["date", "price_cny_per_gram"],
            "required_fields": ["date", "price_cny_per_gram"],
            "target_signals": ["黄金", "价格", "元/克"],
            "unit_hints": ["元/克"],
            "time_hints": ["最近三个月", "日度"],
            "require_structured": True,
        }
        csv_text = (
            "date,price_cny_per_gram\n"
            "2026-01-01,681.2 元/克\n"
            "2026-01-02,682.5 元/克\n"
            "2026-01-03,680.8 元/克\n"
        )

        analysis = _analyze_web_fetch_candidate_content(
            url="https://example.com/data/gold-history.csv",
            context_text="黄金价格 元/克 最近三个月",
            output_text=csv_text,
            protocol=protocol,
            query_keywords=["黄金", "价格", "元/克", "最近三个月", "daily"],
        )

        self.assertTrue(bool(analysis.get("acceptable")))
        self.assertGreaterEqual(int(analysis.get("distinct_date_hits") or 0), 2)
        self.assertGreaterEqual(int(analysis.get("date_price_pair_hits") or 0), 1)


    def test_web_fetch_protocol_preserves_llm_query_priority(self):
        from backend.src.actions.handlers.tool_call import _normalize_web_fetch_protocol

        protocol = _normalize_web_fetch_protocol(
            {
                "search_queries": [
                    "黄金 历史价格 元/克 CSV 日度",
                    "gold historical price cny per gram csv",
                    "黄金 历史价格 元/克 CSV 日度",
                ],
                "required_fields": ["date", "price_cny_per_gram"],
                "target_signals": ["黄金", "价格", "元/克"],
            },
            fallback_query="请你帮我收集最近三个月的黄金的价格数据，单位元/克，并保存为csv文件",
            source="llm",
        )

        queries = list(protocol.get("search_queries") or [])
        self.assertGreaterEqual(len(queries), 2)
        self.assertEqual("黄金 历史价格 元/克 CSV 日度", queries[0])
        self.assertEqual("gold historical price cny per gram csv", queries[1])

    def test_web_fetch_preview_decision_prompt_requires_preserving_unit_and_granularity(self):
        from backend.src.actions.handlers.tool_call import _build_web_fetch_preview_decision_prompt

        prompt = _build_web_fetch_preview_decision_prompt(
            tool_input="请你帮我收集最近三个月的黄金的价格数据，单位元/克，并保存为csv文件",
            protocol={
                "objective": "收集最近三个月黄金价格",
                "required_fields": ["date", "price_cny_per_gram"],
                "target_signals": ["黄金", "价格", "元/克"],
                "time_hints": ["最近三个月", "日度"],
                "unit_hints": ["元/克"],
            },
            queries=["黄金 历史价格 元/克 日度 CSV"],
            candidate_rankings=[],
            candidate_rejections=[],
        )

        self.assertIn("不要擅自把元/克改成美元/盎司", prompt)
        self.assertIn("unit_hints", prompt)

    def test_web_fetch_keyword_search_rewrites_queries_with_llm_feedback(self):
        from backend.src.actions.handlers.tool_call import _execute_web_fetch_keyword_search

        protocol = {
            "version": 2,
            "source": "llm",
            "objective": "收集最近三个月黄金价格并保存 CSV",
            "search_queries": ["黄金 价格 元/克"],
            "required_fields": ["date", "price_cny_per_gram"],
            "required_columns": ["date", "price_cny_per_gram"],
            "target_signals": ["黄金", "价格", "元/克"],
            "unit_hints": ["元/克"],
            "time_hints": ["最近三个月", "日度"],
            "negative_terms": ["论坛", "help", "schema"],
            "deny_domains": [],
            "require_structured": True,
        }

        weak_candidate = (
            '<html><body><h1>黄金市场资讯</h1><p>发布时间 2026-03-01</p>'
            '<p>今日黄金价格小幅波动，更多内容见正文。</p></body></html>'
        )
        strong_candidate = (
            'date,price_cny_per_gram\n'
            '2026-01-01,681.2 元/克\n'
            '2026-01-02,682.5 元/克\n'
            '2026-01-03,680.8 元/克\n'
        )
        emitted = []

        def fake_exec(_exec_spec, url):
            parsed = urlparse(url)
            query_text = ' '.join(
                unquote_plus(value)
                for values in parse_qs(parsed.query or '', keep_blank_values=False).values()
                for value in values
            )
            if parsed.netloc in {"www.bing.com", "cn.bing.com", "www.so.com", "www.baidu.com", "www.sogou.com"}:
                if "历史价格" in query_text or "csv" in query_text.lower() or "日度" in query_text:
                    return 'https://data.example.com/gold-history.csv', None
                return 'https://news.example.com/gold-article', None
            if 'gold-article' in url:
                return weak_candidate, None
            if 'gold-history.csv' in url:
                return strong_candidate, None
            return '', '404'

        llm_side_effect = [
            {
                "record": {
                    "status": "success",
                    "response": json.dumps(
                        {
                            "decision": "retry",
                            "retry_queries": ["黄金 历史价格 元/克 CSV 日度"],
                            "notes": ["当前候选偏资讯页，改搜历史CSV数据"],
                        },
                        ensure_ascii=False,
                    ),
                }
            },
            {
                "record": {
                    "status": "success",
                    "response": json.dumps(
                        {
                            "decision": "accept",
                            "selected_url": "https://data.example.com/gold-history.csv",
                            "notes": ["命中多日期CSV数据页"],
                        },
                        ensure_ascii=False,
                    ),
                }
            },
        ]

        with patch("backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec", side_effect=fake_exec), patch(
            "backend.src.actions.handlers.tool_call.create_llm_call",
            side_effect=llm_side_effect,
        ):
            result = _execute_web_fetch_keyword_search(
                {"command": "echo ok", "workdir": "/tmp"},
                "请你帮我收集最近三个月的黄金的价格数据，单位元/克，并保存为csv文件",
                protocol=protocol,
                context={
                    "task_id": 1,
                    "run_id": 1,
                    "model": "deepseek-chat",
                    "event_sink": lambda payload: emitted.append(payload),
                },
            )

        self.assertTrue(bool(result.get("ok")))
        self.assertIn("gold-history.csv", str((result.get("selected_candidate") or {}).get("url") or ""))
        self.assertTrue(any(str(item.get("type") or "") == "search_progress" and str(item.get("stage") or item.get("payload", {}).get("stage") or "") == "query_rewrite_done" for item in emitted))
        self.assertTrue(any("自动改写查询并重试" in str(item) for item in (result.get("warnings") or [])))


    def test_web_fetch_protocol_replaces_placeholder_hints_with_task_inference(self):
        from backend.src.actions.handlers.tool_call import _normalize_web_fetch_protocol

        protocol = _normalize_web_fetch_protocol(
            {
                "target_signals": ["任务对象", "核心指标"],
                "unit_hints": ["原始单位"],
                "time_hints": ["最近一段时间"],
            },
            fallback_query="请你帮我收集最近三个月的黄金的价格数据，单位元/克，并保存为csv文件",
            source="llm",
        )

        self.assertIn("黄金", list(protocol.get("target_signals") or []))
        self.assertNotIn("任务对象", list(protocol.get("target_signals") or []))
        self.assertIn("元/克", list(protocol.get("unit_hints") or []))
        self.assertNotIn("原始单位", list(protocol.get("unit_hints") or []))
        self.assertIn("最近三个月", list(protocol.get("time_hints") or []))

    def test_web_fetch_retry_query_normalization_adds_protocol_anchor_variant(self):
        from backend.src.actions.handlers.tool_call import _normalize_web_fetch_retry_queries

        queries = _normalize_web_fetch_retry_queries(
            ["historical gold prices 2026 Q1 daily USD per ounce table"],
            fallback_query="请你帮我收集最近三个月的黄金的价格数据，单位元/克，并保存为csv文件",
            target_signals=["黄金", "价格"],
            unit_hints=["元/克"],
            time_hints=["最近三个月", "日度"],
        )

        self.assertTrue(any("usd per ounce" in str(item).lower() for item in queries))
        self.assertTrue(any("黄金" in str(item) and "元/克" in str(item) for item in queries))
        self.assertTrue(any(("最近三个月" in str(item)) or ("日度" in str(item)) for item in queries))

    def test_web_fetch_keyword_search_rewrite_preserves_protocol_anchors_when_llm_drifts(self):
        from backend.src.actions.handlers.tool_call import _execute_web_fetch_keyword_search

        protocol = {
            "version": 2,
            "source": "llm",
            "objective": "收集最近三个月黄金价格并保存 CSV",
            "search_queries": ["黄金 价格 元/克"],
            "required_fields": ["date", "price_cny_per_gram"],
            "required_columns": ["date", "price_cny_per_gram"],
            "target_signals": ["黄金", "价格"],
            "unit_hints": ["元/克"],
            "time_hints": ["最近三个月", "日度"],
            "negative_terms": ["论坛", "help", "schema"],
            "deny_domains": [],
            "require_structured": True,
        }

        weak_candidate = (
            '<html><body><h1>黄金市场资讯</h1><p>发布时间 2026-03-01</p>'
            '<p>今日黄金价格小幅波动，更多内容见正文。</p></body></html>'
        )
        strong_candidate = (
            'date,price_cny_per_gram\n'
            '2026-01-01,681.2 元/克\n'
            '2026-01-02,682.5 元/克\n'
            '2026-01-03,680.8 元/克\n'
        )
        emitted = []

        def fake_exec(_exec_spec, url):
            parsed = urlparse(url)
            query_text = ' '.join(
                unquote_plus(value)
                for values in parse_qs(parsed.query or '', keep_blank_values=False).values()
                for value in values
            )
            if parsed.netloc in {"www.bing.com", "cn.bing.com", "www.so.com", "www.baidu.com", "www.sogou.com"}:
                if "元/克" in query_text and ("最近三个月" in query_text or "日度" in query_text):
                    return 'https://data.example.com/gold-history.csv', None
                return 'https://news.example.com/gold-article', None
            if 'gold-article' in url:
                return weak_candidate, None
            if 'gold-history.csv' in url:
                return strong_candidate, None
            return '', '404'

        llm_side_effect = [
            {
                "record": {
                    "status": "success",
                    "response": json.dumps(
                        {
                            "decision": "retry",
                            "retry_queries": ["historical gold prices 2026 Q1 daily USD per ounce table"],
                            "notes": ["候选不足，尝试改写搜索词"],
                        },
                        ensure_ascii=False,
                    ),
                }
            },
            {
                "record": {
                    "status": "success",
                    "response": json.dumps(
                        {
                            "decision": "accept",
                            "selected_url": "https://data.example.com/gold-history.csv",
                            "notes": ["命中多日期 CSV 数据页"],
                        },
                        ensure_ascii=False,
                    ),
                }
            },
        ]

        with patch("backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec", side_effect=fake_exec), patch(
            "backend.src.actions.handlers.tool_call.create_llm_call",
            side_effect=llm_side_effect,
        ):
            result = _execute_web_fetch_keyword_search(
                {"command": "echo ok", "workdir": "/tmp"},
                "请你帮我收集最近三个月的黄金的价格数据，单位元/克，并保存为csv文件",
                protocol=protocol,
                context={
                    "task_id": 1,
                    "run_id": 1,
                    "model": "deepseek-chat",
                    "event_sink": lambda payload: emitted.append(payload),
                },
            )

        self.assertTrue(bool(result.get("ok")))
        self.assertIn("gold-history.csv", str((result.get("selected_candidate") or {}).get("url") or ""))
        rewrite_events = [
            item for item in emitted
            if str(item.get("type") or "") == "search_progress" and str(item.get("stage") or "") == "query_rewrite_done"
        ]
        self.assertTrue(rewrite_events)
        retry_queries = list(rewrite_events[-1].get("retry_queries") or [])
        self.assertTrue(any("元/克" in str(item) for item in retry_queries))
        self.assertTrue(any(("最近三个月" in str(item)) or ("日度" in str(item)) for item in retry_queries))

    def test_web_fetch_protocol_prompt_strips_illustrative_step_examples(self):
        from backend.src.actions.handlers.tool_call import _build_web_fetch_protocol_prompt

        prompt = _build_web_fetch_protocol_prompt(
            task_message="请你帮我收集最近三个月的黄金价格数据，单位元/克，并保存为csv文件",
            step_title="tool_call:web_fetch 抓取黄金价格数据源（例如黄金期货GC=F页面）",
            tool_input="黄金价格 最近三个月 历史数据 CSV",
        )

        self.assertIn("当前步骤：tool_call:web_fetch 抓取黄金价格数据源", prompt)
        self.assertNotIn("GC=F", prompt)

    def test_normalize_web_fetch_protocol_strips_illustrative_objective(self):
        from backend.src.actions.handlers.tool_call import _normalize_web_fetch_protocol

        protocol = _normalize_web_fetch_protocol(
            {
                "objective": "抓取黄金价格数据源（例如黄金期货GC=F页面）",
                "search_queries": ["黄金价格 最近三个月 历史数据 CSV"],
            },
            fallback_query="黄金价格 最近三个月 元/克 历史数据 CSV",
            source="llm",
        )

        self.assertEqual(protocol.get("objective"), "抓取黄金价格数据源")
        self.assertNotIn("GC=F", protocol.get("objective") or "")


    def test_resolve_protocol_search_queries_prefers_grounded_queries(self):
        from backend.src.actions.handlers.tool_call import _resolve_protocol_search_queries

        protocol = {
            "search_queries": [
                "黄金 价格 历史数据 日度 CSV",
                "gold price daily historical data API",
                "黄金价格 近三个月 表格 元/克",
            ],
            "target_signals": ["黄金", "价格", "元/克"],
            "unit_hints": ["元/克"],
            "time_hints": ["最近三个月", "日度"],
        }

        queries = _resolve_protocol_search_queries("黄金价格 近三个月 历史数据", protocol)

        self.assertGreater(len(queries), 0)
        self.assertIn("元/克", queries[0])
        self.assertIn("日度", queries[0])
        self.assertNotEqual(queries[0], "黄金价格 近三个月 历史数据")



if __name__ == "__main__":
    unittest.main()
