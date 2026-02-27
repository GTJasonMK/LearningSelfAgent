# -*- coding: utf-8 -*-
"""SSE 解析器单元测试。"""

import json
import unittest

from backend.src.cli.sse import SseEvent, iter_sse_events, iter_sse_stream, parse_event_block


class TestParseEventBlock(unittest.TestCase):
    """parse_event_block 单元测试。"""

    def test_delta_event(self):
        """解析普通 delta 事件。"""
        raw = 'data: {"delta": "正在执行...\\n"}'
        event = parse_event_block(raw)
        self.assertEqual(event.event, "message")
        self.assertIsNotNone(event.json_data)
        self.assertEqual(event.json_data["delta"], "正在执行...\n")

    def test_named_event(self):
        """解析带 event: 行的事件。"""
        raw = 'event: done\ndata: {"type": "done"}'
        event = parse_event_block(raw)
        self.assertEqual(event.event, "done")
        self.assertEqual(event.json_data["type"], "done")

    def test_error_event(self):
        """解析 error 事件。"""
        raw = 'event: error\ndata: {"message": "执行失败"}'
        event = parse_event_block(raw)
        self.assertEqual(event.event, "error")
        self.assertEqual(event.json_data["message"], "执行失败")

    def test_comment_line_ignored(self):
        """注释行被忽略。"""
        raw = ': keep-alive\ndata: {"delta": "hi"}'
        event = parse_event_block(raw)
        self.assertEqual(event.json_data["delta"], "hi")

    def test_multiline_data(self):
        """多行 data 拼接。"""
        raw = 'data: line1\ndata: line2'
        event = parse_event_block(raw)
        self.assertEqual(event.data, "line1\nline2")
        # 非 JSON，json_data 应为 None
        self.assertIsNone(event.json_data)

    def test_data_optional_space(self):
        """data: 后的可选空格只移除一个。"""
        raw = 'data:  {"delta": "x"}'
        event = parse_event_block(raw)
        # 移除一个空格后剩余 ' {"delta": "x"}'，能解析为 JSON
        self.assertIsNotNone(event.json_data)
        self.assertEqual(event.json_data["delta"], "x")

    def test_no_data_lines(self):
        """只有 event: 行、没有 data: 行。"""
        raw = "event: ping"
        event = parse_event_block(raw)
        self.assertEqual(event.event, "ping")
        self.assertEqual(event.data, "")

    def test_run_created_event(self):
        """解析 run_created 结构化事件。"""
        payload = {"type": "run_created", "task_id": 1, "run_id": 5, "status": "running"}
        raw = f"data: {json.dumps(payload)}"
        event = parse_event_block(raw)
        self.assertEqual(event.json_data["type"], "run_created")
        self.assertEqual(event.json_data["task_id"], 1)
        self.assertEqual(event.json_data["run_id"], 5)

    def test_plan_event(self):
        """解析 plan 事件。"""
        payload = {"type": "plan", "items": [{"id": 1, "brief": "步骤1"}, {"id": 2, "brief": "步骤2"}]}
        raw = f"data: {json.dumps(payload, ensure_ascii=False)}"
        event = parse_event_block(raw)
        self.assertEqual(event.json_data["type"], "plan")
        self.assertEqual(len(event.json_data["items"]), 2)

    def test_carriage_return_normalized(self):
        """\\r 被规范化移除。"""
        raw = "data: hello\r\ndata: world"
        event = parse_event_block(raw)
        self.assertEqual(event.data, "hello\nworld")

    def test_non_json_data(self):
        """非 JSON 的 data 行，json_data 为 None。"""
        raw = "data: plain text message"
        event = parse_event_block(raw)
        self.assertEqual(event.data, "plain text message")
        self.assertIsNone(event.json_data)


class TestIterSseEvents(unittest.TestCase):
    """iter_sse_events 单元测试。"""

    def test_multiple_events(self):
        """解析多个事件。"""
        text = (
            'data: {"delta": "hello"}\n\n'
            'data: {"delta": " world"}\n\n'
            'event: done\ndata: {"type": "done"}\n\n'
        )
        events = list(iter_sse_events(text))
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].json_data["delta"], "hello")
        self.assertEqual(events[1].json_data["delta"], " world")
        self.assertEqual(events[2].event, "done")

    def test_empty_text(self):
        """空文本不产出事件。"""
        events = list(iter_sse_events(""))
        self.assertEqual(len(events), 0)

    def test_only_whitespace(self):
        """纯空白文本不产出事件。"""
        events = list(iter_sse_events("  \n\n  "))
        self.assertEqual(len(events), 0)

    def test_windows_line_endings(self):
        """兼容 \\r\\n\\r\\n 分隔符。"""
        text = 'data: {"delta": "a"}\r\n\r\ndata: {"delta": "b"}\r\n\r\n'
        events = list(iter_sse_events(text))
        self.assertEqual(len(events), 2)


class TestIterSseStream(unittest.TestCase):
    """iter_sse_stream 流式解析测试。"""

    def test_chunked_stream(self):
        """模拟分块到达的文本流。"""
        chunks = [
            'data: {"delta": "hel',
            'lo"}\n\ndata: {"del',
            'ta": " world"}\n\n',
        ]
        events = list(iter_sse_stream(chunks))
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].json_data["delta"], "hello")
        self.assertEqual(events[1].json_data["delta"], " world")

    def test_trailing_data_without_separator(self):
        """末尾没有 \\n\\n 的残留数据也能产出事件。"""
        chunks = ['data: {"delta": "tail"}']
        events = list(iter_sse_stream(chunks))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].json_data["delta"], "tail")

    def test_empty_chunks(self):
        """空块不产出事件。"""
        events = list(iter_sse_stream([]))
        self.assertEqual(len(events), 0)

    def test_mixed_events_and_comments(self):
        """混合事件和注释：注释块也会被解析（data 为空）。"""
        chunks = [
            ': keepalive\n\ndata: {"delta": "hi"}\n\nevent: done\ndata: {"type":"done"}\n\n'
        ]
        events = list(iter_sse_stream(chunks))
        # 注释块产出一个空 data 事件 + 2 个正常事件
        self.assertEqual(len(events), 3)
        # 第一个是注释块（data 为空）
        self.assertEqual(events[0].data, "")
        self.assertEqual(events[1].json_data["delta"], "hi")
        self.assertEqual(events[2].event, "done")


if __name__ == "__main__":
    unittest.main()
