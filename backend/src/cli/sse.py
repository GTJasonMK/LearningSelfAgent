# -*- coding: utf-8 -*-
"""
SSE（Server-Sent Events）流式解析器。

与前端 streaming.js 的 parseSseEventBlock / consumeNextSseEventBlock 逻辑对齐：
- 以 ``\\n\\n`` 分割事件块
- ``event:`` 行设置事件类型（默认 ``message``）
- ``data:`` 行累积拼接（多行 data 用 ``\\n`` 连接）
- ``:`` 开头的行为注释，忽略
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Iterator, Optional


@dataclass(frozen=True)
class SseEvent:
    """解析后的 SSE 事件。"""

    event: str = "message"
    data: str = ""
    json_data: Optional[Dict[str, Any]] = field(default=None, repr=False)


def parse_event_block(raw_block: str) -> SseEvent:
    """
    从单个事件块文本解析出 SseEvent。

    参数:
        raw_block: 一个不含分隔空行的事件块原始文本
    """
    event_name = "message"
    data_lines: list[str] = []

    normalized = raw_block.replace("\r", "")
    for line in normalized.split("\n"):
        # 注释行
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            event_name = line[6:].strip()
        elif line.startswith("data:"):
            payload = line[5:]
            # SSE 规范：data: 后可跟一个可选空格，只移除一个
            if payload.startswith(" "):
                payload = payload[1:]
            data_lines.append(payload)

    data_str = "\n".join(data_lines)

    # 尝试 JSON 解析
    json_data: Optional[Dict[str, Any]] = None
    if data_str.strip():
        try:
            parsed = json.loads(data_str)
            if isinstance(parsed, dict):
                json_data = parsed
        except (json.JSONDecodeError, ValueError):
            pass

    return SseEvent(event=event_name, data=data_str, json_data=json_data)


def iter_sse_events(text: str) -> Iterator[SseEvent]:
    """
    从完整的 SSE 文本中迭代解析事件。

    参数:
        text: 包含多个事件块的完整 SSE 文本
    """
    if not text:
        return

    # 按 \n\n 分割事件块（兼容 \r\n\r\n）
    import re

    blocks = re.split(r"\r?\n\r?\n", text)
    for block in blocks:
        stripped = block.strip()
        if not stripped:
            continue
        yield parse_event_block(stripped)


def iter_sse_stream(lines: Iterable[str]) -> Iterator[SseEvent]:
    """
    从文本行流中流式解析 SSE 事件（用于 httpx 流式响应）。

    每当缓冲区中出现 ``\\n\\n`` 分隔符，就产出一个事件。

    参数:
        lines: 文本块迭代器（如 httpx response.iter_text()）
    """
    buffer = ""
    for chunk in lines:
        buffer += chunk
        # 循环提取所有已完成的事件块
        while "\n\n" in buffer:
            block_text, buffer = buffer.split("\n\n", 1)
            stripped = block_text.strip()
            if stripped:
                yield parse_event_block(stripped)

    # 处理末尾可能没有 \n\n 结尾的残留数据
    stripped = buffer.strip()
    if stripped:
        yield parse_event_block(stripped)
