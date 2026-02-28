# -*- coding: utf-8 -*-
"""CLI 流式事件渲染工具。"""

from __future__ import annotations

from typing import Literal
from typing import Any

from backend.src.cli.output import (
    console,
    print_error,
    print_json,
    print_sse_delta,
    print_sse_status,
)


def _status_style(status: str) -> str:
    value = str(status or "").strip().lower()
    if value == "done":
        return "green"
    if value == "running":
        return "yellow"
    if value == "waiting":
        return "magenta"
    if value in {"failed", "stopped"}:
        return "red"
    return "cyan"


RenderOutcome = Literal["done", "error", "other"]


def render_stream_event(event: Any, output_json: bool, *, done_message: str) -> RenderOutcome:
    """统一渲染 SSE 事件并返回事件结果类型。"""
    if output_json and getattr(event, "json_data", None):
        print_json(event.json_data)
        data = getattr(event, "json_data", None)
        if isinstance(data, dict) and data.get("type") == "done":
            return "done"
        return "other"

    if getattr(event, "event", "") == "error":
        msg = ""
        data = getattr(event, "json_data", None)
        if isinstance(data, dict):
            msg = str(data.get("message") or "")
        print_error(msg or str(getattr(event, "data", "") or ""))
        return "error"

    data = getattr(event, "json_data", None)
    if getattr(event, "event", "") == "done" or (isinstance(data, dict) and data.get("type") == "done"):
        print_sse_status("完成", done_message, "green")
        return "done"

    if not isinstance(data, dict):
        raw = str(getattr(event, "data", "") or "")
        if raw.strip():
            print_sse_delta(raw)
        return "other"

    event_type = str(data.get("type") or "")

    if "delta" in data:
        print_sse_delta(str(data.get("delta") or ""))
        return "other"

    if event_type == "run_created":
        print_sse_status("创建", f"任务 #{data.get('task_id', '')} 运行 #{data.get('run_id', '')}")
        return "other"

    if event_type == "run_status":
        status = str(data.get("status") or "")
        print_sse_status("状态", status, _status_style(status))
        return "other"

    if event_type == "plan":
        items = data.get("items", [])
        if isinstance(items, list) and items:
            print_sse_status("规划", f"{len(items)} 个步骤")
            for item in items:
                if not isinstance(item, dict):
                    continue
                brief = str(item.get("brief") or item.get("title") or "").strip()
                console.print(f"  [dim]{item.get('id', '')}.[/dim] {brief}")
        return "other"

    if event_type == "plan_delta":
        changes = data.get("changes", [])
        if isinstance(changes, list):
            for change in changes:
                if not isinstance(change, dict):
                    continue
                step_id = change.get("id") or change.get("step_order") or ""
                status = str(change.get("status") or "")
                title = str(change.get("title") or change.get("brief") or "")
                summary = f"{step_id}. {title} [{status}]".strip()
                print_sse_status("步骤", summary, _status_style(status))
        return "other"

    if event_type == "need_input":
        question = str(data.get("question") or data.get("message") or "智能体需要你的输入")
        print_sse_status("输入", question, "magenta")
        choices = data.get("choices")
        if isinstance(choices, list) and choices:
            for idx, choice in enumerate(choices, start=1):
                if not isinstance(choice, dict):
                    continue
                label = str(choice.get("label") or choice.get("value") or "").strip()
                value = str(choice.get("value") or "").strip()
                if value and value != label:
                    console.print(f"  [dim]{idx}.[/dim] {label} [dim]=> {value}[/dim]")
                else:
                    console.print(f"  [dim]{idx}.[/dim] {label}")
        token = str(data.get("prompt_token") or "").strip()
        if token:
            console.print(f"[dim]prompt_token: {token}[/dim]")
        session_key = str(data.get("session_key") or "").strip()
        if session_key:
            console.print(f"[dim]session_key: {session_key}[/dim]")
        return "other"

    if event_type == "review":
        verdict = str(data.get("verdict") or "")
        print_sse_status("审查", f"审查结论: {verdict}", "blue")
        return "other"

    if event_type == "memory_item":
        content = str(data.get("content") or "")[:50]
        print_sse_status("记忆", f"已沉淀: {content}", "green")
        return "other"

    return "other"
