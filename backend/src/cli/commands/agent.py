# -*- coding: utf-8 -*-
"""
智能体对话命令。

通过 SSE 流式接口与智能体交互。
"""

from __future__ import annotations

import sys

import click

from backend.src.cli.client import CliError
from backend.src.cli.output import (
    console,
    print_error,
    print_json,
    print_sse_delta,
    print_sse_status,
)


@click.command()
@click.argument("message")
@click.option(
    "--mode",
    type=click.Choice(["do", "think", "auto"]),
    default=None,
    help="执行模式",
)
@click.option("--model", default=None, help="LLM 模型名称")
@click.option("--max-steps", default=None, type=int, help="最大执行步骤数")
@click.option("--dry-run", is_flag=True, default=False, help="仅规划不执行")
@click.pass_context
def ask(
    ctx: click.Context,
    message: str,
    mode: str | None,
    model: str | None,
    max_steps: int | None,
    dry_run: bool,
) -> None:
    """与智能体对话（自然语言指令执行）"""
    client = ctx.obj["client"]
    output_json = ctx.obj["output_json"]

    payload: dict = {"message": message}
    if mode:
        payload["mode"] = mode
    if model:
        payload["model"] = model
    if max_steps is not None:
        payload["max_steps"] = max_steps
    if dry_run:
        payload["dry_run"] = True

    try:
        for event in client.stream_post("/agent/command/stream", json_data=payload):
            _render_event(event, output_json)
        # 流结束后换行
        console.print()
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)
    except KeyboardInterrupt:
        console.print("\n[yellow]已中断[/yellow]")
        sys.exit(130)


def _render_event(event, output_json: bool) -> None:
    """渲染 SSE 流式事件。"""
    if output_json and event.json_data:
        print_json(event.json_data)
        return

    if event.event == "error":
        msg = ""
        if event.json_data:
            msg = event.json_data.get("message", event.data)
        print_error(msg or event.data)
        return

    if event.event == "done" or (event.json_data and event.json_data.get("type") == "done"):
        print_sse_status("完成", "执行完毕", "green")
        return

    if not event.json_data:
        # 非 JSON 的 data 行直接输出
        if event.data.strip():
            print_sse_delta(event.data)
        return

    data = event.json_data
    event_type = data.get("type", "")

    if "delta" in data:
        print_sse_delta(data["delta"])
    elif event_type == "run_created":
        print_sse_status("创建", f"任务 #{data.get('task_id', '')} 运行 #{data.get('run_id', '')}")
    elif event_type == "run_status":
        status = data.get("status", "")
        style = "green" if status == "done" else "yellow" if status == "running" else "red"
        print_sse_status("状态", status, style)
    elif event_type == "plan":
        items = data.get("items", [])
        if items:
            print_sse_status("规划", f"{len(items)} 个步骤")
            for item in items:
                brief = item.get("brief", item.get("title", ""))
                console.print(f"  [dim]{item.get('id', '')}.[/dim] {brief}")
    elif event_type == "plan_delta":
        step_id = data.get("id", "")
        status = data.get("status", "")
        title = data.get("title", data.get("brief", ""))
        style = "green" if status == "done" else "yellow" if status == "running" else "red"
        print_sse_status("步骤", f"{step_id}. {title} [{status}]", style)
    elif event_type == "need_input":
        message_text = data.get("message", "智能体需要你的输入")
        print_sse_status("输入", message_text, "magenta")
    elif event_type == "review":
        verdict = data.get("verdict", "")
        print_sse_status("审查", f"审查结论: {verdict}", "blue")
    elif event_type == "memory_item":
        content = str(data.get("content", ""))[:50]
        print_sse_status("记忆", f"已沉淀: {content}", "green")
