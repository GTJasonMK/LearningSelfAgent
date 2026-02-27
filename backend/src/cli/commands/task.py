# -*- coding: utf-8 -*-
"""
任务管理命令组。

支持任务 CRUD、统计和流式执行。
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
    print_success,
    print_summary,
    print_task_detail,
    print_tasks_table,
    print_warning,
)


@click.group()
def task() -> None:
    """任务管理"""
    pass


@task.command(name="list")
@click.option("--date", default=None, help="筛选日期（YYYY-MM-DD）")
@click.option("--days", default=None, type=int, help="最近天数")
@click.pass_context
def task_list(ctx: click.Context, date: str | None, days: int | None) -> None:
    """列出任务"""
    client = ctx.obj["client"]
    params: dict = {}
    if date:
        params["date"] = date
    if days is not None:
        params["days"] = days
    try:
        data = client.get("/tasks", params=params or None)
        tasks = data.get("tasks", data) if isinstance(data, dict) else data
        if isinstance(tasks, dict):
            tasks = tasks.get("tasks", [])
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_tasks_table(tasks if isinstance(tasks, list) else [])
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


@task.command()
@click.argument("title")
@click.pass_context
def create(ctx: click.Context, title: str) -> None:
    """创建任务"""
    client = ctx.obj["client"]
    try:
        data = client.post("/tasks", json_data={"title": title})
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            task_id = data.get("id", "?")
            print_success(f"任务已创建 (ID: {task_id})")
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


@task.command()
@click.argument("task_id", type=int)
@click.pass_context
def get(ctx: click.Context, task_id: int) -> None:
    """获取任务详情"""
    client = ctx.obj["client"]
    try:
        data = client.get(f"/tasks/{task_id}")
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_task_detail(data)
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


@task.command()
@click.argument("task_id", type=int)
@click.option("--status", default=None, help="更新状态")
@click.option("--title", default=None, help="更新标题")
@click.pass_context
def update(ctx: click.Context, task_id: int, status: str | None, title: str | None) -> None:
    """更新任务"""
    client = ctx.obj["client"]
    payload: dict = {}
    if status is not None:
        payload["status"] = status
    if title is not None:
        payload["title"] = title
    if not payload:
        print_warning("未指定任何更新字段（--status / --title）")
        return
    try:
        data = client.patch(f"/tasks/{task_id}", json_data=payload)
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_success(f"任务 #{task_id} 已更新")
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


@task.command()
@click.argument("task_id", type=int)
@click.pass_context
def execute(ctx: click.Context, task_id: int) -> None:
    """执行任务（SSE 流式输出）"""
    client = ctx.obj["client"]
    try:
        for event in client.stream_post(f"/tasks/{task_id}/execute/stream"):
            _render_stream_event(event, ctx.obj["output_json"])
        # 流结束后换行
        console.print()
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)
    except KeyboardInterrupt:
        console.print("\n[yellow]已中断[/yellow]")
        sys.exit(130)


@task.command()
@click.pass_context
def summary(ctx: click.Context) -> None:
    """任务统计"""
    client = ctx.obj["client"]
    try:
        data = client.get("/tasks/summary")
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_summary(data, "任务统计")
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


def _render_stream_event(event, output_json: bool) -> None:
    """渲染 SSE 流式事件。"""
    from backend.src.cli.output import print_json as pj

    if output_json and event.json_data:
        pj(event.json_data)
        return

    if event.event == "error":
        msg = ""
        if event.json_data:
            msg = event.json_data.get("message", event.data)
        print_error(msg or event.data)
        return

    if event.event == "done" or (event.json_data and event.json_data.get("type") == "done"):
        print_sse_status("完成", "任务执行完毕", "green")
        return

    if not event.json_data:
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
        print_sse_status("状态", f"{status}", style)
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
        message = data.get("message", "需要输入")
        print_sse_status("输入", message, "magenta")
    elif event_type == "memory_item":
        print_sse_status("记忆", f"已沉淀记忆: {data.get('content', '')[:50]}", "green")
