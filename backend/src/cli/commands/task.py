# -*- coding: utf-8 -*-
"""
任务管理命令组。

支持任务 CRUD、统计和流式执行。
"""

from __future__ import annotations

import sys

import click

from backend.src.cli.client import CliError
from backend.src.cli.commands.stream_render import render_stream_event
from backend.src.cli.output import (
    console,
    print_error,
    print_json,
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
        if not isinstance(data, dict):
            raise CliError("后端响应格式错误：/tasks 应返回对象", exit_code=1)
        tasks = data.get("items")
        if not isinstance(tasks, list):
            raise CliError("后端响应格式错误：/tasks 缺少 items 列表", exit_code=1)
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_tasks_table(tasks)
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
        if not isinstance(data, dict):
            raise CliError("后端响应格式错误：POST /tasks 应返回对象", exit_code=1)
        task = data.get("task")
        if not isinstance(task, dict):
            raise CliError("后端响应格式错误：POST /tasks 缺少 task 对象", exit_code=1)
        task_id = task.get("id")
        if task_id is None:
            raise CliError("后端响应格式错误：POST /tasks 的 task.id 缺失", exit_code=1)
        if ctx.obj["output_json"]:
            print_json(data)
        else:
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
        if not isinstance(data, dict):
            raise CliError(f"后端响应格式错误：/tasks/{task_id} 应返回对象", exit_code=1)
        task = data.get("task")
        if not isinstance(task, dict):
            raise CliError(f"后端响应格式错误：/tasks/{task_id} 缺少 task 对象", exit_code=1)
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_task_detail(task)
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
        if not isinstance(data, dict) or not isinstance(data.get("task"), dict):
            raise CliError(f"后端响应格式错误：PATCH /tasks/{task_id} 缺少 task 对象", exit_code=1)
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_success(f"任务 #{task_id} 已更新")
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


@task.command()
@click.argument("task_id", type=int)
@click.option("--run-summary", default=None, help="执行摘要（对应 run_summary）")
@click.option("--max-retries", default=None, type=int, help="失败重试次数（对应 max_retries）")
@click.option("--on-failure", default=None, help="失败策略（对应 on_failure）")
@click.pass_context
def execute(
    ctx: click.Context,
    task_id: int,
    run_summary: str | None,
    max_retries: int | None,
    on_failure: str | None,
) -> None:
    """执行任务（SSE 流式输出）"""
    client = ctx.obj["client"]
    payload: dict = {}
    if run_summary is not None:
        payload["run_summary"] = run_summary
    if max_retries is not None:
        payload["max_retries"] = max_retries
    if on_failure is not None:
        payload["on_failure"] = on_failure
    try:
        seen_done = False
        seen_error = False
        for event in client.stream_post(f"/tasks/{task_id}/execute/stream", json_data=payload):
            outcome = _render_stream_event(event, ctx.obj["output_json"])
            if outcome == "done":
                seen_done = True
            elif outcome == "error":
                seen_error = True
        # 流结束后换行
        console.print()
        if seen_error:
            sys.exit(1)
        if not seen_done:
            print_error("任务流已结束，但未收到 done 事件（执行状态不可信）")
            sys.exit(1)
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


def _render_stream_event(event, output_json: bool):
    """渲染 SSE 流式事件。"""
    return render_stream_event(event, output_json, done_message="任务执行完毕")
