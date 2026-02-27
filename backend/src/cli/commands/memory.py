# -*- coding: utf-8 -*-
"""
记忆管理命令组。

支持完整的记忆 CRUD、搜索和统计功能。
"""

from __future__ import annotations

import sys

import click

from backend.src.cli.client import CliError
from backend.src.cli.output import (
    print_error,
    print_json,
    print_memory_detail,
    print_memory_table,
    print_success,
    print_summary,
    print_warning,
)


@click.group()
def memory() -> None:
    """记忆系统管理"""
    pass


@memory.command(name="list")
@click.option("--offset", default=0, type=int, help="分页偏移")
@click.option("--limit", default=20, type=int, help="每页数量")
@click.pass_context
def memory_list(ctx: click.Context, offset: int, limit: int) -> None:
    """列出记忆项"""
    client = ctx.obj["client"]
    try:
        data = client.get("/memory/items", params={"offset": offset, "limit": limit})
        items = data.get("items", [])
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_memory_table(items)
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


@memory.command()
@click.argument("content")
@click.option(
    "--type",
    "memory_type",
    type=click.Choice(["lesson", "insight", "failure", "success"]),
    default=None,
    help="记忆类型",
)
@click.option("--tags", default=None, help="标签（逗号分隔）")
@click.pass_context
def create(ctx: click.Context, content: str, memory_type: str | None, tags: str | None) -> None:
    """创建记忆项"""
    client = ctx.obj["client"]
    payload: dict = {"content": content}
    if memory_type:
        payload["memory_type"] = memory_type
    if tags:
        payload["tags"] = [t.strip() for t in tags.split(",") if t.strip()]
    try:
        data = client.post("/memory/items", json_data=payload)
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            item_id = data.get("id", "?")
            print_success(f"记忆项已创建 (ID: {item_id})")
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


@memory.command()
@click.argument("item_id", type=int)
@click.pass_context
def get(ctx: click.Context, item_id: int) -> None:
    """获取记忆项详情"""
    client = ctx.obj["client"]
    try:
        data = client.get(f"/memory/items/{item_id}")
        item = data.get("item", data)
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_memory_detail(item)
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


@memory.command()
@click.argument("item_id", type=int)
@click.option("--content", default=None, help="更新内容")
@click.option("--type", "memory_type", default=None, help="更新类型")
@click.option("--tags", default=None, help="更新标签（逗号分隔）")
@click.pass_context
def update(
    ctx: click.Context,
    item_id: int,
    content: str | None,
    memory_type: str | None,
    tags: str | None,
) -> None:
    """更新记忆项"""
    client = ctx.obj["client"]
    payload: dict = {}
    if content is not None:
        payload["content"] = content
    if memory_type is not None:
        payload["memory_type"] = memory_type
    if tags is not None:
        payload["tags"] = [t.strip() for t in tags.split(",") if t.strip()]

    if not payload:
        print_warning("未指定任何更新字段（--content / --type / --tags）")
        return

    try:
        data = client.patch(f"/memory/items/{item_id}", json_data=payload)
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_success(f"记忆项 #{item_id} 已更新")
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


@memory.command()
@click.argument("item_id", type=int)
@click.option("--yes", is_flag=True, help="跳过确认")
@click.pass_context
def delete(ctx: click.Context, item_id: int, yes: bool) -> None:
    """删除记忆项"""
    if not yes:
        click.confirm(f"确定要删除记忆项 #{item_id} 吗？", abort=True)
    client = ctx.obj["client"]
    try:
        data = client.delete(f"/memory/items/{item_id}")
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_success(f"记忆项 #{item_id} 已删除")
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


@memory.command()
@click.argument("query")
@click.pass_context
def search(ctx: click.Context, query: str) -> None:
    """搜索记忆"""
    client = ctx.obj["client"]
    try:
        data = client.get("/memory/search", params={"q": query})
        items = data.get("items", [])
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_memory_table(items)
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


@memory.command()
@click.pass_context
def summary(ctx: click.Context) -> None:
    """记忆汇总统计"""
    client = ctx.obj["client"]
    try:
        data = client.get("/memory/summary")
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_summary(data, "记忆汇总")
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)
