# -*- coding: utf-8 -*-
"""技能管理命令组。"""

from __future__ import annotations

import sys

import click

from backend.src.cli.client import CliError
from backend.src.cli.output import (
    print_error,
    print_json,
    print_skill_detail,
    print_skills_table,
)


@click.group()
def skill() -> None:
    """技能管理"""
    pass


@skill.command(name="list")
@click.pass_context
def skill_list(ctx: click.Context) -> None:
    """列出技能"""
    client = ctx.obj["client"]
    try:
        data = client.get("/memory/skills")
        items = data.get("items", data) if isinstance(data, dict) else data
        if isinstance(items, dict):
            items = items.get("items", [])
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_skills_table(items if isinstance(items, list) else [])
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


@skill.command()
@click.argument("skill_id", type=int)
@click.pass_context
def get(ctx: click.Context, skill_id: int) -> None:
    """获取技能详情"""
    client = ctx.obj["client"]
    try:
        data = client.get(f"/memory/skills/{skill_id}")
        item = data.get("item", data) if isinstance(data, dict) else data
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_skill_detail(item if isinstance(item, dict) else data)
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


@skill.command()
@click.argument("query")
@click.pass_context
def search(ctx: click.Context, query: str) -> None:
    """搜索技能"""
    client = ctx.obj["client"]
    try:
        data = client.get("/memory/skills/search", params={"q": query})
        items = data.get("items", [])
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_skills_table(items)
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)
