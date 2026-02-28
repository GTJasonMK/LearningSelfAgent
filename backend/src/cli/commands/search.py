# -*- coding: utf-8 -*-
"""统一搜索命令。"""

from __future__ import annotations

import sys

import click

from backend.src.cli.client import CliError
from backend.src.cli.output import print_error, print_json, print_search_results


@click.command()
@click.argument("query")
@click.option("--limit", default=10, type=int, help="每类最大返回数")
@click.pass_context
def search(ctx: click.Context, query: str, limit: int) -> None:
    """统一搜索（跨记忆、技能、图谱）"""
    client = ctx.obj["client"]
    try:
        data = client.get("/search", params={"q": query, "limit": limit})
        if not isinstance(data, dict):
            raise CliError("后端响应格式错误：/search 应返回对象", exit_code=1)
        memory_items = data.get("memory")
        skills_items = data.get("skills")
        graph = data.get("graph")
        graph_nodes = graph.get("nodes") if isinstance(graph, dict) else None
        if not isinstance(memory_items, list) or not isinstance(skills_items, list) or not isinstance(graph_nodes, list):
            raise CliError("后端响应格式错误：/search 需包含 memory/skills/graph.nodes", exit_code=1)
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_search_results(data)
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)
