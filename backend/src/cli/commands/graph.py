# -*- coding: utf-8 -*-
"""知识图谱命令组。"""

from __future__ import annotations

import sys

import click

from backend.src.cli.client import CliError
from backend.src.cli.output import (
    print_error,
    print_graph_edges_table,
    print_graph_nodes_table,
    print_json,
    print_summary,
)


@click.group()
def graph() -> None:
    """知识图谱管理"""
    pass


@graph.command()
@click.pass_context
def summary(ctx: click.Context) -> None:
    """图谱统计"""
    client = ctx.obj["client"]
    try:
        data = client.get("/memory/graph")
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_summary(data, "知识图谱")
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


@graph.command()
@click.pass_context
def nodes(ctx: click.Context) -> None:
    """列出图谱节点"""
    client = ctx.obj["client"]
    try:
        data = client.get("/memory/graph/nodes")
        items = data.get("nodes", data.get("items", []))
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_graph_nodes_table(items if isinstance(items, list) else [])
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


@graph.command()
@click.pass_context
def edges(ctx: click.Context) -> None:
    """列出图谱边"""
    client = ctx.obj["client"]
    try:
        data = client.get("/memory/graph/edges")
        items = data.get("edges", data.get("items", []))
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_graph_edges_table(items if isinstance(items, list) else [])
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)
