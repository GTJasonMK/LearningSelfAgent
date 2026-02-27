# -*- coding: utf-8 -*-
"""聊天记录命令组。"""

from __future__ import annotations

import sys

import click

from backend.src.cli.client import CliError
from backend.src.cli.output import print_chat_messages, print_error, print_json


@click.group()
def chat() -> None:
    """聊天记录管理"""
    pass


@chat.command(name="list")
@click.option("--limit", default=20, type=int, help="获取条数")
@click.option("--before-id", default=None, type=int, help="在此 ID 之前的消息")
@click.pass_context
def chat_list(ctx: click.Context, limit: int, before_id: int | None) -> None:
    """列出聊天消息"""
    client = ctx.obj["client"]
    params: dict = {"limit": limit}
    if before_id is not None:
        params["before_id"] = before_id
    try:
        data = client.get("/chat/messages", params=params)
        messages = data.get("messages", data.get("items", []))
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_chat_messages(messages if isinstance(messages, list) else [])
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


@chat.command()
@click.argument("query")
@click.pass_context
def search(ctx: click.Context, query: str) -> None:
    """搜索聊天消息"""
    client = ctx.obj["client"]
    try:
        data = client.get("/chat/search", params={"q": query})
        messages = data.get("messages", data.get("items", []))
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_chat_messages(messages if isinstance(messages, list) else [])
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)
