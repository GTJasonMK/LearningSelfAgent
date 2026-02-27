# -*- coding: utf-8 -*-
"""配置管理命令。"""

from __future__ import annotations

import sys

import click

from backend.src.cli.client import CliError
from backend.src.cli.output import print_error, print_json, print_summary


@click.group()
def config() -> None:
    """配置管理"""
    pass


@config.command()
@click.pass_context
def show(ctx: click.Context) -> None:
    """查看应用配置"""
    client = ctx.obj["client"]
    try:
        data = client.get("/config")
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_summary(data, "应用配置")
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)


@config.command()
@click.pass_context
def llm(ctx: click.Context) -> None:
    """查看 LLM 配置"""
    client = ctx.obj["client"]
    try:
        data = client.get("/config/llm")
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_summary(data, "LLM 配置")
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)
