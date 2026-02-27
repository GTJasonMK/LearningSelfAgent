# -*- coding: utf-8 -*-
"""健康检查命令。"""

from __future__ import annotations

import sys

import click

from backend.src.cli.client import CliError
from backend.src.cli.output import console, print_error, print_json, print_success


@click.command()
@click.pass_context
def health(ctx: click.Context) -> None:
    """检查后端服务健康状态"""
    client = ctx.obj["client"]
    try:
        data = client.get("/health")
        if ctx.obj["output_json"]:
            print_json(data)
        else:
            print_success("后端服务正常")
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)
