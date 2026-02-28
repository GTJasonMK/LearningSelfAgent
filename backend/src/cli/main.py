# -*- coding: utf-8 -*-
"""
LearningSelfAgent CLI 顶层命令组。

用法：
    python -m backend.src.cli [全局选项] <子命令> [子命令选项]
    python scripts/lsa.py [全局选项] <子命令> [子命令选项]
"""

from __future__ import annotations

import sys

import click

from backend.src.cli.client import ApiClient, CliError
from backend.src.cli.output import console, print_error


@click.group()
@click.option(
    "--host",
    default="127.0.0.1",
    envvar="LSA_BACKEND_HOST",
    help="后端主机地址",
)
@click.option(
    "--port",
    default=None,
    type=int,
    envvar="LSA_BACKEND_PORT",
    help="后端端口（默认 8123）",
)
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    default=False,
    help="以 JSON 格式输出",
)
@click.option(
    "--timeout",
    default=30,
    type=int,
    help="HTTP 请求超时秒数（SSE 流式除外）",
)
@click.pass_context
def cli(ctx: click.Context, host: str, port: int | None, output_json: bool, timeout: int) -> None:
    """LearningSelfAgent 命令行工具"""
    ctx.ensure_object(dict)
    ctx.obj["client"] = ApiClient(host=host, port=port, timeout=timeout)
    ctx.obj["output_json"] = output_json


def _handle_cli_error(func):
    """装饰器：统一处理 CliError 并设置退出码。"""

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except CliError as exc:
            print_error(str(exc))
            sys.exit(exc.exit_code)
        except KeyboardInterrupt:
            console.print("\n[yellow]已中断[/yellow]")
            sys.exit(130)

    # 保留原始函数的元数据（click 需要）
    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__
    wrapper.__click_params__ = getattr(func, "__click_params__", [])
    return wrapper


# ── 注册子命令 ──


def _register_commands() -> None:
    """延迟导入并注册所有子命令，避免循环导入。"""
    from backend.src.cli.commands.agent import ask, resume
    from backend.src.cli.commands.chat import chat
    from backend.src.cli.commands.config import config
    from backend.src.cli.commands.graph import graph
    from backend.src.cli.commands.health import health
    from backend.src.cli.commands.memory import memory
    from backend.src.cli.commands.search import search
    from backend.src.cli.commands.skill import skill
    from backend.src.cli.commands.task import task

    cli.add_command(ask)
    cli.add_command(resume)
    cli.add_command(task)
    cli.add_command(memory)
    cli.add_command(skill)
    cli.add_command(graph)
    cli.add_command(search)
    cli.add_command(config)
    cli.add_command(chat)
    cli.add_command(health)


_register_commands()
