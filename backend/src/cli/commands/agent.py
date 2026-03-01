# -*- coding: utf-8 -*-
"""
智能体对话命令。

通过 SSE 流式接口与智能体交互。
"""

from __future__ import annotations

import json
import sys

import click

from backend.src.cli.client import CliError
from backend.src.cli.output import (
    console,
    print_error,
)
from backend.src.cli.commands.stream_session import run_stream_session


def _parse_json_option(raw: str | None, option_name: str) -> dict | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except Exception as exc:
        raise CliError(f"{option_name} 不是合法 JSON: {exc}", exit_code=1)
    if not isinstance(parsed, dict):
        raise CliError(f"{option_name} 必须是 JSON 对象", exit_code=1)
    return parsed


@click.command()
@click.argument("message")
@click.option(
    "--mode",
    type=click.Choice(["do", "think", "auto"]),
    default=None,
    help="执行模式",
)
@click.option("--model", default=None, help="LLM 模型名称")
@click.option("--max-steps", default=None, type=int, help="最大执行步骤数")
@click.option("--dry-run", is_flag=True, default=False, help="仅规划不执行")
@click.option("--parameters-json", default=None, help='透传 parameters（JSON 对象），如 \'{"temperature":0.2}\'')
@click.option("--think-config-json", default=None, help='透传 think_config（JSON 对象）')
@click.pass_context
def ask(
    ctx: click.Context,
    message: str,
    mode: str | None,
    model: str | None,
    max_steps: int | None,
    dry_run: bool,
    parameters_json: str | None,
    think_config_json: str | None,
) -> None:
    """与智能体对话（自然语言指令执行）"""
    client = ctx.obj["client"]
    output_json = ctx.obj["output_json"]

    payload: dict = {"message": message}
    if mode:
        payload["mode"] = mode
    if model:
        payload["model"] = model
    if max_steps is not None:
        payload["max_steps"] = max_steps
    if dry_run:
        payload["dry_run"] = True
    parameters = _parse_json_option(parameters_json, "--parameters-json")
    if parameters is not None:
        payload["parameters"] = parameters
    think_config = _parse_json_option(think_config_json, "--think-config-json")
    if think_config is not None:
        payload["think_config"] = think_config

    try:
        result = run_stream_session(
            client=client,
            path="/agent/command/stream",
            payload=payload,
            output_json=output_json,
            done_message="执行完毕",
            enable_agent_replay=True,
        )
        # 流结束后换行
        console.print()
        if result.seen_error:
            sys.exit(1)
        if not result.seen_done:
            print_error("流式请求已结束，但未收到 done/stream_end 事件（执行状态不可信）")
            sys.exit(1)
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)
    except KeyboardInterrupt:
        console.print("\n[yellow]已中断[/yellow]")
        sys.exit(130)


@click.command(name="resume")
@click.argument("run_id", type=int)
@click.argument("message")
@click.option("--prompt-token", default=None, help="need_input 返回的 prompt_token")
@click.option("--session-key", default=None, help="need_input 返回的 session_key")
@click.pass_context
def resume(
    ctx: click.Context,
    run_id: int,
    message: str,
    prompt_token: str | None,
    session_key: str | None,
) -> None:
    """继续执行 waiting/stopped/failed 的 agent run（流式）。"""
    client = ctx.obj["client"]
    output_json = ctx.obj["output_json"]

    payload: dict = {"run_id": int(run_id), "message": message}
    if prompt_token:
        payload["prompt_token"] = str(prompt_token).strip()
    if session_key:
        payload["session_key"] = str(session_key).strip()

    try:
        result = run_stream_session(
            client=client,
            path="/agent/command/resume/stream",
            payload=payload,
            output_json=output_json,
            done_message="恢复执行完毕",
            enable_agent_replay=True,
        )
        console.print()
        if result.seen_error:
            sys.exit(1)
        if not result.seen_done:
            print_error("恢复流已结束，但未收到 done/stream_end 事件（执行状态不可信）")
            sys.exit(1)
    except CliError as exc:
        print_error(str(exc))
        sys.exit(exc.exit_code)
    except KeyboardInterrupt:
        console.print("\n[yellow]已中断[/yellow]")
        sys.exit(130)
