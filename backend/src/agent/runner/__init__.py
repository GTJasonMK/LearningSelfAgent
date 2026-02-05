"""
Agent Runner（任务执行器）

职责：
- 封装 Agent 的"路由（chat/do/think）"与"Plan-ReAct 流式执行"主链路；
- API 层只负责参数校验与 StreamingResponse 组装，避免路由文件成为上帝类。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # 仅用于类型检查；运行时使用懒导入以降低模块 import 的耦合度（便于离线单测）。
    from backend.src.api.schemas import AgentCommandResumeStreamRequest, AgentCommandStreamRequest, AgentRouteRequest

__all__ = [
    "route_agent_mode",
    "review_repair",
    "stream_agent_command",
    "stream_agent_command_resume",
    "stream_agent_think_command",
]


def __getattr__(name: str):
    # 运行时懒导入：避免 import backend.src.agent.runner 时强耦合所有子模块；
    # 同时让 unittest.mock.patch("backend.src.agent.runner.<submodule>...") 可正常工作。
    #
    # 说明：
    # - Python 3.12 的 unittest.mock.patch 会通过 pkgutil.resolve_name 逐段 getattr；
    # - 若这里不显式暴露子模块，patch("backend.src.agent.runner.stream_think_run...") 会失败；
    # - 因此把 runner 目录下的关键子模块做成“可 getattr 的懒加载属性”。
    from importlib import import_module

    submodules = {
        # 外部 API/测试常用 patch 入口
        "route_mode",
        "react_loop",
        "react_state_manager",
        "stream_new_run",
        "stream_resume_run",
        "stream_think_run",
        "stream_pump",
        "think_parallel_loop",
        "review_repair",
        # 计划事件/反馈（部分测试会 patch 常量或函数）
        "plan_events",
        "feedback",
        # 预留：逐步把 stream_* 重复逻辑抽到 pipeline
        "execution_pipeline",
    }

    if name in submodules:
        return import_module(f"backend.src.agent.runner.{name}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def route_agent_mode(payload: "AgentRouteRequest"):
    # 懒导入：避免 import backend.src.agent.runner 时强依赖 pydantic/fastapi
    from backend.src.agent.runner.route_mode import route_agent_mode as _impl

    return _impl(payload)


def stream_agent_command(payload: "AgentCommandStreamRequest"):
    from backend.src.agent.runner.stream_new_run import stream_agent_command as _impl

    return _impl(payload)


def stream_agent_command_resume(payload: "AgentCommandResumeStreamRequest"):
    from backend.src.agent.runner.stream_resume_run import (
        stream_agent_command_resume as _impl,
    )

    return _impl(payload)


def stream_agent_think_command(payload: "AgentCommandStreamRequest"):
    from backend.src.agent.runner.stream_think_run import stream_agent_think_command as _impl

    return _impl(payload)
