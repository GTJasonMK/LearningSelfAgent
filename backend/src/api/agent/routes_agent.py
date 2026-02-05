from fastapi import APIRouter

from backend.src.agent.runner import route_agent_mode, stream_agent_command, stream_agent_think_command
from backend.src.api.schemas import AgentCommandStreamRequest, AgentRouteRequest

router = APIRouter()


@router.post("/agent/route")
def agent_route(payload: AgentRouteRequest):
    """
    自动模式选择：让 LLM 决定当前输入是否需要启用 plan/ReAct。
    返回：{"mode":"chat|do|think","confidence":0-1,"reason":"..."}
    """

    return route_agent_mode(payload)


@router.post("/agent/command/stream")
def agent_command_stream(payload: AgentCommandStreamRequest):
    """
    自然语言指令执行（SSE 流式）：先规划 plan，再按 ReAct 逐步决定 action 并执行，持续回传进度给桌宠。

    参数:
        - message: 用户指令
        - max_steps: 最大步骤数（可选）
        - model: LLM 模型（可选）
        - parameters: LLM 参数（可选）
        - dry_run: 仅规划不执行（可选）
        - mode: 执行模式，do（默认）/ think（多模型协作）/ auto（自动升降级 do↔think）
        - think_config: Think 模式配置（可选）
    """
    # 根据 mode 参数分流到不同的执行流程
    mode = (payload.mode or "").strip().lower()
    if not mode:
        mode = "do"
    if mode == "think":
        return stream_agent_think_command(payload)
    if mode == "auto":
        # 成本策略：自动升降级 do↔think（优先按路由器判断复杂度）。
        # 注意：route 可能返回 chat；但本接口语义是“执行任务”，因此 chat 统一降级为 do。
        try:
            routed = route_agent_mode(
                AgentRouteRequest(message=payload.message, model=payload.model, parameters=payload.parameters)
            )
            routed_mode = str((routed or {}).get("mode") or "").strip().lower()
            if routed_mode == "think":
                return stream_agent_think_command(payload)
        except Exception:
            # 路由失败兜底：保持系统可用，继续按 do 执行
            pass
    # 默认走 do 模式
    return stream_agent_command(payload)
