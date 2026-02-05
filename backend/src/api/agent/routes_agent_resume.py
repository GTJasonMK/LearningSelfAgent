from fastapi import APIRouter

from backend.src.agent.runner import stream_agent_command_resume
from backend.src.api.schemas import AgentCommandResumeStreamRequest

router = APIRouter()


@router.post("/agent/command/resume/stream")
def agent_command_resume_stream(payload: AgentCommandResumeStreamRequest):
    """
    继续执行一个进入 waiting/stopped 的 agent run（SSE 流式）。
    """

    return stream_agent_command_resume(payload)
