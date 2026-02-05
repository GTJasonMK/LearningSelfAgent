from typing import Optional, Tuple

from backend.src.constants import ERROR_MESSAGE_COMMAND_FAILED
from backend.src.services.execution.shell_command import run_shell_command


def execute_shell_command(payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 shell_command：运行命令并返回 stdout/stderr/returncode。
    """
    command = payload.get("command")
    if isinstance(command, str):
        if not command.strip():
            raise ValueError("shell_command.command 不能为空")
    elif isinstance(command, list):
        if not command:
            raise ValueError("shell_command.command 不能为空")
    else:
        raise ValueError("shell_command.command 不能为空")

    result, error_message = run_shell_command(payload)
    if error_message:
        raise ValueError(error_message)
    if not isinstance(result, dict):
        raise ValueError(ERROR_MESSAGE_COMMAND_FAILED)

    ok = bool(result.get("ok"))
    if not ok:
        stdout = str(result.get("stdout") or "").strip()
        stderr = str(result.get("stderr") or "").strip()
        rc = result.get("returncode")
        detail = stderr or stdout or (str(rc) if rc is not None else "")
        detail = detail.strip()
        raise ValueError(f"{ERROR_MESSAGE_COMMAND_FAILED}:{detail}" if detail else ERROR_MESSAGE_COMMAND_FAILED)
    return result, None
