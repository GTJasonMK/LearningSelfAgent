from __future__ import annotations

from typing import Optional

from backend.src.common.utils import (
    as_bool,
    error_response,
    now_iso,
    parse_json_list,
    parse_json_value,
    render_prompt,
)
from backend.src.services.execution.shell_command import run_shell_command as _run_shell_command
from backend.src.services.permissions.permission_checks import ensure_exec_permission, ensure_write_permission
from backend.src.services.permissions.permissions_store import has_write_permission as _has_write_permission


def has_write_permission() -> bool:
    return _has_write_permission()


def run_shell_command(payload: dict) -> tuple[Optional[dict], Optional[str]]:
    return _run_shell_command(payload)
