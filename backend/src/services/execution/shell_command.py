from __future__ import annotations

import locale
import os
import re
import shlex
import subprocess
import sys
import uuid
from typing import Optional, Tuple

from backend.src.common.python_code import has_risky_inline_control_flow, normalize_python_c_source
from backend.src.constants import (
    AGENT_EXPERIMENT_DIR_REL,
    ERROR_MESSAGE_COMMAND_FAILED,
    ERROR_MESSAGE_PERMISSION_DENIED,
    ERROR_MESSAGE_PROMPT_RENDER_FAILED,
)
from backend.src.services.permissions.permissions_store import has_exec_permission


_WINDOWS_CMD_BUILTINS = {
    "assoc",
    "break",
    "call",
    "cd",
    "chdir",
    "cls",
    "copy",
    "date",
    "del",
    "dir",
    "echo",
    "endlocal",
    "erase",
    "exit",
    "for",
    "ftype",
    "goto",
    "if",
    "md",
    "mkdir",
    "mklink",
    "move",
    "path",
    "pause",
    "popd",
    "prompt",
    "pushd",
    "rd",
    "rem",
    "ren",
    "rename",
    "rmdir",
    "set",
    "setlocal",
    "shift",
    "start",
    "time",
    "title",
    "type",
    "ver",
    "verify",
    "vol",
}


def _normalize_windows_builtin_command_line(raw_command: str, args: list[str]) -> str:
    """
    Windows 内建命令（dir/copy/...）统一路径分隔符，避免 `backend/.agent/...` 被 cmd 误判。
    """
    if not raw_command:
        return ""
    if not args:
        return raw_command

    normalized_tokens: list[str] = []
    for idx, token in enumerate(args):
        current = str(token or "")
        if idx > 0:
            lowered = current.lower()
            is_url = lowered.startswith("http://") or lowered.startswith("https://")
            is_option = current.startswith("/") and len(current) >= 2 and current[1].isalpha() and not re.match(
                r"^/[A-Za-z]:", current
            )
            if not is_url and not is_option and "/" in current:
                current = current.replace("/", "\\")
        normalized_tokens.append(current)

    try:
        return subprocess.list2cmdline(normalized_tokens)
    except Exception:
        return raw_command


def _decode_subprocess_stream(value: object) -> str:
    """
    将 subprocess 输出统一解码为字符串，避免编码不一致导致 UnicodeDecodeError。
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value

    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
    else:
        raw = str(value).encode("utf-8", errors="replace")

    preferred = str(locale.getpreferredencoding(False) or "").strip()
    encodings: list[str] = ["utf-8"]
    if preferred:
        encodings.append(preferred)
    if os.name == "nt":
        encodings.extend(["gb18030", "gbk"])

    seen = set()
    for enc in encodings:
        normalized = str(enc or "").strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        try:
            return raw.decode(normalized, errors="strict")
        except Exception:
            continue

    fallback = preferred or "utf-8"
    try:
        return raw.decode(fallback, errors="replace")
    except Exception:
        return raw.decode("utf-8", errors="replace")



def run_shell_command(payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行本地命令（供 shell_command/tool_call 复用）。

    返回：(result, error_message)
    - result: {"stdout": str, "stderr": str, "returncode": int|None, "ok": bool}
    - error_message: 业务错误字符串（用于写入 task_steps.error 或输出到 UI）
    """
    command = payload.get("command")
    if not command:
        return None, ERROR_MESSAGE_PROMPT_RENDER_FAILED

    raw_command_str = command if isinstance(command, str) else None
    if isinstance(command, str):
        # Windows 上路径常含反斜杠：用 posix=False 避免把 `\\` 当作转义符吞掉
        args = shlex.split(command, posix=os.name != "nt")
        if os.name == "nt":
            # shlex(posix=False) 会保留引号（例如 `"print(1)"`），而 subprocess(list) 在 Windows
            # 会把引号作为参数内容传给进程，导致 python -c 执行“字符串字面量”而非代码。
            # 这里仅剥离最外层成对引号，保持路径中的反斜杠不被吞掉。
            cleaned: list[str] = []
            for item in args:
                text = str(item)
                if len(text) >= 2 and ((text[0] == text[-1] == '"') or (text[0] == text[-1] == "'")):
                    text = text[1:-1]
                cleaned.append(text)
            args = cleaned
    elif isinstance(command, list):
        args = [str(item) for item in command]
        # 修复 python -c 未加引号导致代码被拆成多个参数的情况（Windows 常见）。
        # 注意：若 args[2] 已是完整代码且 args[3:] 是脚本参数，则不要合并。
        try:
            if (
                len(args) >= 4
                and str(args[1]).strip() == "-c"
                and str(args[0]).strip().lower() in {"python", "python3", "py", os.path.basename(sys.executable).lower()}
            ):
                code_head = str(args[2] or "").strip()

                def _looks_like_python_code(text: str) -> bool:
                    lowered = text.lower()
                    return any(
                        token in lowered
                        for token in (
                            "import",
                            "from ",
                            "def ",
                            "class ",
                            "print",
                            ";",
                            "=",
                            "lambda",
                        )
                    )

                # 仅当 code_head 看起来不像完整代码时才合并，避免把脚本参数拼进代码里
                if not _looks_like_python_code(code_head):
                    code = " ".join(args[2:]).strip()
                    if code:
                        args = [args[0], "-c", code]
        except Exception:
            pass
    else:
        return None, ERROR_MESSAGE_PROMPT_RENDER_FAILED

    # 统一 python 可执行文件：
    # - 许多机器上 `python` 不在 PATH（尤其是 Windows 环境），但后端进程本身一定有 sys.executable；
    # - 这样 Agent 通过 file_write + shell_command/tool_call 执行脚本时更稳定，不需要用户手动提供 python 路径。
    if args:
        try:
            head = str(args[0] or "").strip()
            is_bare = bool(head) and not os.path.isabs(head) and ("/" not in head) and ("\\" not in head)
            if is_bare:
                low = os.path.splitext(head)[0].lower()
                if low in {"python", "python3", "py"}:
                    args = [sys.executable] + args[1:]
                # 统一 pip：许多 Windows 环境没有把 pip.exe 放到 PATH，但后端进程一定有 sys.executable。
                # 用 `python -m pip ...` 替代裸 pip，避免 [WinError 2] 找不到可执行文件。
                elif low in {"pip", "pip3"}:
                    args = [sys.executable, "-m", "pip"] + args[1:]
        except Exception:
            pass

    # Windows：兼容 dir/copy 等 cmd 内建命令（subprocess 不能直接执行）。
    # 说明：我们优先建议模型用 python -c 做文件/文本处理，但为了鲁棒性仍做这一层兜底。
    if os.name == "nt" and raw_command_str and args:
        try:
            head = str(args[0] or "").strip().lower()
            if head in _WINDOWS_CMD_BUILTINS:
                builtin_command = _normalize_windows_builtin_command_line(str(raw_command_str), args)
                args = ["cmd.exe", "/c", builtin_command]
        except Exception:
            pass

    workdir = payload.get("workdir")
    if not has_exec_permission(workdir):
        return None, ERROR_MESSAGE_PERMISSION_DENIED

    # 将 python -c 代码自动落盘为脚本再执行（避免多行/结构化语句触发语法错误）
    if args and workdir:
        try:
            head = str(args[0] or "").strip()
            head_lower = os.path.splitext(head)[0].lower()
            is_python = head_lower in {"python", "python3", "py"} or os.path.basename(head).lower() == os.path.basename(sys.executable).lower()
            if is_python and len(args) >= 3 and str(args[1]).strip() == "-c":
                code = str(args[2] or "").strip()
                if code:
                    if has_risky_inline_control_flow(code):
                        return {
                            "stdout": "",
                            "stderr": "complex python -c requires file_write script",
                            "returncode": 1,
                            "ok": False,
                        }, None
                    code = normalize_python_c_source(code, compile_name="<shell_python_c>")
                    script_dir = os.path.join(workdir, AGENT_EXPERIMENT_DIR_REL)
                    os.makedirs(script_dir, exist_ok=True)
                    script_path = os.path.join(script_dir, f"python_c_{uuid.uuid4().hex}.py")
                    with open(script_path, "w", encoding="utf-8") as handle:
                        handle.write(code + "\n")
                    extra_args = [str(item) for item in args[3:]]
                    args = [sys.executable, script_path] + extra_args
        except Exception:
            pass

    timeout_ms = payload.get("timeout_ms")
    timeout = timeout_ms / 1000 if isinstance(timeout_ms, (int, float)) else None
    stdin_text = payload.get("stdin")
    if stdin_text is None:
        stdin_text = ""
    if not isinstance(stdin_text, str):
        try:
            stdin_text = str(stdin_text)
        except Exception:
            stdin_text = ""

    stdin_bytes = str(stdin_text).encode("utf-8", errors="replace")

    try:
        result = subprocess.run(
            args,
            cwd=workdir,
            capture_output=True,
            text=False,
            input=stdin_bytes,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {
            "stdout": "",
            "stderr": "timeout",
            "returncode": None,
            "ok": False,
        }, None
    except FileNotFoundError as exc:
        return None, f"{ERROR_MESSAGE_COMMAND_FAILED}:{exc}"
    except (ValueError, OSError) as exc:
        return None, f"{ERROR_MESSAGE_COMMAND_FAILED}:{exc}"
    except Exception as exc:
        return None, f"{ERROR_MESSAGE_COMMAND_FAILED}:{exc}"

    return {
        "stdout": _decode_subprocess_stream(result.stdout),
        "stderr": _decode_subprocess_stream(result.stderr),
        "returncode": result.returncode,
        "ok": result.returncode == 0,
    }, None
