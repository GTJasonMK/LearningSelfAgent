from __future__ import annotations

import os
import shlex
from typing import List, Optional

from backend.src.common.path_utils import normalize_windows_abs_path_on_posix
from backend.src.common.utils import parse_json_dict


def truncate_inline_text(text: object, max_chars: int = 220) -> str:
    raw = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    raw = " ".join(raw.split()).strip()
    if not raw:
        return ""
    limit = max(1, int(max_chars))
    if len(raw) <= limit:
        return raw
    return f"{raw[: max(0, limit - 1)]}…"


def load_json_object(value: object) -> Optional[dict]:
    if isinstance(value, dict):
        return dict(value)
    if not isinstance(value, str):
        return None
    text = str(value).strip()
    if not text:
        return None
    parsed = parse_json_dict(text)
    return dict(parsed) if isinstance(parsed, dict) else None


def parse_command_tokens(command: object) -> List[str]:
    if isinstance(command, list):
        return [str(item) for item in command if str(item).strip()]
    if isinstance(command, str):
        text = str(command).strip()
        if not text:
            return []
        args = shlex.split(text, posix=os.name != "nt")
        if os.name != "nt":
            return [str(item) for item in args]
        cleaned: List[str] = []
        for item in args:
            token = str(item)
            if len(token) >= 2 and ((token[0] == token[-1] == '"') or (token[0] == token[-1] == "'")):
                token = token[1:-1]
            cleaned.append(token)
        return cleaned
    return []


def resolve_path_with_workdir(raw_path: str, workdir: str) -> str:
    text = normalize_windows_abs_path_on_posix(str(raw_path or "").strip())
    if not text:
        return ""
    base = normalize_windows_abs_path_on_posix(str(workdir or "").strip())
    if not base:
        base = os.getcwd()
    if not os.path.isabs(base):
        base = os.path.abspath(base)
    if os.path.isabs(text):
        return os.path.abspath(text)
    return os.path.abspath(os.path.join(base, text))
