from __future__ import annotations

import re

"""
task_steps.error / 运行时错误字符串的轻量结构化约定。

目标：
- 不改 DB schema（error 仍是字符串），但允许在字符串里携带稳定的 error_code；
- 让编排策略（replan/重试/阻断）优先基于 code 判断，而不是做脆弱的关键字匹配；
- 对 UI 仍保持可读：`[code=xxx] 人类可读错误信息`。
"""

_TASK_ERROR_CODE_RE = re.compile(r"\[code=([a-z0-9_]+)\]", re.IGNORECASE)

# 归类为“外部源失败”的错误码（用于换源/重试/重规划策略）。
# 约束：
# - 这里仅定义“来源可用性”相关语义，不包含业务校验错误（例如参数契约不匹配）。
_SOURCE_FAILURE_CODE_EXACT = {
    "rate_limited",
    "web_fetch_blocked",
    "missing_api_key",
    "service_unavailable",
    "timeout",
    "dns_resolution_failed",
    "tls_handshake_failed",
    "network_unreachable",
}

# 对未来新增 code 保持前向兼容：只要遵循统一前缀，即可自动被识别为源失败。
_SOURCE_FAILURE_CODE_PREFIXES = (
    "http_",
    "dns_",
    "tls_",
    "network_",
    "proxy_",
    "connection_",
)


def extract_task_error_code(error_text: str) -> str:
    """
    从错误文本中提取约定的 code；若不存在则返回空字符串。
    """
    if not isinstance(error_text, str) or not error_text.strip():
        return ""
    match = _TASK_ERROR_CODE_RE.search(error_text)
    if not match:
        return ""
    return str(match.group(1) or "").strip().lower()


def normalize_task_error_code(code: str) -> str:
    """
    归一化错误码（小写、去首尾空白）；空值返回空字符串。
    """
    return str(code or "").strip().lower()


def is_source_failure_error_code(code: str) -> bool:
    """
    判断错误码是否属于“外部源不可用/不可达”类别。

    用途：
    - ReAct 失败策略：决定是否强制 replan（换源优先）。
    - 失败摘要：聚合同类 source 失败，反馈给后续提示词约束。
    """
    normalized = normalize_task_error_code(code)
    if not normalized:
        return False
    if normalized in _SOURCE_FAILURE_CODE_EXACT:
        return True
    if any(normalized.startswith(prefix) for prefix in _SOURCE_FAILURE_CODE_PREFIXES):
        return True
    if normalized.endswith("_timeout"):
        return True
    return False


def format_task_error(*, code: str, message: str) -> str:
    """
    为错误消息附加 `[code=...]` 前缀（若 message 已包含 code 则保持原样）。
    """
    msg = str(message or "").strip()
    normalized_code = str(code or "").strip().lower()
    if not normalized_code:
        return msg
    if extract_task_error_code(msg):
        return msg
    if not msg:
        return f"[code={normalized_code}]"
    return f"[code={normalized_code}] {msg}"
