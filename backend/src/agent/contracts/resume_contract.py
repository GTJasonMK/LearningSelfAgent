from __future__ import annotations


def validate_waiting_resume_contract(
    *,
    required_session_key: str,
    request_session_key: str,
    required_prompt_token: str,
    request_prompt_token: str,
) -> str:
    """
    waiting 状态下 resume 的最小契约校验。

    返回值：
    - 空字符串：通过
    - 非空字符串：错误提示（供 API 直接返回给前端）
    """
    required_session = str(required_session_key or "").strip()
    request_session = str(request_session_key or "").strip()
    required_token = str(required_prompt_token or "").strip()
    request_token = str(request_prompt_token or "").strip()

    if required_session:
        if not request_session:
            return "resume 缺少 session_key，请刷新后重试"
        if request_session != required_session:
            return "resume session_key 不匹配，请刷新后重试"

    if required_token:
        if not request_token:
            return "resume 缺少 prompt_token，请刷新后重试"
        if request_token != required_token:
            return "resume prompt_token 不匹配，请刷新后重试"

    return ""
