import json
from typing import Optional, Tuple

import httpx

from backend.src.constants import HTTP_REQUEST_DEFAULT_TIMEOUT_MS


def _truncate_inline(text: object, max_chars: int = 220) -> str:
    raw = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    raw = " ".join(raw.split()).strip()
    if not raw:
        return ""
    limit = max(1, int(max_chars))
    if len(raw) <= limit:
        return raw
    return f"{raw[: max(0, limit - 1)]}…"


def _extract_business_error_message(text: str) -> Optional[str]:
    """
    从 JSON 响应里提取业务失败信息。

    约定：若 payload 包含 success=false，则视为业务失败。
    """
    raw = str(text or "").strip()
    if not raw:
        return None
    if not (raw.startswith("{") or raw.startswith("[")):
        return None
    try:
        obj = json.loads(raw)
    except Exception:
        return None

    if not isinstance(obj, dict):
        return None

    if obj.get("success") is not False:
        return None

    err = obj.get("error")
    message = ""
    if isinstance(err, dict):
        message = str(
            err.get("message")
            or err.get("msg")
            or err.get("error")
            or err.get("detail")
            or ""
        ).strip()
        code = err.get("statusCode") if err.get("statusCode") is not None else err.get("code")
        if code is not None:
            code_text = str(code).strip()
            if code_text:
                message = f"code={code_text} {message}".strip()
    elif err is not None:
        message = str(err).strip()

    if not message:
        message = str(obj.get("message") or obj.get("msg") or obj.get("detail") or "").strip()

    return message or "success=false"


def execute_http_request(payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 http_request：发起 HTTP 请求并返回响应文本。

    默认启用业务成功门禁：当 JSON 中包含 success=false 时，步骤按失败处理，
    避免后续链路在错误响应上继续“编造数据”。
    """
    url = payload.get("url")
    if not isinstance(url, str) or not url.strip():
        raise ValueError("http_request.url 不能为空")

    method = str(payload.get("method") or "GET").strip().upper()
    headers = payload.get("headers") if isinstance(payload.get("headers"), dict) else None
    params = payload.get("params") if isinstance(payload.get("params"), dict) else None
    data = payload.get("data")
    json_data = payload.get("json")
    allow_redirects = payload.get("allow_redirects")
    if allow_redirects is None:
        allow_redirects = True
    timeout_ms = payload.get("timeout_ms")
    if timeout_ms is None and payload.get("timeout") is not None:
        timeout_ms = payload.get("timeout")
    try:
        timeout_ms_value = (
            float(timeout_ms)
            if timeout_ms is not None
            else float(HTTP_REQUEST_DEFAULT_TIMEOUT_MS)
        )
    except Exception:
        timeout_ms_value = float(HTTP_REQUEST_DEFAULT_TIMEOUT_MS)
    timeout = float(timeout_ms_value) / 1000 if timeout_ms_value > 0 else None

    strict_business_success = payload.get("strict_business_success")
    if strict_business_success is None:
        strict_business_success = True

    strict_status_code = payload.get("strict_status_code")
    if strict_status_code is None:
        strict_status_code = True

    max_bytes = payload.get("max_bytes")
    if max_bytes is not None:
        try:
            max_bytes = int(max_bytes)
        except Exception:
            max_bytes = None

    encoding = payload.get("encoding")
    if not isinstance(encoding, str) or not encoding.strip():
        encoding = None

    try:
        with httpx.Client(timeout=timeout) as client:
            with client.stream(
                method,
                url,
                headers=headers,
                params=params,
                data=data,
                json=json_data,
                follow_redirects=bool(allow_redirects),
            ) as resp:
                if isinstance(max_bytes, int) and max_bytes > 0:
                    remaining = int(max_bytes)
                    chunks: list[bytes] = []
                    for chunk in resp.iter_bytes():
                        if not chunk:
                            continue
                        if remaining <= 0:
                            break
                        if len(chunk) > remaining:
                            chunks.append(chunk[:remaining])
                            remaining = 0
                            break
                        chunks.append(chunk)
                        remaining -= len(chunk)
                    raw = b"".join(chunks)
                else:
                    raw = resp.read() or b""
        if isinstance(max_bytes, int) and max_bytes > 0:
            raw = raw[:max_bytes]
        use_encoding = encoding or resp.encoding or "utf-8"
        try:
            text = raw.decode(use_encoding, errors="ignore")
        except Exception:
            text = raw.decode("utf-8", errors="ignore")

        # 默认启用状态码门禁：HTTP>=400 直接视为失败，避免把 429/403/404 页面当作“成功抓取证据”继续执行。
        try:
            status_code = int(resp.status_code)
        except Exception:
            status_code = 0
        if bool(strict_status_code) and status_code >= 400:
            preview = _truncate_inline(text, 260)
            url_text = str(resp.url)
            tail = f" {preview}" if preview else ""
            return None, f"http_request HTTP {status_code}: {url_text}{tail}"

        if bool(strict_business_success):
            business_error = _extract_business_error_message(text)
            if business_error:
                return None, f"http_request 业务失败: {business_error}"

        return {
            "url": str(resp.url),
            "status_code": int(resp.status_code),
            "headers": dict(resp.headers),
            "bytes": len(raw),
            "content": text,
        }, None
    except Exception as exc:
        return None, f"http_request 执行失败: {exc}"
