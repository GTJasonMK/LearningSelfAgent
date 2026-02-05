from typing import Optional, Tuple

import httpx

from backend.src.constants import HTTP_REQUEST_DEFAULT_TIMEOUT_MS


def execute_http_request(payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 http_request：发起 HTTP 请求并返回响应文本。
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
        return {
            "url": str(resp.url),
            "status_code": int(resp.status_code),
            "headers": dict(resp.headers),
            "bytes": len(raw),
            "content": text,
        }, None
    except Exception as exc:
        return None, f"http_request 执行失败: {exc}"
