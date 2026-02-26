from typing import Optional, Tuple
from urllib.parse import urlparse

from backend.src.actions.handlers.common_utils import truncate_inline_text
from backend.src.common.utils import parse_json_dict

try:
    import httpx  # type: ignore
except ModuleNotFoundError:
    class _MissingHttpxClient:
        def __init__(self, *_args, **_kwargs):
            raise ModuleNotFoundError("No module named 'httpx'")

    class _MissingHttpxModule:
        Client = _MissingHttpxClient

    httpx = _MissingHttpxModule()  # type: ignore[assignment]

from backend.src.constants import HTTP_REQUEST_DEFAULT_TIMEOUT_MS
from backend.src.common.task_error_codes import format_task_error

# HTTP 错误状态码时的预览截断长度
_ERROR_PREVIEW_CHARS = 260


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
    obj = parse_json_dict(raw)
    if not obj:
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


def _normalize_host(url_text: object) -> str:
    text = str(url_text or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
        host = str(parsed.netloc or "").strip().lower()
    except Exception:
        host = ""
    if not host:
        return ""
    host = host.split("@", 1)[-1]
    host = host.split(":", 1)[0].strip()
    if host.startswith("www."):
        host = host[4:]
    return host


def _reorder_source_candidates(candidates: list[str]) -> list[str]:
    """
    候选源重排：优先尝试“不同 host”的首个候选，再回填同 host 的剩余候选。
    """
    out: list[str] = []
    used_indexes = set()
    seen_hosts = set()

    for idx, source_url in enumerate(candidates):
        host = _normalize_host(source_url) or f"__nohost__:{idx}"
        if host in seen_hosts:
            continue
        seen_hosts.add(host)
        out.append(source_url)
        used_indexes.add(idx)

    for idx, source_url in enumerate(candidates):
        if idx in used_indexes:
            continue
        out.append(source_url)

    return out


def _classify_http_status_error_code(status_code: int) -> str:
    if int(status_code) == 429:
        return "rate_limited"
    if int(status_code) == 403:
        return "web_fetch_blocked"
    if int(status_code) == 401:
        return "missing_api_key"
    if int(status_code) >= 500:
        return "service_unavailable"
    return f"http_{int(status_code)}"


def _classify_business_error_code(message: str) -> str:
    lowered = str(message or "").strip().lower()
    if not lowered:
        return ""
    if (
        "rate limit" in lowered
        or "too many requests" in lowered
        or "quota exceeded" in lowered
        or "限流" in lowered
    ):
        return "rate_limited"
    if (
        "api key" in lowered
        or "access key" in lowered
        or "missing key" in lowered
        or "missing token" in lowered
        or "invalid key" in lowered
        or "unauthorized" in lowered
    ):
        return "missing_api_key"
    return ""


def _classify_exception_error_code(error_text: str) -> str:
    lowered = str(error_text or "").strip().lower()
    if not lowered:
        return "service_unavailable"
    if (
        "could not resolve host" in lowered
        or "name or service not known" in lowered
        or "temporary failure in name resolution" in lowered
        or "nodename nor servname provided" in lowered
        or "dns" in lowered
    ):
        return "dns_resolution_failed"
    if "tls" in lowered or "ssl" in lowered or "certificate verify failed" in lowered:
        return "tls_handshake_failed"
    if "timeout" in lowered or "timed out" in lowered:
        return "timeout"
    if (
        "connection refused" in lowered
        or "connection reset" in lowered
        or "network is unreachable" in lowered
        or "no route to host" in lowered
        or "failed to establish a new connection" in lowered
    ):
        return "network_unreachable"
    return "service_unavailable"


def _is_host_level_failure_code(code: str) -> bool:
    normalized = str(code or "").strip().lower()
    if not normalized:
        return False
    if normalized in {
        "dns_resolution_failed",
        "tls_handshake_failed",
        "network_unreachable",
        "timeout",
        "rate_limited",
        "web_fetch_blocked",
        "missing_api_key",
        "service_unavailable",
        "http_429",
    }:
        return True
    if normalized.startswith("http_5"):
        return True
    return False


def execute_http_request(payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 http_request：发起 HTTP 请求并返回响应文本。

    默认启用业务成功门禁：当 JSON 中包含 success=false 时，步骤按失败处理，
    避免后续链路在错误响应上继续“编造数据”。
    """
    url = payload.get("url")
    if not isinstance(url, str) or not url.strip():
        raise ValueError("http_request.url 不能为空")
    primary_url = str(url).strip()

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

    fallback_urls_raw = payload.get("fallback_urls")
    fallback_urls: list[str] = []
    if isinstance(fallback_urls_raw, str) and fallback_urls_raw.strip():
        fallback_urls.append(str(fallback_urls_raw).strip())
    elif isinstance(fallback_urls_raw, list):
        for item in fallback_urls_raw:
            if not isinstance(item, str):
                continue
            text = str(item).strip()
            if not text:
                continue
            fallback_urls.append(text)

    source_candidates: list[str] = []
    seen_sources = set()
    for item in [primary_url, *fallback_urls]:
        text = str(item or "").strip()
        if not text or text in seen_sources:
            continue
        seen_sources.add(text)
        source_candidates.append(text)
    if not source_candidates:
        return None, "http_request 执行失败: 未提供有效请求源"
    ordered_candidates = _reorder_source_candidates(source_candidates)

    def _execute_once(request_url: str) -> Tuple[Optional[dict], Optional[str], str]:
        with httpx.Client(timeout=timeout) as client:
            with client.stream(
                method,
                request_url,
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
                # 在 stream context 关闭前保存响应元数据，避免连接释放后属性不可用
                resp_encoding = resp.encoding
                resp_status_code = resp.status_code
                resp_url = str(resp.url)
                resp_headers = dict(resp.headers)
        if isinstance(max_bytes, int) and max_bytes > 0:
            raw = raw[:max_bytes]
        use_encoding = encoding or resp_encoding or "utf-8"
        try:
            text = raw.decode(use_encoding, errors="ignore")
        except Exception:
            text = raw.decode("utf-8", errors="ignore")

        # 默认启用状态码门禁：HTTP>=400 直接视为失败，避免把 429/403/404 页面当作"成功抓取证据"继续执行。
        try:
            status_code = int(resp_status_code)
        except Exception:
            status_code = 0
        if bool(strict_status_code) and status_code >= 400:
            preview = truncate_inline_text(text, _ERROR_PREVIEW_CHARS)
            url_text = resp_url
            tail = f" {preview}" if preview else ""
            code = _classify_http_status_error_code(int(status_code))
            return None, format_task_error(
                code=code,
                message=f"http_request HTTP {status_code}: {url_text}{tail}",
            ), code

        if bool(strict_business_success):
            business_error = _extract_business_error_message(text)
            if business_error:
                business_code = _classify_business_error_code(business_error)
                message_text = f"http_request 业务失败: {business_error}"
                if business_code:
                    message_text = format_task_error(code=business_code, message=message_text)
                return None, message_text, business_code

        return {
            "url": resp_url,
            "status_code": int(resp_status_code),
            "headers": resp_headers,
            "bytes": len(raw),
            "content": text,
            "source_url": str(request_url),
        }, None, ""

    errors: list[str] = []
    last_error = "http_request 执行失败: 未知错误"
    blocked_hosts = set()
    for idx, source_url in enumerate(ordered_candidates):
        source_host = _normalize_host(source_url)
        if source_host and source_host in blocked_hosts:
            skipped_error = f"http_request 跳过同 host 备选源: host={source_host}"
            if len(ordered_candidates) > 1:
                errors.append(f"source#{idx + 1} {source_url} -> {skipped_error}")
            last_error = skipped_error
            continue
        try:
            result, error_message, error_code = _execute_once(source_url)
        except Exception as exc:
            error_code = _classify_exception_error_code(str(exc))
            result, error_message = None, format_task_error(
                code=error_code,
                message=f"http_request 执行失败: {exc}",
            )
        if error_message:
            last_error = str(error_message)
            if source_host and _is_host_level_failure_code(error_code):
                blocked_hosts.add(source_host)
            if len(ordered_candidates) > 1:
                errors.append(f"source#{idx + 1} {source_url} -> {last_error}")
            continue
        if result is None:
            last_error = "http_request 执行失败: 空响应"
            if len(ordered_candidates) > 1:
                errors.append(f"source#{idx + 1} {source_url} -> {last_error}")
            continue
        if len(ordered_candidates) > 1:
            result["source_index"] = int(idx)
            result["source_count"] = int(len(ordered_candidates))
        return result, None

    if len(ordered_candidates) <= 1:
        return None, last_error

    summary = " | ".join(errors[:4])
    if len(errors) > 4:
        summary = f"{summary} | ... 共{len(errors)}个失败源"
    return None, f"http_request 候选源全部失败: {summary or last_error}"
