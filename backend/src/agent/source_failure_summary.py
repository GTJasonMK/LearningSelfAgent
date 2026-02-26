from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from backend.src.common.utils import coerce_int
from backend.src.common.task_error_codes import extract_task_error_code, is_source_failure_error_code

_URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
_SIGNATURE_CODE_RE = re.compile(r"\|code:([a-z0-9_]+)")


def _normalize_host(url_text: str) -> str:
    parsed = urlparse(str(url_text or "").strip())
    host = str(parsed.netloc or "").strip().lower()
    if not host:
        return ""
    host = host.split("@", 1)[-1]
    host = host.split(":", 1)[0].strip()
    if host.startswith("www."):
        host = host[4:]
    return host


def _extract_hosts(text: str) -> List[str]:
    hosts: List[str] = []
    seen = set()
    for raw_url in _URL_RE.findall(str(text or "")):
        normalized_url = str(raw_url or "").rstrip(".,;:)]}\"'")
        host = _normalize_host(normalized_url)
        if not host or host in seen:
            continue
        seen.add(host)
        hosts.append(host)
    return hosts


def _infer_source_failure_code(text: str) -> str:
    lowered = str(text or "").lower()
    if not lowered:
        return ""
    if ("rate limit" in lowered) or ("too many requests" in lowered) or (" 429" in lowered):
        return "rate_limited"
    if (
        "missing_access_key" in lowered
        or "missing api key" in lowered
        or "missing_api_key" in lowered
        or ("access key" in lowered and "missing" in lowered)
        or ("api key" in lowered and "missing" in lowered)
    ):
        return "missing_api_key"
    if ("handshake" in lowered) and (("ssl" in lowered) or ("tls" in lowered)):
        return "tls_handshake_failed"
    if (
        "could not resolve host" in lowered
        or "temporary failure in name resolution" in lowered
        or "name or service not known" in lowered
        or "nodename nor servname provided" in lowered
    ):
        return "dns_resolution_failed"
    if (
        "network is unreachable" in lowered
        or "no route to host" in lowered
        or "connection refused" in lowered
        or "connection reset by peer" in lowered
    ):
        return "network_unreachable"
    if ("403" in lowered) or ("forbidden" in lowered):
        return "http_403"
    if ("404" in lowered) or ("not found" in lowered):
        return "http_404"
    if ("429" in lowered) or ("too many requests" in lowered):
        return "http_429"
    if re.search(r"\b5\d{2}\b", lowered):
        return "http_5xx"
    if ("503" in lowered) or ("service unavailable" in lowered):
        return "service_unavailable"
    if ("timeout" in lowered) or ("timed out" in lowered):
        return "timeout"
    return ""


def _is_source_failure_code(code: str) -> bool:
    return is_source_failure_error_code(str(code or ""))


def _extract_code_from_signature(signature: str) -> str:
    match = _SIGNATURE_CODE_RE.search(str(signature or "").lower())
    if not match:
        return ""
    return str(match.group(1) or "").strip().lower()


def summarize_recent_source_failures_for_prompt(
    *,
    observations: List[str],
    error: str = "",
    failure_signatures: Optional[dict] = None,
    max_items: int = 6,
) -> str:
    """
    提取“最近外部源失败”摘要，供提示词约束换源策略使用。
    """
    host_code_counts: Dict[Tuple[str, str], int] = {}
    signature_code_counts: Dict[str, int] = {}

    recent_observations = [str(item or "").strip() for item in list(observations or [])[-12:]]
    candidate_texts = [item for item in recent_observations if item]
    error_text = str(error or "").strip()
    if error_text:
        candidate_texts.append(error_text)

    for text in candidate_texts:
        lowered = text.lower()
        if "[code=" not in lowered and "fail" not in lowered:
            continue

        code = extract_task_error_code(text) or _infer_source_failure_code(text)
        if code and not _is_source_failure_code(code):
            continue
        hosts = _extract_hosts(text)
        if not code and not hosts:
            continue

        normalized_code = str(code or "unknown_source_error").strip().lower()
        normalized_hosts = hosts or ["(unknown_host)"]
        for host in normalized_hosts:
            key = (host, normalized_code)
            host_code_counts[key] = coerce_int(host_code_counts.get(key), default=0) + 1

    if isinstance(failure_signatures, dict):
        for signature, info in failure_signatures.items():
            code = _extract_code_from_signature(str(signature or ""))
            if not _is_source_failure_code(code):
                continue
            count = 0
            if isinstance(info, dict):
                count = coerce_int(info.get("count"), default=0)
            if count <= 0:
                continue
            signature_code_counts[code] = coerce_int(signature_code_counts.get(code), default=0) + count

    # 避免重复表达：若某个 code 已在 host 维度体现，则仅保留“未覆盖”的历史汇总。
    covered_codes: Dict[str, int] = {}
    for (_, code), count in host_code_counts.items():
        covered_codes[code] = coerce_int(covered_codes.get(code), default=0) + coerce_int(count, default=0)
    for code in list(signature_code_counts.keys()):
        remain = coerce_int(signature_code_counts.get(code), default=0) - coerce_int(
            covered_codes.get(code), default=0
        )
        if remain > 0:
            signature_code_counts[code] = remain
        else:
            signature_code_counts.pop(code, None)

    if not host_code_counts and not signature_code_counts:
        return "(无)"

    lines: List[str] = []
    sorted_host_items = sorted(
        host_code_counts.items(),
        key=lambda item: (-coerce_int(item[1], default=0), str(item[0][1]), str(item[0][0])),
    )
    for (host, code), count in sorted_host_items[: max(1, coerce_int(max_items, default=6))]:
        if host == "(unknown_host)":
            lines.append(f"- code={code} count={count}")
        else:
            lines.append(f"- host={host} code={code} count={count}")

    remain_slots = max(0, coerce_int(max_items, default=6) - len(lines))
    if remain_slots > 0 and signature_code_counts:
        for code, count in sorted(
            signature_code_counts.items(),
            key=lambda item: (-coerce_int(item[1], default=0), str(item[0])),
        )[
            :remain_slots
        ]:
            lines.append(f"- code={code} recent_count={count}")

    return "\n".join(lines) if lines else "(无)"
