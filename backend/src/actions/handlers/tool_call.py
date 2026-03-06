import base64
import hashlib
import json
import os
import re
import shlex
import time
from functools import lru_cache
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

from backend.src.actions.handlers.common_utils import (
    load_json_object,
    parse_command_tokens,
    resolve_path_with_workdir,
    truncate_inline_text,
)
from backend.src.common.path_utils import normalize_windows_abs_path_on_posix
from backend.src.common.text_sanitize import strip_illustrative_example_clauses
from backend.src.common.task_error_codes import format_task_error
from backend.src.common.utils import parse_json_dict, parse_json_value, parse_positive_int
from backend.src.constants import (
    ACTION_TYPE_TOOL_CALL,
    AGENT_EXPERIMENT_DIR_REL,
    AGENT_SHELL_COMMAND_DEFAULT_TIMEOUT_MS,
    AUTO_TOOL_DESCRIPTION_TEMPLATE,
    AUTO_TOOL_PREFIX,
    DEFAULT_TOOL_VERSION,
    ERROR_MESSAGE_PROMPT_RENDER_FAILED,
    TOOL_NAME_WEB_FETCH,
    WEB_FETCH_BLOCK_MARKERS_DEFAULT,
    AGENT_WEB_FETCH_BLOCK_MARKERS_ENV,
    AGENT_WEB_FETCH_BLOCK_MARKERS_MAX,
    AGENT_WEB_FETCH_FALLBACK_URL_TEMPLATES_ENV,
    AGENT_WEB_FETCH_FALLBACK_MAX_CANDIDATES,
    WEB_FETCH_FALLBACK_URL_TEMPLATES_DEFAULT,
    AGENT_WEB_FETCH_SEARCH_URL_TEMPLATES_ENV,
    WEB_FETCH_SEARCH_URL_TEMPLATES_DEFAULT,
    AGENT_WEB_FETCH_SEARCH_MAX_RESULTS,
    AGENT_WEB_FETCH_SEARCH_MAX_PAGES,
    TOOL_METADATA_SOURCE_AUTO,
    SHELL_COMMAND_REQUIRE_FILE_WRITE_BINDING_DEFAULT,
)
from backend.src.services.execution.shell_command import run_shell_command
from backend.src.services.debug.safe_debug import safe_write_debug as _safe_write_debug
from backend.src.services.llm.llm_calls import create_llm_call
from backend.src.services.tools.tool_records import create_tool_record as _create_tool_record
from backend.src.repositories.task_steps_repo import list_task_steps_for_run
from backend.src.repositories.tools_repo import (
    get_tool,
    get_tool_by_name,
    get_tool_metadata_by_id,
    get_tool_metadata_by_name,
)
from backend.src.services.permissions.permissions_store import is_tool_enabled

_WEB_FETCH_PROTOCOL_CONTEXT_KEY = "web_fetch_protocol_v1"
_WEB_FETCH_PROTOCOL_VERSION = 2
_WEB_FETCH_REQUIRE_PROTOCOL_ENV = "AGENT_WEB_FETCH_REQUIRE_PROTOCOL"
_DEFAULT_WEB_FETCH_DENY_DOMAINS = (
    "youtube.com",
    "bing.com",
    "bing.net",
    "mm.bing.net",
    "tc.mm.bing.net",
    "google.com",
    "duckduckgo.com",
    "baidu.com",
    "w3.org",
)

_WEB_FETCH_NEGATIVE_TERMS_DEFAULT = (
    "forum",
    "community",
    "discussion",
    "help",
    "support",
    "docs",
    "documentation",
    "schema",
    "login",
    "signup",
    "register",
    "captcha",
    "xhtml",
    "namespace",
    "论坛",
    "社区",
    "帮助",
    "文档",
    "登录",
)
_WEB_FETCH_POSITIVE_URL_TERMS = (
    "api",
    "csv",
    "json",
    "data",
    "dataset",
    "history",
    "historical",
    "price",
    "prices",
    "gold",
    "daily",
    "table",
    "quote",
    "quotes",
)
_WEB_FETCH_NOISE_URL_TERMS = (
    "forum",
    "community",
    "discussion",
    "help",
    "support",
    "docs",
    "documentation",
    "schema",
    "login",
    "signup",
    "register",
    "captcha",
)
_WEB_FETCH_GENERIC_PAGE_NOISE_TERMS = (
    "sign in",
    "signin",
    "login",
    "log in",
    "account",
    "profile",
    "privacy",
    "cookie",
    "cookies",
    "terms of use",
    "all rights reserved",
    "avatar",
    "inbox",
    "mailbox",
    "myprofile",
    "expressionprofile",
    "profilephoto",
)
_WEB_FETCH_NOISE_HOST_PREFIXES = (
    "login.",
    "auth.",
    "account.",
    "profile.",
    "passport.",
    "mail.",
    "storage.",
)
_WEB_FETCH_NOISE_PATH_TERMS = (
    "/login",
    "/signin",
    "/signup",
    "/register",
    "/account",
    "/accounts",
    "/auth",
    "/profile",
    "/profiles",
    "/mail",
    "/inbox",
    "profilephoto",
    "expressionprofile",
    "avatar",
    "/privacy",
    "/cookies",
)
_WEB_FETCH_ANCHOR_NOISE_TERMS = (
    "sign in",
    "login",
    "log in",
    "privacy",
    "cookie",
    "cookies",
    "terms",
    "settings",
    "feedback",
    "help",
    "support",
    "next",
    "previous",
    "images",
    "videos",
    "maps",
)
_WEB_FETCH_REQUIRED_FIELD_ALIASES = {
    "date": (
        "date",
        "日期",
        "时间",
        "day",
        "daily",
        "交易日",
        "日线",
    ),
    "price": (
        "price",
        "价格",
        "金价",
        "quote",
        "收盘价",
        "开盘价",
        "现价",
    ),
    "currency_cny": (
        "cny",
        "人民币",
        "rmb",
        "元",
    ),
    "unit_gram": (
        "gram",
        "grams",
        "g",
        "克",
        "元/克",
        "人民币/克",
        "cny/g",
    ),
}
_WEB_FETCH_PROTOCOL_SIGNAL_PLACEHOLDERS = (
    "任务对象",
    "核心对象",
    "核心指标",
    "关键指标",
    "指标名称",
    "主题对象",
    "数据对象",
    "object",
    "metric",
    "signal",
)
_WEB_FETCH_PROTOCOL_UNIT_PLACEHOLDERS = (
    "原始单位",
    "原单位",
    "原始币种",
    "单位待确认",
    "原始口径",
    "raw unit",
    "original unit",
    "original currency",
    "task unit",
)
_WEB_FETCH_PROTOCOL_TIME_PLACEHOLDERS = (
    "最近一段时间",
    "目标时间范围",
    "时间粒度",
    "合适粒度",
    "time range",
    "time granularity",
)
_WEB_FETCH_QUERY_NOISE_TERMS = (
    "请你",
    "帮我",
    "收集",
    "整理",
    "获取",
    "保存",
    "输出",
    "最近",
    "最近的",
    "最近三个月",
    "最近三个月的",
    "最近一个月",
    "最近一个月的",
    "最近一周",
    "最近半年",
    "并保存为csv文件",
    "并保存为csv",
    "保存为csv文件",
    "保存为csv",
    "csv文件",
)
_WEB_FETCH_EVENT_CANDIDATE_LIMIT = 5
_WEB_FETCH_PREVIEW_SAMPLE_CHARS = 12000
_WEB_FETCH_MIN_ACCEPT_SCORE = 18


def _normalize_web_fetch_marker(phrase: object, tag: object) -> Optional[Tuple[str, str]]:
    text = str(phrase or "").strip().lower()
    code = str(tag or "").strip().lower()
    if not text:
        return None
    if not code:
        code = "custom_blocked"
    return text, code


def _iter_env_web_fetch_markers(raw_env: str) -> List[Tuple[str, str]]:
    text = str(raw_env or "").strip()
    if not text:
        return []
    payload = parse_json_value(text)
    if not isinstance(payload, list):
        return []

    parsed: List[Tuple[str, str]] = []
    for item in payload:
        if isinstance(item, str):
            normalized = _normalize_web_fetch_marker(item, "custom_blocked")
            if normalized:
                parsed.append(normalized)
            continue
        if isinstance(item, list) and len(item) >= 1:
            phrase = item[0]
            tag = item[1] if len(item) >= 2 else "custom_blocked"
            normalized = _normalize_web_fetch_marker(phrase, tag)
            if normalized:
                parsed.append(normalized)
            continue
        if isinstance(item, dict):
            normalized = _normalize_web_fetch_marker(item.get("phrase"), item.get("tag") or item.get("code"))
            if normalized:
                parsed.append(normalized)
    return parsed


@lru_cache(maxsize=1)
def _get_web_fetch_block_markers() -> List[Tuple[str, str]]:
    """
    统一获取 web_fetch 拦截判定规则（默认 + 环境变量扩展）。

    环境变量格式（JSON array）：
    - ["blocked by upstream"]
    - [["blocked by upstream", "request_blocked"]]
    - [{"phrase":"blocked by upstream","tag":"request_blocked"}]
    """
    merged: List[Tuple[str, str]] = []
    seen = set()
    for phrase, tag in list(WEB_FETCH_BLOCK_MARKERS_DEFAULT or []):
        normalized = _normalize_web_fetch_marker(phrase, tag)
        if not normalized:
            continue
        if normalized[0] in seen:
            continue
        seen.add(normalized[0])
        merged.append(normalized)

    env_markers = _iter_env_web_fetch_markers(os.getenv(AGENT_WEB_FETCH_BLOCK_MARKERS_ENV, ""))
    for phrase, tag in env_markers:
        if phrase in seen:
            continue
        seen.add(phrase)
        merged.append((phrase, tag))

    try:
        limit = max(1, int(AGENT_WEB_FETCH_BLOCK_MARKERS_MAX or 64))
    except Exception:
        limit = 64
    return merged[:limit]


def _detect_web_fetch_block_reason(output_text: str) -> Optional[str]:
    """
    尝试识别 web_fetch 返回的“反爬/限流/拦截页面”。

    说明：
    - curl -f 只能识别 HTTP>=400；但部分站点会返回 200 + 拦截页面正文；
    - 这些正文不应作为“抓取成功证据”继续进入 json_parse/task_output，否则会诱发“编数据”。
    """
    raw = str(output_text or "").strip()
    if not raw:
        return None
    lowered = raw.lower()
    sample = lowered[:4000]
    for phrase, tag in _get_web_fetch_block_markers():
        if phrase in sample:
            return tag
    # 兜底：部分站点只返回状态行/标题，不包含完整描述文本。
    if re.search(r"\bhttp/[0-9.]+\s+429\b", sample):
        return "too_many_requests"
    if re.search(r"\bhttp/[0-9.]+\s+403\b", sample):
        return "access_denied"
    if re.search(r"\bhttp/[0-9.]+\s+503\b", sample):
        return "service_unavailable"
    return None


def _detect_web_fetch_semantic_error(output_text: str) -> Optional[str]:
    """
    检测 web_fetch 的“业务语义失败”（例如 success=false / error 对象）。

    背景：
    - 某些数据接口会返回 200 + JSON 错误体（如 missing_access_key）；
    - 这类响应不应被当作抓取成功继续流转，否则会诱发后续空产物/伪结果。
    """
    raw = str(output_text or "").strip()
    if not raw:
        return None

    parsed = parse_json_dict(raw)
    if not parsed:
        return None

    success_value = parsed.get("success")
    status_text = str(parsed.get("status") or "").strip().lower()
    error_obj = parsed.get("error")

    has_error_payload = bool(
        isinstance(error_obj, dict)
        or (isinstance(error_obj, str) and str(error_obj).strip())
    )
    explicit_failure = (success_value is False) or (status_text in {"error", "failed", "fail"})
    if not explicit_failure and not has_error_payload:
        return None

    if isinstance(error_obj, dict):
        error_type = str(
            error_obj.get("type")
            or error_obj.get("code")
            or error_obj.get("name")
            or ""
        ).strip()
        error_message = truncate_inline_text(
            error_obj.get("info")
            or error_obj.get("message")
            or error_obj.get("detail")
            or "",
            180,
        )
    else:
        error_type = ""
        error_message = truncate_inline_text(error_obj, 180)

    if error_type and error_message:
        return f"{error_type}: {error_message}"
    if error_type:
        return error_type
    if error_message:
        return error_message

    return "semantic_error"


def _iter_env_web_fetch_fallback_templates(raw_env: str) -> List[str]:
    text = str(raw_env or "").strip()
    if not text:
        return []
    payload = parse_json_value(text)
    if not isinstance(payload, list):
        return []
    parsed: List[str] = []
    for item in payload:
        current = str(item or "").strip()
        if current:
            parsed.append(current)
    return parsed


def _normalize_web_fetch_url_candidate(value: object) -> str:
    text = str(value or "").strip().strip("()[]{}<>,\"'`")
    if not text:
        return ""
    # URL 候选中出现空白通常是“URL + 关键词”混合输入，
    # 若误判为 URL 会导致 curl 直接报 malformed URL。
    # 该场景应回退到关键词检索路径，而不是直连 URL。
    if re.search(r"\s", text):
        return ""
    for _ in range(2):
        parsed = urlparse(text)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return ""
        host_text = str(parsed.hostname or "").strip().lower()
        if not _is_valid_web_fetch_host(host_text):
            return ""
        unwrapped = _extract_web_fetch_redirect_target(parsed)
        if not unwrapped or unwrapped == text:
            break
        text = unwrapped
    return text


def _extract_web_fetch_host(value: object) -> str:
    text = _normalize_web_fetch_url_candidate(value)
    if not text:
        return ""
    try:
        parsed = urlparse(text)
        host = str(parsed.hostname or "").strip().lower()
        return host if _is_valid_web_fetch_host(host) else ""
    except Exception:
        return ""


def _is_valid_web_fetch_host(host: str) -> bool:
    text = str(host or "").strip().lower()
    if not text:
        return False
    if len(text) > 253:
        return False
    if ".." in text or text.startswith(".") or text.endswith("."):
        return False
    if text.startswith("-") or text.endswith("-"):
        return False
    # 过滤 script 拼接噪声 host（如 ","http）。
    if not re.fullmatch(r"[a-z0-9.-]+", text):
        return False
    return True


_WEB_FETCH_REDIRECT_QUERY_KEYS = (
    "url",
    "u",
    "uddg",
    "target",
    "dest",
    "destination",
    "redirect",
    "redirect_url",
    "r",
    "q",
)
_WEB_FETCH_SECOND_LEVEL_SUFFIXES = {
    "co.uk",
    "org.uk",
    "ac.uk",
    "com.au",
    "com.br",
    "co.jp",
    "com.cn",
    "net.cn",
    "org.cn",
    "gov.cn",
    "com.tw",
    "com.hk",
}
_WEB_FETCH_KNOWN_SEARCH_HOST_FAMILIES = {
    "bing.com",
    "bing.net",
    "duckduckgo.com",
    "baidu.com",
    "google.com",
    "yahoo.com",
    "yandex.ru",
    "sogou.com",
    "so.com",
}
_WEB_FETCH_SEARCH_QUERY_KEYS = {
    "q",
    "query",
    "wd",
    "word",
    "keyword",
    "keywords",
    "search",
    "text",
}
_WEB_FETCH_SEARCH_LIKE_PATHS = {
    "",
    "/",
    "/s",
    "/search",
    "/html",
    "/results",
    "/query",
    "/web",
}


def _read_bool(value: object, *, default: bool) -> bool:
    if value is None:
        return bool(default)
    text = str(value).strip().lower()
    if not text:
        return bool(default)
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _is_web_fetch_protocol_required(context: Optional[dict]) -> bool:
    if isinstance(context, dict) and "web_fetch_require_protocol" in context:
        return _read_bool(context.get("web_fetch_require_protocol"), default=True)
    return _read_bool(os.getenv(_WEB_FETCH_REQUIRE_PROTOCOL_ENV, "1"), default=True)


def _extract_first_json_object(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    start = raw.find("{")
    if start < 0:
        return ""
    depth = 0
    in_string = False
    escaped = False
    for idx in range(start, len(raw)):
        ch = raw[idx]
        if in_string:
            if escaped:
                escaped = False
                continue
            if ch == "\\":
                escaped = True
                continue
            if ch == "\"":
                in_string = False
            continue
        if ch == "\"":
            in_string = True
            continue
        if ch == "{":
            depth += 1
            continue
        if ch == "}":
            depth -= 1
            if depth == 0:
                return raw[start : idx + 1]
    return ""


def _coerce_nonempty_str_list(value: object) -> List[str]:
    if isinstance(value, str):
        text = str(value).strip()
        return [text] if text else []
    if not isinstance(value, list):
        return []
    out: List[str] = []
    seen: Set[str] = set()
    for item in value:
        current = str(item or "").strip()
        if not current or current in seen:
            continue
        seen.add(current)
        out.append(current)
    return out


def _normalize_domain_rule(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if text.startswith("http://") or text.startswith("https://"):
        try:
            parsed = urlparse(text)
            text = str(parsed.netloc or "").strip().lower()
        except Exception:
            return ""
    text = text.strip(".")
    if ":" in text:
        text = text.split(":", 1)[0].strip()
    if "/" in text:
        text = text.split("/", 1)[0].strip()
    return text


def _host_matches_domain_rule(host: str, rule: str) -> bool:
    host_text = str(host or "").strip().lower()
    rule_text = _normalize_domain_rule(rule)
    if not host_text or not rule_text:
        return False
    return host_text == rule_text or host_text.endswith("." + rule_text)


def _looks_like_non_url_query(query: str) -> bool:
    text = str(query or "").strip()
    if not text:
        return False
    normalized = _normalize_web_fetch_url_candidate(text)
    return not bool(normalized)


def _dedupe_web_fetch_strings(values: List[str]) -> List[str]:
    items: List[str] = []
    seen: Set[str] = set()
    for value in values or []:
        current = str(value or "").strip()
        if not current:
            continue
        lowered = current.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        items.append(current)
    return items


def _coerce_web_fetch_required_fields(payload: dict) -> List[str]:
    required_fields = _coerce_nonempty_str_list(payload.get("required_fields"))
    if not required_fields:
        required_fields = _coerce_nonempty_str_list(payload.get("required_columns"))
    return _dedupe_web_fetch_strings(required_fields)



def _infer_default_web_fetch_required_fields(
    *,
    fallback_query: str,
    target_signals: List[str],
    time_hints: List[str],
    unit_hints: List[str],
) -> List[str]:
    merged = " ".join(
        [str(fallback_query or "")] + list(target_signals or []) + list(time_hints or []) + list(unit_hints or [])
    ).lower()
    inferred: List[str] = []

    temporal_terms = (
        "最近",
        "历史",
        "日度",
        "日线",
        "周度",
        "周线",
        "月度",
        "月线",
        "季度",
        "趋势",
        "日期",
        "时间",
        "date",
        "day",
        "daily",
        "weekly",
        "monthly",
        "history",
        "historical",
        "timeseries",
        "trend",
    )
    if any(term in merged for term in temporal_terms):
        inferred.append("date")

    gold_terms = ("黄金", "gold", "xau", "au99", "au9999")
    gold_unit_terms = ("元/克", "人民币/克", "cny/g", "cny per gram", "gram")
    price_terms = ("价格", "price", "quote", "金价", "rate", "收盘", "close", "现价", "开盘", "high", "low")
    value_terms = (
        "value",
        "数值",
        "指标",
        "amount",
        "count",
        "volume",
        "销量",
        "下载量",
        "人数",
        "temperature",
        "温度",
        "index",
        "指数",
        "yield",
        "收益率",
        "利率",
        "pct",
        "percent",
        "百分比",
    )
    if any(term in merged for term in gold_terms) and any(term in merged for term in gold_unit_terms):
        inferred.append("price_cny_per_gram")
    elif any(term in merged for term in price_terms):
        inferred.append("price")
    elif any(term in merged for term in value_terms):
        inferred.append("value")

    if not inferred:
        if any(term in merged for term in temporal_terms):
            inferred = ["date", "value"]
        else:
            inferred = ["value"]
    elif inferred == ["date"]:
        inferred.append("value")

    return _dedupe_web_fetch_strings(inferred)



def _expand_web_fetch_signal_candidates(term: str) -> List[str]:
    text = str(term or "").strip()
    if not text:
        return []
    pieces = re.split(r"[的、，,;；|]+", text)
    expanded: List[str] = []
    for piece in pieces:
        current = str(piece or "").strip()
        if not current:
            continue
        current = re.sub(r"^(?:单位|币种|指标|对象)", "", current).strip()
        current = re.sub(r"(?:数据|信息|列表)$", "", current).strip()
        if current:
            expanded.append(current)
    return _dedupe_web_fetch_strings(expanded or [text])



def _infer_default_web_fetch_target_signals(*, fallback_query: str, required_fields: List[str]) -> List[str]:
    ignored_terms = {
        "csv",
        "json",
        "api",
        "table",
        "dataset",
        "历史",
        "history",
        "historical",
        "daily",
        "weekly",
        "monthly",
        "最近",
        "数据",
        "表格",
        "download",
        "下载",
        "结构化",
        "structured",
        "并",
        "为",
    }
    ignored_terms.update(str(item or "").strip().lower() for item in (required_fields or []))

    signal_source = _compact_web_fetch_query(fallback_query) or _normalize_web_fetch_query(fallback_query)
    signals: List[str] = []
    for term in _extract_web_fetch_query_terms(signal_source):
        for candidate in _expand_web_fetch_signal_candidates(str(term or "")):
            lowered = str(candidate or "").strip().lower()
            if not lowered or lowered in ignored_terms:
                continue
            if len(str(candidate or "").strip()) <= 1:
                continue
            signals.append(str(candidate or "").strip())

    if not signals:
        for field in required_fields or []:
            current = str(field or "").strip()
            lowered = current.lower()
            if not current or lowered in {"date", "value"}:
                continue
            signals.append(current)

    return _dedupe_web_fetch_strings(signals[:6])



def _infer_default_web_fetch_unit_hints(*, fallback_query: str) -> List[str]:
    unit_markers = (
        "元/克",
        "人民币/克",
        "cny/g",
        "usd/oz",
        "usd",
        "cny",
        "rmb",
        "eur",
        "gbp",
        "jpy",
        "kg",
        "g",
        "gram",
        "grams",
        "克",
        "公斤",
        "吨",
        "oz",
        "ounce",
        "%",
        "pct",
        "percent",
    )
    hints: List[str] = []
    for term in _extract_web_fetch_query_terms(fallback_query):
        current = str(term or "").strip()
        if not current:
            continue
        current = re.sub(r"^(?:单位|币种)", "", current).strip()
        lowered = current.lower()
        if not current:
            continue
        if "/" in current or "%" in current or any(marker in lowered or marker in current for marker in unit_markers):
            hints.append(current)
    return _dedupe_web_fetch_strings(hints[:4])



def _infer_default_web_fetch_time_hints(*, fallback_query: str) -> List[str]:
    raw = _normalize_web_fetch_query(fallback_query)
    if not raw:
        return []

    hints: List[str] = []
    for pattern in (
        r"最近(?:三个月|一个月|半年|一年|一周|\d+天|\d+周|\d+个月|\d+年)",
        r"过去(?:\d+天|\d+周|\d+个月|\d+年)",
        r"近(?:\d+天|\d+周|\d+个月|\d+年)",
    ):
        hints.extend(re.findall(pattern, raw, flags=re.IGNORECASE))

    lowered = raw.lower()
    for token in ("历史", "日度", "日线", "周度", "周线", "月度", "月线", "季度", "daily", "weekly", "monthly", "historical", "history"):
        if token.lower() in lowered:
            hints.append(token)
    return _dedupe_web_fetch_strings(hints[:6])


def _extract_web_fetch_query_terms(value: object) -> List[str]:
    text = str(value or "").strip()
    if not text:
        return []
    raw_terms = re.findall(r"[A-Za-z0-9_./+-]{2,}|[一-鿿/]{1,16}", text)
    terms: List[str] = []
    for raw in raw_terms:
        term = str(raw or "").strip()
        if not term:
            continue
        lowered = term.lower()
        if lowered in _WEB_FETCH_QUERY_NOISE_TERMS:
            continue
        if term in {"数据", "文件", "任务", "处理", "下载"}:
            continue
        if lowered in {"http", "https", "www"}:
            continue
        if _normalize_web_fetch_url_candidate(term):
            continue
        if term.startswith("//"):
            continue
        if re.fullmatch(r"[a-z0-9.-]+\.[a-z]{2,}([/?#].*)?", lowered):
            continue
        if term in {"date", "price_cny_per_gram"}:
            continue
        terms.append(term)
    return _dedupe_web_fetch_strings(terms)



def _compact_web_fetch_query(value: object) -> str:
    base = _normalize_web_fetch_query(value)
    if not base:
        return ""
    compacted = str(base)
    for noise in _WEB_FETCH_QUERY_NOISE_TERMS:
        compacted = compacted.replace(noise, " ")
    terms = _extract_web_fetch_query_terms(compacted)
    if not terms:
        terms = _extract_web_fetch_query_terms(base)
    return _normalize_web_fetch_query(" ".join(terms[:10]))



def _score_web_fetch_search_query_quality(
    query: str,
    *,
    target_signals: List[str],
    unit_hints: List[str],
    time_hints: List[str],
) -> int:
    current = _normalize_web_fetch_query(query)
    if not current:
        return -999
    terms = _extract_web_fetch_query_terms(current)
    if not terms:
        return -999
    lowered = current.lower()
    score = 0
    term_count = len(terms)
    score += max(0, 14 - abs(term_count - 6))
    score -= max(0, term_count - 12) * 2
    score -= max(0, len(current) - 90) // 8
    score += _count_unique_substring_hits(lowered, [str(item or "").lower() for item in (target_signals or [])]) * 3
    score += _count_unique_substring_hits(lowered, [str(item or "").lower() for item in (unit_hints or [])]) * 4
    score += _count_unique_substring_hits(lowered, [str(item or "").lower() for item in (time_hints or [])]) * 2
    if any(term in lowered for term in ("历史", "日度", "日线", "table", "csv", "json", "api", "history", "daily")):
        score += 4
    if any(term in lowered for term in ("最近三个月", "最近一个月", "最近半年", "三个月", "一个月")):
        score -= 4
    has_cjk_query = bool(re.search(r"[一-鿿]", current))
    has_cjk_target = any(re.search(r"[一-鿿]", str(item or "")) for item in (target_signals or []))
    if has_cjk_query and has_cjk_target:
        score += 6
    if (not has_cjk_query) and has_cjk_target:
        score -= 2
    return int(score)



def _join_web_fetch_query_parts(parts: List[str]) -> str:
    return _normalize_web_fetch_query(" ".join(str(item or "").strip() for item in (parts or []) if str(item or "").strip()))



def _merge_web_fetch_search_queries(*groups: List[str], limit: int = 8) -> List[str]:
    merged: List[str] = []
    for group in groups:
        for item in group or []:
            current = _sanitize_web_fetch_search_query(_normalize_web_fetch_query(item))
            if not current or not _looks_like_non_url_query(current):
                continue
            merged.append(current)
    return _dedupe_web_fetch_strings(merged)[: max(1, int(limit or 1))]



def _build_web_fetch_retry_query_variants(
    *,
    query: object,
    fallback_query: str,
    target_signals: List[str],
    unit_hints: List[str],
    time_hints: List[str],
) -> List[str]:
    normalized = _sanitize_web_fetch_search_query(_compact_web_fetch_query(query))
    if not normalized:
        return []

    target_values = _dedupe_web_fetch_strings(
        [str(item or "") for item in (target_signals or [])]
        or _infer_default_web_fetch_target_signals(
            fallback_query=fallback_query,
            required_fields=[],
        )
    )
    unit_values = _dedupe_web_fetch_strings(
        [str(item or "") for item in (unit_hints or [])]
        or _infer_default_web_fetch_unit_hints(fallback_query=fallback_query)
    )
    time_values = _dedupe_web_fetch_strings(
        [str(item or "") for item in (time_hints or [])]
        or _infer_default_web_fetch_time_hints(fallback_query=fallback_query)
    )

    lowered = normalized.lower()
    has_target = bool(_count_unique_substring_hits(lowered, [str(item or "").lower() for item in target_values]))
    has_unit = bool(_count_unique_substring_hits(lowered, [str(item or "").lower() for item in unit_values]))
    has_time = bool(_count_unique_substring_hits(lowered, [str(item or "").lower() for item in time_values]))

    variants: List[str] = [normalized]
    anchor_parts: List[str] = []
    if not has_target:
        anchor_parts.extend(target_values[:3])
    if not has_unit:
        anchor_parts.extend(unit_values[:2])
    if not has_time:
        anchor_parts.extend(time_values[:2])
    if anchor_parts:
        anchored = _join_web_fetch_query_parts([normalized] + _dedupe_web_fetch_strings(anchor_parts))
        if anchored and anchored not in variants:
            variants.append(anchored)
    return variants



def _normalize_web_fetch_retry_queries(
    values: object,
    *,
    fallback_query: str,
    target_signals: List[str],
    unit_hints: List[str],
    time_hints: List[str],
) -> List[str]:
    candidates: List[str] = []
    for item in _coerce_nonempty_str_list(values):
        candidates.extend(
            _build_web_fetch_retry_query_variants(
                query=item,
                fallback_query=fallback_query,
                target_signals=target_signals,
                unit_hints=unit_hints,
                time_hints=time_hints,
            )
        )
    return _merge_web_fetch_search_queries(candidates, limit=6)



def _build_default_web_fetch_search_queries(
    *,
    fallback_query: str,
    required_fields: List[str],
    target_signals: List[str],
    unit_hints: List[str],
    time_hints: List[str],
) -> List[str]:
    fallback_terms = _extract_web_fetch_query_terms(fallback_query)
    zh_core = [term for term in list(target_signals or []) if re.search(r"[一-鿿]", str(term or ""))]
    if not zh_core:
        zh_core = [term for term in fallback_terms if re.search(r"[一-鿿]", str(term or ""))]
    en_core = [term for term in list(target_signals or []) if re.fullmatch(r"[A-Za-z0-9_./+-]{2,}", str(term or ""))]
    if not en_core:
        en_core = [term for term in fallback_terms if re.fullmatch(r"[A-Za-z0-9_./+-]{2,}", str(term or ""))]

    zh_base = " ".join(_dedupe_web_fetch_strings([str(item or "") for item in zh_core])[:4])
    en_base = " ".join(_dedupe_web_fetch_strings([str(item or "") for item in en_core])[:4])
    unit_text = " ".join(_dedupe_web_fetch_strings([str(item or "") for item in unit_hints])[:2])
    time_text = " ".join(_dedupe_web_fetch_strings([str(item or "") for item in time_hints])[:2])
    direct_query = _sanitize_web_fetch_search_query(_compact_web_fetch_query(fallback_query))

    candidates: List[str] = []
    if direct_query:
        candidates.append(direct_query)
    if zh_base:
        candidates.extend(
            [
                _join_web_fetch_query_parts([zh_base, unit_text, "历史", "价格", "日度"]),
                _join_web_fetch_query_parts([zh_base, unit_text, time_text or "历史", "表格"]),
                _join_web_fetch_query_parts([zh_base, unit_text, "CSV", "下载"]),
                _join_web_fetch_query_parts([zh_base, unit_text, "JSON", "API"]),
            ]
        )
    if en_base:
        candidates.extend(
            [
                _join_web_fetch_query_parts([en_base, "historical", "price", "table"]),
                _join_web_fetch_query_parts([en_base, "csv", "dataset"]),
                _join_web_fetch_query_parts([en_base, "json", "api", "timeseries"]),
            ]
        )
    if not candidates:
        candidates = ["结构化 数据 历史 表格", "structured data table json api"]

    normalized = [
        _sanitize_web_fetch_search_query(_compact_web_fetch_query(item))
        for item in candidates
    ]
    return _merge_web_fetch_search_queries(normalized, limit=6)


def _build_web_fetch_protocol_cache_key(*, task_message: str, step_title: str, tool_input: str) -> str:
    raw = "\n".join([
        _normalize_web_fetch_query(task_message),
        _normalize_web_fetch_query(strip_illustrative_example_clauses(step_title)),
        _normalize_web_fetch_query(strip_illustrative_example_clauses(tool_input)),
    ])
    if not raw.strip():
        return ""
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _build_web_fetch_protocol_prompt(*, task_message: str, step_title: str, tool_input: str) -> str:
    clean_step_title = strip_illustrative_example_clauses(step_title)
    clean_tool_input = strip_illustrative_example_clauses(tool_input)
    lines = [
        "你是数据抓取协议规划器。请先制定“先协议后搜索”的协议，输出严格 JSON。",
        "禁止输出解释、禁止 markdown 代码块。",
        "输出格式：",
        "{",
        '  "objective":"...",',
        '  "search_queries":["..."],',
        '  "required_fields":["date","value"],',
        '  "target_signals":["黄金","价格"],',
        '  "negative_terms":["论坛","帮助"],',
        '  "source_types":["api","csv","html_table"],',
        '  "time_hints":["最近三个月","日度"],',
        '  "unit_hints":["元/克"],',
        '  "language_hints":["zh","en"],',
        '  "deny_domains":["..."],',
        '  "require_structured": true',
        "}",
        "约束：",
        "1) objective 必须直接对应当前任务，不要改写成其他领域，也不要默认成黄金/汇率等示例任务。",
        "2) search_queries 必须给出 3-6 组可执行查询，覆盖结构化数据/API、普通网页表格、必要时英文检索。",
        "2.1) query 必须是搜索引擎关键词风格，不要写自然语言指令，不要写完整句子。",
        "2.2) 优先任务对象 + 指标 + 单位 + 历史/日度/表格/API 等高信息量词，避免把所有策略堆到一条里。",
        "2.3) 如果任务明确给了单位/币种/时间粒度（如 元/克、CNY/g、日度、最近三个月），每条 query 都应尽量保留这些约束，不要擅自切换到其他单位或口径。",
        "3) required_fields 必须写“用于验证候选页是否相关”的关键字段；若是时序任务，通常至少包含 date + 核心数值字段。",
        "4) target_signals 只写真正要在候选页中验证的信息信号，不要写站点名，也不要直接写猜测出来的 host/API 路径。",
        "4.1) 不要把“任务对象/核心指标/原始单位”这类占位词原样输出到 JSON，必须替换成当前任务里的真实对象、指标和单位。",
        "5) negative_terms 只写通用噪声词，如 forum/help/docs/login/schema，不要硬编码具体域名。",
        "6) deny_domains 仅填明显无关/噪声域名（搜索引擎缩略图、视频站等）。",
        "7) 协议只规划‘怎么搜索/怎么验证’，不要在这里直接编造具体 API URL；URL 应在搜索或已有观测阶段再发现。",
        f"任务：{task_message or '(空)'}",
        f"当前步骤：{clean_step_title or step_title or '(空)'}",
        f"当前输入：{clean_tool_input or tool_input or '(空)'}",
    ]
    return "\n".join(lines)


def _contains_web_fetch_placeholder_hint(value: str, placeholders: Tuple[str, ...]) -> bool:
    lowered = str(value or "").strip().lower()
    if not lowered:
        return False
    return any(str(item or "").strip().lower() in lowered for item in (placeholders or ()))



def _sanitize_web_fetch_hint_values(
    values: List[str],
    *,
    fallback_values: List[str],
    placeholders: Tuple[str, ...],
) -> List[str]:
    cleaned = [
        str(item or "").strip()
        for item in (values or [])
        if str(item or "").strip()
        and not _contains_web_fetch_placeholder_hint(str(item or ""), placeholders)
    ]
    deduped = _dedupe_web_fetch_strings(cleaned)
    if deduped:
        return deduped
    return _dedupe_web_fetch_strings([str(item or "").strip() for item in (fallback_values or []) if str(item or "").strip()])



def _normalize_web_fetch_protocol(payload: object, *, fallback_query: str, source: str, fallback_reason: str = "") -> dict:
    obj = payload if isinstance(payload, dict) else {}
    compact_goal = _compact_web_fetch_query(fallback_query)
    objective = strip_illustrative_example_clauses(str(obj.get("objective") or "").strip())
    if not objective:
        if compact_goal:
            objective = f"围绕“{truncate_inline_text(compact_goal, 60)}”获取可验证的结构化数据。"
        else:
            objective = "获取与当前任务直接相关、可验证的结构化数据。"

    inferred_time_hints = _infer_default_web_fetch_time_hints(fallback_query=fallback_query)
    inferred_unit_hints = _infer_default_web_fetch_unit_hints(fallback_query=fallback_query)
    time_hints = _sanitize_web_fetch_hint_values(
        _coerce_nonempty_str_list(obj.get("time_hints")),
        fallback_values=inferred_time_hints,
        placeholders=_WEB_FETCH_PROTOCOL_TIME_PLACEHOLDERS,
    )
    unit_hints = _sanitize_web_fetch_hint_values(
        _coerce_nonempty_str_list(obj.get("unit_hints")),
        fallback_values=inferred_unit_hints,
        placeholders=_WEB_FETCH_PROTOCOL_UNIT_PLACEHOLDERS,
    )
    required_fields = _coerce_web_fetch_required_fields(obj)
    inferred_target_signals = _infer_default_web_fetch_target_signals(
        fallback_query=fallback_query,
        required_fields=required_fields,
    )
    target_signals = _sanitize_web_fetch_hint_values(
        _coerce_nonempty_str_list(obj.get("target_signals")),
        fallback_values=inferred_target_signals,
        placeholders=_WEB_FETCH_PROTOCOL_SIGNAL_PLACEHOLDERS,
    )
    if not required_fields:
        required_fields = _infer_default_web_fetch_required_fields(
            fallback_query=fallback_query,
            target_signals=target_signals,
            time_hints=time_hints,
            unit_hints=unit_hints,
        )
    if not target_signals:
        target_signals = _infer_default_web_fetch_target_signals(
            fallback_query=fallback_query,
            required_fields=required_fields,
        )

    negative_terms = _dedupe_web_fetch_strings(
        _coerce_nonempty_str_list(obj.get("negative_terms"))
        or list(_WEB_FETCH_NEGATIVE_TERMS_DEFAULT)
    )
    source_types = _dedupe_web_fetch_strings(
        _coerce_nonempty_str_list(obj.get("source_types"))
        or ["api", "csv", "json", "html_table"]
    )
    language_hints = _dedupe_web_fetch_strings(
        _coerce_nonempty_str_list(obj.get("language_hints"))
        or ["zh", "en"]
    )

    normalized_queries: List[str] = []
    for item in _coerce_nonempty_str_list(obj.get("search_queries")):
        current = _sanitize_web_fetch_search_query(_normalize_web_fetch_query(item))
        if current:
            normalized_queries.append(current)
    search_queries = _merge_web_fetch_search_queries(
        normalized_queries,
        _build_default_web_fetch_search_queries(
            fallback_query=fallback_query,
            required_fields=required_fields,
            target_signals=target_signals,
            unit_hints=unit_hints,
            time_hints=time_hints,
        ),
        limit=8,
    )
    if not search_queries:
        fallback_clean = _sanitize_web_fetch_search_query(_compact_web_fetch_query(fallback_query))
        if fallback_clean:
            search_queries = [fallback_clean]
    if not search_queries:
        search_queries = ["结构化 数据 历史 表格", "structured data table json api"]

    deny_domains = list(_DEFAULT_WEB_FETCH_DENY_DOMAINS)
    for item in _coerce_nonempty_str_list(obj.get("deny_domains")):
        current = _normalize_domain_rule(item)
        if current and current not in deny_domains:
            deny_domains.append(current)

    protocol = {
        "version": _WEB_FETCH_PROTOCOL_VERSION,
        "source": str(source or "llm").strip() or "llm",
        "objective": objective,
        "search_queries": search_queries,
        "required_columns": list(required_fields),
        "required_fields": required_fields,
        "target_signals": target_signals,
        "negative_terms": negative_terms,
        "source_types": source_types,
        "time_hints": time_hints,
        "unit_hints": unit_hints,
        "language_hints": language_hints,
        "deny_domains": deny_domains,
        "require_structured": _read_bool(obj.get("require_structured"), default=True),
    }
    if fallback_reason:
        protocol["fallback_reason"] = str(fallback_reason or "").strip()
    return protocol


def _build_fallback_web_fetch_protocol(*, fallback_query: str, reason: str) -> dict:
    return _normalize_web_fetch_protocol(
        {},
        fallback_query=fallback_query,
        source="fallback",
        fallback_reason=reason,
    )



def _get_web_fetch_llm_context(context: Optional[dict]) -> Tuple[int, int, str]:
    if not isinstance(context, dict):
        return 0, 0, ""
    try:
        task_id = int(parse_positive_int(context.get("task_id")) or 0)
    except Exception:
        task_id = 0
    try:
        run_id = int(parse_positive_int(context.get("run_id")) or 0)
    except Exception:
        run_id = 0
    return task_id, run_id, str(context.get("model") or "").strip()



def _ensure_web_fetch_protocol(
    *,
    task_id: int,
    run_id: int,
    step_row,
    tool_input: str,
    context: Optional[dict],
) -> Tuple[dict, List[str]]:
    if not _is_web_fetch_protocol_required(context):
        return {}, []

    task_message = str((context or {}).get("message") or "").strip() if isinstance(context, dict) else ""
    step_title = strip_illustrative_example_clauses(str(step_row.get("title") or "").strip()) if isinstance(step_row, dict) else ""
    clean_tool_input = strip_illustrative_example_clauses(str(tool_input or "").strip())
    fallback_query = _normalize_web_fetch_query(clean_tool_input or task_message)
    cache_key = _build_web_fetch_protocol_cache_key(
        task_message=task_message,
        step_title=step_title,
        tool_input=str(clean_tool_input or ""),
    )

    cached = context.get(_WEB_FETCH_PROTOCOL_CONTEXT_KEY) if isinstance(context, dict) else None
    if isinstance(cached, dict) and int(cached.get("version") or 0) == int(_WEB_FETCH_PROTOCOL_VERSION):
        if str(cached.get("intent_key") or "") == cache_key and _coerce_nonempty_str_list(cached.get("search_queries")):
            return cached, []

    prompt = _build_web_fetch_protocol_prompt(
        task_message=task_message,
        step_title=step_title,
        tool_input=str(clean_tool_input or ""),
    )

    warnings: List[str] = []
    _task_id, _run_id, selected_model = _get_web_fetch_llm_context(context)
    try:
        payload = {
            "task_id": int(task_id),
            "run_id": int(run_id),
            "prompt": prompt,
            "parameters": {
                "temperature": 0,
                "timeout_seconds": 20,
                "retry_max_attempts": 1,
            },
        }
        if selected_model:
            payload["model"] = selected_model
        llm_result = create_llm_call(payload)
        record = llm_result.get("record") if isinstance(llm_result, dict) else None
        response_text = str((record or {}).get("response") or "")
        parsed = parse_json_dict(response_text)
        if not isinstance(parsed, dict):
            embedded = _extract_first_json_object(response_text)
            parsed = parse_json_dict(embedded) if embedded else None
        if isinstance(parsed, dict):
            protocol = _normalize_web_fetch_protocol(parsed, fallback_query=fallback_query, source="llm")
            protocol["intent_key"] = cache_key
            if isinstance(context, dict):
                context[_WEB_FETCH_PROTOCOL_CONTEXT_KEY] = protocol
            return protocol, []
        warnings.append("web_fetch 协议解析失败，已切换为兜底协议")
    except Exception as exc:
        warnings.append(f"web_fetch 协议生成失败，已切换为兜底协议: {truncate_inline_text(exc, 160)}")

    fallback = _build_fallback_web_fetch_protocol(
        fallback_query=fallback_query,
        reason=warnings[-1] if warnings else "llm_protocol_unavailable",
    )
    fallback["intent_key"] = cache_key
    if isinstance(context, dict):
        context[_WEB_FETCH_PROTOCOL_CONTEXT_KEY] = fallback
    return fallback, warnings


def _resolve_protocol_search_queries(tool_input: str, protocol: Optional[dict]) -> List[str]:
    clean_tool_input = _sanitize_web_fetch_search_query(
        _normalize_web_fetch_query(strip_illustrative_example_clauses(tool_input))
    )
    if not isinstance(protocol, dict):
        return [clean_tool_input] if clean_tool_input else []

    target_signals = _get_web_fetch_target_signals(protocol)
    unit_hints = _get_web_fetch_unit_hints(protocol)
    time_hints = _get_web_fetch_time_hints(protocol)

    candidates: List[str] = []
    for item in _coerce_nonempty_str_list(protocol.get("search_queries")):
        candidates.extend(
            _build_web_fetch_retry_query_variants(
                query=item,
                fallback_query=clean_tool_input,
                target_signals=target_signals,
                unit_hints=unit_hints,
                time_hints=time_hints,
            )
        )
    if clean_tool_input:
        candidates.extend(
            _build_web_fetch_retry_query_variants(
                query=clean_tool_input,
                fallback_query=clean_tool_input,
                target_signals=target_signals,
                unit_hints=unit_hints,
                time_hints=time_hints,
            )
        )

    deduped = _dedupe_web_fetch_strings(candidates)
    ranked = sorted(
        enumerate(deduped),
        key=lambda item: (
            -_score_web_fetch_search_query_quality(
                item[1],
                target_signals=target_signals,
                unit_hints=unit_hints,
                time_hints=time_hints,
            ),
            item[0],
        ),
    )
    ordered = [query for _idx, query in ranked]
    if clean_tool_input and ordered and not (target_signals or unit_hints or time_hints):
        ordered = [clean_tool_input] + [query for query in ordered if query != clean_tool_input]
    return ordered[:6]


def _get_protocol_deny_domains(protocol: Optional[dict]) -> Set[str]:
    if not isinstance(protocol, dict):
        return set()
    domains = _coerce_nonempty_str_list(protocol.get("deny_domains"))
    normalized: Set[str] = set()
    for item in domains:
        current = _normalize_domain_rule(item)
        if current:
            normalized.add(current)
    return normalized


def _get_web_fetch_required_fields(protocol: Optional[dict]) -> List[str]:
    if not isinstance(protocol, dict):
        return ["date", "value"]
    required_fields = _coerce_web_fetch_required_fields(protocol)
    return required_fields or ["date", "value"]


def _is_host_denied_by_protocol(host: str, deny_domains: Set[str]) -> bool:
    for rule in deny_domains or set():
        if _host_matches_domain_rule(host, rule):
            return True
    return False


def _get_web_fetch_target_signals(protocol: Optional[dict]) -> List[str]:
    if not isinstance(protocol, dict):
        return []
    return _dedupe_web_fetch_strings(_coerce_nonempty_str_list(protocol.get("target_signals")))


def _get_web_fetch_negative_terms(protocol: Optional[dict]) -> List[str]:
    if not isinstance(protocol, dict):
        return list(_WEB_FETCH_NEGATIVE_TERMS_DEFAULT)
    return _dedupe_web_fetch_strings(
        _coerce_nonempty_str_list(protocol.get("negative_terms"))
        or list(_WEB_FETCH_NEGATIVE_TERMS_DEFAULT)
    )


def _get_web_fetch_unit_hints(protocol: Optional[dict]) -> List[str]:
    if not isinstance(protocol, dict):
        return []
    return _dedupe_web_fetch_strings(_coerce_nonempty_str_list(protocol.get("unit_hints")))


def _get_web_fetch_time_hints(protocol: Optional[dict]) -> List[str]:
    if not isinstance(protocol, dict):
        return []
    return _dedupe_web_fetch_strings(_coerce_nonempty_str_list(protocol.get("time_hints")))


def _emit_web_fetch_event(context: Optional[dict], event_type: str, **data) -> None:
    if not isinstance(context, dict):
        return
    sink = context.get("event_sink")
    if not callable(sink):
        return
    payload = {
        "type": str(event_type or "").strip() or "search_progress",
        "tool": TOOL_NAME_WEB_FETCH,
    }
    for key in ("task_id", "run_id", "step_id", "step_order", "step_title"):
        value = context.get(key)
        if value not in (None, ""):
            payload[key] = value
    for key, value in data.items():
        if value in (None, "", [], {}):
            continue
        payload[key] = value
    try:
        sink(payload)
    except Exception:
        return


def _decode_web_fetch_url_value(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for _ in range(3):
        decoded = unquote(text)
        if decoded == text:
            break
        text = decoded.strip()
    return text


def _looks_like_http_url(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    try:
        parsed = urlparse(text)
    except Exception:
        return False
    return str(parsed.scheme or "").lower() in {"http", "https"} and bool(parsed.netloc)


def _try_decode_web_fetch_base64_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    if lowered.startswith("a1"):
        raw = raw[2:]
    if not re.fullmatch(r"[A-Za-z0-9_-]{12,}={0,2}", raw):
        return ""
    padded = raw + ("=" * (-len(raw) % 4))
    try:
        decoded_bytes = base64.urlsafe_b64decode(padded.encode("utf-8"))
        decoded_text = decoded_bytes.decode("utf-8", errors="ignore")
    except Exception:
        return ""
    return _decode_web_fetch_url_value(decoded_text)


def _extract_web_fetch_redirect_target(parsed_url) -> str:
    if parsed_url is None:
        return ""
    try:
        query_map = parse_qs(str(parsed_url.query or ""), keep_blank_values=False)
    except Exception:
        query_map = {}

    for key in _WEB_FETCH_REDIRECT_QUERY_KEYS:
        values = query_map.get(key) or []
        for raw_value in values:
            decoded = _decode_web_fetch_url_value(raw_value)
            if _looks_like_http_url(decoded):
                return decoded
            decoded_base64 = _try_decode_web_fetch_base64_url(decoded)
            if _looks_like_http_url(decoded_base64):
                return decoded_base64
    return ""


def _extract_web_fetch_host_family(host: str) -> str:
    text = str(host or "").strip().lower()
    if not text:
        return ""
    parts = [part for part in text.split(".") if part]
    if len(parts) <= 2:
        return text
    tail2 = ".".join(parts[-2:])
    if tail2 in _WEB_FETCH_SECOND_LEVEL_SUFFIXES and len(parts) >= 3:
        return ".".join(parts[-3:])
    return tail2


def _is_known_search_engine_family(family: str) -> bool:
    current = str(family or "").strip().lower()
    if not current:
        return False
    return current in _WEB_FETCH_KNOWN_SEARCH_HOST_FAMILIES



def _is_web_fetch_search_like_candidate_url(url: str) -> bool:
    normalized = str(url or "").strip()
    if not normalized:
        return False
    try:
        parsed = urlparse(normalized)
    except Exception:
        return False
    host = str(parsed.netloc or "").strip().lower()
    family = _extract_web_fetch_host_family(host)
    if family and _is_known_search_engine_family(family):
        return True

    query_map = parse_qs(parsed.query or "", keep_blank_values=False)
    if not query_map:
        return False
    has_search_query = any(str(key or "").strip().lower() in _WEB_FETCH_SEARCH_QUERY_KEYS for key in query_map.keys())
    if not has_search_query:
        return False

    path = str(parsed.path or "").strip().lower()
    if path in _WEB_FETCH_SEARCH_LIKE_PATHS:
        return True
    if any(marker in path for marker in ("/search", "/results", "/query", "/serp")):
        return True
    if any(str(key or "").strip().lower() in {"src", "from", "s_type", "searchtype"} for key in query_map.keys()):
        return True
    return False


def _build_exchangerate_fallback_urls(input_url: str) -> List[str]:
    """
    exchangerate.host 常见问题是返回 missing_access_key，这里提供免 key 备用源。
    """
    normalized = _normalize_web_fetch_url_candidate(input_url)
    if not normalized:
        return []
    parsed = urlparse(normalized)
    host = str(parsed.netloc or "").strip().lower()
    if "exchangerate.host" not in host:
        return []

    query = parse_qs(parsed.query or "", keep_blank_values=False)
    base = str((query.get("base") or [""])[0] or "").strip().upper()
    symbols_raw = str((query.get("symbols") or [""])[0] or "").strip().upper()
    start_date = str((query.get("start_date") or [""])[0] or "").strip()
    end_date = str((query.get("end_date") or [""])[0] or "").strip()
    symbols = ",".join([token for token in (part.strip() for part in symbols_raw.split(",")) if token])

    candidates: List[str] = []
    if base and symbols and start_date and end_date:
        candidates.append(
            f"https://api.frankfurter.app/{start_date}..{end_date}?from={base}&to={symbols}"
        )
    elif base and symbols:
        candidates.append(
            f"https://api.frankfurter.app/latest?from={base}&to={symbols}"
        )
    elif base:
        candidates.append(f"https://api.frankfurter.app/latest?from={base}")

    if base:
        candidates.append(f"https://open.er-api.com/v6/latest/{base}")
    return candidates


def _render_web_fetch_fallback_template(template: str, source_url: str) -> str:
    normalized = _normalize_web_fetch_url_candidate(source_url)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    rendered = str(template or "")
    substitutions = {
        "{url}": normalized,
        "{scheme}": str(parsed.scheme or ""),
        "{host}": str(parsed.netloc or ""),
        "{path}": str(parsed.path or ""),
        "{query}": str(parsed.query or ""),
    }
    for token, value in substitutions.items():
        rendered = rendered.replace(token, value)
    return _normalize_web_fetch_url_candidate(rendered)


def _build_web_fetch_fallback_urls(source_url: str) -> List[str]:
    normalized = _normalize_web_fetch_url_candidate(source_url)
    if not normalized:
        return []

    merged_templates: List[str] = []
    for item in WEB_FETCH_FALLBACK_URL_TEMPLATES_DEFAULT or ():
        current = str(item or "").strip()
        if current:
            merged_templates.append(current)
    for item in _iter_env_web_fetch_fallback_templates(
        os.getenv(AGENT_WEB_FETCH_FALLBACK_URL_TEMPLATES_ENV, "")
    ):
        merged_templates.append(item)

    rendered_urls: List[str] = []
    for template in merged_templates:
        candidate = _render_web_fetch_fallback_template(template, normalized)
        if candidate:
            rendered_urls.append(candidate)

    fallback_urls = _build_exchangerate_fallback_urls(normalized) + rendered_urls

    deduped: List[str] = []
    seen = {normalized}
    for item in fallback_urls:
        current = _normalize_web_fetch_url_candidate(item)
        if not current or current in seen:
            continue
        seen.add(current)
        deduped.append(current)

    try:
        limit = max(1, int(AGENT_WEB_FETCH_FALLBACK_MAX_CANDIDATES or 4))
    except Exception:
        limit = 4
    return deduped[:limit]


def _iter_env_web_fetch_search_templates(raw_env: str) -> List[str]:
    text = str(raw_env or "").strip()
    if not text:
        return []
    payload = parse_json_value(text)
    if not isinstance(payload, list):
        return []
    parsed: List[str] = []
    for item in payload:
        current = str(item or "").strip()
        if current:
            parsed.append(current)
    return parsed


def _normalize_web_fetch_query(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = re.sub(r"\s+", " ", text)
    return normalized[:240]


def _sanitize_web_fetch_search_query(value: object) -> str:
    base = _normalize_web_fetch_query(value)
    if not base:
        return ""
    tokens = [str(item or "").strip() for item in re.split(r"\s+", base) if str(item or "").strip()]
    if not tokens:
        return ""
    kept: List[str] = []
    url_tokens = 0
    for token in tokens:
        if _normalize_web_fetch_url_candidate(token):
            url_tokens += 1
            continue
        if re.fullmatch(r"[-_/.,:;|]+", token):
            continue
        kept.append(token)
    if kept:
        return _normalize_web_fetch_query(" ".join(kept))
    # 全是 URL 时保留原值，避免过度清洗为空。
    if url_tokens > 0:
        return base
    return base


def _render_web_fetch_search_template(template: str, query: str) -> str:
    current_query = _normalize_web_fetch_query(query)
    if not current_query:
        return ""
    rendered = str(template or "")
    substitutions = {
        "{query}": quote_plus(current_query),
        "{query_raw}": current_query,
    }
    for token, value in substitutions.items():
        rendered = rendered.replace(token, value)
    return _normalize_web_fetch_url_candidate(rendered)


def _build_web_fetch_search_urls(query: str) -> List[str]:
    current_query = _normalize_web_fetch_query(query)
    if not current_query:
        return []
    templates: List[str] = []
    for item in WEB_FETCH_SEARCH_URL_TEMPLATES_DEFAULT or ():
        current = str(item or "").strip()
        if current:
            templates.append(current)
    for item in _iter_env_web_fetch_search_templates(
        os.getenv(AGENT_WEB_FETCH_SEARCH_URL_TEMPLATES_ENV, "")
    ):
        templates.append(item)

    urls: List[str] = []
    seen: Set[str] = set()
    for template in templates:
        url = _render_web_fetch_search_template(template, current_query)
        if not url or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _build_web_fetch_query_keywords(query: str) -> List[str]:
    text = _normalize_web_fetch_query(query).lower()
    if not text:
        return []
    candidates: List[str] = []
    for token in re.split(r"\s+", text):
        current = str(token or "").strip()
        if len(current) >= 2:
            candidates.append(current)
    # 额外补充英文/数字 token，提升对 API/CSV 等关键字的召回
    for token in re.findall(r"[a-z0-9_./-]{2,}", text):
        current = str(token or "").strip()
        if len(current) >= 2:
            candidates.append(current)
    deduped: List[str] = []
    seen: Set[str] = set()
    for item in candidates:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _count_web_fetch_query_keyword_hits(text: str, query_keywords: List[str]) -> int:
    sample = str(text or "").strip().lower()
    if not sample:
        return 0
    hits = 0
    for keyword in query_keywords or []:
        current = str(keyword or "").strip().lower()
        if current and current in sample:
            hits += 1
    return hits


def _score_web_fetch_candidate_link(*, url: str, context_text: str, query_keywords: List[str]) -> int:
    lowered_url = str(url or "").strip().lower()
    lowered_context = str(context_text or "").strip().lower()
    merged = f"{lowered_url} {lowered_context}"
    score = 0
    for keyword in query_keywords:
        if keyword and keyword in merged:
            score += 1
        if keyword and keyword in lowered_url:
            score += 1

    try:
        parsed = urlparse(lowered_url)
    except Exception:
        parsed = None
    path = str(parsed.path or "").strip().lower() if parsed else ""
    host = str(parsed.netloc or "").strip().lower() if parsed else ""

    if path == "/1999/xhtml":
        score -= 8
    if host.endswith("w3.org") and (
        "xhtml" in path or "rdf" in path or "xml" in path or "namespace" in path
    ):
        score -= 6
    if re.fullmatch(r"/\d{4}/[a-z0-9_.-]{2,40}", path):
        score -= 3

    if any(term in lowered_url for term in _WEB_FETCH_POSITIVE_URL_TERMS):
        score += 3
    if any(term in merged for term in _WEB_FETCH_NOISE_URL_TERMS):
        score -= 4
    if host.startswith(_WEB_FETCH_NOISE_HOST_PREFIXES):
        score -= 6
    if any(term in path for term in _WEB_FETCH_NOISE_PATH_TERMS):
        score -= 8
    if path in {"", "/", "/home", "/index", "/all", "/app/main"}:
        score -= 2
    if host and len(host.split(".")) <= 2 and path in {"", "/"}:
        score -= 2
    return int(score)


def _build_web_fetch_candidate_preview(sample: str) -> str:
    text = re.sub(r"\s+", " ", str(sample or "")).strip()
    return truncate_inline_text(text, 200)


def _count_web_fetch_page_noise_hits(sample: str) -> int:
    return _count_unique_substring_hits(str(sample or ""), list(_WEB_FETCH_GENERIC_PAGE_NOISE_TERMS))


def _is_web_fetch_require_structured(protocol: Optional[dict]) -> bool:
    if not isinstance(protocol, dict):
        return True
    return _read_bool(protocol.get("require_structured"), default=True)


def _count_unique_substring_hits(sample: str, keywords: List[str]) -> int:
    hits = 0
    lowered = str(sample or "").lower()
    for keyword in _dedupe_web_fetch_strings([str(item or "").lower() for item in (keywords or [])]):
        if keyword and keyword in lowered:
            hits += 1
    return hits


def _extract_web_fetch_required_field_evidence(
    *,
    sample: str,
    required_fields: List[str],
    unit_hints: List[str],
    target_signals: List[str],
    date_hits: int,
) -> Dict[str, List[str]]:
    lowered = str(sample or "").lower()
    evidence: Dict[str, List[str]] = {}
    unit_terms = _dedupe_web_fetch_strings(
        [str(item or "").lower() for item in (unit_hints or [])] + list(_WEB_FETCH_REQUIRED_FIELD_ALIASES["currency_cny"])
    )
    price_terms = _dedupe_web_fetch_strings(
        list(_WEB_FETCH_REQUIRED_FIELD_ALIASES["price"])
        + [str(item or "").lower() for item in (target_signals or [])]
    )
    date_terms = _dedupe_web_fetch_strings(list(_WEB_FETCH_REQUIRED_FIELD_ALIASES["date"]))

    for raw_field in required_fields or []:
        field = str(raw_field or "").strip().lower()
        if not field:
            continue
        matched: List[str] = []
        if field in lowered:
            matched.append(field)

        if field in {"date", "day", "datetime", "trade_date", "trading_date"}:
            if date_hits > 0:
                matched.append("date_pattern")
            for alias in date_terms:
                if alias and alias in lowered:
                    matched.append(alias)
        elif field in {"price_cny_per_gram", "price_per_gram_cny", "gold_price_cny_per_gram"}:
            found_price_terms = [alias for alias in price_terms if alias and alias in lowered]
            found_unit_terms = [alias for alias in unit_terms if alias and alias in lowered]
            matched.extend(found_price_terms[:2])
            matched.extend(found_unit_terms[:2])
            if found_price_terms and found_unit_terms:
                matched.append("semantic_price_unit_pair")
        else:
            chunks = [part for part in re.split(r"[_\W]+", field) if len(part) >= 2]
            for alias in chunks:
                if alias and alias in lowered:
                    matched.append(alias)

        deduped = _dedupe_web_fetch_strings(matched)
        if deduped:
            evidence[field] = deduped
    return evidence


def _detect_structured_content_signals(sample: str) -> int:
    structured_hits = 0
    text = str(sample or "")
    lowered = text.lower()
    if "<table" in lowered or "<tr" in lowered:
        structured_hits += 1
    lines = [str(line or "").strip() for line in text.splitlines() if str(line or "").strip()]
    csv_like_lines = 0
    for line in lines[:8]:
        if line.count(",") >= 1:
            csv_like_lines += 1
    if csv_like_lines >= 2:
        structured_hits += 1
    if re.search(r"(^|\n)\s*[^,\n]+,[^,\n]+,[^,\n]+", text):
        structured_hits += 1
    parsed = parse_json_value(text)
    if isinstance(parsed, (dict, list)):
        structured_hits += 2
    return structured_hits

def _normalize_web_fetch_date_token(token: str) -> str:
    text = str(token or "").strip()
    if not text:
        return ""
    normalized = text.replace("年", "-").replace("月", "-").replace("日", "")
    normalized = normalized.replace("/", "-").replace(".", "-")
    normalized = re.sub(r"-+", "-", normalized).strip("-")
    return normalized


def _count_distinct_web_fetch_dates(sample: str) -> int:
    seen: Set[str] = set()
    for token in re.findall(r"(?:20\d{2}[-/.年]\d{1,2}(?:[-/.月]\d{1,2})?)", str(sample or "")):
        normalized = _normalize_web_fetch_date_token(token)
        if normalized:
            seen.add(normalized)
    return len(seen)


def _count_web_fetch_date_price_pair_hits(sample: str) -> int:
    text = str(sample or "")
    patterns = [
        r"(?:20\d{2}[-/.年]\d{1,2}(?:[-/.月]\d{1,2})?).{0,80}?\b\d{2,5}(?:\.\d{1,4})?\b.{0,20}?(?:元/克|人民币/克|cny/?g|price)",
        r"(?:元/克|人民币/克|cny/?g|price).{0,40}?(?:20\d{2}[-/.年]\d{1,2}(?:[-/.月]\d{1,2})?).{0,80}?\b\d{2,5}(?:\.\d{1,4})?\b",
        r"(?:^|\n)\s*(?:20\d{2}[-/.]\d{1,2}[-/.]\d{1,2})\s*,\s*\d{2,5}(?:\.\d{1,4})?",
    ]
    hits = 0
    for pattern in patterns:
        hits += len(re.findall(pattern, text, flags=re.IGNORECASE))
    return hits


def _is_historical_structured_target(protocol: Optional[dict], query_keywords: List[str]) -> bool:
    time_hints = _get_web_fetch_time_hints(protocol)
    target_signals = _get_web_fetch_target_signals(protocol)
    merged = " ".join(list(time_hints) + list(target_signals) + list(query_keywords or [])).lower()
    if not merged:
        return False
    return any(
        term in merged
        for term in ("最近", "三个月", "3个月", "历史", "日度", "日线", "daily", "historical", "history")
    )



def _web_fetch_requires_date(required_fields: List[str], historical_target: bool, time_hints: List[str]) -> bool:
    if historical_target:
        return True
    if time_hints:
        return True
    date_like_fields = {"date", "day", "datetime", "trade_date", "trading_date", "time", "timestamp", "period"}
    for field in required_fields or []:
        lowered = str(field or "").strip().lower()
        if not lowered:
            continue
        if lowered in date_like_fields:
            return True
        if ("date" in lowered) or ("time" in lowered):
            return True
    return False



def _web_fetch_requires_numeric(required_fields: List[str], unit_hints: List[str], target_signals: List[str]) -> bool:
    if unit_hints:
        return True
    numeric_markers = (
        "price",
        "quote",
        "rate",
        "value",
        "amount",
        "count",
        "volume",
        "score",
        "index",
        "temperature",
        "yield",
        "pct",
        "percent",
        "价格",
        "利率",
        "汇率",
        "指数",
        "温度",
        "下载量",
        "人数",
        "成交量",
        "数量",
    )
    merged = " ".join(list(required_fields or []) + list(target_signals or [])).lower()
    if not merged:
        return False
    return any(marker in merged for marker in numeric_markers)


def _analyze_web_fetch_candidate_content(
    *,
    url: str,
    context_text: str,
    output_text: str,
    protocol: Optional[dict],
    query_keywords: List[str],
) -> dict:
    sample = str(output_text or "")[:_WEB_FETCH_PREVIEW_SAMPLE_CHARS]
    lowered = sample.lower()
    required_fields = _get_web_fetch_required_fields(protocol)
    target_signals = _get_web_fetch_target_signals(protocol)
    unit_hints = _get_web_fetch_unit_hints(protocol)
    time_hints = _get_web_fetch_time_hints(protocol)
    negative_terms = _get_web_fetch_negative_terms(protocol)
    historical_target = _is_historical_structured_target(protocol, query_keywords)

    url_score = _score_web_fetch_candidate_link(
        url=url,
        context_text=context_text,
        query_keywords=query_keywords,
    )
    date_hits = len(re.findall(r"(?:20\d{2}[-/.年]\d{1,2}(?:[-/.月]\d{1,2})?)", sample))
    distinct_date_hits = _count_distinct_web_fetch_dates(sample)
    price_hits = len(re.findall(r"\b\d{2,5}(?:\.\d{1,4})?\b", sample))
    date_price_pair_hits = _count_web_fetch_date_price_pair_hits(sample)
    required_field_evidence = _extract_web_fetch_required_field_evidence(
        sample=sample,
        required_fields=required_fields,
        unit_hints=unit_hints,
        target_signals=target_signals,
        date_hits=date_hits,
    )
    required_hits = len(required_field_evidence)
    signal_hits = _count_unique_substring_hits(lowered, [item.lower() for item in target_signals])
    unit_hits = _count_unique_substring_hits(lowered, [item.lower() for item in unit_hints])
    negative_hits = _count_unique_substring_hits(lowered, [item.lower() for item in negative_terms])
    page_noise_hits = _count_web_fetch_page_noise_hits(lowered)
    structured_hits = _detect_structured_content_signals(sample)
    require_structured = _is_web_fetch_require_structured(protocol)
    min_required_hits = 1 if len(required_fields) <= 1 else min(2, len(required_fields))
    has_required_schema = required_hits >= min_required_hits
    has_semantic_anchor = bool(unit_hits > 0 or signal_hits > 0 or has_required_schema)

    requires_date = _web_fetch_requires_date(required_fields, historical_target, time_hints)
    requires_numeric = _web_fetch_requires_numeric(required_fields, unit_hints, target_signals)
    requires_unit = bool(unit_hints)
    requires_pair_evidence = bool(historical_target and requires_date and requires_numeric)

    has_numeric_evidence = bool(price_hits >= 2 and ((not requires_date) or date_hits > 0))
    has_structured_evidence = bool(structured_hits > 0)
    has_multi_date_evidence = bool((not requires_date) or distinct_date_hits >= 2)
    has_date_price_pair_evidence = bool((not requires_pair_evidence) or date_price_pair_hits > 0)

    score = int(url_score)
    score += required_hits * 6
    score += signal_hits * 4
    score += unit_hits * 6
    score += min(date_hits, 5) * 2
    score += min(distinct_date_hits, 4) * 2
    score += min(price_hits, 6)
    score += min(date_price_pair_hits, 4) * 4
    score += structured_hits * 3
    score -= negative_hits * 4
    score -= page_noise_hits * 6

    evidence: List[str] = []
    if required_hits:
        evidence.append(f"required_fields={required_hits}")
    for field_name, matches in list(required_field_evidence.items())[:3]:
        evidence.append(f"field:{field_name}={'/'.join(matches[:2])}")
    if signal_hits:
        evidence.append(f"signals={signal_hits}")
    if unit_hits:
        evidence.append(f"units={unit_hits}")
    if date_hits:
        evidence.append(f"dates={min(date_hits, 9)}")
    if distinct_date_hits:
        evidence.append(f"distinct_dates={min(distinct_date_hits, 9)}")
    if date_price_pair_hits:
        evidence.append(f"date_price_pairs={min(date_price_pair_hits, 9)}")
    if structured_hits:
        evidence.append(f"structured={structured_hits}")
    if page_noise_hits:
        evidence.append(f"page_noise={page_noise_hits}")

    rejections: List[str] = []
    if not sample.strip():
        rejections.append("候选页内容为空")
    if required_hits <= 0:
        rejections.append("缺少 required_fields 信号")
    if target_signals and signal_hits <= 0:
        rejections.append("缺少 target_signals 信号")
    if requires_unit and unit_hits <= 0:
        rejections.append("缺少单位信号")
    if requires_date and date_hits <= 0:
        rejections.append("缺少日期信号")
    if requires_numeric and price_hits < 2:
        rejections.append("缺少稳定数值证据")
    if negative_hits > 0:
        rejections.append("命中通用噪声词")
    if page_noise_hits > 0:
        rejections.append("命中账号/门户噪声信号")
    if require_structured and not has_required_schema:
        rejections.append("结构化任务缺少关键字段证据")
    if require_structured and not has_structured_evidence:
        rejections.append("结构化任务缺少结构化内容证据")
    if require_structured and requires_numeric and not has_numeric_evidence:
        if requires_date:
            rejections.append("结构化任务缺少日期+数值证据")
        else:
            rejections.append("结构化任务缺少稳定数值证据")
    if require_structured and not has_semantic_anchor:
        rejections.append("结构化任务缺少语义/单位锚点")
    if require_structured and historical_target and not has_multi_date_evidence:
        rejections.append("历史结构化任务缺少多日期证据")
    if require_structured and requires_pair_evidence and not has_date_price_pair_evidence:
        rejections.append("历史结构化任务缺少日期-数值成对证据")

    if require_structured:
        acceptable = bool(sample.strip()) and all(
            (
                has_required_schema,
                has_structured_evidence,
                has_semantic_anchor,
                (not requires_numeric or has_numeric_evidence),
                (not historical_target or has_multi_date_evidence),
                (not requires_pair_evidence or has_date_price_pair_evidence),
                page_noise_hits <= 0,
            )
        )
    else:
        acceptable = bool(sample.strip()) and (
            score >= int(_WEB_FETCH_MIN_ACCEPT_SCORE)
            or (
                required_hits > 0
                and (not requires_date or date_hits > 0)
                and ((not requires_unit or unit_hits > 0) or signal_hits >= 2 or has_required_schema)
            )
        ) and page_noise_hits <= 1

    return {
        "url": url,
        "host": _extract_web_fetch_host(url),
        "score": int(score),
        "url_score": int(url_score),
        "required_hits": int(required_hits),
        "signal_hits": int(signal_hits),
        "unit_hits": int(unit_hits),
        "date_hits": int(date_hits),
        "distinct_date_hits": int(distinct_date_hits),
        "price_hits": int(price_hits),
        "date_price_pair_hits": int(date_price_pair_hits),
        "structured_hits": int(structured_hits),
        "negative_hits": int(negative_hits),
        "page_noise_hits": int(page_noise_hits),
        "acceptable": bool(acceptable),
        "evidence": evidence,
        "required_field_evidence": required_field_evidence,
        "rejections": rejections,
        "preview": _build_web_fetch_candidate_preview(sample or context_text),
    }


def _normalize_web_fetch_anchor_text(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    text = re.sub(r"\s+", " ", text).strip()
    return text



def _build_web_fetch_anchor_context(sample: str, anchor_start: int, anchor_end: int, anchor_text: str) -> str:
    start = max(0, int(anchor_start) - 220)
    end = min(len(sample), int(anchor_end) + 260)
    snippet = sample[start:end]
    snippet = re.sub(r"<script[^>]*>.*?</script>", " ", snippet, flags=re.IGNORECASE | re.DOTALL)
    snippet = re.sub(r"<style[^>]*>.*?</style>", " ", snippet, flags=re.IGNORECASE | re.DOTALL)
    snippet = re.sub(r"<[^>]+>", " ", snippet)
    snippet = re.sub(r"\s+", " ", snippet).strip()
    merged = f"{anchor_text} {snippet}".strip()
    return truncate_inline_text(merged, 280)



def _is_web_fetch_anchor_noise(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return True
    if len(lowered) <= 2:
        return True
    return any(term in lowered for term in _WEB_FETCH_ANCHOR_NOISE_TERMS)



def _is_web_fetch_search_results_page(sample: str) -> bool:
    lowered = str(sample or "").lower()
    return any(
        marker in lowered
        for marker in (
            'id="b_results"',
            'class="b_algo"',
            'class="res-list"',
            'data-mdurl=',
            'sogou_vr_',
            'markdown content:',
        )
    )



def _build_web_fetch_search_result_context(title: str, snippet: str) -> str:
    merged = " ".join([str(title or "").strip(), str(snippet or "").strip()]).strip()
    merged = re.sub(r"\s+", " ", merged).strip()
    return truncate_inline_text(merged, 280)



def _build_web_fetch_generic_search_anchor_context(sample: str, anchor_end: int, anchor_text: str) -> str:
    tail = sample[int(anchor_end) : min(len(sample), int(anchor_end) + 240)]
    stops = []
    for pattern in (r"<a\b", r"</li>", r"</div>", r"</article>", r"</section>"):
        match = re.search(pattern, tail, flags=re.IGNORECASE)
        if match:
            stops.append(int(match.start()))
    if stops:
        tail = tail[: min(stops)]
    snippet = _normalize_web_fetch_anchor_text(tail)
    return _build_web_fetch_search_result_context(anchor_text, snippet)



def _extract_web_fetch_links_from_bing_blocks(sample: str) -> List[Tuple[str, str, int]]:
    candidates: List[Tuple[str, str, int]] = []
    for match in re.finditer(r'<li class="b_algo"[^>]*>(.*?)</li>', sample, flags=re.IGNORECASE | re.DOTALL):
        block = str(match.group(1) or "")
        title_match = re.search(
            r"<h2[^>]*>\s*<a[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not title_match:
            continue
        href = str(title_match.group(1) or "")
        title = _normalize_web_fetch_anchor_text(title_match.group(2) or "")
        if _is_web_fetch_anchor_noise(title):
            continue
        snippet_match = re.search(
            r'<div class="b_caption"[^>]*>.*?<p[^>]*>(.*?)</p>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        snippet = _normalize_web_fetch_anchor_text(snippet_match.group(1) or "") if snippet_match else ""
        context_text = _build_web_fetch_search_result_context(title, snippet)
        candidates.append((href, context_text or title, len(candidates)))
    return candidates



def _extract_web_fetch_links_from_360_blocks(sample: str) -> List[Tuple[str, str, int]]:
    candidates: List[Tuple[str, str, int]] = []
    for match in re.finditer(r'<li class="res-list"[^>]*>(.*?)</li>', sample, flags=re.IGNORECASE | re.DOTALL):
        block = str(match.group(1) or "")
        title_match = re.search(
            r"<h3[^>]*>\s*<a[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not title_match:
            continue
        wrapper_url = str(title_match.group(1) or "")
        title = _normalize_web_fetch_anchor_text(title_match.group(2) or "")
        if _is_web_fetch_anchor_noise(title):
            continue
        direct_match = re.search(r"\bdata-mdurl=[\"']([^\"']+)[\"']", block, flags=re.IGNORECASE)
        href = str(direct_match.group(1) or "") if direct_match else wrapper_url
        snippet_match = re.search(r'<span class="res-list-summary"[^>]*>(.*?)</span>', block, flags=re.IGNORECASE | re.DOTALL)
        snippet = _normalize_web_fetch_anchor_text(snippet_match.group(1) or "") if snippet_match else ""
        context_text = _build_web_fetch_search_result_context(title, snippet)
        candidates.append((href, context_text or title, len(candidates)))
    return candidates



def _extract_web_fetch_links_from_search_data_attrs(sample: str) -> List[Tuple[str, str, int]]:
    candidates: List[Tuple[str, str, int]] = []
    for attr_name in ("data-mdurl", "data-url"):
        pattern = rf"\b{attr_name}=[\"'](https?://[^\"']+)[\"']"
        for match in re.finditer(pattern, sample, flags=re.IGNORECASE):
            url = str(match.group(1) or "")
            context_text = _build_web_fetch_anchor_context(sample, match.start(), match.end(), "")
            candidates.append((url, context_text, len(candidates)))
    return candidates



def _extract_web_fetch_link_records_from_text(
    raw_text: str,
    *,
    exclude_hosts: Set[str],
    exclude_host_families: Optional[Set[str]] = None,
    query: str = "",
    force_search_result_page: bool = False,
) -> List[dict]:
    text = str(raw_text or "")
    if not text.strip():
        return []

    sample = text[:400000]
    query_keywords = _build_web_fetch_query_keywords(query)
    candidates: List[Tuple[str, str, int, str]] = []
    structured_search_page = bool(force_search_result_page) or _is_web_fetch_search_results_page(sample)

    for href, context_text, _ordinal in _extract_web_fetch_links_from_bing_blocks(sample):
        candidates.append((href, context_text, len(candidates), "bing_block"))
    for href, context_text, _ordinal in _extract_web_fetch_links_from_360_blocks(sample):
        candidates.append((href, context_text, len(candidates), "360_block"))
    for href, context_text, _ordinal in _extract_web_fetch_links_from_search_data_attrs(sample):
        candidates.append((href, context_text, len(candidates), "search_data_attr"))

    markdown_matches = re.findall(
        r"\[([^\]]*)\]\((https?://[^\s)]+)\)",
        sample,
        flags=re.IGNORECASE,
    )
    for anchor_text, url in markdown_matches:
        candidates.append((str(url or ""), str(anchor_text or ""), len(candidates), "markdown_link"))

    if (not structured_search_page) or (structured_search_page and not candidates):
        for match in re.finditer(
            r"<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
            sample,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            href = str(match.group(1) or "")
            anchor_html = str(match.group(2) or "")
            anchor_text = _normalize_web_fetch_anchor_text(str(anchor_html or ""))
            if _is_web_fetch_anchor_noise(anchor_text):
                continue
            if structured_search_page:
                context_text = _build_web_fetch_generic_search_anchor_context(sample, match.end(), anchor_text)
            else:
                context_text = _build_web_fetch_anchor_context(sample, match.start(), match.end(), anchor_text)
            candidates.append((str(href or ""), context_text or anchor_text, len(candidates), "html_anchor"))

    if ((not structured_search_page) or (structured_search_page and not candidates)) and len(candidates) < 3:
        for match in re.finditer(r"https?://[^\s\"'<>]+", sample, flags=re.IGNORECASE):
            url = str(match.group(0) or "")
            if structured_search_page:
                context_text = url
            else:
                start = max(0, int(match.start()) - 80)
                end = min(len(sample), int(match.end()) + 80)
                context_text = sample[start:end]
            candidates.append((url, context_text, len(candidates), "plain_url"))

    skip_suffixes = (
        ".css",
        ".js",
        ".svg",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".ico",
        ".woff",
        ".woff2",
        ".ttf",
        ".map",
        ".xml",
    )
    ranked: List[dict] = []
    seen: Set[str] = set()
    blocked_families = set(exclude_host_families or set())
    for item, context_text, ordinal, source in candidates:
        current = str(item or "").strip().strip("()[]{}<>,\"'`")
        url = _normalize_web_fetch_url_candidate(current)
        if not url:
            continue
        host = _extract_web_fetch_host(url)
        if host and host in exclude_hosts:
            continue
        if host:
            family = _extract_web_fetch_host_family(host)
            if family and family in blocked_families:
                continue
        lowered_path = str(urlparse(url).path or "").strip().lower()
        if lowered_path.endswith(skip_suffixes):
            continue
        if url in seen:
            continue

        normalized_context = truncate_inline_text(str(context_text or "").strip(), 280)
        merged_context = f"{url} {normalized_context}".strip()
        keyword_hits = _count_web_fetch_query_keyword_hits(merged_context, query_keywords)
        score = _score_web_fetch_candidate_link(
            url=url,
            context_text=normalized_context,
            query_keywords=query_keywords,
        )

        search_like = bool(_is_web_fetch_search_like_candidate_url(url))
        url_positive_hits = _count_unique_substring_hits(url, list(_WEB_FETCH_POSITIVE_URL_TERMS))
        url_noise_hits = _count_unique_substring_hits(merged_context, list(_WEB_FETCH_NOISE_URL_TERMS))
        host_noise = bool(host and host.startswith(_WEB_FETCH_NOISE_HOST_PREFIXES))
        path_noise = any(term in lowered_path for term in _WEB_FETCH_NOISE_PATH_TERMS)
        if host_noise or path_noise:
            continue
        if structured_search_page and str(source or "") != "plain_url" and keyword_hits <= 0 and url_positive_hits <= 0:
            continue
        portal_like = bool(lowered_path in {"", "/", "/home", "/index", "/all", "/app/main"})
        path_depth = len([part for part in lowered_path.split("/") if part])
        structured_search_mismatch = bool(
            structured_search_page
            and str(source or "") != "plain_url"
            and keyword_hits <= 0
            and int(score) <= 0
        )

        seen.add(url)
        ranked.append(
            {
                "url": url,
                "host": host,
                "context_text": normalized_context,
                "ordinal": int(ordinal),
                "score": int(score),
                "keyword_hits": int(keyword_hits),
                "source": str(source or "").strip() or "unknown",
                "signals": {
                    "search_like": bool(search_like),
                    "keyword_hits": int(keyword_hits),
                    "url_positive_hits": int(url_positive_hits),
                    "url_noise_hits": int(url_noise_hits),
                    "host_noise": bool(host_noise),
                    "portal_like": bool(portal_like),
                    "path_depth": int(path_depth),
                    "structured_search_page": bool(structured_search_page),
                    "structured_search_mismatch": bool(structured_search_mismatch),
                    "soft_score": int(score),
                },
            }
        )

    ranked.sort(
        key=lambda item: (
            -int(item.get("score") or 0),
            -int(item.get("keyword_hits") or 0),
            int(item.get("ordinal") or 0),
        )
    )
    return ranked


def _extract_web_fetch_links_from_text(
    raw_text: str,
    *,
    exclude_hosts: Set[str],
    exclude_host_families: Optional[Set[str]] = None,
    query: str = "",
) -> List[str]:
    return [
        str(item.get("url") or "")
        for item in _extract_web_fetch_link_records_from_text(
            raw_text,
            exclude_hosts=exclude_hosts,
            exclude_host_families=exclude_host_families,
            query=query,
        )
        if str(item.get("url") or "").strip()
    ]


def _build_web_fetch_candidate_selection_prompt(
    *,
    tool_input: str,
    protocol: Optional[dict],
    candidates: List[dict],
) -> str:
    objective = str((protocol or {}).get("objective") or tool_input or "").strip()
    if not objective:
        objective = "从候选中挑选最值得预览的真实内容页。"
    required_fields = ", ".join(_get_web_fetch_required_fields(protocol)[:4]) or "(无)"
    target_signals = ", ".join(_get_web_fetch_target_signals(protocol)[:6]) or "(无)"
    time_hints = ", ".join(_get_web_fetch_time_hints(protocol)[:4]) or "(无)"
    unit_hints = ", ".join(_get_web_fetch_unit_hints(protocol)[:4]) or "(无)"

    lines = [
        "你在做 web_fetch 候选页选择。",
        f"目标: {objective}",
        f"required_fields: {required_fields}",
        f"target_signals: {target_signals}",
        f"time_hints: {time_hints}",
        f"unit_hints: {unit_hints}",
        "任务: 只根据给定候选，选出最值得预览的 URL 顺序。",
        "约束:",
        "- 不能编造新 URL，只能从候选中选择。",
        "- 优先真实内容页/数据页/表格/API/CSV/JSON 页。",
        "- search_like=true 代表它更像搜索/聚合入口，不应优先，但如果候选全弱，可保留靠后预览。",
        "- 如果任务存在明确单位/币种/时间粒度，优先保留与其一致的候选；单位或口径明显不匹配的候选应降权。",
        "- 不要因为站点名熟悉就优先，必须基于 context_preview 与 signals 判断。",
        "输出 JSON: {\"selected_urls\": [\"...\"], \"notes\": [\"...\"]}",
        "selected_urls 最多 6 个，按最优到最差排序。",
        "候选列表:",
    ]
    for index, item in enumerate(candidates[:10], start=1):
        payload = {
            "url": str(item.get("url") or ""),
            "host": str(item.get("host") or ""),
            "query": str(item.get("query") or ""),
            "context_preview": truncate_inline_text(str(item.get("context_text") or ""), 180),
            "signals": dict(item.get("signals") or {}),
            "initial_score": int(item.get("initial_score") or item.get("score") or 0),
        }
        lines.append(f"{index}. {json.dumps(payload, ensure_ascii=False)}")
    return "\n".join(lines)



def _select_web_fetch_candidate_urls_with_llm(
    *,
    tool_input: str,
    protocol: Optional[dict],
    candidates: List[dict],
    context: Optional[dict],
) -> Tuple[List[str], List[str]]:
    task_id, run_id, selected_model = _get_web_fetch_llm_context(context)
    if task_id <= 0 or run_id <= 0:
        _emit_web_fetch_event(
            context,
            "search_progress",
            stage="candidate_selector_skipped",
            reason="missing_task_context",
        )
        return [], []
    if len(candidates or []) < 4:
        _emit_web_fetch_event(
            context,
            "search_progress",
            stage="candidate_selector_skipped",
            reason="not_enough_candidates",
            candidate_count=len(candidates or []),
        )
        return [], []

    prompt = _build_web_fetch_candidate_selection_prompt(
        tool_input=tool_input,
        protocol=protocol,
        candidates=candidates,
    )
    valid_urls = {str(item.get("url") or "").strip() for item in candidates if str(item.get("url") or "").strip()}
    if not valid_urls:
        _emit_web_fetch_event(
            context,
            "search_progress",
            stage="candidate_selector_skipped",
            reason="empty_candidates",
        )
        return [], []

    _emit_web_fetch_event(
        context,
        "search_progress",
        stage="candidate_selector_started",
        candidate_count=len(candidates or []),
        model=selected_model,
    )

    try:
        payload = {
            "task_id": int(task_id),
            "run_id": int(run_id),
            "prompt": prompt,
            "parameters": {
                "temperature": 0,
                "timeout_seconds": 18,
                "retry_max_attempts": 1,
            },
        }
        if selected_model:
            payload["model"] = selected_model
        llm_result = create_llm_call(payload)
        record = llm_result.get("record") if isinstance(llm_result, dict) else None
        response_text = str((record or {}).get("response") or "")
        parsed = parse_json_dict(response_text)
        if not isinstance(parsed, dict):
            embedded = _extract_first_json_object(response_text)
            parsed = parse_json_dict(embedded) if embedded else None
        if not isinstance(parsed, dict):
            _emit_web_fetch_event(
                context,
                "search_progress",
                stage="candidate_selector_failed",
                reason="invalid_json",
                message=truncate_inline_text(response_text, 200),
                model=selected_model,
            )
            return [], []
    except Exception as exc:
        _emit_web_fetch_event(
            context,
            "search_progress",
            stage="candidate_selector_failed",
            reason="llm_call_failed",
            message=truncate_inline_text(str(exc), 200),
            model=selected_model,
        )
        return [], []

    selected_urls: List[str] = []
    raw_selected = parsed.get("selected_urls") if isinstance(parsed.get("selected_urls"), list) else []
    for raw in raw_selected:
        current = raw.get("url") if isinstance(raw, dict) else raw
        url = _normalize_web_fetch_url_candidate(current)
        if not url or url not in valid_urls or url in selected_urls:
            continue
        selected_urls.append(url)

    notes = _coerce_nonempty_str_list(parsed.get("notes"))[:4]
    _emit_web_fetch_event(
        context,
        "search_progress",
        stage="candidate_selector_done",
        selected_urls=selected_urls[:_WEB_FETCH_EVENT_CANDIDATE_LIMIT],
        selected_count=len(selected_urls),
        notes=notes,
        model=selected_model,
    )
    return selected_urls[:6], notes



def _rerank_web_fetch_candidates_with_llm(
    *,
    tool_input: str,
    protocol: Optional[dict],
    candidates: List[dict],
    context: Optional[dict],
) -> Tuple[List[dict], List[str]]:
    selected_urls, notes = _select_web_fetch_candidate_urls_with_llm(
        tool_input=tool_input,
        protocol=protocol,
        candidates=candidates,
        context=context,
    )
    if not selected_urls:
        return candidates, []

    by_url = {
        str(item.get("url") or "").strip(): dict(item)
        for item in candidates
        if str(item.get("url") or "").strip()
    }
    selected_set = set(selected_urls)
    reordered: List[dict] = []
    for index, url in enumerate(selected_urls, start=1):
        item = by_url.get(url)
        if not isinstance(item, dict):
            continue
        patched = dict(item)
        patched["llm_rank"] = int(index)
        patched["llm_selected"] = True
        reordered.append(patched)

    for item in candidates:
        url = str(item.get("url") or "").strip()
        if not url or url in selected_set:
            continue
        patched = dict(item)
        patched["llm_selected"] = False
        reordered.append(patched)

    return reordered, notes



def _build_web_fetch_preview_decision_prompt(
    *,
    tool_input: str,
    protocol: Optional[dict],
    queries: List[str],
    candidate_rankings: List[dict],
    candidate_rejections: List[dict],
) -> str:
    objective = str((protocol or {}).get("objective") or tool_input or "").strip()
    if not objective:
        objective = "选择最可能直接满足任务的数据页面，或给出更好的下一轮搜索词。"
    required_fields = ", ".join(_get_web_fetch_required_fields(protocol)[:4]) or "(无)"
    target_signals = ", ".join(_get_web_fetch_target_signals(protocol)[:6]) or "(无)"
    time_hints = ", ".join(_get_web_fetch_time_hints(protocol)[:4]) or "(无)"
    unit_hints = ", ".join(_get_web_fetch_unit_hints(protocol)[:4]) or "(无)"
    lines = [
        "你在做 web_fetch 预览候选决策。",
        f"目标: {objective}",
        f"required_fields: {required_fields}",
        f"target_signals: {target_signals}",
        f"time_hints: {time_hints}",
        f"unit_hints: {unit_hints}",
        f"previous_queries: {json.dumps(list(queries or [])[:8], ensure_ascii=False)}",
        "你必须在 accept / retry / reject 中三选一。",
        "规则:",
        "- accept: 仅当某个候选已经明显展示出真实可用的数据/表格/API 证据。",
        "- retry: 当前候选大多是 captcha、门户、聚合页、文章页，或缺少关键字段/单位/多日期证据时，给出新的搜索 query。",
        "- reject: 当前候选彻底无关，且没有比 previous_queries 更合理的改写方向。",
        "- 如果协议里给出了明确 unit_hints / time_hints，请在 retry_queries 中尽量保留它们，不要擅自把元/克改成美元/盎司、把日度改成其他粒度。",
        "- 不能编造新 URL，selected_url 只能从候选列表里选。",
        "- retry_queries 必须是搜索引擎关键词风格，不要输出 URL，不要简单重复 previous_queries。",
        '输出 JSON: {"decision":"accept|retry|reject","selected_url":"...","retry_queries":["..."],"notes":["..."]}',
        "候选预览:",
    ]
    for index, item in enumerate(candidate_rankings[:6], start=1):
        payload = {
            "url": str(item.get("url") or ""),
            "query": str(item.get("query") or ""),
            "score": int(item.get("score") or 0),
            "heuristic_acceptable": bool(item.get("acceptable")),
            "evidence": list(item.get("evidence") or [])[:6],
            "rejections": list(item.get("rejections") or [])[:6],
            "signals": dict(item.get("signals") or {}),
            "preview": truncate_inline_text(str(item.get("preview") or ""), 220),
        }
        lines.append(f"{index}. {json.dumps(payload, ensure_ascii=False)}")
    if candidate_rejections:
        lines.append("已知失败/弱候选:")
        for index, item in enumerate(candidate_rejections[:6], start=1):
            payload = {
                "url": str(item.get("url") or ""),
                "reason": str(item.get("reason") or ""),
                "detail": truncate_inline_text(str(item.get("detail") or ""), 180),
                "preview": truncate_inline_text(str(item.get("preview") or ""), 160),
                "score": int(item.get("score") or 0),
            }
            lines.append(f"R{index}. {json.dumps(payload, ensure_ascii=False)}")
    return "\n".join(lines)



def _decide_web_fetch_preview_candidates_with_llm(
    *,
    tool_input: str,
    protocol: Optional[dict],
    queries: List[str],
    candidate_rankings: List[dict],
    candidate_rejections: List[dict],
    context: Optional[dict],
) -> dict:
    task_id, run_id, selected_model = _get_web_fetch_llm_context(context)
    if task_id <= 0 or run_id <= 0:
        _emit_web_fetch_event(
            context,
            "search_progress",
            stage="candidate_decider_skipped",
            reason="missing_task_context",
        )
        return {}
    if not candidate_rankings:
        _emit_web_fetch_event(
            context,
            "search_progress",
            stage="candidate_decider_skipped",
            reason="no_preview_candidates",
        )
        return {}

    valid_urls = {str(item.get("url") or "").strip() for item in candidate_rankings if str(item.get("url") or "").strip()}
    prompt = _build_web_fetch_preview_decision_prompt(
        tool_input=tool_input,
        protocol=protocol,
        queries=queries,
        candidate_rankings=candidate_rankings,
        candidate_rejections=candidate_rejections,
    )

    _emit_web_fetch_event(
        context,
        "search_progress",
        stage="candidate_decider_started",
        candidate_count=len(candidate_rankings),
        model=selected_model,
    )

    try:
        payload = {
            "task_id": int(task_id),
            "run_id": int(run_id),
            "prompt": prompt,
            "parameters": {
                "temperature": 0,
                "timeout_seconds": 20,
                "retry_max_attempts": 1,
            },
        }
        if selected_model:
            payload["model"] = selected_model
        llm_result = create_llm_call(payload)
        record = llm_result.get("record") if isinstance(llm_result, dict) else None
        response_text = str((record or {}).get("response") or "")
        parsed = parse_json_dict(response_text)
        if not isinstance(parsed, dict):
            embedded = _extract_first_json_object(response_text)
            parsed = parse_json_dict(embedded) if embedded else None
        if not isinstance(parsed, dict):
            _emit_web_fetch_event(
                context,
                "search_progress",
                stage="candidate_decider_failed",
                reason="invalid_json",
                message=truncate_inline_text(response_text, 200),
                model=selected_model,
            )
            return {}
    except Exception as exc:
        _emit_web_fetch_event(
            context,
            "search_progress",
            stage="candidate_decider_failed",
            reason="llm_call_failed",
            message=truncate_inline_text(str(exc), 200),
            model=selected_model,
        )
        return {}

    decision = str(parsed.get("decision") or "").strip().lower()
    retry_queries = _normalize_web_fetch_retry_queries(
        parsed.get("retry_queries"),
        fallback_query=str(tool_input or ""),
        target_signals=_get_web_fetch_target_signals(protocol),
        unit_hints=_get_web_fetch_unit_hints(protocol),
        time_hints=_get_web_fetch_time_hints(protocol),
    )
    notes = _coerce_nonempty_str_list(parsed.get("notes"))[:4]
    selected_url = _normalize_web_fetch_url_candidate(parsed.get("selected_url"))
    if selected_url not in valid_urls:
        selected_url = ""

    if decision not in {"accept", "retry", "reject"}:
        if selected_url:
            decision = "accept"
        elif retry_queries:
            decision = "retry"
        else:
            decision = "reject"

    if decision == "accept" and not selected_url:
        _emit_web_fetch_event(
            context,
            "search_progress",
            stage="candidate_decider_failed",
            reason="missing_selected_url",
            notes=notes,
            model=selected_model,
        )
        return {}

    _emit_web_fetch_event(
        context,
        "search_progress",
        stage="candidate_decider_done",
        decision=decision,
        selected_url=selected_url,
        retry_queries=retry_queries,
        notes=notes,
        model=selected_model,
    )
    return {
        "decision": decision,
        "selected_url": selected_url,
        "retry_queries": retry_queries,
        "notes": notes,
    }



def _execute_web_fetch_keyword_search(
    exec_spec: dict,
    tool_input: str,
    *,
    protocol: Optional[dict] = None,
    context: Optional[dict] = None,
) -> dict:
    retry_depth = 0
    if isinstance(context, dict):
        try:
            retry_depth = int(parse_positive_int(context.get("_web_fetch_retry_depth")) or 0)
        except Exception:
            retry_depth = 0
    queries = _resolve_protocol_search_queries(str(tool_input or ""), protocol)
    if not queries:
        return {
            "ok": False,
            "output_text": "",
            "warnings": [],
            "attempts": [],
            "error_code": "web_fetch_blocked",
            "error_message": "web_fetch 输入为空，无法执行关键词检索",
        }

    try:
        max_results = max(1, int(AGENT_WEB_FETCH_SEARCH_MAX_RESULTS or 8))
    except Exception:
        max_results = 8
    try:
        max_pages = max(1, int(AGENT_WEB_FETCH_SEARCH_MAX_PAGES or 5))
    except Exception:
        max_pages = 5
    preview_limit = max(max_pages, min(max_results * 2, 10))

    attempts: List[dict] = []
    warnings: List[str] = []
    blocked_hosts: Set[str] = set()
    denied_domains = _get_protocol_deny_domains(protocol)
    candidate_records: Dict[str, dict] = {}
    preview_outputs: Dict[str, str] = {}
    last_output = ""
    final_error_code = "web_fetch_blocked"
    final_reason = "search_failed"
    final_detail = ""

    _emit_web_fetch_event(
        context,
        "search_progress",
        stage="search_started",
        queries=queries,
        protocol_source=str((protocol or {}).get("source") or "") if isinstance(protocol, dict) else "",
    )

    for query_index, query in enumerate(queries, start=1):
        search_urls = _build_web_fetch_search_urls(query)
        if not search_urls:
            attempts.append(
                {
                    "stage": "search",
                    "query": query,
                    "status": "failed",
                    "error_code": "web_fetch_blocked",
                    "reason": "search_template_missing",
                    "detail": "未配置可用搜索模板",
                }
            )
            final_error_code = "web_fetch_blocked"
            final_reason = "search_template_missing"
            final_detail = "未配置可用搜索模板"
            continue

        search_hosts: Set[str] = set(
            host for host in (_extract_web_fetch_host(url) for url in search_urls) if host
        )
        search_host_families: Set[str] = set(
            family
            for family in (_extract_web_fetch_host_family(host) for host in search_hosts)
            if family and _is_known_search_engine_family(family)
        )
        query_keywords = _build_web_fetch_query_keywords(query)
        _emit_web_fetch_event(
            context,
            "search_progress",
            stage="search_query",
            query=query,
            query_index=query_index,
            total_queries=len(queries),
            search_url_count=len(search_urls),
        )

        for search_url in search_urls:
            search_host = _extract_web_fetch_host(search_url)
            output_text, exec_error = _execute_tool_with_exec_spec(exec_spec, search_url)
            current_output = str(output_text or "")
            if current_output.strip():
                last_output = current_output
            classified = _classify_web_fetch_result(current_output, exec_error)
            if str(classified.get("ok")) == "1":
                extracted = _extract_web_fetch_link_records_from_text(
                    current_output,
                    exclude_hosts=search_hosts,
                    exclude_host_families=search_host_families,
                    query=query,
                    force_search_result_page=bool(search_host and _is_known_search_engine_family(_extract_web_fetch_host_family(search_host))),
                )
                added = 0
                host_hit_count: Dict[str, int] = {}
                for ordinal, candidate in enumerate(extracted, start=1):
                    page_url = str(candidate.get("url") or "")
                    if not page_url:
                        continue
                    host = str(candidate.get("host") or "") or _extract_web_fetch_host(page_url)
                    if host and _is_host_denied_by_protocol(host, denied_domains):
                        continue
                    if host:
                        current_host_hits = int(host_hit_count.get(host) or 0)
                        if current_host_hits >= 3:
                            continue
                    current_score = int(candidate.get("score") or 0)
                    existing = candidate_records.get(page_url)
                    if isinstance(existing, dict):
                        if int(existing.get("initial_score") or 0) >= int(current_score):
                            continue
                    candidate_records[page_url] = {
                        "url": page_url,
                        "host": host,
                        "query": query,
                        "context_text": str(candidate.get("context_text") or ""),
                        "search_url": search_url,
                        "ordinal": int(ordinal),
                        "initial_score": int(current_score),
                        "keyword_hits": int(candidate.get("keyword_hits") or 0),
                        "source": str(candidate.get("source") or ""),
                        "signals": dict(candidate.get("signals") or {}),
                    }
                    if host:
                        host_hit_count[host] = int(host_hit_count.get(host) or 0) + 1
                    added += 1
                    if len(candidate_records) >= preview_limit * 2:
                        break
                attempts.append(
                    {
                        "stage": "search",
                        "query": query,
                        "url": search_url,
                        "host": search_host,
                        "status": "ok",
                        "error_code": "",
                        "reason": "links_extracted" if added > 0 else "no_links",
                        "detail": f"提取候选链接 {added} 条",
                    }
                )
                if len(candidate_records) >= preview_limit * 2:
                    break
                continue

            current_code = str(classified.get("error_code") or "web_fetch_blocked")
            current_reason = str(classified.get("reason") or "search_failed")
            current_detail = str(classified.get("detail") or "")
            attempts.append(
                {
                    "stage": "search",
                    "query": query,
                    "url": search_url,
                    "host": search_host,
                    "status": "failed",
                    "error_code": current_code,
                    "reason": current_reason,
                    "detail": current_detail,
                }
            )
            final_error_code = current_code
            final_reason = current_reason or final_reason
            final_detail = current_detail or final_detail
            if search_host and _should_block_host_after_web_fetch_error(current_code, current_reason):
                blocked_hosts.add(search_host)

        if len(candidate_records) >= preview_limit * 2:
            break

    initial_ranked = sorted(
        candidate_records.values(),
        key=lambda item: (-int(item.get("initial_score") or 0), int(item.get("ordinal") or 0)),
    )
    initial_ranked, llm_selector_notes = _rerank_web_fetch_candidates_with_llm(
        tool_input=str(tool_input or ""),
        protocol=protocol,
        candidates=initial_ranked,
        context=context,
    )
    if llm_selector_notes:
        _emit_web_fetch_event(
            context,
            "search_progress",
            stage="candidate_rerank",
            notes=llm_selector_notes,
            selected_urls=[
                str(item.get("url") or "")
                for item in initial_ranked[: min(preview_limit, _WEB_FETCH_EVENT_CANDIDATE_LIMIT)]
                if bool(item.get("llm_selected"))
            ],
        )
    if not initial_ranked:
        summary = _build_web_fetch_attempt_summary(attempts)
        message = (
            f"web_fetch 关键词检索未提取到可用页面：{truncate_inline_text(' | '.join(queries), 180)}"
        )
        if summary:
            message += f"；尝试摘要：{summary}"
        if final_detail:
            message += f"；最后错误：{truncate_inline_text(final_detail, 220)}"
        _emit_web_fetch_event(
            context,
            "search_progress",
            stage="search_failed",
            reason="no_candidate_pages",
            error_code=final_error_code or "web_fetch_blocked",
            message=message,
        )
        return {
            "ok": False,
            "output_text": last_output,
            "warnings": warnings,
            "attempts": attempts,
            "candidate_rankings": [],
            "candidate_rejections": [],
            "error_code": final_error_code or "web_fetch_blocked",
            "error_message": message,
        }

    _emit_web_fetch_event(
        context,
        "search_candidates",
        total_candidates=len(initial_ranked),
        candidates=[
            {
                "url": str(item.get("url") or ""),
                "host": str(item.get("host") or ""),
                "query": str(item.get("query") or ""),
                "context_preview": truncate_inline_text(str(item.get("context_text") or ""), 180),
                "initial_score": int(item.get("initial_score") or 0),
                "keyword_hits": int(item.get("keyword_hits") or 0),
                "source": str(item.get("source") or ""),
                "signals": dict(item.get("signals") or {}),
                "llm_selected": bool(item.get("llm_selected")),
                "llm_rank": int(item.get("llm_rank") or 0),
            }
            for item in initial_ranked[:_WEB_FETCH_EVENT_CANDIDATE_LIMIT]
        ],
    )

    accepted_candidates: List[dict] = []
    candidate_rankings: List[dict] = []
    candidate_rejections: List[dict] = []

    for candidate in initial_ranked[:preview_limit]:
        page_url = str(candidate.get("url") or "")
        host = _extract_web_fetch_host(page_url)
        if host and _is_host_denied_by_protocol(host, denied_domains):
            rejection = {
                "url": page_url,
                "host": host,
                "reason": "protocol_domain_denied",
                "detail": "命中协议 deny_domains，已跳过",
            }
            candidate_rejections.append(rejection)
            attempts.append(
                {
                    "stage": "preview",
                    "url": page_url,
                    "host": host,
                    "status": "skipped",
                    "error_code": "protocol_domain_denied",
                    "reason": "protocol_domain_denied",
                    "detail": "命中协议 deny_domains，已跳过",
                }
            )
            continue
        if host and host in blocked_hosts:
            rejection = {
                "url": page_url,
                "host": host,
                "reason": "same_host_blocked",
                "detail": "同 host 已判定不可用，跳过重复预览",
            }
            candidate_rejections.append(rejection)
            attempts.append(
                {
                    "stage": "preview",
                    "url": page_url,
                    "host": host,
                    "status": "skipped",
                    "error_code": "same_host_blocked",
                    "reason": "same_host_blocked",
                    "detail": "同 host 已判定不可用，跳过重复预览",
                }
            )
            continue

        _emit_web_fetch_event(
            context,
            "search_progress",
            stage="candidate_preview",
            url=page_url,
            host=host,
            query=str(candidate.get("query") or ""),
        )
        output_text, exec_error = _execute_tool_with_exec_spec(exec_spec, page_url)
        current_output = str(output_text or "")
        if current_output.strip():
            last_output = current_output
        classified = _classify_web_fetch_result(current_output, exec_error)
        if str(classified.get("ok")) != "1":
            current_code = str(classified.get("error_code") or "candidate_preview_empty")
            current_reason = str(classified.get("reason") or "candidate_preview_failed")
            current_detail = str(classified.get("detail") or "")
            attempts.append(
                {
                    "stage": "preview",
                    "url": page_url,
                    "host": host,
                    "status": "failed",
                    "error_code": current_code,
                    "reason": current_reason,
                    "detail": current_detail,
                }
            )
            candidate_rejections.append(
                {
                    "url": page_url,
                    "host": host,
                    "reason": current_reason,
                    "detail": current_detail,
                }
            )
            final_error_code = current_code
            final_reason = current_reason or final_reason
            final_detail = current_detail or final_detail
            if host and _should_block_host_after_web_fetch_error(current_code, current_reason):
                blocked_hosts.add(host)
            continue

        preview_outputs[page_url] = current_output
        analysis = _analyze_web_fetch_candidate_content(
            url=page_url,
            context_text=str(candidate.get("context_text") or candidate.get("query") or ""),
            output_text=current_output,
            protocol=protocol,
            query_keywords=_build_web_fetch_query_keywords(str(candidate.get("query") or "")),
        )
        ranking = {
            "url": page_url,
            "host": host,
            "query": str(candidate.get("query") or ""),
            "context_preview": truncate_inline_text(str(candidate.get("context_text") or ""), 180),
            "search_url": str(candidate.get("search_url") or ""),
            "initial_score": int(candidate.get("initial_score") or 0),
            "keyword_hits": int(candidate.get("keyword_hits") or 0),
            "signals": dict(candidate.get("signals") or {}),
            "llm_selected": bool(candidate.get("llm_selected")),
            "llm_rank": int(candidate.get("llm_rank") or 0),
            "score": int(analysis.get("score") or 0),
            "evidence": list(analysis.get("evidence") or []),
            "rejections": list(analysis.get("rejections") or []),
            "preview": str(analysis.get("preview") or ""),
            "acceptable": bool(analysis.get("acceptable")),
        }
        candidate_rankings.append(ranking)
        attempts.append(
            {
                "stage": "preview",
                "url": page_url,
                "host": host,
                "status": "ok" if bool(analysis.get("acceptable")) else "weak",
                "error_code": "" if bool(analysis.get("acceptable")) else "candidate_missing_required_fields",
                "reason": "selected_candidate" if bool(analysis.get("acceptable")) else "low_relevance",
                "detail": "; ".join(list(analysis.get("evidence") or [])[:4]) or "; ".join(list(analysis.get("rejections") or [])[:2]),
            }
        )
        if bool(analysis.get("acceptable")):
            accepted = dict(ranking)
            accepted["output_text"] = current_output
            accepted_candidates.append(accepted)
        else:
            candidate_rejections.append(
                {
                    "url": page_url,
                    "host": host,
                    "reason": "low_relevance",
                    "detail": "; ".join(list(analysis.get("rejections") or [])[:3]) or "候选页弱相关",
                    "preview": str(analysis.get("preview") or ""),
                    "score": int(analysis.get("score") or 0),
                }
            )

    candidate_rankings.sort(key=lambda item: (-int(item.get("score") or 0), -int(item.get("initial_score") or 0)))
    if candidate_rejections:
        _emit_web_fetch_event(
            context,
            "search_rejected",
            total_rejected=len(candidate_rejections),
            rejected=candidate_rejections[:_WEB_FETCH_EVENT_CANDIDATE_LIMIT],
        )

    llm_decision = _decide_web_fetch_preview_candidates_with_llm(
        tool_input=str(tool_input or ""),
        protocol=protocol,
        queries=queries,
        candidate_rankings=candidate_rankings,
        candidate_rejections=candidate_rejections,
        context=context,
    )
    ranking_by_url = {
        str(item.get("url") or "").strip(): dict(item)
        for item in candidate_rankings
        if str(item.get("url") or "").strip()
    }
    decision = str(llm_decision.get("decision") or "").strip().lower()
    decision_notes = [str(item) for item in (llm_decision.get("notes") or []) if str(item or "").strip()]
    retry_queries = [str(item) for item in (llm_decision.get("retry_queries") or []) if str(item or "").strip()]

    if decision == "accept":
        selected = ranking_by_url.get(str(llm_decision.get("selected_url") or "").strip())
        if isinstance(selected, dict):
            selected_event = {
                "url": str(selected.get("url") or ""),
                "host": str(selected.get("host") or ""),
                "query": str(selected.get("query") or ""),
                "score": int(selected.get("score") or 0),
                "evidence": list(selected.get("evidence") or []),
                "preview": str(selected.get("preview") or ""),
                "decision_source": "llm_preview_decider",
            }
            _emit_web_fetch_event(context, "search_selected", selected=selected_event)
            if decision_notes:
                warnings.append("候选决策说明：" + " | ".join(decision_notes[:3]))
            warnings.append(
                "web_fetch 已通过关键词检索命中页面："
                f"{truncate_inline_text(str(selected.get('query') or ''), 80)} -> "
                f"{truncate_inline_text(str(selected.get('url') or ''), 120)}"
            )
            return {
                "ok": True,
                "output_text": str(preview_outputs.get(str(selected.get("url") or ""), "") or ""),
                "warnings": warnings,
                "attempts": attempts,
                "search_attempts": attempts,
                "selected_candidate": selected_event,
                "candidate_rankings": candidate_rankings,
                "candidate_rejections": candidate_rejections,
                "evidence_summary": "; ".join(list(selected.get("evidence") or [])[:5]),
                "error_code": "",
                "error_message": "",
            }

    current_query_set = {str(item).strip().lower() for item in queries if str(item).strip()}
    new_retry_queries = [item for item in retry_queries if str(item).strip().lower() not in current_query_set]
    if decision == "retry" and new_retry_queries and retry_depth < 1:
        retry_protocol = dict(protocol or {})
        retry_protocol["search_queries"] = _merge_web_fetch_search_queries(new_retry_queries, queries, limit=8)
        retry_context = dict(context or {})
        retry_context["_web_fetch_retry_depth"] = int(retry_depth) + 1
        _emit_web_fetch_event(
            context,
            "search_progress",
            stage="query_rewrite_done",
            retry_queries=new_retry_queries[:4],
            notes=decision_notes[:4],
        )
        retry_result = _execute_web_fetch_keyword_search(
            exec_spec,
            tool_input,
            protocol=retry_protocol,
            context=retry_context,
        )
        merged_attempts = attempts + [
            dict(item) for item in (retry_result.get("attempts") or []) if isinstance(item, dict)
        ]
        retry_warnings = [str(item) for item in (retry_result.get("warnings") or []) if str(item or "").strip()]
        warnings.append("web_fetch 已根据候选失败反馈自动改写查询并重试。")
        warnings.extend(retry_warnings)
        retry_result = dict(retry_result)
        retry_result["attempts"] = merged_attempts
        retry_result["search_attempts"] = merged_attempts
        retry_result["warnings"] = warnings
        return retry_result

    if accepted_candidates:
        accepted_candidates.sort(
            key=lambda item: (-int(item.get("score") or 0), -int(item.get("initial_score") or 0))
        )
        selected = accepted_candidates[0]
        selected_event = {
            "url": str(selected.get("url") or ""),
            "host": str(selected.get("host") or ""),
            "query": str(selected.get("query") or ""),
            "score": int(selected.get("score") or 0),
            "evidence": list(selected.get("evidence") or []),
            "preview": str(selected.get("preview") or ""),
        }
        _emit_web_fetch_event(context, "search_selected", selected=selected_event)
        if decision_notes:
            warnings.append("候选决策说明：" + " | ".join(decision_notes[:3]))
        warnings.append(
            "web_fetch 已通过关键词检索命中页面："
            f"{truncate_inline_text(str(selected.get('query') or ''), 80)} -> "
            f"{truncate_inline_text(str(selected.get('url') or ''), 120)}"
        )
        return {
            "ok": True,
            "output_text": str(selected.get("output_text") or ""),
            "warnings": warnings,
            "attempts": attempts,
            "search_attempts": attempts,
            "selected_candidate": selected_event,
            "candidate_rankings": candidate_rankings,
            "candidate_rejections": candidate_rejections,
            "evidence_summary": "; ".join(list(selected.get("evidence") or [])[:5]),
            "error_code": "",
            "error_message": "",
        }

    summary = _build_web_fetch_attempt_summary(attempts)
    final_message = (
        f"web_fetch 候选页弱相关，未发现满足字段/时间/单位信号的来源：{truncate_inline_text(' | '.join(queries), 180)}"
    )
    if summary:
        final_message += f"；尝试摘要：{summary}"
    if final_detail:
        final_message += f"；最后错误：{truncate_inline_text(final_detail, 220)}"
    _emit_web_fetch_event(
        context,
        "search_progress",
        stage="search_failed",
        reason="low_relevance_candidates",
        error_code="low_relevance_candidates",
        message=final_message,
    )
    return {
        "ok": False,
        "output_text": last_output,
        "warnings": warnings,
        "attempts": attempts,
        "search_attempts": attempts,
        "candidate_rankings": candidate_rankings,
        "candidate_rejections": candidate_rejections,
        "evidence_summary": "; ".join(
            [str(item.get("preview") or "") for item in candidate_rankings[:2] if str(item.get("preview") or "").strip()]
        ),
        "error_code": "low_relevance_candidates",
        "error_message": final_message,
    }


def _classify_web_fetch_exec_error(error_text: str) -> Tuple[str, str]:
    lowered = str(error_text or "").strip().lower()
    if not lowered:
        return "web_fetch_blocked", "exec_error"
    if "429" in lowered or "too many requests" in lowered or "rate limit" in lowered or "daily hits limit" in lowered:
        return "rate_limited", "too_many_requests"
    if "403" in lowered or "access denied" in lowered or "forbidden" in lowered:
        return "web_fetch_blocked", "access_denied"
    if "503" in lowered or "service unavailable" in lowered:
        return "service_unavailable", "service_unavailable"
    if "missing_access_key" in lowered or "access key" in lowered or "api key" in lowered:
        return "missing_api_key", "missing_access_key"
    if "timeout" in lowered or "timed out" in lowered:
        return "timeout", "timeout"
    return "web_fetch_blocked", "exec_error"


def _classify_web_fetch_result(output_text: str, exec_error: Optional[str]) -> Dict[str, str]:
    if exec_error:
        code, reason = _classify_web_fetch_exec_error(exec_error)
        return {
            "ok": "0",
            "error_code": code,
            "reason": reason,
            "detail": truncate_inline_text(exec_error, 240),
        }

    block_reason = _detect_web_fetch_block_reason(output_text)
    if block_reason:
        code = "rate_limited" if block_reason == "too_many_requests" else "web_fetch_blocked"
        return {
            "ok": "0",
            "error_code": code,
            "reason": block_reason,
            "detail": truncate_inline_text(output_text, 240),
        }

    semantic_error = _detect_web_fetch_semantic_error(output_text)
    if semantic_error:
        semantic_lower = str(semantic_error).lower()
        code = "missing_api_key" if ("missing_access_key" in semantic_lower or "access key" in semantic_lower) else "web_fetch_blocked"
        reason = "missing_access_key" if code == "missing_api_key" else "semantic_error"
        return {
            "ok": "0",
            "error_code": code,
            "reason": reason,
            "detail": truncate_inline_text(semantic_error, 240),
        }

    return {"ok": "1", "error_code": "", "reason": "", "detail": ""}


def _should_block_host_after_web_fetch_error(error_code: str, reason: str) -> bool:
    normalized_code = str(error_code or "").strip().lower()
    normalized_reason = str(reason or "").strip().lower()
    if normalized_code in {"missing_api_key", "rate_limited", "timeout", "service_unavailable"}:
        return True
    if normalized_code != "web_fetch_blocked":
        return False
    host_block_reasons = {
        "access_denied",
        "request_blocked",
        "cloudflare",
        "captcha",
        "verify_human",
        "enable_javascript",
        "missing_access_key",
        "exec_error",
    }
    return normalized_reason in host_block_reasons


def _build_web_fetch_attempt_summary(attempts: List[dict]) -> str:
    chunks: List[str] = []
    for item in attempts[-6:]:
        status = str(item.get("status") or "").strip()
        host = str(item.get("host") or "").strip() or "unknown-host"
        code = str(item.get("error_code") or "").strip()
        if code:
            chunks.append(f"{host}:{status}:{code}")
        else:
            chunks.append(f"{host}:{status}")
    return " | ".join(chunks)


def _execute_web_fetch_with_fallback(
    exec_spec: dict,
    tool_input: str,
    *,
    protocol: Optional[dict] = None,
    context: Optional[dict] = None,
) -> dict:
    """
    web_fetch 自动换源执行：
    - 原始 URL 失败后尝试有限候选源；
    - 对同 host 的硬失败（403/missing key/限流）进行去重，避免无意义重试。
    """
    primary = _normalize_web_fetch_url_candidate(tool_input)
    if not primary:
        return _execute_web_fetch_keyword_search(exec_spec, str(tool_input), protocol=protocol, context=context)

    candidates = [primary] + _build_web_fetch_fallback_urls(primary)
    attempts: List[dict] = []
    warnings: List[str] = []
    blocked_hosts: Set[str] = set()
    denied_domains = _get_protocol_deny_domains(protocol)
    last_output = ""
    final_error_code = "web_fetch_blocked"
    final_reason = "unknown"
    final_detail = ""

    for index, candidate_url in enumerate(candidates):
        host = _extract_web_fetch_host(candidate_url)
        if host and _is_host_denied_by_protocol(host, denied_domains):
            attempts.append(
                {
                    "url": candidate_url,
                    "host": host,
                    "status": "skipped",
                    "error_code": "protocol_domain_denied",
                    "reason": "protocol_domain_denied",
                    "detail": "命中协议 deny_domains，已跳过",
                }
            )
            continue
        if index > 0 and host and host in blocked_hosts:
            attempts.append(
                {
                    "url": candidate_url,
                    "host": host,
                    "status": "skipped",
                    "error_code": "same_host_blocked",
                    "reason": "same_host_blocked",
                    "detail": "同 host 已判定不可用，跳过重复重试",
                }
            )
            continue

        output_text, exec_error = _execute_tool_with_exec_spec(exec_spec, candidate_url)
        current_output = str(output_text or "")
        if current_output.strip():
            last_output = current_output
        classified = _classify_web_fetch_result(current_output, exec_error)

        if str(classified.get("ok")) == "1":
            attempts.append(
                {
                    "url": candidate_url,
                    "host": host,
                    "status": "ok",
                    "error_code": "",
                    "reason": "",
                    "detail": "",
                }
            )
            if index > 0:
                warnings.append(
                    "web_fetch 已自动切换到备用源："
                    f"{truncate_inline_text(primary, 120)} -> "
                    f"{truncate_inline_text(candidate_url, 120)}"
                )
            return {
                "ok": True,
                "output_text": current_output,
                "warnings": warnings,
                "attempts": attempts,
                "error_code": "",
                "error_message": "",
            }

        current_code = str(classified.get("error_code") or "web_fetch_blocked")
        current_reason = str(classified.get("reason") or "")
        current_detail = str(classified.get("detail") or "")
        attempts.append(
            {
                "url": candidate_url,
                "host": host,
                "status": "failed",
                "error_code": current_code,
                "reason": current_reason,
                "detail": current_detail,
            }
        )
        final_error_code = current_code
        final_reason = current_reason or final_reason
        final_detail = current_detail or final_detail
        if host and _should_block_host_after_web_fetch_error(current_code, current_reason):
            blocked_hosts.add(host)

    # URL 直连与备用源都失败时，按协议回退到关键词检索，避免对单一源反复试错。
    keyword_retry = _execute_web_fetch_keyword_search(exec_spec, str(tool_input), protocol=protocol, context=context)
    if bool(keyword_retry.get("ok")):
        retry_attempts = [dict(item) for item in (keyword_retry.get("attempts") or []) if isinstance(item, dict)]
        merged_attempts = attempts + retry_attempts
        retry_warnings = [str(item) for item in (keyword_retry.get("warnings") or []) if str(item or "").strip()]
        warnings.append("web_fetch URL直连失败，已按协议回退到关键词检索。")
        warnings.extend(retry_warnings)
        output_text = str(keyword_retry.get("output_text") or "")
        if output_text.strip():
            last_output = output_text
        return {
            "ok": True,
            "output_text": last_output,
            "warnings": warnings,
            "attempts": merged_attempts,
            "error_code": "",
            "error_message": "",
        }

    retry_attempts = [dict(item) for item in (keyword_retry.get("attempts") or []) if isinstance(item, dict)]
    if retry_attempts:
        attempts.extend(retry_attempts)
    retry_output = str(keyword_retry.get("output_text") or "")
    if retry_output.strip():
        last_output = retry_output
    retry_warnings = [str(item) for item in (keyword_retry.get("warnings") or []) if str(item or "").strip()]
    warnings.extend(retry_warnings)
    retry_code = str(keyword_retry.get("error_code") or "").strip().lower()
    retry_message = str(keyword_retry.get("error_message") or "").strip()
    if retry_code:
        final_error_code = retry_code
    if retry_message:
        final_detail = retry_message

    summary = _build_web_fetch_attempt_summary(attempts)
    final_message = (
        f"web_fetch 全部候选源失败（{final_reason}）：{truncate_inline_text(primary, 180)}"
    )
    if summary:
        final_message += f"；尝试摘要：{summary}"
    if final_detail:
        final_message += f"；最后错误：{truncate_inline_text(final_detail, 220)}"

    return {
        "ok": False,
        "output_text": last_output,
        "warnings": warnings,
        "attempts": attempts,
        "error_code": final_error_code or "web_fetch_blocked",
        "error_message": final_message,
    }


def _detect_structured_tool_error(
    *,
    tool_name: str,
    output_text: str,
) -> Optional[Tuple[str, str]]:
    """
    通用结构化失败识别：
    - 工具返回 JSON 且显式声明失败时，不能继续当作“成功输出”。
    - 重点兜底：status=failed / success=false / ok=false。
    - 对 status=partial：若存在 errors 且没有 applied，视为失败（避免“部分失败”被误报完成）。
    """
    payload = parse_json_dict(output_text)
    if not isinstance(payload, dict):
        return None

    status_text = str(payload.get("status") or "").strip().lower()
    success_value = payload.get("success")
    ok_value = payload.get("ok")
    errors = payload.get("errors")
    applied = payload.get("applied")
    error_code = str(payload.get("error_code") or "").strip().lower()
    summary = truncate_inline_text(payload.get("summary") or "", 220)

    has_errors = isinstance(errors, list) and any(isinstance(item, dict) or str(item or "").strip() for item in errors)
    has_applied = isinstance(applied, list) and any(bool(item) for item in applied)

    explicit_failed = (
        (success_value is False)
        or (ok_value is False)
        or (status_text in {"error", "failed", "fail"})
    )
    if explicit_failed:
        code = error_code or "tool_semantic_failed"
        message = f"{tool_name or 'tool'} 返回失败状态"
        if status_text:
            message += f"（status={status_text}）"
        if summary:
            message += f"：{summary}"
        return code, message

    if status_text == "partial" and has_errors and not has_applied:
        code = error_code or "tool_partial_failed"
        message = f"{tool_name or 'tool'} 返回 partial 且包含错误"
        if summary:
            message += f"：{summary}"
        return code, message

    return None


def _normalize_exec_spec(exec_spec: dict) -> dict:
    """
    tool_metadata.exec 兼容归一化：
    - 常见别名：shell -> command，timeout -> timeout_ms
    - args 可能被模型输出成字符串：转为 command
    - command 可能被模型输出成 list：转为 args

    约定：最终使用字段
    - type: "shell"（可省略，若提供了 command/args 会自动按 shell 执行）
    - command: str 或 args: list[str]
    - timeout_ms: int（可选）
    - workdir: str（建议必填）
    """
    if not isinstance(exec_spec, dict):
        return {}
    spec = dict(exec_spec)

    # 常见别名：shell -> command
    shell_value = spec.get("shell")
    if (
        isinstance(shell_value, str)
        and shell_value.strip()
        and not spec.get("command")
    ):
        spec["command"] = shell_value.strip()

    # args: str -> command
    args_value = spec.get("args")
    if isinstance(args_value, str) and args_value.strip():
        if not spec.get("command"):
            spec["command"] = args_value.strip()
        spec.pop("args", None)

    # command: list -> args
    cmd_value = spec.get("command")
    if isinstance(cmd_value, list) and cmd_value:
        existing_args = spec.get("args")
        if isinstance(existing_args, list) and existing_args:
            # 兼容：模型可能同时输出 command(list) + args(list)。这里把两者拼接成“完整命令 token 列表”，
            # 避免丢失 command(list) 导致仅执行 args（常见错误：args=["GC=F"] -> [WinError 2]）。
            spec["args"] = [str(v) for v in cmd_value] + [str(v) for v in existing_args]
        else:
            spec["args"] = [str(v) for v in cmd_value]
        spec.pop("command", None)

    # timeout: 兼容字段；若 < 1000 认为是秒，否则认为是毫秒
    if spec.get("timeout_ms") is None and spec.get("timeout") is not None:
        value = spec.get("timeout")
        try:
            num = float(value)
            spec["timeout_ms"] = int(num * 1000) if 0 < num < 1000 else int(num)
        except Exception:
            pass

    return spec


def _load_tool_metadata_from_db(tool_id: object, tool_name: Optional[str]) -> Optional[dict]:
    """
    读取 tools_items.metadata（JSON）并解析为 dict。
    """
    tool_id_value = parse_positive_int(tool_id, default=None)
    if tool_id_value is None and not tool_name:
        return None
    if tool_id_value is not None:
        return get_tool_metadata_by_id(tool_id=int(tool_id_value))
    return get_tool_metadata_by_name(name=str(tool_name or ""))


def _coerce_optional_tool_int_fields(payload: dict) -> None:
    """
    归一化 tool_call 可选整数字段：
    - 空字符串 -> None
    - 可解析整数 -> int
    - 非法值 -> 抛出结构化错误（供上层策略识别为不可重试契约问题）
    """
    if not isinstance(payload, dict):
        return
    for key in ("tool_id", "task_id", "run_id", "skill_id"):
        if key not in payload:
            continue
        raw = payload.get(key)
        if raw is None:
            continue
        if isinstance(raw, str) and not raw.strip():
            payload[key] = None
            continue
        parsed = parse_positive_int(raw, default=None)
        if parsed is None:
            raise ValueError(
                format_task_error(
                    code="invalid_action_payload",
                    message=f"tool_call.{key} 必须为正整数或空",
                )
            )
        payload[key] = int(parsed)


def _coerce_optional_tool_text_fields(payload: dict) -> None:
    """
    归一化 tool_call 可选文本字段：
    - 空字符串 -> None，避免下游把空值判定为非法状态。
    - reuse_status 统一小写。
    """
    if not isinstance(payload, dict):
        return
    for key in ("tool_version", "tool_description", "reuse_status", "reuse_notes"):
        if key not in payload:
            continue
        raw = payload.get(key)
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            payload[key] = None
            continue
        if key == "reuse_status":
            payload[key] = text.lower()
        else:
            payload[key] = text


def _has_nonempty_exec_spec(spec: dict) -> bool:
    """判断 exec_spec 是否包含有效内容（兼容模型输出空 exec {}）。"""
    return bool(
        str(spec.get("type") or "").strip()
        or (isinstance(spec.get("args"), list) and spec.get("args"))
        or str(spec.get("command") or "").strip()
    )


def _resolve_tool_exec_spec(payload: dict) -> Optional[dict]:
    """
    优先从 payload.tool_metadata 读取 exec，其次从 tools_items.metadata 读取 exec。
    """
    meta = payload.get("tool_metadata")
    if isinstance(meta, dict):
        exec_spec = meta.get("exec")
        if isinstance(exec_spec, dict):
            exec_spec = _normalize_exec_spec(exec_spec)
            if _has_nonempty_exec_spec(exec_spec):
                return exec_spec
    meta = _load_tool_metadata_from_db(payload.get("tool_id"), payload.get("tool_name"))
    if isinstance(meta, dict):
        exec_spec = meta.get("exec")
        if isinstance(exec_spec, dict):
            normalized = _normalize_exec_spec(exec_spec)
            if _has_nonempty_exec_spec(normalized):
                return normalized

    # 再兜底：从实验目录脚本推断执行定义（防止“已写脚本但漏填 exec”中断）。
    inferred = _infer_exec_spec_from_workspace_script(payload)
    if isinstance(inferred, dict):
        return _normalize_exec_spec(inferred)
    return None


def _infer_exec_spec_from_workspace_script(payload: dict) -> Optional[dict]:
    """
    兜底：当 tool_metadata.exec 缺失时，尝试从实验目录推断脚本执行命令。

    适用场景：模型先写了 `backend/.agent/workspace/<tool_name>.py`，
    但下一步 tool_call 漏填了 tool_metadata.exec。
    """
    tool_name = str(payload.get("tool_name") or "").strip()
    if not tool_name:
        return None

    workdir = ""
    meta = payload.get("tool_metadata")
    if isinstance(meta, dict):
        exec_meta = meta.get("exec")
        if isinstance(exec_meta, dict):
            workdir = str(exec_meta.get("workdir") or "").strip()
    if not workdir:
        workdir = os.getcwd()

    workspace_dir = os.path.join(workdir, str(AGENT_EXPERIMENT_DIR_REL).replace("/", os.sep))
    candidates = [
        (os.path.join(workspace_dir, f"{tool_name}.py"), "python"),
        (os.path.join(workspace_dir, f"{tool_name}.sh"), "sh"),
        (os.path.join(workspace_dir, f"{tool_name}.ps1"), "powershell"),
        (os.path.join(workspace_dir, f"{tool_name}.bat"), None),
        (os.path.join(workspace_dir, f"{tool_name}.cmd"), None),
    ]

    for script_path, launcher in candidates:
        if not os.path.exists(script_path):
            continue
        rel_script = os.path.relpath(script_path, workdir)
        rel_script = rel_script.replace("\\", "/")
        command = f"{launcher} {rel_script}" if launcher else rel_script
        return {
            "type": "shell",
            "command": command,
            "workdir": workdir,
            "timeout_ms": AGENT_SHELL_COMMAND_DEFAULT_TIMEOUT_MS,
        }

    return None


def _looks_like_executable_token(token: str) -> bool:
    head = str(token or "").strip()
    if not head:
        return False
    lowered = head.lower()
    if lowered.startswith("-"):
        return False
    ext = os.path.splitext(lowered)[1]
    if ext in {".py", ".js", ".ts", ".sh", ".ps1", ".bat", ".cmd", ".txt", ".md", ".csv", ".json"}:
        return False
    if "/" in head or "\\" in head:
        return True
    if lowered.endswith(".exe"):
        return True
    return lowered in {
        "python",
        "python3",
        "py",
        "pip",
        "pip3",
        "curl",
        "wget",
        "node",
        "npm",
        "npx",
        "git",
        "cmd",
        "cmd.exe",
        "powershell",
        "pwsh",
        "uv",
        "uvicorn",
    }


def _extract_script_candidates_from_tokens(tokens: List[str]) -> List[str]:
    if not tokens:
        return []

    first = str(tokens[0] or "").strip()
    if not first:
        return []

    first_name = os.path.splitext(os.path.basename(first))[0].lower()
    if first_name in {"python", "python3", "py"}:
        if len(tokens) >= 2 and str(tokens[1] or "").strip() in {"-c", "-m"}:
            return []
        for token in tokens[1:]:
            current = str(token or "").strip()
            if not current or current.startswith("-"):
                continue
            if current.lower().endswith((".py", ".sh", ".ps1", ".bat", ".cmd")):
                return [current]
            return []
        return []

    if first.lower().endswith((".py", ".sh", ".ps1", ".bat", ".cmd")):
        return [first]
    return []


def _extract_script_candidates_from_exec_spec(exec_spec: dict, tool_input: str) -> List[str]:
    if not isinstance(exec_spec, dict):
        return []

    args = exec_spec.get("args")
    command = exec_spec.get("command")
    tokens: List[str] = []

    if isinstance(args, list) and args:
        formatted_args = [str(item).replace("{input}", tool_input) for item in args]
        if isinstance(command, str) and command.strip():
            command_tokens = parse_command_tokens(str(command).replace("{input}", tool_input))
            if command_tokens:
                if _looks_like_executable_token(formatted_args[0]):
                    tokens = formatted_args
                else:
                    tokens = command_tokens + formatted_args
            else:
                tokens = formatted_args
        else:
            tokens = formatted_args
    elif isinstance(command, str) and command.strip():
        tokens = parse_command_tokens(str(command).replace("{input}", tool_input))
    elif isinstance(command, list) and command:
        tokens = [str(item).replace("{input}", tool_input) for item in command if str(item).strip()]

    return _extract_script_candidates_from_tokens(tokens)


def _collect_written_script_paths_for_run(
    *,
    task_id: int,
    run_id: int,
    current_step_id: Optional[int],
    workdir: str,
) -> Set[str]:
    paths: Set[str] = set()
    try:
        rows = list_task_steps_for_run(task_id=int(task_id), run_id=int(run_id))
    except Exception:
        return paths

    for row in rows or []:
        if not row:
            continue
        try:
            row_id = int(row["id"]) if row["id"] is not None else None
        except Exception:
            row_id = None
        if current_step_id is not None and row_id == int(current_step_id):
            continue

        status = str(row["status"] or "").strip().lower() if "status" in row.keys() else ""
        if status != "done":
            continue

        detail_obj = load_json_object(row["detail"] if "detail" in row.keys() else None)
        action_type = str(detail_obj.get("type") or "").strip().lower() if isinstance(detail_obj, dict) else ""
        if action_type not in {"file_write", "file_append"}:
            continue

        payload_obj = detail_obj.get("payload") if isinstance(detail_obj, dict) else None
        result_obj = load_json_object(row["result"] if "result" in row.keys() else None)

        raw_path = ""
        if isinstance(result_obj, dict):
            raw_path = str(result_obj.get("path") or "").strip()
        if not raw_path and isinstance(payload_obj, dict):
            raw_path = str(payload_obj.get("path") or "").strip()

        resolved = resolve_path_with_workdir(raw_path, workdir)
        if resolved:
            paths.add(os.path.normcase(resolved))

    return paths


def _enforce_tool_exec_script_dependency(
    *,
    task_id: int,
    run_id: int,
    step_row,
    exec_spec: dict,
    tool_input: str,
) -> Optional[str]:
    """
    强约束：tool_call.exec 若引用脚本文件，必须满足“脚本存在 + 当前 run 已有 file_write/file_append 成功步骤”。

    目的：避免出现“先执行工具脚本，后补写脚本”导致的不可重复失败。
    """
    if not SHELL_COMMAND_REQUIRE_FILE_WRITE_BINDING_DEFAULT:
        return None
    if not isinstance(exec_spec, dict):
        return None
    require_binding_value = exec_spec.get("require_file_write_binding")
    if require_binding_value is not None:
        if isinstance(require_binding_value, bool) and not require_binding_value:
            return None
        lowered = str(require_binding_value).strip().lower()
        if lowered in {"0", "false", "no", "off"}:
            return None

    workdir = normalize_windows_abs_path_on_posix(str(exec_spec.get("workdir") or "").strip())
    if not workdir:
        workdir = os.getcwd()

    script_candidates = _extract_script_candidates_from_exec_spec(exec_spec, tool_input)
    if not script_candidates:
        return None

    current_step_id = None
    try:
        if hasattr(step_row, "keys") and "id" in step_row.keys() and step_row["id"] is not None:
            current_step_id = int(step_row["id"])
    except Exception:
        current_step_id = None

    written_paths = _collect_written_script_paths_for_run(
        task_id=int(task_id),
        run_id=int(run_id),
        current_step_id=current_step_id,
        workdir=workdir,
    )

    missing_paths: List[str] = []
    unbound_paths: List[str] = []
    for candidate in script_candidates:
        absolute_path = resolve_path_with_workdir(candidate, workdir)
        if not absolute_path:
            continue
        if not os.path.exists(absolute_path):
            missing_paths.append(absolute_path)
            continue
        normalized = os.path.normcase(absolute_path)
        if normalized not in written_paths:
            unbound_paths.append(absolute_path)

    if missing_paths:
        return (
            f"工具执行失败: 脚本不存在: {', '.join(missing_paths)}"
            "（请先执行 file_write/file_append 并确认落盘）"
        )
    if unbound_paths:
        return (
            f"工具执行失败: 脚本依赖未绑定: {', '.join(unbound_paths)}"
            "（当前 run 未发现对应的 file_write/file_append 成功步骤）"
        )
    return None


def _execute_tool_with_exec_spec(exec_spec: dict, tool_input: str) -> Tuple[Optional[str], Optional[str]]:
    """
    执行工具（目前仅支持 shell）。
    返回：(output_text, error_message)
    """
    exec_spec = _normalize_exec_spec(exec_spec)

    args = exec_spec.get("args")
    command = exec_spec.get("command")
    timeout_ms = exec_spec.get("timeout_ms")
    workdir = exec_spec.get("workdir") or os.getcwd()

    exec_type = (exec_spec.get("type") or "").strip().lower()
    # 兼容：模型可能漏填 type，但提供了 args/command。此时默认按 shell 执行，避免直接失败。
    if not exec_type:
        has_cmd = bool((isinstance(args, list) and args) or (isinstance(command, str) and command.strip()))
        if has_cmd:
            exec_type = "shell"
        else:
            return None, "工具未配置 exec.type（仅支持 shell），且缺少 command/args"

    if exec_type != "shell":
        # 兼容：部分模型会输出 type="empty"/"cmd" 等无效值，但同时给了 command/args。
        # 若存在可执行命令，则按 shell 兜底继续执行，避免“自举工具”链路被无意义阻断。
        has_cmd = bool((isinstance(args, list) and args) or (isinstance(command, str) and command.strip()))
        if has_cmd and exec_type in {"empty", "cmd", "command"}:
            exec_type = "shell"
        else:
            return None, f"不支持的工具执行类型: {exec_type}"

    def _split_command_text(text: str) -> list[str]:
        tokens = shlex.split(text, posix=os.name != "nt")
        if os.name == "nt":
            # 参见 services/execution/shell_command.py：Windows 下要剥离最外层引号，
            # 否则 python -c 会把代码当作字符串字面量导致无输出。
            cleaned: list[str] = []
            for item in tokens:
                s = str(item)
                if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
                    s = s[1:-1]
                cleaned.append(s)
            tokens = cleaned
        return [str(t).replace("{input}", tool_input) for t in tokens]

    def _looks_like_executable_token(token: str) -> bool:
        head = str(token or "").strip()
        if not head:
            return False
        low = head.lower()
        if low.startswith("-"):
            return False
        # 常见脚本/数据文件不是可执行文件（Windows 尤其如此）：
        # - 若把 *.py 当作命令执行，会触发 [WinError 2]（找不到可执行文件）；
        # - 正确做法是由 exec.command/args 提供 python，并把脚本路径作为参数追加。
        ext = os.path.splitext(low)[1]
        if ext in {".py", ".js", ".ts", ".sh", ".ps1", ".txt", ".md", ".csv", ".json"}:
            return False
        if "/" in head or "\\" in head:
            return True
        if low.endswith(".exe") or low.endswith(".bat") or low.endswith(".cmd"):
            return True
        return low in {
            "python",
            "python3",
            "py",
            "pip",
            "pip3",
            "curl",
            "wget",
            "node",
            "npm",
            "npx",
            "git",
            "cmd.exe",
            "powershell",
            "pwsh",
            "uv",
            "uvicorn",
        }

    cmd_value = None
    uses_input_placeholder = False
    if isinstance(args, list) and args:
        formatted_args = []
        for item in args:
            text = str(item)
            if "{input}" in text:
                uses_input_placeholder = True
            formatted_args.append(text.replace("{input}", tool_input))
        if isinstance(command, str) and command.strip():
            # 兼容：模型常把 exec.command 当作“主命令”，把 exec.args 当作“附加参数”。
            # 若 args 看起来不像可执行文件（例如 ["GC=F","3mo","1d"]），则把它们追加到 command 的 token 后面。
            if _looks_like_executable_token(formatted_args[0]):
                cmd_value = formatted_args
            else:
                cmd_value = _split_command_text(command) + formatted_args
        else:
            cmd_value = formatted_args
    elif isinstance(command, str) and command.strip():
        if "{input}" in command:
            uses_input_placeholder = True
        cmd_value = command.replace("{input}", tool_input)
    else:
        return None, "工具未配置 command/args"

    retry_cfg = exec_spec.get("retry")
    max_attempts = 1
    backoff_ms = 0
    if isinstance(retry_cfg, dict):
        try:
            max_attempts = int(retry_cfg.get("max_attempts") or retry_cfg.get("attempts") or 1)
        except Exception:
            max_attempts = 1
        try:
            backoff_ms = int(retry_cfg.get("backoff_ms") or retry_cfg.get("delay_ms") or 0)
        except Exception:
            backoff_ms = 0

    # 防止无限重试卡死：即使配置过大也做一个上限保护
    if max_attempts <= 0:
        max_attempts = 1
    if max_attempts > 6:
        max_attempts = 6
    if backoff_ms < 0:
        backoff_ms = 0

    last_error = None
    last_result = None

    for attempt in range(0, max_attempts):
        result, error_message = run_shell_command(
            {
                "command": cmd_value,
                "workdir": workdir,
                "timeout_ms": timeout_ms,
                "stdin": tool_input if not uses_input_placeholder else "",
            }
        )
        if error_message:
            last_error = error_message
            last_result = None
        elif not isinstance(result, dict):
            last_error = "工具执行返回格式异常"
            last_result = None
        else:
            last_error = None
            last_result = dict(result)
            if bool(last_result.get("ok")):
                stdout = str(last_result.get("stdout") or "")
                stderr = str(last_result.get("stderr") or "")
                output_text = stdout.strip() or stderr.strip()
                return output_text or "", None

            stdout = str(last_result.get("stdout") or "")
            stderr = str(last_result.get("stderr") or "")
            rc = last_result.get("returncode")
            detail = stderr.strip() or stdout.strip() or (str(rc) if rc is not None else "")
            last_error = f"工具执行失败: {detail}".strip()

        # 最后一次失败：直接返回
        if attempt >= max_attempts - 1:
            break

        # 有 retry 配置才进入重试分支
        if max_attempts <= 1:
            break

        # 简单退避（可配置），避免瞬时抖动导致的连续失败
        if backoff_ms > 0:
            try:
                time.sleep(float(backoff_ms) / 1000.0)
            except Exception:
                pass

    if last_error:
        return None, last_error
    if isinstance(last_result, dict):
        stdout = str(last_result.get("stdout") or "")
        stderr = str(last_result.get("stderr") or "")
        rc = last_result.get("returncode")
        detail = stderr.strip() or stdout.strip() or (str(rc) if rc is not None else "")
        return None, f"工具执行失败: {detail}".strip()
    return None, "工具执行失败"


def _build_tool_metadata(task_id: int, run_id: int, step_row, payload: dict) -> dict:
    tool_input = payload.get("input")
    tool_output = payload.get("output")

    if isinstance(tool_input, dict):
        input_schema = {"type": "object", "keys": list(tool_input.keys())}
    elif isinstance(tool_input, list):
        input_schema = {"type": "list", "length": len(tool_input)}
    elif tool_input is None:
        input_schema = {"type": "empty"}
    else:
        input_schema = {"type": "text"}

    if isinstance(tool_output, dict):
        output_schema = {"type": "object", "keys": list(tool_output.keys())}
    elif isinstance(tool_output, list):
        output_schema = {"type": "list", "length": len(tool_output)}
    elif tool_output is None:
        output_schema = {"type": "empty"}
    else:
        output_schema = {"type": "text"}

    step_id = None
    step_title = None
    if hasattr(step_row, "keys"):
        if "id" in step_row.keys():
            step_id = step_row["id"]
        if "title" in step_row.keys():
            step_title = step_row["title"]
    return {
        "source": TOOL_METADATA_SOURCE_AUTO,
        "task_id": task_id,
        "run_id": run_id,
        "step_id": step_id,
        "step_title": step_title,
        "input_sample": tool_input,
        "output_sample": tool_output,
        "input_schema": input_schema,
        "output_schema": output_schema,
        "action_type": ACTION_TYPE_TOOL_CALL,
    }


def execute_tool_call(
    task_id: int,
    run_id: int,
    step_row,
    payload: dict,
    context: Optional[dict] = None,
) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 tool_call：
    - output 允许为空：若为空则尝试按 metadata.exec 执行并回填
    - 若工具不存在则自动创建 tools_items，并可自动沉淀为 skill（失败不阻塞本步）
    """
    tool_input = payload.get("input")
    tool_output = payload.get("output")
    _coerce_optional_tool_int_fields(payload)
    _coerce_optional_tool_text_fields(payload)
    if tool_output is None:
        payload["output"] = ""
        tool_output = ""

    if not isinstance(tool_input, str) or not tool_input.strip():
        raise ValueError("tool_call.input 不能为空")
    if not isinstance(tool_output, str):
        raise ValueError("tool_call.output 必须是字符串")

    if payload.get("tool_id") is None and not payload.get("tool_name"):
        tool_name = f"{AUTO_TOOL_PREFIX}_{task_id}_{step_row['id']}"
        payload["tool_name"] = tool_name
        step_title = None
        if hasattr(step_row, "keys") and "title" in step_row.keys():
            step_title = step_row["title"]
        payload.setdefault(
            "tool_description",
            AUTO_TOOL_DESCRIPTION_TEMPLATE.format(step_title=step_title or tool_name),
        )
        payload.setdefault("tool_version", DEFAULT_TOOL_VERSION)

    # 检查工具是否被禁用
    tool_name_value = str(payload.get("tool_name") or "").strip()
    tool_id_value = parse_positive_int(payload.get("tool_id"), default=None)
    if not tool_name_value and tool_id_value is not None:
        try:
            row = get_tool(tool_id=int(tool_id_value))
            tool_name_value = str(row["name"] or "").strip() if row else ""
        except Exception:
            tool_name_value = ""
    if tool_name_value and not is_tool_enabled(tool_name_value):
        raise ValueError(f"tool 已禁用: {tool_name_value}")

    # 判断是否为“新创建工具”：用于后续沉淀为技能卡（skill）
    tool_was_missing = False
    try:
        if payload.get("tool_id") is None and payload.get("tool_name"):
            existed = get_tool_by_name(name=str(payload.get("tool_name") or ""))
            tool_was_missing = existed is None
    except Exception:
        tool_was_missing = False

    if payload.get("tool_metadata") is None:
        payload["tool_metadata"] = _build_tool_metadata(task_id, run_id, step_row, payload)

    # tool_call 必须真实执行：禁止让模型在 output 里“手填/编造结果”。
    # - 若 tools_items.metadata.exec 已存在：无论 output 是否为空，都以真实执行结果覆盖 output；
    # - 若 exec 不存在：直接报错，促使模型“先补齐工具的 exec 再继续”（工具自举）。
    exec_spec = _resolve_tool_exec_spec(payload)
    if exec_spec is None:
        raise ValueError(
            format_task_error(
                code="missing_tool_exec_spec",
                message=(
                    "tool_call 缺少可执行定义：请在 tool_metadata.exec 中提供 "
                    "type=shell，且包含 command(str) 或 args(list)，可选 timeout_ms，建议 workdir；"
                    "并把 output 留空让系统真实执行"
                ),
            )
        )

    allow_empty_output = False
    if isinstance(exec_spec, dict):
        try:
            allow_empty_output = bool(exec_spec.get("allow_empty_output"))
        except Exception:
            allow_empty_output = False

    dependency_error = _enforce_tool_exec_script_dependency(
        task_id=int(task_id),
        run_id=int(run_id),
        step_row=step_row,
        exec_spec=exec_spec,
        tool_input=str(tool_input),
    )
    if dependency_error:
        raise ValueError(format_task_error(code="tool_exec_contract_error", message=dependency_error))

    warnings: List[str] = []
    web_fetch_error_code = ""
    web_fetch_error_message = ""
    web_fetch_attempts: List[dict] = []
    web_fetch_protocol: Optional[dict] = None
    if tool_name_value == TOOL_NAME_WEB_FETCH:
        web_fetch_protocol, protocol_warnings = _ensure_web_fetch_protocol(
            task_id=int(task_id),
            run_id=int(run_id),
            step_row=step_row if isinstance(step_row, dict) else {},
            tool_input=str(tool_input),
            context=context if isinstance(context, dict) else None,
        )
        warnings.extend([str(item) for item in protocol_warnings if str(item or "").strip()])
        web_fetch_result = _execute_web_fetch_with_fallback(
            exec_spec,
            str(tool_input),
            protocol=web_fetch_protocol if isinstance(web_fetch_protocol, dict) else None,
            context=context if isinstance(context, dict) else None,
        )
        output_text = str(web_fetch_result.get("output_text") or "")
        web_fetch_error_code = str(web_fetch_result.get("error_code") or "").strip().lower()
        web_fetch_error_message = str(web_fetch_result.get("error_message") or "").strip()
        if isinstance(web_fetch_result.get("attempts"), list):
            web_fetch_attempts = [dict(item) for item in web_fetch_result.get("attempts") if isinstance(item, dict)]
        warnings.extend([str(item) for item in (web_fetch_result.get("warnings") or []) if str(item or "").strip()])
    else:
        output_text, exec_error = _execute_tool_with_exec_spec(exec_spec, str(tool_input))
        if exec_error:
            # 检测 TLS/SSL 握手失败
            lowered_err = str(exec_error).lower()
            if "handshake" in lowered_err and ("ssl" in lowered_err or "tls" in lowered_err):
                raise ValueError(format_task_error(code="tls_handshake_failed", message=exec_error))
            raise ValueError(exec_error)
        output_text = str(output_text or "")
        warnings = []

    if tool_was_missing and not output_text.strip():
        warnings.append("新创建工具执行输出为空：建议让工具打印关键结果/关键日志，或使用文件落盘并在后续步骤验证产物。")
    if not output_text.strip() and not allow_empty_output:
        warnings.append("工具输出为空：若该工具以文件落盘为主，请设置 exec.allow_empty_output=true 并补充验证步骤。")

    payload["output"] = output_text
    # 让 metadata 里保留一次可读的样例，便于后续沉淀为技能/回放调试
    if isinstance(payload.get("tool_metadata"), dict):
        payload["tool_metadata"]["input_sample"] = str(tool_input)
        payload["tool_metadata"]["output_sample"] = output_text

    if payload.get("task_id") is None:
        payload["task_id"] = int(task_id)
    if payload.get("run_id") is None:
        payload["run_id"] = int(run_id)
    result = _create_tool_record(payload)
    record = result.get("record") if isinstance(result, dict) else None
    if not isinstance(record, dict):
        raise ValueError(ERROR_MESSAGE_PROMPT_RENDER_FAILED)
    if warnings:
        record["warnings"] = warnings
    if web_fetch_attempts:
        record["attempts"] = web_fetch_attempts
    if tool_name_value == TOOL_NAME_WEB_FETCH:
        for key in ("selected_candidate", "candidate_rankings", "candidate_rejections", "evidence_summary", "search_attempts"):
            value = web_fetch_result.get(key) if isinstance(web_fetch_result, dict) else None
            if value not in (None, "", [], {}):
                record[key] = value
        if isinstance(web_fetch_protocol, dict):
            record["protocol"] = dict(web_fetch_protocol)

    # web_fetch：在记录调用后返回结构化失败，避免“完成态掩盖失败”。
    if tool_name_value == TOOL_NAME_WEB_FETCH:
        if web_fetch_error_message:
            error_code = web_fetch_error_code or "web_fetch_blocked"
            return record, format_task_error(
                code=error_code,
                message=web_fetch_error_message,
            )

    structured_error = _detect_structured_tool_error(
        tool_name=tool_name_value,
        output_text=output_text,
    )
    if structured_error:
        code, message = structured_error
        return record, format_task_error(code=code, message=message)

    # 新工具：此处只负责“真实执行 + 记录调用”，不直接沉淀为 skill。
    # 说明：
    # - 新工具会先以 tools_items.metadata.approval.status=draft 形式保存；
    # - 只有在 run 成功结束且 Eval Agent 评估通过后，才会自动批准并生成 tool skill。
    if tool_was_missing:
        _safe_write_debug(
            int(task_id),
            int(run_id),
            message="tool.created_draft",
            data={"tool_id": record.get("tool_id"), "tool_name": record.get("tool_name")},
            level="info",
        )

    return record, None
