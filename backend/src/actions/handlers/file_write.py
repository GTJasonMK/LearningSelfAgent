import os
import re
from typing import Optional, Tuple

from backend.src.actions.file_write import write_text_file

# 仅拦截“明显模拟数据”标记，避免对正常业务词汇误报。
_SIMULATED_DATA_PATTERNS = (
    re.compile(r"\b(simulated?|synthetic|fabricated|mock|fake|dummy)\b", re.IGNORECASE),
    re.compile(r"(模拟数据|虚构数据|示例数据|假数据|随机生成|测试数据)"),
)

# 当前仅对业务 CSV 启用前置门禁，避免误伤代码/文档写入。
_BUSINESS_DATA_EXTENSIONS = {".csv"}


def _is_business_data_path(path: str) -> bool:
    ext = os.path.splitext(str(path or "").strip().lower())[1]
    return ext in _BUSINESS_DATA_EXTENSIONS


def _detect_simulated_marker(text: object) -> Optional[str]:
    raw = str(text or "")
    if not raw.strip():
        return None
    for pattern in _SIMULATED_DATA_PATTERNS:
        match = pattern.search(raw)
        if match:
            return str(match.group(0) or "").strip() or None
    return None


def _collect_context_evidence_text(context: Optional[dict]) -> str:
    if not isinstance(context, dict):
        return ""

    chunks = []

    parse_text = str(context.get("latest_parse_input_text") or "").strip()
    if parse_text:
        chunks.append(parse_text)

    observations = context.get("observations")
    if isinstance(observations, list):
        for item in observations[-3:]:
            line = str(item or "").strip()
            if line:
                chunks.append(line)

    auto_retry = context.get("latest_shell_auto_retry")
    if isinstance(auto_retry, dict):
        for key in ("initial_stdout", "initial_stderr", "fallback_url", "trigger"):
            value = str(auto_retry.get(key) or "").strip()
            if value:
                chunks.append(value)

    return "\n".join(chunks)


def _maybe_warn_business_data_write(path: str, content: str, context: Optional[dict]) -> Optional[str]:
    if not _is_business_data_path(path):
        return None

    if isinstance(context, dict):
        # 与现有上下文开关对齐：默认开启，只有显式 False 才关闭。
        enabled = context.get("enforce_business_data_source_guard")
        if enabled is None:
            enabled = context.get("enforce_csv_artifact_quality", True)
        if enabled is False:
            return None

    content_marker = _detect_simulated_marker(content)
    context_marker = _detect_simulated_marker(_collect_context_evidence_text(context))
    if not content_marker and not context_marker:
        return None

    marker = content_marker or context_marker or "simulated"
    return (
        "file_write 检测到业务 CSV 可能来自低可信来源/模拟数据标记"
        f"（{marker}）。建议先完成真实数据抓取与解析，再落盘结果。"
    )


def execute_file_write(
    payload: dict,
    *,
    context: Optional[dict] = None,
) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 file_write：写入文本文件。
    """
    path = payload.get("path")
    if not isinstance(path, str) or not path.strip():
        raise ValueError("file_write.path 不能为空")

    content = payload.get("content")
    if content is None:
        content = ""
    if not isinstance(content, str):
        raise ValueError("file_write.content 必须是字符串")

    warnings = []
    warn_text = _maybe_warn_business_data_write(path=path, content=content, context=context)
    if warn_text:
        warnings.append(warn_text)
        if isinstance(context, dict):
            items = context.get("quality_warnings")
            if not isinstance(items, list):
                items = []
            items.append(warn_text)
            context["quality_warnings"] = items

    encoding = payload.get("encoding") or "utf-8"
    if not isinstance(encoding, str) or not encoding.strip():
        encoding = "utf-8"

    result = write_text_file(path=path, content=content, encoding=encoding)
    if warnings:
        try:
            result = dict(result or {})
        except Exception:
            result = {"path": str(path), "bytes": 0}
        result["warnings"] = warnings
    return result, None
