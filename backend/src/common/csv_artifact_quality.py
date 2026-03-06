from __future__ import annotations

import re
from datetime import date
from typing import Dict, List, Optional

from backend.src.constants import (
    AGENT_ARTIFACT_CSV_MAX_PLACEHOLDER_RATIO,
    AGENT_ARTIFACT_CSV_MIN_DATE_SPAN_DAYS,
    AGENT_ARTIFACT_CSV_MIN_NUMERIC_RATIO,
    AGENT_ARTIFACT_CSV_MIN_NUMERIC_ROWS,
    AGENT_ARTIFACT_CSV_MIN_ROWS,
)


def parse_csv_numeric(text: str) -> Optional[float]:
    raw = str(text or "").strip()
    if not raw:
        return None
    raw = raw.replace(",", "")
    raw = raw.replace("，", "")
    raw = raw.replace("元/克", "")
    raw = raw.replace("元", "")
    if raw.endswith("%"):
        raw = raw[:-1]
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def parse_csv_iso_date(text: str) -> Optional[date]:
    raw = str(text or "").strip()
    if not raw:
        return None
    normalized = raw.replace("/", "-").replace(".", "-")
    if not re.match(r"^\d{4}-\d{1,2}-\d{1,2}$", normalized):
        return None
    parts = normalized.split("-")
    try:
        return date(int(parts[0]), int(parts[1]), int(parts[2]))
    except Exception:
        return None


def load_csv_quality_stats_from_text(content: str) -> Dict[str, object]:
    rows_total = 0
    numeric_rows = 0
    placeholder_rows = 0
    date_values: List[date] = []
    placeholders = ("暂无", "n/a", "na", "none", "null", "无数据", "待补充", "tbd")

    raw_lines = [str(line or "").strip() for line in str(content or "").splitlines()]
    clean_lines = [line for line in raw_lines if line]
    if not clean_lines:
        return {
            "rows_total": 0,
            "numeric_rows": 0,
            "placeholder_rows": 0,
            "numeric_ratio": 0.0,
            "placeholder_ratio": 0.0,
            "date_span_days": 0,
            "issues": ["csv_empty"],
        }

    first_line = clean_lines[0].lower()
    has_header = ("日期" in clean_lines[0]) or ("date" in first_line)
    data_lines = clean_lines[1:] if has_header else clean_lines

    for line in data_lines:
        cells = [cell.strip() for cell in re.split(r"[,，]\s*", line) if str(cell).strip()]
        if not cells:
            continue
        rows_total += 1

        joined = " ".join(cells).lower()
        if any(mark in joined for mark in placeholders):
            placeholder_rows += 1

        numeric_hit = False
        candidate_cells = cells[1:] if len(cells) > 1 else cells
        for cell in candidate_cells:
            if parse_csv_numeric(cell) is not None:
                numeric_hit = True
                break
        if numeric_hit:
            numeric_rows += 1

        date_candidate = parse_csv_iso_date(cells[0])
        if date_candidate is not None:
            date_values.append(date_candidate)

    numeric_ratio = float(numeric_rows) / float(rows_total) if rows_total > 0 else 0.0
    placeholder_ratio = float(placeholder_rows) / float(rows_total) if rows_total > 0 else 0.0
    span_days = 0
    if len(date_values) >= 2:
        span_days = abs((max(date_values) - min(date_values)).days)

    issues: List[str] = []
    if rows_total < int(AGENT_ARTIFACT_CSV_MIN_ROWS):
        issues.append("rows_insufficient")
    if numeric_rows < int(AGENT_ARTIFACT_CSV_MIN_NUMERIC_ROWS):
        issues.append("numeric_rows_insufficient")
    if numeric_ratio < float(AGENT_ARTIFACT_CSV_MIN_NUMERIC_RATIO):
        issues.append("numeric_ratio_low")
    if placeholder_ratio > float(AGENT_ARTIFACT_CSV_MAX_PLACEHOLDER_RATIO):
        issues.append("placeholder_ratio_high")
    if span_days < int(AGENT_ARTIFACT_CSV_MIN_DATE_SPAN_DAYS):
        issues.append("date_span_too_short")

    return {
        "rows_total": rows_total,
        "numeric_rows": numeric_rows,
        "placeholder_rows": placeholder_rows,
        "numeric_ratio": round(numeric_ratio, 4),
        "placeholder_ratio": round(placeholder_ratio, 4),
        "date_span_days": span_days,
        "issues": issues,
    }


def load_csv_quality_stats(path: str) -> Dict[str, object]:
    with open(path, "r", encoding="utf-8", errors="ignore") as handle:
        return load_csv_quality_stats_from_text(handle.read())


def build_csv_quality_failure_text(path: str, stats: Dict[str, object]) -> str:
    issues = [str(item).strip() for item in (stats.get("issues") or []) if str(item).strip()]
    if not issues:
        issues = ["csv_quality_failed"]
    return f"{path}:{','.join(issues)}"
