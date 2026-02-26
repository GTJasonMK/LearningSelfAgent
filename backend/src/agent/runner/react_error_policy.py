# -*- coding: utf-8 -*-
"""
ReAct 错误策略（可配置矩阵）。

目标：
- 规则外置化，避免在业务函数里散落硬编码；
- 仍保持默认策略可用，不依赖外部配置文件；
- 通过环境变量覆盖，便于开发阶段快速实验。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import FrozenSet, Tuple

from backend.src.common.task_error_codes import extract_task_error_code, is_source_failure_error_code

_REACT_ERROR_POLICY_ENV = "AGENT_REACT_ERROR_POLICY_MATRIX"


@dataclass(frozen=True)
class ReactErrorPolicyMatrix:
    structural_replan_codes: FrozenSet[str]
    env_replan_codes: FrozenSet[str]
    legacy_keywords: Tuple[str, ...]


_DEFAULT_MATRIX = ReactErrorPolicyMatrix(
    structural_replan_codes=frozenset(
        {
            "invalid_action_type",
            "invalid_action_payload",
            "missing_tool_input",
            "plan_patch_not_action",
            "policy_blocked_python_c",
            "script_arg_contract_mismatch",
        }
    ),
    env_replan_codes=frozenset({"dependency_missing"}),
    legacy_keywords=(
        "tool_call.input 不能为空",
        "action 输出不是有效 JSON",
        "action.payload 不是对象",
        "action.type 不能为空",
        "action.type 非法",
        "LLM调用失败",
        "Connection error",
        "Read timed out",
        "timeout",
        "could not resolve host",
        "too many requests",
        "service unavailable",
        "handshake",
        "csv_artifact_quality_failed",
        "高风险单行控制流 python -c",
        "complex python -c requires file_write script",
    ),
)


def _normalize_code_set(items: object) -> FrozenSet[str]:
    if not isinstance(items, (list, tuple, set, frozenset)):
        return frozenset()
    out = []
    for item in items:
        text = str(item or "").strip().lower()
        if not text:
            continue
        out.append(text)
    return frozenset(out)


def _normalize_keyword_tuple(items: object) -> Tuple[str, ...]:
    if not isinstance(items, (list, tuple, set, frozenset)):
        return tuple()
    out = []
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        out.append(text)
    return tuple(out)


@lru_cache(maxsize=8)
def _resolve_react_error_policy_matrix_cached(raw: str) -> ReactErrorPolicyMatrix:
    if not raw:
        return _DEFAULT_MATRIX

    try:
        obj = json.loads(raw)
    except Exception:
        return _DEFAULT_MATRIX
    if not isinstance(obj, dict):
        return _DEFAULT_MATRIX

    structural = _normalize_code_set(obj.get("structural_replan_codes"))
    env_codes = _normalize_code_set(obj.get("env_replan_codes"))
    keywords = _normalize_keyword_tuple(obj.get("legacy_keywords"))
    return ReactErrorPolicyMatrix(
        structural_replan_codes=structural or _DEFAULT_MATRIX.structural_replan_codes,
        env_replan_codes=env_codes or _DEFAULT_MATRIX.env_replan_codes,
        legacy_keywords=keywords or _DEFAULT_MATRIX.legacy_keywords,
    )


def resolve_react_error_policy_matrix() -> ReactErrorPolicyMatrix:
    """
    解析 ReAct 错误策略矩阵。

    说明：
    - 结果按环境变量原始值缓存；
    - 环境变量变化时会命中新 key，避免旧缓存导致策略不生效。
    """
    raw = str(os.getenv(_REACT_ERROR_POLICY_ENV) or "").strip()
    return _resolve_react_error_policy_matrix_cached(raw)


# 向后兼容：保留对外 cache_clear 入口（测试/调试会调用）。
resolve_react_error_policy_matrix.cache_clear = _resolve_react_error_policy_matrix_cached.cache_clear  # type: ignore[attr-defined]


def should_force_replan_on_action_error(error_text: str) -> bool:
    """
    判断是否应该因 action 错误强制触发 replan。
    """
    try:
        matrix = resolve_react_error_policy_matrix()
        code = extract_task_error_code(str(error_text or ""))
        if code in matrix.structural_replan_codes:
            return True
        if code in matrix.env_replan_codes:
            return True
        if is_source_failure_error_code(code):
            return True
        return any(keyword in str(error_text or "") for keyword in matrix.legacy_keywords)
    except Exception:
        return False
