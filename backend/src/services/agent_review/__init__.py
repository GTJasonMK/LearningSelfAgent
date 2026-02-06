"""
评估（agent review）相关的通用工具函数。

说明：
- 该目录用于复用“评估输出归一化/证据引用清洗/沉淀门槛”等逻辑，
  避免在后处理与 API 评估接口中出现重复实现导致口径漂移。
"""

from backend.src.services.agent_review.review_normalize import (  # noqa: F401
    apply_distill_gate,
    filter_evidence_refs,
    normalize_issues,
)

