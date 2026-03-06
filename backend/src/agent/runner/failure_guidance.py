# -*- coding: utf-8 -*-
"""失败分类到策略提示的软映射。"""

from __future__ import annotations

from typing import Dict, List


def _normalize_list(raw: object) -> List[str]:
    if not isinstance(raw, list):
        return []
    items: List[str] = []
    for item in raw:
        text = str(item or "").strip()
        if not text or text in items:
            continue
        items.append(text)
    return items


def build_failure_guidance(agent_state: Dict) -> str:
    if not isinstance(agent_state, dict):
        return "(无)"
    pending = agent_state.get("pending_retry_requirements")
    goal_progress = agent_state.get("goal_progress") if isinstance(agent_state.get("goal_progress"), dict) else {}
    if not isinstance(pending, dict) or not bool(pending.get("active")):
        return "(无)"

    failure_class = str(pending.get("failure_class") or agent_state.get("last_failure_class") or "").strip().lower()
    goal_state = str(goal_progress.get("state") or "none").strip().lower() or "none"
    must_change = _normalize_list(pending.get("must_change"))
    retry_constraints = _normalize_list(pending.get("retry_constraints"))

    lines: List[str] = []
    if failure_class:
        lines.append(f"- failure_class={failure_class}")
    if must_change:
        lines.append(f"- 优先改变：{', '.join(must_change[:4])}")

    soft_strategy: List[str] = []
    if failure_class == "source_unavailable":
        soft_strategy.extend([
            "优先扩大候选来源集合，再选择最相关且可验证的来源。",
            "允许更换搜索关键词、来源类型、抓取路径；不要执着于单一 host。",
            "若当前还没有真实样本，不要先写解析脚本、校验脚本或结果文件；先拿到样本再继续。",
        ])
    elif failure_class == "contract_error":
        soft_strategy.extend([
            "优先缩小动作规模并修正 payload/协议，不要先切换任务目标。",
            "允许改用更稳定的动作表达，但应保持目标一致。",
        ])
    elif failure_class in {"llm_transient", "llm_rate_limit"}:
        soft_strategy.extend([
            "优先拆小步骤、减少一次性输出复杂度。",
            "允许保留目标与总体方案，但需改变动作粒度或生成方式。",
        ])
    elif failure_class == "artifact_missing":
        soft_strategy.extend([
            "优先补齐中间产物，再继续依赖该产物的后续步骤。",
            "允许改变执行顺序，但不要跳过必要验证。",
        ])
    else:
        soft_strategy.extend([
            "若当前路径无明显进展，可换工具路径、换中间表示或换步骤拆分方式。",
            "允许重新组织实现步骤，但要保留目标导向与可验证性。",
        ])

    if goal_state in {"none", "regressed"}:
        soft_strategy.append("当前目标进展不足，下一轮应优先获取更直接的目标证据，而不是继续包装结果。")
    elif goal_state == "partial":
        soft_strategy.append("当前已有部分证据，可在保持自由选择的前提下优先补齐缺失约束。")

    for item in retry_constraints[:4]:
        soft_strategy.append(item)

    # 明确保留模型自由，不把提示写成硬编码命令。
    lines.append("- 这些是优先策略提示，不是唯一解；若发现更强证据路径，可自主偏离，但必须解释性地体现变化。")
    for item in soft_strategy[:6]:
        lines.append(f"- {item}")
    return "\n".join(lines) if lines else "(无)"
