# -*- coding: utf-8 -*-
"""
ReAct 循环状态管理模块。

提供状态持久化、Replan 上下文准备等公共逻辑。
"""

import logging
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.common.utils import now_iso
from backend.src.constants import (
    AGENT_MAX_STEPS_UNLIMITED,
    AGENT_REACT_PERSIST_MIN_INTERVAL_SECONDS,
    AGENT_REACT_REPLAN_MAX_ATTEMPTS,
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_STOPPED,
    RUN_STATUS_WAITING,
)
from backend.src.repositories.task_runs_repo import update_task_run

logger = logging.getLogger(__name__)

_PERSIST_THROTTLE_LOCK = threading.Lock()
_PERSIST_THROTTLE_STATE: Dict[int, float] = {}


@dataclass
class ReplanContext:
    """Replan 上下文信息。"""
    can_replan: bool
    replan_attempts: int
    done_count: int
    remaining_limit: Optional[int]
    max_steps_value: int
    reason: str = ""


def prepare_replan_context(
    *,
    step_order: int,
    agent_state: Dict,
    max_steps_limit: Optional[int],
    plan_titles: List[str],
) -> ReplanContext:
    """
    准备 Replan 上下文：判断是否可以重新规划，计算相关参数。

    Args:
        step_order: 当前步骤序号（1-based）
        agent_state: Agent 状态字典
        max_steps_limit: 最大步骤数限制
        plan_titles: 计划标题列表

    Returns:
        ReplanContext 包含是否可 replan、已完成步数、剩余限制等信息
    """
    try:
        replan_attempts = int(agent_state.get("replan_attempts") or 0)
    except Exception:
        replan_attempts = 0

    done_count = max(0, int(step_order) - 1)
    remaining_limit: Optional[int] = None

    if isinstance(max_steps_limit, int) and max_steps_limit > 0:
        remaining_limit = max_steps_limit - done_count

    # 检查是否可以 replan
    can_replan = True
    reason = ""

    if remaining_limit is not None and remaining_limit <= 0:
        can_replan = False
        reason = f"remaining_limit={remaining_limit}"

    if replan_attempts >= int(AGENT_REACT_REPLAN_MAX_ATTEMPTS or 0):
        can_replan = False
        reason = f"replan_attempts={replan_attempts} >= max={AGENT_REACT_REPLAN_MAX_ATTEMPTS}"

    # 计算 max_steps_value
    if remaining_limit is not None:
        max_steps_value = int(remaining_limit)
    else:
        # 开发阶段无上限：避免 replan 在“当前计划长度”处被误限流（例如需要插入补救步骤时被拦截）。
        if isinstance(max_steps_limit, int) and max_steps_limit > 0:
            max_steps_value = int(max_steps_limit)
        else:
            max_steps_value = int(AGENT_MAX_STEPS_UNLIMITED)

    return ReplanContext(
        can_replan=can_replan,
        replan_attempts=replan_attempts,
        done_count=done_count,
        remaining_limit=remaining_limit,
        max_steps_value=max_steps_value,
        reason=reason,
    )


def persist_loop_state(
    *,
    run_id: int,
    plan_struct: PlanStructure,
    agent_state: Dict,
    step_order: int,
    observations: List[str],
    context: Dict,
    paused: Optional[dict] = None,
    status: Optional[str] = None,
    safe_write_debug: Optional[Callable] = None,
    task_id: Optional[int] = None,
    where: str = "loop",
    force: bool = False,
) -> bool:
    """
    统一的循环状态持久化。
    """
    try:
        agent_state["paused"] = paused
        agent_state["step_order"] = step_order
        agent_state["observations"] = observations
        agent_state["context"] = context
        updated_at = now_iso()

        # ReAct/do 落库节流：
        # - 高频 update_task_run 会放大 SQLite 写入与锁竞争；
        # - waiting/failed/done/stopped/收尾必须立即落盘，保证可恢复与审计一致；
        # - 非关键状态允许按时间窗口合并落盘（避免每个 before_step/after_step 都写库）。
        try:
            min_interval = float(AGENT_REACT_PERSIST_MIN_INTERVAL_SECONDS or 0)
        except Exception:
            min_interval = 0.0
        if min_interval < 0:
            min_interval = 0.0

        critical_status = str(status or "").strip()
        is_critical = critical_status in {
            RUN_STATUS_WAITING,
            RUN_STATUS_FAILED,
            RUN_STATUS_DONE,
            RUN_STATUS_STOPPED,
        }

        is_final_step = False
        try:
            is_final_step = int(step_order) >= int(plan_struct.step_count) + 1
        except Exception:
            is_final_step = False

        if not bool(force) and not is_critical and not is_final_step and min_interval > 0:
            now_value = time.monotonic()
            with _PERSIST_THROTTLE_LOCK:
                last_at = _PERSIST_THROTTLE_STATE.get(int(run_id))
            if last_at is not None and (now_value - float(last_at)) < float(min_interval):
                return True

        update_kwargs = {
            "run_id": int(run_id),
            "agent_plan": plan_struct.to_agent_plan_payload(),
            "agent_state": agent_state,
            "updated_at": updated_at,
        }
        if status is not None:
            update_kwargs["status"] = status

        update_task_run(**update_kwargs)
        if min_interval > 0:
            # 终态/收尾后清理节流缓存，避免长生命周期进程下内存缓慢增长。
            should_cleanup = False
            if is_critical and critical_status in {RUN_STATUS_DONE, RUN_STATUS_FAILED, RUN_STATUS_STOPPED}:
                should_cleanup = True
            if is_final_step:
                should_cleanup = True
            with _PERSIST_THROTTLE_LOCK:
                if should_cleanup:
                    _PERSIST_THROTTLE_STATE.pop(int(run_id), None)
                else:
                    _PERSIST_THROTTLE_STATE[int(run_id)] = time.monotonic()
        return True

    except Exception as exc:
        if safe_write_debug and task_id is not None:
            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.state.persist_failed",
                data={"where": where, "step_order": int(step_order), "error": str(exc)},
                level="warning",
            )
        return False


def persist_plan_only(
    *,
    run_id: int,
    plan_struct: PlanStructure,
    safe_write_debug: Optional[Callable] = None,
    task_id: Optional[int] = None,
    step_order: int = 0,
    where: str = "plan_only",
) -> bool:
    """
    仅持久化计划（用于 plan_patch 后立即保存）。
    """
    try:
        updated_at = now_iso()
        update_task_run(
            run_id=int(run_id),
            agent_plan=plan_struct.to_agent_plan_payload(),
            updated_at=updated_at,
        )
        return True

    except Exception as exc:
        if safe_write_debug and task_id is not None:
            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.plan.persist_failed",
                data={"where": where, "step_order": int(step_order), "error": str(exc)},
                level="warning",
            )
        return False
