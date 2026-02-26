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
from backend.src.common.utils import coerce_int, now_iso
from backend.src.constants import (
    AGENT_MAX_STEPS_UNLIMITED,
    AGENT_REACT_PERSIST_MIN_INTERVAL_SECONDS,
    AGENT_REACT_REPLAN_MAX_ATTEMPTS,
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_STOPPED,
    RUN_STATUS_WAITING,
)
from backend.src.services.tasks.task_queries import update_task_run

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
    replan_attempts = coerce_int(agent_state.get("replan_attempts"), default=0)

    step_order_value = coerce_int(step_order, default=0)
    done_count = max(0, step_order_value - 1)
    remaining_limit: Optional[int] = None

    if isinstance(max_steps_limit, int) and max_steps_limit > 0:
        remaining_limit = max_steps_limit - done_count

    # 检查是否可以 replan
    can_replan = True
    reason = ""

    if remaining_limit is not None and remaining_limit <= 0:
        can_replan = False
        reason = f"remaining_limit={remaining_limit}"

    replan_max_attempts = coerce_int(AGENT_REACT_REPLAN_MAX_ATTEMPTS, default=0)
    if replan_max_attempts > 0 and replan_attempts >= replan_max_attempts:
        can_replan = False
        reason = f"replan_attempts={replan_attempts} >= max={replan_max_attempts}"

    # 计算 max_steps_value
    if remaining_limit is not None:
        max_steps_value = coerce_int(remaining_limit, default=0)
    else:
        # 开发阶段无上限：避免 replan 在“当前计划长度”处被误限流（例如需要插入补救步骤时被拦截）。
        if isinstance(max_steps_limit, int) and max_steps_limit > 0:
            max_steps_value = coerce_int(max_steps_limit, default=AGENT_MAX_STEPS_UNLIMITED)
        else:
            max_steps_value = coerce_int(AGENT_MAX_STEPS_UNLIMITED, default=9999)

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
        run_id_value = coerce_int(run_id, default=0)
        step_order_value = coerce_int(step_order, default=0)
        task_id_value = coerce_int(task_id, default=0) if task_id is not None else 0
        agent_state["paused"] = paused
        agent_state["step_order"] = step_order_value
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
            is_final_step = step_order_value >= coerce_int(plan_struct.step_count, default=0) + 1
        except Exception:
            is_final_step = False

        if not bool(force) and not is_critical and not is_final_step and min_interval > 0:
            now_value = time.monotonic()
            with _PERSIST_THROTTLE_LOCK:
                last_at = _PERSIST_THROTTLE_STATE.get(run_id_value)
                if last_at is not None and (now_value - float(last_at)) < float(min_interval):
                    return True
                # 预占时间戳，防止并发线程同时通过节流检查
                _PERSIST_THROTTLE_STATE[run_id_value] = now_value

        update_kwargs = {
            "run_id": run_id_value,
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
                    _PERSIST_THROTTLE_STATE.pop(run_id_value, None)
                else:
                    _PERSIST_THROTTLE_STATE[run_id_value] = time.monotonic()
        return True

    except Exception as exc:
        if safe_write_debug and task_id_value > 0:
            safe_write_debug(
                task_id=task_id_value,
                run_id=run_id_value,
                message="agent.state.persist_failed",
                data={"where": where, "step_order": step_order_value, "error": str(exc)},
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
        run_id_value = coerce_int(run_id, default=0)
        task_id_value = coerce_int(task_id, default=0) if task_id is not None else 0
        step_order_value = coerce_int(step_order, default=0)
        updated_at = now_iso()
        update_task_run(
            run_id=run_id_value,
            agent_plan=plan_struct.to_agent_plan_payload(),
            updated_at=updated_at,
        )
        return True

    except Exception as exc:
        if safe_write_debug and task_id_value > 0:
            safe_write_debug(
                task_id=task_id_value,
                run_id=run_id_value,
                message="agent.plan.persist_failed",
                data={"where": where, "step_order": step_order_value, "error": str(exc)},
                level="warning",
            )
        return False


def resolve_executor(agent_state: Dict, step_order: int) -> Optional[str]:
    """从 agent_state.executor_assignments 中按 step_order 查找执行器名称。"""
    try:
        step_order_value = coerce_int(step_order, default=0)
        assignments = agent_state.get("executor_assignments") if isinstance(agent_state, dict) else None
        if isinstance(assignments, list):
            for a in assignments:
                if not isinstance(a, dict):
                    continue
                order_value = coerce_int(a.get("step_order"), default=0)
                if order_value <= 0:
                    continue
                if order_value != step_order_value:
                    continue
                ev = str(a.get("executor") or "").strip()
                return ev or None
    except Exception:
        pass
    return None
