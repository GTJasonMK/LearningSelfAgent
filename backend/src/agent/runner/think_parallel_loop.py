# -*- coding: utf-8 -*-
"""
Think 模式：依赖驱动的并行调度执行器。

设计目标（激进开发阶段）：
- 让 Think 执行阶段真正做到“按依赖并行”而非仅“按 executor 选模型顺序跑”；
- 保持最小可用：支持 allow gate、user_prompt(waiting)、task_output 兜底、步骤落库与状态持久化；
- 对计划做“并行安全约束”：task_output/确认满意度默认依赖所有前置步骤，避免提前输出/提前等待。

说明：
- 该执行器不尝试在并行执行过程中支持 plan_patch/replan（会导致 step_order 漂移与并发冲突）。
  失败场景交由 think runner 外层的“多模型反思机制”插入修复步骤后再继续执行。
"""

from __future__ import annotations

import json
import queue
import re
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, Generator, List, Optional, Set, Tuple

from backend.src.actions.registry import action_types_line
from backend.src.agent.observation import _truncate_observation
from backend.src.agent.plan_utils import extract_file_write_target_path
from backend.src.agent.runner.feedback import is_task_feedback_step_title
from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.react_loop_impl import _enforce_allow_constraints
from backend.src.agent.runner.react_step_executor import (
    build_observation_line,
    generate_action_with_retry,
    handle_task_output_fallback,
    handle_user_prompt_action,
    yield_memory_write_event,
    yield_visible_result,
)
from backend.src.agent.runner.plan_events import sse_plan_delta
from backend.src.agent.runner.react_state_manager import persist_loop_state
from backend.src.agent.think.think_execution import _infer_executor_from_allow
from backend.src.common.utils import now_iso
from backend.src.constants import (
    ACTION_TYPE_FILE_APPEND,
    ACTION_TYPE_FILE_DELETE,
    ACTION_TYPE_FILE_READ,
    ACTION_TYPE_HTTP_REQUEST,
    ACTION_TYPE_LLM_CALL,
    ACTION_TYPE_SHELL_COMMAND,
    ACTION_TYPE_TASK_OUTPUT,
    ACTION_TYPE_TOOL_CALL,
    ACTION_TYPE_USER_PROMPT,
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_WAITING,
    STEP_STATUS_RUNNING,
    STREAM_TAG_EXEC,
    STREAM_TAG_EXECUTOR,
    STREAM_TAG_FAIL,
    STREAM_TAG_OK,
    STREAM_TAG_SKIP,
    STREAM_TAG_STEP,
    AGENT_EXPERIMENT_DIR_REL,
    AGENT_THINK_PARALLEL_PERSIST_MIN_INTERVAL_SECONDS,
    AGENT_REACT_STEP_PROMPT_TEMPLATE,
    ASSISTANT_OUTPUT_STYLE_GUIDE,
)
from backend.src.repositories.task_steps_repo import (
    TaskStepCreateParams,
    create_task_step,
    mark_task_step_done,
    mark_task_step_failed,
)
from backend.src.services.llm.llm_client import sse_json
from backend.src.services.tasks.task_run_lifecycle import check_missing_artifacts


@dataclass
class ThinkParallelLoopResult:
    run_status: str
    last_step_order: int


def _has_success_validation_step(plan_struct: PlanStructure) -> bool:
    """
    判断是否存在"成功的验证步骤"（用于 artifacts 任务的最终输出门闩）。
    - 允许类型：shell_command / tool_call
    - 标题包含"验证/校验/检查/自测"等关键词
    - 状态必须是 done
    """
    keywords = ("验证", "校验", "检查", "自测", "verify", "validate", "check", "test")
    for step in plan_struct.steps:
        allow_set = set(step.allow or [])
        if ACTION_TYPE_SHELL_COMMAND not in allow_set and ACTION_TYPE_TOOL_CALL not in allow_set:
            continue
        if step.status != "done":
            continue
        if any(key in str(step.title or "") for key in keywords):
            return True
    return False


def _has_prior_http_success_step(
    *,
    current_idx: int,
    plan_struct: PlanStructure,
) -> tuple[bool, bool]:
    """
    判断 task_output 之前是否存在 http_request 依赖，且依赖里至少一次成功。
    """
    has_http_requirement = False
    has_http_success = False
    upper = max(0, int(current_idx))
    for idx in range(upper):
        step = plan_struct.get_step(idx)
        if step is None:
            continue
        allow_set = set(step.allow or [])
        is_http_step = ACTION_TYPE_HTTP_REQUEST in allow_set or str(step.title or "").startswith("http_request:")
        if not is_http_step:
            continue
        has_http_requirement = True
        if step.status == "done":
            has_http_success = True
            break
    return has_http_requirement, has_http_success


def _build_dependency_map(
    *,
    plan_struct: PlanStructure,
    dependencies: Optional[List[dict]],
) -> List[List[int]]:
    """
    构建每个 step 的依赖列表（0-based step_index）。

    支持两种输入结构（都来自 docs/agent/think 模块）：
    - elaborate 输出：{"from_step":0,"to_step":1}
    - executor_assign 输出：{"step_index":2,"depends_on":[0,1]}
    """
    plan_titles = [s.title for s in plan_struct.steps]
    plan_allows = [list(s.allow) for s in plan_struct.steps]
    plan_artifacts = list(plan_struct.artifacts)
    total = len(plan_titles or [])
    dep_sets: List[Set[int]] = [set() for _ in range(total)]

    # docs/agent：依赖索引约定为 0-based，但 LLM 可能误输出 1-based。
    # 这里做一次全局归一化：选择“有效边数更多”的解释方案。
    dep_index_base = 0
    if dependencies and total > 0:
        def _to_int(value: object) -> Optional[int]:
            try:
                return int(value)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                return None

        def _count_valid_edges(base: int) -> int:
            n = 0
            for dep in dependencies or []:
                if not isinstance(dep, dict):
                    continue
                if "from_step" in dep and "to_step" in dep:
                    a = _to_int(dep.get("from_step"))
                    b = _to_int(dep.get("to_step"))
                    if a is None or b is None:
                        continue
                    a -= int(base)
                    b -= int(base)
                    if 0 <= a < total and 0 <= b < total and a != b:
                        n += 1
                    continue
                if "step_index" in dep and "depends_on" in dep:
                    si = _to_int(dep.get("step_index"))
                    if si is None:
                        continue
                    si -= int(base)
                    if not (0 <= si < total):
                        continue
                    raw = dep.get("depends_on")
                    if not isinstance(raw, list):
                        continue
                    for item in raw:
                        d = _to_int(item)
                        if d is None:
                            continue
                        d -= int(base)
                        if 0 <= d < total and d != si:
                            n += 1
                    continue
            return n

        valid0 = _count_valid_edges(0)
        valid1 = _count_valid_edges(1)
        if valid1 > valid0:
            dep_index_base = 1
        elif valid0 == valid1 and valid1 > 0:
            # tie-break：若出现 total（典型 1-based 的“最后一步”），优先按 1-based 解释。
            raw_values: List[int] = []
            for dep in dependencies or []:
                if not isinstance(dep, dict):
                    continue
                if "from_step" in dep and "to_step" in dep:
                    a = _to_int(dep.get("from_step"))
                    b = _to_int(dep.get("to_step"))
                    if a is not None:
                        raw_values.append(int(a))
                    if b is not None:
                        raw_values.append(int(b))
                    continue
                if "step_index" in dep:
                    si = _to_int(dep.get("step_index"))
                    if si is not None:
                        raw_values.append(int(si))
                raw = dep.get("depends_on")
                if isinstance(raw, list):
                    for item in raw:
                        d = _to_int(item)
                        if d is not None:
                            raw_values.append(int(d))
            if raw_values and max(raw_values) == int(total):
                dep_index_base = 1

    def _normalize_path_token(path: str) -> str:
        value = str(path or "").strip().strip("`'\"").strip()
        value = value.replace("\\", "/")
        while value.startswith("./"):
            value = value[2:]
        return value.strip()

    def _extract_prefixed_target_path(step_title: str, prefix: str) -> str:
        """
        从步骤标题中提取 file_* 的目标路径（docs/agent 约定）。
        - 支持 `file_read:relative/path.md ...`
        - 支持 `file_read:"path with space.md" ...`
        """
        raw = str(step_title or "").strip()
        if not raw:
            return ""
        pfx = str(prefix or "").strip()
        if not pfx:
            return ""
        match = re.match(rf"^{re.escape(pfx)}[:：]\s*(\"[^\"]+\"|'[^']+'|\S+)", raw)
        if not match:
            return ""
        value = str(match.group(1) or "").strip()
        if (value.startswith("\"") and value.endswith("\"")) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1].strip()
        return value

    # 1) 来自 LLM 的显式依赖
    for dep in dependencies or []:
        if not isinstance(dep, dict):
            continue
        if "from_step" in dep and "to_step" in dep:
            try:
                from_step = int(dep.get("from_step")) - int(dep_index_base)
                to_step = int(dep.get("to_step")) - int(dep_index_base)
            except (TypeError, ValueError):
                continue
            if 0 <= from_step < total and 0 <= to_step < total and from_step != to_step:
                dep_sets[to_step].add(from_step)
            continue

        if "step_index" in dep and "depends_on" in dep:
            try:
                step_index = int(dep.get("step_index")) - int(dep_index_base)
            except (TypeError, ValueError):
                continue
            if not (0 <= step_index < total):
                continue
            raw_deps = dep.get("depends_on")
            if not isinstance(raw_deps, list):
                continue
            for item in raw_deps:
                try:
                    d = int(item) - int(dep_index_base)
                except (TypeError, ValueError):
                    continue
                if 0 <= d < total and d != step_index:
                    dep_sets[step_index].add(d)
            continue

    # 2) artifacts 引用的隐式依赖（尽量 deterministic，不再调用 LLM）
    file_producers: Dict[str, int] = {}
    artifacts_raw = [str(a or "").strip() for a in (plan_artifacts or []) if str(a or "").strip()]
    artifacts_norm = [_normalize_path_token(a) for a in artifacts_raw]

    for idx, (title, allow) in enumerate(zip(plan_titles or [], plan_allows or [])):
        allow_set = set(allow or [])
        text = str(title or "")
        text_norm = text.replace("\\", "/")

        # 2.1) artifacts 引用依赖（title 引用某个 artifact -> 依赖产出该文件的步骤）
        if artifacts_norm:
            for raw_artifact, norm_artifact in zip(artifacts_raw, artifacts_norm):
                if not norm_artifact:
                    continue
                hit = (raw_artifact and raw_artifact in text) or (norm_artifact and norm_artifact in text_norm)
                if not hit:
                    continue
                producer = file_producers.get(norm_artifact)
                if producer is not None and int(producer) < int(idx):
                    dep_sets[idx].add(int(producer))

        # 2.2) file_* 显式读写依赖（即使 plan_artifacts 为空也可工作）
        if ACTION_TYPE_FILE_READ in allow_set:
            target = _normalize_path_token(_extract_prefixed_target_path(text, "file_read"))
            if target and target in file_producers:
                producer = int(file_producers[target])
                if producer < idx:
                    dep_sets[idx].add(producer)
        if ACTION_TYPE_FILE_APPEND in allow_set:
            target = _normalize_path_token(_extract_prefixed_target_path(text, "file_append"))
            if target and target in file_producers:
                producer = int(file_producers[target])
                if producer < idx:
                    dep_sets[idx].add(producer)
        if ACTION_TYPE_FILE_DELETE in allow_set:
            target = _normalize_path_token(_extract_prefixed_target_path(text, "file_delete"))
            if target and target in file_producers:
                producer = int(file_producers[target])
                if producer < idx:
                    dep_sets[idx].add(producer)

        # 2.3) 当前步骤产出文件：更新 producer（必须在“依赖推断之后”执行，避免覆盖最近前置 writer）
        if "file_write" in allow_set:
            target = _normalize_path_token(extract_file_write_target_path(text))
            if target:
                file_producers[target] = int(idx)
        if ACTION_TYPE_FILE_APPEND in allow_set:
            target = _normalize_path_token(_extract_prefixed_target_path(text, "file_append"))
            if target:
                file_producers[target] = int(idx)
        if ACTION_TYPE_FILE_DELETE in allow_set:
            target = _normalize_path_token(_extract_prefixed_target_path(text, "file_delete"))
            if target:
                file_producers[target] = int(idx)

    # 3) 关键门闩：task_output / 确认满意度 必须在所有前置步骤之后
    task_output_indices = [
        i
        for i, allow in enumerate(plan_allows or [])
        if isinstance(allow, list) and ACTION_TYPE_TASK_OUTPUT in set(allow or [])
    ]
    if task_output_indices:
        out_idx = task_output_indices[-1]
        for j in range(0, out_idx):
            dep_sets[out_idx].add(j)

    # 确认满意度（user_prompt）也必须最后（依赖 task_output + 全部前置）
    if plan_titles and is_task_feedback_step_title(str(plan_titles[-1] or "")):
        feedback_idx = len(plan_titles) - 1
        for j in range(0, feedback_idx):
            dep_sets[feedback_idx].add(j)

    # 转 list（稳定排序）
    return [sorted(list(s)) for s in dep_sets]


def _has_cycle(dep_map: List[List[int]]) -> bool:
    """
    简单环检测：Kahn 拓扑排序。
    """
    n = len(dep_map or [])
    if n <= 1:
        return False
    indeg = [0] * n
    outs: List[List[int]] = [[] for _ in range(n)]
    for to_idx, deps in enumerate(dep_map):
        for from_idx in deps or []:
            if 0 <= from_idx < n and 0 <= to_idx < n and from_idx != to_idx:
                indeg[to_idx] += 1
                outs[from_idx].append(to_idx)
    q: List[int] = [i for i in range(n) if indeg[i] == 0]
    visited = 0
    while q:
        i = q.pop()
        visited += 1
        for j in outs[i]:
            indeg[j] -= 1
            if indeg[j] == 0:
                q.append(j)
    return visited != n


def run_think_parallel_loop(
    *,
    task_id: int,
    run_id: int,
    message: str,
    workdir: str,
    model: str,
    parameters: dict,
    plan_struct: PlanStructure,
    tools_hint: str,
    skills_hint: str,
    memories_hint: str,
    graph_hint: str,
    agent_state: Dict,
    context: Dict,
    observations: List[str],
    start_step_order: int,
    end_step_order_inclusive: Optional[int],
    variables_source: str,
    step_llm_config_resolver: Optional[
        Callable[[int, str, List[str]], Tuple[Optional[str], Optional[dict]]]
    ],
    dependencies: Optional[List[dict]],
    executor_roles: Optional[List[str]],
    llm_call: Callable[[dict], dict],
    execute_step_action: Callable[..., tuple[Optional[dict], Optional[str]]],
    safe_write_debug: Callable[..., None],
) -> Generator[str, None, ThinkParallelLoopResult]:
    """
    Think 并行执行主循环（同步 generator）。

    返回值（StopIteration.value）：
    - ThinkParallelLoopResult(run_status, last_step_order)
    """
    # 保险丝：计划为空直接失败，避免 0/None 越界
    if not plan_struct.steps:
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} 计划为空，无法执行\n"})
        return ThinkParallelLoopResult(run_status=RUN_STATUS_FAILED, last_step_order=0)

    total = plan_struct.step_count

    # 依赖图
    dep_map = _build_dependency_map(
        plan_struct=plan_struct,
        dependencies=dependencies,
    )
    if _has_cycle(dep_map):
        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.think.parallel.dep_cycle",
            data={"dependencies": dependencies, "plan_len": total},
            level="warning",
        )
        # 降级：当依赖有环时，直接串行（依赖全部前置）
        dep_map = [list(range(0, i)) for i in range(total)]

    # Step 范围：start_step_order 为 1-based
    try:
        start_order = int(start_step_order)
    except (TypeError, ValueError):
        start_order = 1
    if start_order < 1:
        start_order = 1
    if start_order > total:
        start_order = total
    start_idx = start_order - 1

    # 可选：限制本次并行 loop 只调度到某个 step（用于把尾部反馈闭环交回顺序执行器处理）
    end_idx = total - 1
    if end_step_order_inclusive is not None:
        try:
            end_order = int(end_step_order_inclusive)
        except (TypeError, ValueError):
            end_order = total
        if end_order < 1:
            # 允许"执行空区间"（用于仅剩尾部反馈闭环的场景）
            end_idx = -1
        else:
            if end_order > total:
                end_order = total
            end_idx = end_order - 1

    # executor 线程池：默认按 docs 的 3 角色；但若 plan 实际出现其他 role，则动态补齐
    roles: List[str] = []
    if isinstance(executor_roles, list):
        roles = [str(r).strip() for r in executor_roles if str(r).strip()]
    if not roles:
        roles = ["executor_doc", "executor_code", "executor_test"]

    executor_for_step: Dict[int, str] = {}
    for i, step in enumerate(plan_struct.steps):
        role = _infer_executor_from_allow(list(step.allow or []), str(step.title or ""))
        role = str(role or "").strip() or "executor_code"
        executor_for_step[i] = role
        if role not in roles:
            roles.append(role)

    # 依赖图/分工信息落库（写入 agent_state，便于 resume/审计/调试）。
    # 注意：这里只写入“执行器实际使用的最终依赖图”（含 artifacts 推导 + task_output/反馈门闩），
    # 便于后续恢复时复用，避免仅靠本地推断丢失 LLM 给出的显式依赖。
    dep_payload = [
        {
            "step_index": int(i),
            "depends_on": [int(d) for d in (dep_map[i] if 0 <= i < len(dep_map) else [])],
        }
        for i in range(0, total)
    ]
    try:
        if isinstance(agent_state, dict):
            agent_state["think_parallel_dependencies"] = dep_payload
            agent_state["think_parallel_roles"] = list(roles)
            agent_state["think_parallel_executors"] = [
                {"step_order": int(i) + 1, "executor": str(executor_for_step.get(i) or "")}
                for i in range(0, total)
            ]
    except (TypeError, ValueError, AttributeError):
        pass
    safe_write_debug(
        task_id=int(task_id),
        run_id=int(run_id),
        message="agent.think.parallel.dep_graph",
        data={"roles": list(roles), "dependencies": dep_payload},
        level="info",
    )

    # 共享状态
    out_q: "queue.Queue[str]" = queue.Queue()
    state_lock = threading.Lock()
    cond = threading.Condition(state_lock)
    stop_event = threading.Event()
    db_lock = threading.Lock()

    completed: Set[int] = set()
    running: Set[int] = set()
    # 并行场景下的 step 上下文隔离：
    # - 避免多个线程互相覆盖 context.last_llm_response，导致 task_output 兜底串台；
    # - 仅在依赖满足时，允许把“依赖链路中最近的 llm_call 输出”作为 seed（更接近串行语义）。
    last_llm_by_idx: Dict[int, str] = {}
    max_llm_idx_holder = {"idx": -1}

    # 基于步骤 status 初始化 completed（支持 resume/反思继续）。
    # 注意：并行调度下不再假设 "start_step_order 之前都已完成"，以免跳过未完成步骤。
    for idx, step in enumerate(plan_struct.steps):
        # docs/agent：skipped 也应视为"已结算"，否则会阻塞依赖与最终输出门闩。
        if step.status in {"done", "skipped"}:
            completed.add(idx)

    # 保险丝：若 start_step_order 指向的不是"最早未完成步骤"，则回退到最早未完成 step，
    # 避免并行/反思插入导致 step_order 漂移后跳过未完成步骤。
    first_pending_idx: Optional[int] = None
    for idx in range(0, total):
        if idx in completed:
            continue
        if plan_struct.steps[idx].status not in {"done", "skipped"}:
            first_pending_idx = idx
            break
    if first_pending_idx is not None and int(first_pending_idx) < int(start_idx):
        start_idx = int(first_pending_idx)
        start_order = start_idx + 1

    if end_idx < start_idx:
        # 说明：本次没有可执行的 step（例如仅剩最后的确认满意度步骤）。
        yield sse_json({"delta": f"{STREAM_TAG_SKIP} 无可并行执行的步骤，跳过（start={start_order} end={end_idx + 1}）\n"})
        return ThinkParallelLoopResult(run_status=RUN_STATUS_DONE, last_step_order=max(0, start_order - 1))

    run_status_holder = {"status": RUN_STATUS_DONE}
    last_step_order_holder = {"value": max(0, start_order - 1)}
    first_error_holder = {"step_order": 0, "error": ""}
    waiting_barrier = {"idx": None, "role": None}

    _PAUSED_KEEP = object()

    # persist_loop_state 节流：
    # - 并行步骤可能在短时间内密集完成，若每步都 update_task_run 会造成 SQLite 写入放大与锁竞争；
    # - waiting/failed 等关键状态必须立即落盘，确保可恢复与审计一致；
    # - 普通 done 允许按时间窗口合并落盘（最终收尾仍会强制落盘一次）。
    try:
        persist_min_interval_seconds = float(AGENT_THINK_PARALLEL_PERSIST_MIN_INTERVAL_SECONDS or 0)
    except (TypeError, ValueError):
        persist_min_interval_seconds = 0.0
    if persist_min_interval_seconds < 0:
        persist_min_interval_seconds = 0.0

    persist_ctrl: Dict[str, object] = {
        "dirty": False,
        "last_persist_at": 0.0,
        "pending_step_order": int(start_order),
        "pending_where": "",
        "pending_status": None,
    }

    def _emit(msg: str) -> None:
        try:
            out_q.put_nowait(str(msg))
        except queue.Full:
            return

    def _persist(
        where: str,
        *,
        step_order: int,
        paused: object = _PAUSED_KEEP,
        status: Optional[str] = None,
    ) -> bool:
        paused_value: Optional[dict]
        if paused is _PAUSED_KEEP:
            paused_value = agent_state.get("paused") if isinstance(agent_state, dict) else None
        else:
            paused_value = paused if isinstance(paused, dict) else None

        with db_lock:
            ok = persist_loop_state(
                run_id=int(run_id),
                plan_struct=plan_struct,
                agent_state=agent_state,
                step_order=int(step_order),
                observations=observations,
                context=context,
                paused=paused_value,
                status=status,
                force=True,
                safe_write_debug=safe_write_debug,
                task_id=int(task_id),
                where=where,
            )
        if not ok:
            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.think.parallel.persist_failed",
                data={"where": where, "step_order": int(step_order)},
                level="warning",
            )
        return bool(ok)

    def _next_step_order_for_state() -> int:
        # 以"最小 pending"作为 next step（便于 resume/审计），并不代表执行顺序严格单调
        for idx in range(0, total):
            if idx in completed:
                continue
            if plan_struct.steps[idx].status in {"done", "skipped"}:
                continue
            return idx + 1
        return total + 1

    def _mark_persist_dirty(where: str, *, step_order: int, status: Optional[str] = None) -> None:
        """
        标记需要落盘（必须在 state_lock 内调用）。
        """
        persist_ctrl["dirty"] = True
        persist_ctrl["pending_step_order"] = int(step_order)
        persist_ctrl["pending_where"] = str(where or "think_parallel")
        persist_ctrl["pending_status"] = status

    def _maybe_flush_persist_locked(*, now: Optional[float] = None, force: bool = False) -> None:
        """
        按节流阈值决定是否落盘（必须在 state_lock 内调用）。
        """
        if not bool(persist_ctrl.get("dirty")):
            return
        now_value = float(now) if isinstance(now, (int, float)) else time.monotonic()
        last_at = float(persist_ctrl.get("last_persist_at") or 0)
        if force or persist_min_interval_seconds <= 0 or (now_value - last_at) >= persist_min_interval_seconds:
            where = str(persist_ctrl.get("pending_where") or "think_parallel")
            step_order = int(persist_ctrl.get("pending_step_order") or _next_step_order_for_state())
            status = persist_ctrl.get("pending_status")
            ok = _persist(where, step_order=step_order, status=str(status) if status is not None else None)
            persist_ctrl["last_persist_at"] = now_value
            if ok:
                persist_ctrl["dirty"] = False
                persist_ctrl["pending_where"] = ""
                persist_ctrl["pending_status"] = None
            else:
                # 落盘失败时保留 pending 信息，等待后续 idle/收尾再次尝试（避免“状态更新丢失”）。
                persist_ctrl["dirty"] = True

    def _pick_next_step_for_role(role: str) -> Optional[int]:
        with state_lock:
            if stop_event.is_set():
                return None
            if run_status_holder["status"] != RUN_STATUS_DONE:
                return None

            # user_prompt 全局栅栏：
            # - 一旦存在“依赖已满足的 user_prompt 步骤”，暂停调度其他步骤；
            # - 先等待当前 running 的步骤全部收尾，再执行 user_prompt，避免进入 waiting 后仍有其他输出“收尾”。
            barrier_idx = waiting_barrier.get("idx")
            barrier_role = waiting_barrier.get("role")
            if barrier_idx is None:
                for cand in range(start_idx, end_idx + 1):
                    if cand in completed or cand in running:
                        continue
                    deps = dep_map[cand] if 0 <= cand < len(dep_map) else []
                    if any(d not in completed for d in deps):
                        continue
                    allow_c = list(plan_struct.steps[cand].allow or []) if 0 <= cand < total else []
                    if ACTION_TYPE_USER_PROMPT in set(allow_c or []):
                        waiting_barrier["idx"] = int(cand)
                        waiting_barrier["role"] = str(executor_for_step.get(int(cand)) or "").strip() or None
                        barrier_idx = waiting_barrier.get("idx")
                        barrier_role = waiting_barrier.get("role")
                        break
            else:
                # 若 barrier step 已结算则清空（避免卡住）。
                if int(barrier_idx) in completed:
                    waiting_barrier["idx"] = None
                    waiting_barrier["role"] = None
                    barrier_idx = None
                    barrier_role = None

            if barrier_idx is not None:
                # 仅允许 barrier 所属 role 在“无其他 running”时继续推进。
                if str(barrier_role or "") != str(role or ""):
                    return None
                if any(i != int(barrier_idx) for i in (running or set())):
                    return None

            # 找到当前 role 下依赖满足的最小 idx（稳定）
            for idx in range(start_idx, end_idx + 1):
                if idx in completed or idx in running:
                    continue
                if barrier_idx is not None and int(idx) != int(barrier_idx):
                    continue
                if executor_for_step.get(idx) != role:
                    continue
                deps = dep_map[idx] if 0 <= idx < len(dep_map) else []
                if any(d not in completed for d in deps):
                    continue

                # 标记 running + 更新 plan_struct
                running.add(idx)
                plan_struct.set_step_status(idx, "running")
                _emit(sse_plan_delta(task_id=int(task_id), run_id=int(run_id), plan_items=plan_struct.get_items_payload(), indices=[idx]))

                return idx
            return None

    def _mark_step_finished(idx: int, status: str) -> None:
        with state_lock:
            running.discard(idx)
            if status in {"done", "skipped"}:
                completed.add(idx)
            plan_struct.set_step_status(idx, status)
            _emit(sse_plan_delta(task_id=int(task_id), run_id=int(run_id), plan_items=plan_struct.get_items_payload(), indices=[idx]))

            next_step_order = _next_step_order_for_state()
            force = False
            run_status_for_persist: Optional[str] = None
            if status == "failed":
                force = True
                run_status_for_persist = RUN_STATUS_FAILED
            elif status == "waiting":
                force = True
                run_status_for_persist = RUN_STATUS_WAITING

            _mark_persist_dirty(
                "think_parallel.after_step",
                step_order=int(next_step_order),
                status=run_status_for_persist,
            )
            _maybe_flush_persist_locked(now=time.monotonic(), force=force)
            cond.notify_all()

    def _fail_run(step_order: int, error: str) -> None:
        with state_lock:
            if run_status_holder["status"] != RUN_STATUS_DONE:
                return
            run_status_holder["status"] = RUN_STATUS_FAILED
            first_error_holder["step_order"] = int(step_order)
            first_error_holder["error"] = str(error or "")
            last_step_order_holder["value"] = int(step_order)
            stop_event.set()
            cond.notify_all()

    def _wait_run(step_order: int) -> None:
        with state_lock:
            run_status_holder["status"] = RUN_STATUS_WAITING
            last_step_order_holder["value"] = int(step_order)
            stop_event.set()
            cond.notify_all()

    def _exec_one_step(role: str, idx: int) -> None:
        """
        真正执行一个步骤（在 executor 线程中运行）。
        """
        step_order = idx + 1
        step_obj = plan_struct.steps[idx]
        title = str(step_obj.title or "")
        allow = list(step_obj.allow or [])
        allow_text = " / ".join(allow) if allow else "(未限制)"

        _emit(sse_json({"delta": f"{STREAM_TAG_STEP} [{role}] {title}\n"}))

        # 并行 context 隔离：为每个 step 构造独立 context，避免跨线程污染。
        # 说明：task_output.content 允许为空时，会尝试用 context.last_llm_response 补齐；
        # 若共享 context，可能被其他 step 的 llm_call 覆盖而出现“串台输出”。
        with state_lock:
            step_context = dict(context or {})
            seed_last_llm = ""
            deps = dep_map[idx] if 0 <= idx < len(dep_map) else []
            # 选择“依赖中 step_order 最大”的 llm_call 输出作为 seed（确定性 + 接近串行）
            for d in sorted(deps or [], reverse=True):
                candidate = str(last_llm_by_idx.get(int(d)) or "").strip()
                if candidate:
                    seed_last_llm = candidate
                    break
            if not seed_last_llm:
                seed_last_llm = str(step_context.get("last_llm_response") or "").strip()
            step_context["last_llm_response"] = seed_last_llm

        # artifacts 门闩（并行版）：
        # - 并行执行器不做 plan_patch/replan/autofix，失败交给外层反思插入修复步骤；
        # - 只在“最终输出步骤（allow 含 task_output）”前做必要校验，避免“嘴上完成但未落盘/未验证”。
        allow_set = set(allow or [])
        if ACTION_TYPE_TASK_OUTPUT in allow_set:
            failed_steps: List[int] = []
            with state_lock:
                for s_idx, s in enumerate(plan_struct.steps):
                    if s.status == "failed":
                        failed_steps.append(int(s_idx) + 1)

            if failed_steps:
                _emit(sse_json({"delta": f"{STREAM_TAG_FAIL} 存在失败步骤，无法直接输出结果\n"}))
                safe_write_debug(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    message="agent.think.parallel.artifacts.failed_steps_block_output",
                    data={"failed_steps": failed_steps, "step_order": int(step_order)},
                    level="error",
                )
                _mark_step_finished(idx, "failed")
                _fail_run(step_order, f"prior_failed_steps:{failed_steps}")
                return

            with state_lock:
                has_http_requirement, has_http_success = _has_prior_http_success_step(
                    current_idx=int(idx),
                    plan_struct=plan_struct,
                )
            if has_http_requirement and not has_http_success:
                _emit(sse_json({"delta": f"{STREAM_TAG_FAIL} 缺少可验证的抓取证据，无法直接输出结果\n"}))
                safe_write_debug(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    message="agent.think.parallel.http_evidence.missing",
                    data={"step_order": int(step_order)},
                    level="error",
                )
                _mark_step_finished(idx, "failed")
                _fail_run(step_order, "http_evidence_missing")
                return

            if plan_struct.artifacts:
                missing = check_missing_artifacts(artifacts=list(plan_struct.artifacts or []), workdir=workdir)
                if missing:
                    _emit(sse_json({"delta": f"{STREAM_TAG_FAIL} 未生成文件：{', '.join(missing)}\n"}))
                    safe_write_debug(
                        task_id=int(task_id),
                        run_id=int(run_id),
                        message="agent.think.parallel.artifacts.missing",
                        data={"missing": missing, "step_order": int(step_order)},
                        level="error",
                    )
                    _mark_step_finished(idx, "failed")
                    _fail_run(step_order, f"missing_artifacts:{missing}")
                    return

                with state_lock:
                    has_validation = _has_success_validation_step(plan_struct)
                if not has_validation:
                    _emit(sse_json({"delta": f"{STREAM_TAG_FAIL} 缺少验证步骤，无法直接输出结果\n"}))
                    safe_write_debug(
                        task_id=int(task_id),
                        run_id=int(run_id),
                        message="agent.think.parallel.artifacts.validation_missing",
                        data={"step_order": int(step_order)},
                        level="error",
                    )
                    _mark_step_finished(idx, "failed")
                    _fail_run(step_order, "artifact_validation_missing")
                    return

        # 解析本步的模型/参数覆盖
        step_model = model
        step_params = dict(parameters or {})
        step_params.setdefault("temperature", 0.2)
        step_overrides: Dict = {}
        if step_llm_config_resolver:
            try:
                resolved_model, resolved_params = step_llm_config_resolver(step_order, title, list(allow or []))
            except (TypeError, ValueError, AttributeError, KeyError, RuntimeError) as exc:
                safe_write_debug(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    message="agent.think.parallel.step_llm_config_resolver.failed",
                    data={"step_order": int(step_order), "title": title, "error": str(exc)},
                    level="warning",
                )
                resolved_model, resolved_params = None, None
            if isinstance(resolved_model, str) and resolved_model.strip():
                step_model = resolved_model.strip()
            if isinstance(resolved_params, dict):
                step_overrides = dict(resolved_params)
                step_params.update(step_overrides)

        # 构造 prompt（观测取最后 3 条）
        with state_lock:
            obs_text = "\n".join(f"- {_truncate_observation(o)}" for o in observations[-3:]) or "(无)"

        react_prompt = AGENT_REACT_STEP_PROMPT_TEMPLATE.format(
            now=now_iso(),
            workdir=workdir,
            agent_workspace=AGENT_EXPERIMENT_DIR_REL,
            message=message,
            plan=plan_struct.get_titles_json(),
            step_index=step_order,
            step_title=title,
            allowed_actions=allow_text,
            observations=obs_text,
            graph=graph_hint,
            tools=tools_hint,
            skills=skills_hint,
            memories=memories_hint,
            output_style=ASSISTANT_OUTPUT_STYLE_GUIDE,
            action_types_line=action_types_line(),
        )
        # Think 并行执行器不支持 plan_patch：避免模型“输出了计划修正但实际上不会生效”造成隐性卡死。
        # 与 docs/agent 对齐：Think 失败由外层反思机制插入修复步骤，而不是在并行执行中做 plan_patch。
        react_prompt += "\n额外约束：当前为 Think 并行执行阶段，不支持 plan_patch。请不要输出 plan_patch 字段（或始终为 null）。\n"

        # 生成 action
        action_obj, action_type, payload_obj, action_validate_error, last_action_text = generate_action_with_retry(
            llm_call=llm_call,
            react_prompt=react_prompt,
            task_id=int(task_id),
            run_id=int(run_id),
            step_order=int(step_order),
            step_title=title,
            workdir=workdir,
            model=step_model,
            react_params=step_params,
            variables_source=variables_source,
        )

        if action_validate_error or not action_obj or not action_type:
            _emit(sse_json({"delta": f"{STREAM_TAG_FAIL} {title} action 无效：{action_validate_error}\n"}))
            _mark_step_finished(idx, "failed")
            _fail_run(step_order, action_validate_error or "invalid_action")
            return

        # allow gate：不满足则强制重问一次（与 react_loop_impl 行为一致）
        action_obj, action_type, payload_obj, allow_err = _enforce_allow_constraints(
            task_id=int(task_id),
            run_id=int(run_id),
            step_order=int(step_order),
            step_title=title,
            workdir=workdir,
            allowed=list(allow or []),
            allowed_text=allow_text,
            action_obj=action_obj,
            action_type=action_type,
            payload_obj=payload_obj or {},
            react_prompt=react_prompt,
            model=step_model,
            react_params=step_params,
            variables_source=variables_source,
            llm_call=llm_call,
        )
        if allow_err or not action_obj or not action_type:
            _emit(sse_json({"delta": f"{STREAM_TAG_FAIL} {title} allow 不满足：{allow_err}\n"}))
            _mark_step_finished(idx, "failed")
            _fail_run(step_order, allow_err or "allow_failure")
            return

        payload_obj = payload_obj or {}

        # task_output.content 兜底
        if action_type == ACTION_TYPE_TASK_OUTPUT:
            forced_obj, forced_type, forced_payload, fallback_err = handle_task_output_fallback(
                llm_call=llm_call,
                react_prompt=react_prompt,
                task_id=int(task_id),
                run_id=int(run_id),
                step_order=int(step_order),
                title=title,
                workdir=workdir,
                model=step_model,
                react_params=step_params,
                variables_source=variables_source,
                payload_obj=payload_obj,
                context=step_context,
                safe_write_debug=safe_write_debug,
            )
            if fallback_err:
                _emit(sse_json({"delta": f"{STREAM_TAG_FAIL} {fallback_err}\n"}))
                _mark_step_finished(idx, "failed")
                _fail_run(step_order, fallback_err)
                return
            if forced_obj:
                action_obj = forced_obj
                action_type = forced_type
                payload_obj = forced_payload or {}

        # user_prompt：进入 waiting（在并行模式下也要立刻收敛状态）
        if action_type == "user_prompt":
            for msg in handle_user_prompt_action(
                task_id=int(task_id),
                run_id=int(run_id),
                step_order=int(step_order),
                title=title,
                payload_obj=payload_obj,
                plan_struct=plan_struct,
                agent_state=agent_state,
                safe_write_debug=safe_write_debug,
                db_lock=db_lock,
            ):
                _emit(msg)
            _wait_run(step_order)
            _mark_step_finished(idx, "waiting")
            return

        # Think：llm_call 也要注入 step_model/overrides（否则执行器拿不到 executor 选模结果）
        if action_type == ACTION_TYPE_LLM_CALL and step_llm_config_resolver:
            payload_obj["model"] = step_model
            if step_overrides:
                params = payload_obj.get("parameters")
                if not isinstance(params, dict):
                    params = {}
                merged = dict(params)
                merged.update(step_overrides)
                payload_obj["parameters"] = merged

        # 落库 step + 执行
        detail = json.dumps({"type": action_type, "payload": payload_obj}, ensure_ascii=False)
        created_at = now_iso()
        with db_lock:
            step_id, _c, _u = create_task_step(
                TaskStepCreateParams(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    title=title,
                    status=STEP_STATUS_RUNNING,
                    executor=str(role or "").strip() or None,
                    detail=detail,
                    attempts=1,
                    started_at=created_at,
                    finished_at=None,
                    step_order=int(step_order),
                    created_at=created_at,
                    updated_at=created_at,
                )
            )
        step_row = {"id": step_id, "title": title, "detail": detail}
        result, step_error = execute_step_action(int(task_id), int(run_id), step_row, context=step_context)
        finished_at = now_iso()

        if step_error:
            with db_lock:
                mark_task_step_failed(
                    step_id=int(step_id),
                    error=str(step_error),
                    finished_at=finished_at,
                )
            with state_lock:
                if isinstance(step_context, dict):
                    step_context.pop("latest_parse_input_text", None)
                if isinstance(context, dict):
                    context.pop("latest_parse_input_text", None)
            _emit(sse_json({"delta": f"{STREAM_TAG_FAIL} {title}: {step_error}\n"}))
            _mark_step_finished(idx, "failed")
            _fail_run(step_order, str(step_error))
            return

        # 成功
        result_value = None
        if result is not None:
            try:
                result_value = json.dumps(result, ensure_ascii=False)
            except (TypeError, ValueError):
                result_value = json.dumps({"text": str(result)}, ensure_ascii=False)
        with db_lock:
            mark_task_step_done(step_id=int(step_id), result=result_value, finished_at=finished_at)

        _emit(sse_json({"delta": f"{STREAM_TAG_OK} {title}\n"}))

        # 观测 + 可见输出
        with state_lock:
            obs_line, visible_content = build_observation_line(
                action_type=str(action_type),
                title=title,
                result=result,
                context=step_context,
            )
            observations.append(obs_line)
            if str(action_type) == ACTION_TYPE_LLM_CALL:
                resp = str(step_context.get("last_llm_response") or "").strip()
                if resp:
                    last_llm_by_idx[int(idx)] = resp
                    if int(idx) >= int(max_llm_idx_holder["idx"]):
                        max_llm_idx_holder["idx"] = int(idx)
                        # 仅用“最大 step_order 的 llm_call 输出”更新全局 context，保证确定性。
                        context["last_llm_response"] = resp
            parse_source = str(step_context.get("latest_parse_input_text") or "").strip()
            if parse_source and int(idx) >= int(max_llm_idx_holder["idx"]):
                context["latest_parse_input_text"] = parse_source

        if visible_content:
            _emit(yield_visible_result(visible_content))
        if str(action_type) == "memory_write" and isinstance(result, dict):
            _emit(yield_memory_write_event(task_id=int(task_id), run_id=int(run_id), result=result))

        _mark_step_finished(idx, "done")
        with state_lock:
            # 失败后仍可能有其他线程收尾完成，但反思/错误定位应以“首次失败步骤”为准，
            # 避免 last_step_order 被后续完成步骤覆盖。
            if run_status_holder["status"] == RUN_STATUS_DONE:
                last_step_order_holder["value"] = max(int(last_step_order_holder["value"]), int(step_order))

    def _worker(role: str) -> None:
        while True:
            if stop_event.is_set():
                return
            idx = _pick_next_step_for_role(role)
            if idx is None:
                with state_lock:
                    # 全部完成 / waiting / failed -> 退出
                    remaining = [
                        i
                        for i in range(start_idx, end_idx + 1)
                        if i not in completed and plan_struct.steps[i].status != "done"
                    ]
                    if not remaining or run_status_holder["status"] != RUN_STATUS_DONE:
                        return
                    cond.wait(timeout=0.2)
                continue
            try:
                _exec_one_step(role, idx)
            except BaseException as exc:  # noqa: BLE001
                _emit(sse_json({"delta": f"{STREAM_TAG_FAIL} {plan_struct.steps[idx].title}: exception:{exc}\n"}))
                _mark_step_finished(idx, "failed")
                _fail_run(idx + 1, f"exception:{exc}")
                return

    # 启动
    yield sse_json({"delta": f"{STREAM_TAG_EXECUTOR} 启动依赖并行调度：roles={','.join(roles)}\n"})

    threads = [threading.Thread(target=_worker, args=(role,), daemon=True) for role in roles]
    for t in threads:
        t.start()

    # 主循环：把 out_q 的内容转发为 yield
    last_emit_at = time.monotonic()
    try:
        while True:
            try:
                msg = out_q.get(timeout=0.2)
                last_emit_at = time.monotonic()
                yield msg
            except queue.Empty:
                now_value = time.monotonic()
                deadlock_debug: Optional[dict] = None
                deadlock_emit: Optional[str] = None
                # 若全部结束且队列为空，收尾
                with state_lock:
                    # idle 期间按节流阈值补一次落盘，避免“最后一次完成恰好被节流”后长时间不写库。
                    _maybe_flush_persist_locked(now=now_value, force=False)
                    remaining = [
                        i
                        for i in range(start_idx, end_idx + 1)
                        if i not in completed and plan_struct.steps[i].status != "done"
                    ]
                    if run_status_holder["status"] != RUN_STATUS_DONE:
                        break
                    if not remaining and not running:
                        break

                    # 兜底：并行调度死锁检测
                    # 场景：剩余步骤全部"依赖未满足/依赖在本次区间外"，且当前无 running。
                    # 若不处理，会表现为长时间 idle + 心跳，任务永不结束。
                    if remaining and not running:
                        barrier_idx = waiting_barrier.get("idx")
                        # 若存在 barrier，则仅允许 barrier 步骤推进；否则按 normal 模式找任意可运行步骤。
                        runnable_exists = False
                        for cand in remaining:
                            if barrier_idx is not None and int(cand) != int(barrier_idx):
                                continue
                            deps = dep_map[int(cand)] if 0 <= int(cand) < len(dep_map) else []
                            if any(int(d) not in completed for d in (deps or [])):
                                continue
                            role = str(executor_for_step.get(int(cand)) or "").strip()
                            if role and role in roles:
                                runnable_exists = True
                                break

                        if not runnable_exists:
                            # 组装阻塞诊断（尽量短，避免 SSE 过长）。
                            blocked: List[dict] = []
                            for cand in list(remaining)[:5]:
                                deps = dep_map[int(cand)] if 0 <= int(cand) < len(dep_map) else []
                                missing = [int(d) for d in (deps or []) if int(d) not in completed]
                                missing_outside = [
                                    int(d)
                                    for d in missing
                                    if int(d) < int(start_idx) or int(d) > int(end_idx)
                                ]
                                blocked.append(
                                    {
                                        "step_order": int(cand) + 1,
                                        "title": str(plan_struct.steps[int(cand)].title or ""),
                                        "missing_step_orders": [int(d) + 1 for d in missing],
                                        "missing_outside_window_step_orders": [int(d) + 1 for d in missing_outside],
                                    }
                                )

                            deadlock_debug = {
                                "start_step_order": int(start_order),
                                "end_step_order_inclusive": int(end_idx) + 1,
                                "remaining_step_orders": [int(i) + 1 for i in list(remaining)[:20]],
                                "blocked_preview": blocked,
                                "barrier_step_order": (int(barrier_idx) + 1) if barrier_idx is not None else None,
                            }
                            deadlock_emit = (
                                f"{STREAM_TAG_FAIL} 并行调度死锁：无可运行步骤（remaining={len(remaining)}）"
                            )

                            # 标记失败：让外层反思/修复机制接管（避免无限等待）。
                            run_status_holder["status"] = RUN_STATUS_FAILED
                            deadlock_step_order = int(min(remaining)) + 1
                            first_error_holder["step_order"] = int(deadlock_step_order)
                            first_error_holder["error"] = "deadlock:no_runnable_steps"
                            last_step_order_holder["value"] = max(
                                int(last_step_order_holder["value"]), int(deadlock_step_order)
                            )
                            stop_event.set()

                            # 让 UI/计划状态可见（选择最早阻塞步骤标记 failed）。
                            deadlock_idx = int(min(remaining))
                            plan_struct.set_step_status(deadlock_idx, "failed")
                            _emit(
                                sse_plan_delta(
                                    task_id=int(task_id), run_id=int(run_id), plan_items=plan_struct.get_items_payload(), indices=[deadlock_idx]
                                )
                            )

                            # 失败状态必须立即落盘（不受节流影响）。
                            _mark_persist_dirty(
                                "think_parallel.deadlock",
                                step_order=int(deadlock_step_order),
                                status=RUN_STATUS_FAILED,
                            )
                            _maybe_flush_persist_locked(now=now_value, force=True)
                            cond.notify_all()

                if deadlock_emit:
                    safe_write_debug(
                        task_id=int(task_id),
                        run_id=int(run_id),
                        message="agent.think.parallel.deadlock",
                        data=deadlock_debug or {},
                        level="error",
                    )
                    yield sse_json({"delta": f"{deadlock_emit}\n"})
                    break

                # 避免长时间无输出造成 stream pump idle timeout（保持极弱心跳，不污染 UI）
                if time.monotonic() - last_emit_at > 15:
                    yield sse_json({"delta": f"{STREAM_TAG_EXEC} …\n"})
                    last_emit_at = time.monotonic()
                continue
    finally:
        stop_event.set()
        with state_lock:
            cond.notify_all()
        # 尽量快速退出；daemon 线程可由进程回收
        for t in threads:
            t.join(timeout=0.2)
        # 退出前强制落盘一次，避免节流导致最后若干步状态未写入 DB。
        with state_lock:
            if bool(persist_ctrl.get("dirty")):
                persist_ctrl["pending_where"] = "think_parallel.final_flush"
                _maybe_flush_persist_locked(now=time.monotonic(), force=True)

    status = str(run_status_holder["status"] or RUN_STATUS_DONE)
    last_step_order = int(last_step_order_holder["value"] or 0)
    if status == RUN_STATUS_DONE:
        # done：last_step_order 取已完成最大 step（便于审计）
        done_orders = [i + 1 for i in completed if start_idx <= i <= end_idx]
        if done_orders:
            last_step_order = max(done_orders)
        else:
            last_step_order = max(0, start_order - 1)

    if status == RUN_STATUS_DONE and start_idx > 0:
        yield sse_json({"delta": f"{STREAM_TAG_SKIP} 已跳过 {start_idx} 个已完成步骤\n"})

    yield sse_json({"delta": f"{STREAM_TAG_EXECUTOR} 并行调度结束，status={status}\n"})
    return ThinkParallelLoopResult(run_status=status, last_step_order=int(last_step_order))
