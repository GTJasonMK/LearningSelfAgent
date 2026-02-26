from typing import Optional

from backend.src.common.utils import coerce_int, now_iso
from backend.src.constants import (
    LLM_STATUS_ERROR,
    LLM_STATUS_RUNNING,
    RUN_STATUS_RUNNING,
    RUN_STATUS_STOPPED,
    RUN_STATUS_WAITING,
    STATUS_RUNNING,
    STATUS_STOPPED,
    STATUS_WAITING,
    STEP_STATUS_PLANNED,
    STEP_STATUS_RUNNING,
    STEP_STATUS_WAITING,
)
from backend.src.repositories.task_runs_repo import (
    count_task_runs_by_status,
    get_task_run,
    stop_all_running_task_runs,
    stop_task_run_if_running,
)
from backend.src.repositories.task_steps_repo import (
    count_task_steps_by_status,
    count_task_steps_running_for_run,
    reset_all_running_steps_to_planned,
    reset_running_steps_to_planned_for_run,
)
from backend.src.repositories.tasks_repo import (
    count_tasks_by_status,
    get_task,
    stop_all_running_tasks,
    stop_task_if_running,
)
from backend.src.storage import get_connection


_RUN_IN_PROGRESS_STATUSES = (RUN_STATUS_RUNNING, RUN_STATUS_WAITING)
_TASK_IN_PROGRESS_STATUSES = (STATUS_RUNNING, STATUS_WAITING)
_STEP_IN_PROGRESS_STATUSES = (STEP_STATUS_RUNNING, STEP_STATUS_WAITING)


def _status_count(counter: dict, status: str) -> int:
    return coerce_int(counter.get(status), default=0)


def _sum_status_counts(counter: dict, statuses: tuple[str, ...]) -> int:
    return sum(_status_count(counter, status) for status in statuses)


def _mark_running_llm_records_error(
    *,
    conn,
    reason: str,
    stopped_at: str,
    run_id: Optional[int] = None,
) -> int:
    if run_id is not None:
        where = "run_id = ? AND status = ?"
        where_params = (int(run_id), LLM_STATUS_RUNNING)
    else:
        where = "status = ?"
        where_params = (LLM_STATUS_RUNNING,)
    update_params = (
        LLM_STATUS_ERROR,
        f"aborted:{reason}",
        stopped_at,
        stopped_at,
        *where_params,
    )
    count_sql = f"SELECT COUNT(*) FROM llm_records WHERE {where}"
    update_sql = f"UPDATE llm_records SET status = ?, error = ?, finished_at = ?, updated_at = ? WHERE {where}"
    try:
        row = conn.execute(count_sql, where_params).fetchone()
        count = coerce_int(row[0] if row else 0, default=0)
    except Exception:
        count = 0
    try:
        conn.execute(update_sql, update_params)
    except Exception:
        pass
    return count


def stop_running_task_records(reason: str) -> dict:
    """
    将异常中断遗留的 running 状态统一收敛到 stopped。

    设计目标：
    - Electron 直接 kill uvicorn 时，tasks/task_runs/task_steps 的 finally 可能不会执行，导致 UI 永远显示“正在执行”。
    - stopped 表示“本次执行尝试已结束但任务未完成”，便于用户后续重新点击继续执行（会创建新的 run）。
    """

    stopped_at = now_iso()
    with get_connection() as conn:
        # running/waiting 都属于“非终态且会占用 UI 的进行中状态”，应用退出时应统一收敛到 stopped，
        # 避免用户下次打开仍看到“正在执行/等待输入”而误以为卡死。
        run_counts = {
            status: count_task_runs_by_status(status=status, conn=conn)
            for status in _RUN_IN_PROGRESS_STATUSES
        }
        task_counts = {
            status: count_tasks_by_status(status=status, conn=conn)
            for status in _TASK_IN_PROGRESS_STATUSES
        }
        step_counts = {
            status: count_task_steps_by_status(status=status, conn=conn)
            for status in _STEP_IN_PROGRESS_STATUSES
        }

        # run：running -> stopped（本次尝试结束，因此写 finished_at）
        for status in _RUN_IN_PROGRESS_STATUSES:
            stop_all_running_task_runs(
                from_status=status,
                to_status=RUN_STATUS_STOPPED,
                stopped_at=stopped_at,
                conn=conn,
            )

        # task：running -> stopped（任务整体未完成，不写 finished_at）
        for status in _TASK_IN_PROGRESS_STATUSES:
            stop_all_running_tasks(
                from_status=status,
                to_status=STATUS_STOPPED,
                conn=conn,
            )

        # step：running -> planned（便于下次继续执行时重新跑该步；避免 UI 长期显示 running）
        for status in _STEP_IN_PROGRESS_STATUSES:
            reset_all_running_steps_to_planned(
                from_status=status,
                to_status=STEP_STATUS_PLANNED,
                updated_at=stopped_at,
                conn=conn,
            )

        # llm_records：running -> error（同步调用被 kill/崩溃时，可能残留 running，影响 UI/排查）
        stopped_llm_records = _mark_running_llm_records_error(
            conn=conn,
            reason=reason,
            stopped_at=stopped_at,
            run_id=None,
        )

    return {
        "stopped_at": stopped_at,
        "reason": reason,
        "stopped_runs": _sum_status_counts(run_counts, _RUN_IN_PROGRESS_STATUSES),
        "stopped_tasks": _sum_status_counts(task_counts, _TASK_IN_PROGRESS_STATUSES),
        "reset_steps": _sum_status_counts(step_counts, _STEP_IN_PROGRESS_STATUSES),
        "stopped_llm_records": coerce_int(stopped_llm_records, default=0),
        "details": {
            "stopped_runs_running": _status_count(run_counts, RUN_STATUS_RUNNING),
            "stopped_runs_waiting": _status_count(run_counts, RUN_STATUS_WAITING),
            "stopped_tasks_running": _status_count(task_counts, STATUS_RUNNING),
            "stopped_tasks_waiting": _status_count(task_counts, STATUS_WAITING),
            "reset_steps_running": _status_count(step_counts, STEP_STATUS_RUNNING),
            "reset_steps_waiting": _status_count(step_counts, STEP_STATUS_WAITING),
            "stopped_llm_records": coerce_int(stopped_llm_records, default=0),
        },
    }


def stop_task_run_records(*, task_id: int, run_id: int, reason: str) -> dict:
    """
    将指定 task/run 的 running 状态收敛到 stopped（用于 SSE 连接中断/主动取消）。

    说明：
    - stop_running_task_records() 是全局收敛（会影响所有 running），适合“应用启动/维护接口”；
    - 本函数是定向收敛，仅影响单个 task_id/run_id，避免误伤其他并发任务。
    """
    task_id_value = int(task_id)
    run_id_value = int(run_id)
    stopped_at = now_iso()
    with get_connection() as conn:
        run_row = get_task_run(run_id=run_id_value, conn=conn)
        task_row = get_task(task_id=task_id_value, conn=conn)
        step_counts = {
            status: count_task_steps_running_for_run(
                task_id=task_id_value,
                run_id=run_id_value,
                running_status=status,
                conn=conn,
            )
            for status in _STEP_IN_PROGRESS_STATUSES
        }

        # run：running -> stopped（本次尝试结束，因此写 finished_at）
        for status in _RUN_IN_PROGRESS_STATUSES:
            stop_task_run_if_running(
                run_id=run_id_value,
                from_status=status,
                to_status=RUN_STATUS_STOPPED,
                stopped_at=stopped_at,
                conn=conn,
            )
        # task：running -> stopped（任务整体未完成，不写 finished_at）
        for status in _TASK_IN_PROGRESS_STATUSES:
            stop_task_if_running(
                task_id=task_id_value,
                from_status=status,
                to_status=STATUS_STOPPED,
                conn=conn,
            )
        # step：running -> planned（便于下次继续执行时重新跑该步）
        for status in _STEP_IN_PROGRESS_STATUSES:
            reset_running_steps_to_planned_for_run(
                task_id=task_id_value,
                run_id=run_id_value,
                from_status=status,
                to_status=STEP_STATUS_PLANNED,
                updated_at=stopped_at,
                conn=conn,
            )

        # llm_records：running -> error（定向收敛，避免误伤其他并发 run）
        stopped_llm_records = _mark_running_llm_records_error(
            conn=conn,
            reason=reason,
            stopped_at=stopped_at,
            run_id=run_id_value,
        )

    return {
        "stopped_at": stopped_at,
        "reason": reason,
        "task_id": task_id_value,
        "run_id": run_id_value,
        "previous_task_status": (task_row["status"] if task_row else None),
        "previous_run_status": (run_row["status"] if run_row else None),
        "reset_steps": _sum_status_counts(step_counts, _STEP_IN_PROGRESS_STATUSES),
        "stopped_llm_records": coerce_int(stopped_llm_records, default=0),
        "details": {
            "reset_steps_running": _status_count(step_counts, STEP_STATUS_RUNNING),
            "reset_steps_waiting": _status_count(step_counts, STEP_STATUS_WAITING),
            "stopped_llm_records": coerce_int(stopped_llm_records, default=0),
        },
    }
