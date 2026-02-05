from backend.src.common.utils import now_iso
from backend.src.constants import (
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
        stopped_runs_running = count_task_runs_by_status(status=RUN_STATUS_RUNNING, conn=conn)
        stopped_runs_waiting = count_task_runs_by_status(status=RUN_STATUS_WAITING, conn=conn)
        stopped_tasks_running = count_tasks_by_status(status=STATUS_RUNNING, conn=conn)
        stopped_tasks_waiting = count_tasks_by_status(status=STATUS_WAITING, conn=conn)
        reset_steps_running = count_task_steps_by_status(status=STEP_STATUS_RUNNING, conn=conn)
        reset_steps_waiting = count_task_steps_by_status(status=STEP_STATUS_WAITING, conn=conn)

        # run：running -> stopped（本次尝试结束，因此写 finished_at）
        stop_all_running_task_runs(
            from_status=RUN_STATUS_RUNNING,
            to_status=RUN_STATUS_STOPPED,
            stopped_at=stopped_at,
            conn=conn,
        )
        stop_all_running_task_runs(
            from_status=RUN_STATUS_WAITING,
            to_status=RUN_STATUS_STOPPED,
            stopped_at=stopped_at,
            conn=conn,
        )

        # task：running -> stopped（任务整体未完成，不写 finished_at）
        stop_all_running_tasks(
            from_status=STATUS_RUNNING,
            to_status=STATUS_STOPPED,
            conn=conn,
        )
        stop_all_running_tasks(
            from_status=STATUS_WAITING,
            to_status=STATUS_STOPPED,
            conn=conn,
        )

        # step：running -> planned（便于下次继续执行时重新跑该步；避免 UI 长期显示 running）
        reset_all_running_steps_to_planned(
            from_status=STEP_STATUS_RUNNING,
            to_status=STEP_STATUS_PLANNED,
            updated_at=stopped_at,
            conn=conn,
        )
        reset_all_running_steps_to_planned(
            from_status=STEP_STATUS_WAITING,
            to_status=STEP_STATUS_PLANNED,
            updated_at=stopped_at,
            conn=conn,
        )

    return {
        "stopped_at": stopped_at,
        "reason": reason,
        "stopped_runs": int(stopped_runs_running) + int(stopped_runs_waiting),
        "stopped_tasks": int(stopped_tasks_running) + int(stopped_tasks_waiting),
        "reset_steps": int(reset_steps_running) + int(reset_steps_waiting),
        "details": {
            "stopped_runs_running": int(stopped_runs_running),
            "stopped_runs_waiting": int(stopped_runs_waiting),
            "stopped_tasks_running": int(stopped_tasks_running),
            "stopped_tasks_waiting": int(stopped_tasks_waiting),
            "reset_steps_running": int(reset_steps_running),
            "reset_steps_waiting": int(reset_steps_waiting),
        },
    }


def stop_task_run_records(*, task_id: int, run_id: int, reason: str) -> dict:
    """
    将指定 task/run 的 running 状态收敛到 stopped（用于 SSE 连接中断/主动取消）。

    说明：
    - stop_running_task_records() 是全局收敛（会影响所有 running），适合“应用启动/维护接口”；
    - 本函数是定向收敛，仅影响单个 task_id/run_id，避免误伤其他并发任务。
    """
    stopped_at = now_iso()
    with get_connection() as conn:
        run_row = get_task_run(run_id=int(run_id), conn=conn)
        task_row = get_task(task_id=int(task_id), conn=conn)
        reset_steps_running = count_task_steps_running_for_run(
            task_id=int(task_id),
            run_id=int(run_id),
            running_status=STEP_STATUS_RUNNING,
            conn=conn,
        )
        reset_steps_waiting = count_task_steps_running_for_run(
            task_id=int(task_id),
            run_id=int(run_id),
            running_status=STEP_STATUS_WAITING,
            conn=conn,
        )

        # run：running -> stopped（本次尝试结束，因此写 finished_at）
        stop_task_run_if_running(
            run_id=int(run_id),
            from_status=RUN_STATUS_RUNNING,
            to_status=RUN_STATUS_STOPPED,
            stopped_at=stopped_at,
            conn=conn,
        )
        stop_task_run_if_running(
            run_id=int(run_id),
            from_status=RUN_STATUS_WAITING,
            to_status=RUN_STATUS_STOPPED,
            stopped_at=stopped_at,
            conn=conn,
        )
        # task：running -> stopped（任务整体未完成，不写 finished_at）
        stop_task_if_running(
            task_id=int(task_id),
            from_status=STATUS_RUNNING,
            to_status=STATUS_STOPPED,
            conn=conn,
        )
        stop_task_if_running(
            task_id=int(task_id),
            from_status=STATUS_WAITING,
            to_status=STATUS_STOPPED,
            conn=conn,
        )
        # step：running -> planned（便于下次继续执行时重新跑该步）
        reset_running_steps_to_planned_for_run(
            task_id=int(task_id),
            run_id=int(run_id),
            from_status=STEP_STATUS_RUNNING,
            to_status=STEP_STATUS_PLANNED,
            updated_at=stopped_at,
            conn=conn,
        )
        reset_running_steps_to_planned_for_run(
            task_id=int(task_id),
            run_id=int(run_id),
            from_status=STEP_STATUS_WAITING,
            to_status=STEP_STATUS_PLANNED,
            updated_at=stopped_at,
            conn=conn,
        )

    return {
        "stopped_at": stopped_at,
        "reason": reason,
        "task_id": int(task_id),
        "run_id": int(run_id),
        "previous_task_status": (task_row["status"] if task_row else None),
        "previous_run_status": (run_row["status"] if run_row else None),
        "reset_steps": int(reset_steps_running) + int(reset_steps_waiting),
        "details": {
            "reset_steps_running": int(reset_steps_running),
            "reset_steps_waiting": int(reset_steps_waiting),
        },
    }
