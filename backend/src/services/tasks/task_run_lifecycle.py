import logging
import os
import threading
from typing import List, Optional

from backend.src.common.utils import is_test_env, now_iso
from backend.src.common.path_utils import normalize_windows_abs_path_on_posix
from backend.src.constants import (
    AGENT_TASK_FEEDBACK_STEP_TITLE,
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_RUNNING,
    RUN_STATUS_STOPPED,
    RUN_STATUS_WAITING,
    STATUS_DONE,
    STATUS_FAILED,
    STATUS_RUNNING,
    STATUS_STOPPED,
    STATUS_WAITING,
)
from backend.src.repositories.task_runs_repo import get_task_run, update_task_run
from backend.src.repositories.task_runs_repo import create_task_run as create_task_run_record
from backend.src.repositories.tasks_repo import create_task as create_task_record
from backend.src.repositories.tasks_repo import get_task, update_task
from backend.src.services.debug.debug_output import write_task_debug_output
from backend.src.services.tasks.task_recovery import stop_task_run_records
from backend.src.storage import get_connection

logger = logging.getLogger(__name__)


def _safe_debug(task_id: Optional[int], run_id: Optional[int], message: str, data: Optional[dict] = None, level: str = "debug") -> None:
    """
    生命周期收敛属于“兜底能力”，不应因为 debug 写入失败而影响主链路。
    """
    if task_id is None or run_id is None:
        return
    try:
        write_task_debug_output(
            task_id=int(task_id),
            run_id=int(run_id),
            message=message,
            data=data if isinstance(data, dict) else None,
            level=level,
        )
    except Exception:
        return


def normalize_run_status(run_status: object) -> str:
    value = str(run_status or "").strip()
    if value in {
        RUN_STATUS_DONE,
        RUN_STATUS_FAILED,
        RUN_STATUS_RUNNING,
        RUN_STATUS_STOPPED,
        RUN_STATUS_WAITING,
    }:
        return value
    return RUN_STATUS_FAILED


def create_task_and_run_records_for_agent(*, message: str, created_at: str) -> tuple[int, int]:
    """
    创建 task + run，确保即使后续规划失败也能在时间线里留下可追溯记录。
    """
    with get_connection() as conn:
        task_id, _ = create_task_record(
            title=str(message or "").strip() or "(empty)",
            status=STATUS_RUNNING,
            expectation_id=None,
            started_at=str(created_at or "").strip() or None,
            finished_at=None,
            created_at=str(created_at or "").strip() or None,
            conn=conn,
        )
        run_id, _, _ = create_task_run_record(
            task_id=int(task_id),
            status=RUN_STATUS_RUNNING,
            summary="agent_command_react",
            started_at=str(created_at or "").strip() or None,
            finished_at=None,
            created_at=str(created_at or "").strip() or None,
            updated_at=str(created_at or "").strip() or None,
            conn=conn,
        )
    return int(task_id), int(run_id)


def map_run_status_to_task_status(run_status: object) -> str:
    """
    run.status -> task.status 的映射（保持语义一致）：
    - waiting：任务等待用户输入
    - done：任务完成
    - stopped：本次执行中断但可继续
    - failed：执行失败
    """
    normalized = normalize_run_status(run_status)
    if normalized == RUN_STATUS_WAITING:
        return STATUS_WAITING
    if normalized == RUN_STATUS_DONE:
        return STATUS_DONE
    if normalized == RUN_STATUS_STOPPED:
        return STATUS_STOPPED
    if normalized == RUN_STATUS_RUNNING:
        return STATUS_RUNNING
    return STATUS_FAILED


def finalize_run_and_task_status(*, task_id: int, run_id: int, run_status: object) -> None:
    """
    把 run/task 收敛到目标状态。

    注意：
    - run: done/failed/stopped 会自动写 finished_at；waiting 不会写 finished_at（见 update_task_run）。
    - task: stopped 不写 finished_at（见 update_task）。
    """
    final_at = now_iso()
    normalized_run_status = normalize_run_status(run_status)
    task_status = map_run_status_to_task_status(normalized_run_status)
    update_task_run(run_id=int(run_id), status=normalized_run_status, updated_at=final_at)
    update_task(task_id=int(task_id), status=str(task_status), updated_at=final_at)


def mark_run_failed(*, task_id: int, run_id: int, reason: str) -> None:
    """
    兜底：把 run/task 从 running 收敛到 failed，避免 UI 永久卡住。
    """
    final_at = now_iso()
    try:
        update_task_run(run_id=int(run_id), status=RUN_STATUS_FAILED, updated_at=final_at)
        update_task(task_id=int(task_id), status=STATUS_FAILED, updated_at=final_at)
    except Exception as exc:
        logger.exception("mark_run_failed.persist_failed: %s", exc)
        _safe_debug(
            int(task_id),
            int(run_id),
            "task_run_lifecycle.mark_run_failed.persist_failed",
            {"reason": str(reason or "").strip(), "error": str(exc)},
            level="warning",
        )
        return
    _safe_debug(
        int(task_id),
        int(run_id),
        "task_run_lifecycle.mark_run_failed",
        {"reason": str(reason or "").strip()},
        level="warning",
    )


def check_missing_artifacts(*, artifacts: List[str], workdir: str) -> List[str]:
    """
    artifacts 校验：避免出现“嘴上完成但没有落盘”。
    """
    missing: List[str] = []
    base = str(workdir or "").strip() or os.getcwd()
    for rel in artifacts or []:
        raw = str(rel or "").strip()
        if not raw:
            continue
        candidate = normalize_windows_abs_path_on_posix(raw)
        if not os.path.isabs(candidate):
            candidate = os.path.abspath(os.path.join(base, candidate))
        if not os.path.exists(candidate):
            missing.append(raw)
    return missing


def enqueue_review_on_feedback_waiting(*, task_id: int, run_id: int, agent_state: object) -> None:
    """
    waiting（确认满意度）时触发一次 ensure_agent_review_record，避免前端误以为评估没触发。
    """
    paused = None
    if isinstance(agent_state, dict):
        paused = agent_state.get("paused")
    step_title = str(paused.get("step_title") or "").strip() if isinstance(paused, dict) else ""
    if step_title != AGENT_TASK_FEEDBACK_STEP_TITLE:
        return

    def _worker() -> None:
        try:
            from backend.src.services.tasks.task_postprocess import ensure_agent_review_record

            ensure_agent_review_record(task_id=int(task_id), run_id=int(run_id), skills=[], force=False)
        except Exception:
            return

    try:
        if is_test_env():
            _worker()
        else:
            threading.Thread(target=_worker, daemon=True).start()
    except Exception:
        _worker()


def enqueue_postprocess_thread(*, task_id: int, run_id: int, run_status: object) -> None:
    """
    后处理闭环（后台线程）：
    - done：完整后处理（评估/技能/图谱/记忆兜底）
    - failed/stopped：至少确保评估记录可见（agent_review_records）
    """
    normalized = normalize_run_status(run_status)
    if normalized not in {RUN_STATUS_DONE, RUN_STATUS_FAILED, RUN_STATUS_STOPPED}:
        return

    def _worker() -> None:
        try:
            from backend.src.services.tasks.task_postprocess import (
                ensure_agent_review_record,
                postprocess_task_run,
            )

            task_row = get_task(task_id=int(task_id))
            if not task_row:
                return
            _safe_debug(
                int(task_id),
                int(run_id),
                "task_run_lifecycle.postprocess.started",
                {"run_status": normalized},
                level="info",
            )
            if normalized == RUN_STATUS_DONE:
                postprocess_task_run(
                    task_row=task_row,
                    task_id=int(task_id),
                    run_id=int(run_id),
                    run_status=RUN_STATUS_DONE,
                )
            else:
                ensure_agent_review_record(task_id=int(task_id), run_id=int(run_id), skills=[])
            _safe_debug(
                int(task_id),
                int(run_id),
                "task_run_lifecycle.postprocess.done",
                {"run_status": normalized},
                level="info",
            )
        except Exception as exc:
            logger.exception("postprocess failed: %s", exc)
            _safe_debug(
                int(task_id),
                int(run_id),
                "task_run_lifecycle.postprocess.failed",
                {"run_status": normalized, "error": str(exc)},
                level="warning",
            )
            return

    try:
        if is_test_env():
            _worker()
        else:
            threading.Thread(target=_worker, daemon=True).start()
    except Exception:
        _worker()


def enqueue_stop_task_run_records(*, task_id: Optional[int], run_id: int, reason: str) -> None:
    """
    SSE 断连/主动取消时的定向收敛：把单个 run/task 的 running/waiting 收敛到 stopped，
    并回退 running/waiting step 为 planned（便于下次继续执行）。
    """

    def _worker() -> None:
        resolved_task_id = int(task_id) if task_id is not None else None
        if resolved_task_id is None:
            try:
                row = get_task_run(run_id=int(run_id))
                if row:
                    resolved_task_id = int(row["task_id"])
            except Exception:
                resolved_task_id = None

        if resolved_task_id is None:
            return

        try:
            stop_task_run_records(
                task_id=int(resolved_task_id),
                run_id=int(run_id),
                reason=str(reason or "").strip() or "stream_cancelled",
            )
        except Exception as exc:
            logger.exception("stop_task_run_records failed: %s", exc)
            _safe_debug(
                int(resolved_task_id),
                int(run_id),
                "task_run_lifecycle.stop_task_run_records_failed",
                {"reason": str(reason or "").strip(), "error": str(exc)},
                level="warning",
            )
            return
        _safe_debug(
            int(resolved_task_id),
            int(run_id),
            "task_run_lifecycle.stream.cancelled",
            {"reason": str(reason or "").strip()},
            level="warning",
        )

    try:
        if is_test_env():
            _worker()
        else:
            threading.Thread(target=_worker, daemon=True).start()
    except Exception:
        _worker()
