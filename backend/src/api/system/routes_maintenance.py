import json
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from fastapi import APIRouter

from backend.src.api.schemas import (
    CleanupJobCreate,
    CleanupJobUpdate,
    MaintenanceCleanupRequest,
    MaintenanceKnowledgeAutoDeprecateRequest,
    MaintenanceKnowledgeRollbackRequest,
    MaintenanceKnowledgeRollbackVersionRequest,
    MaintenanceKnowledgeValidateTagsRequest,
    MaintenanceKnowledgeDedupeSkillsRequest,
)
from backend.src.api.utils import ensure_write_permission, error_response, now_iso
from backend.src.constants import (
    CLEANUP_MODE_ARCHIVE,
    CLEANUP_MODE_DELETE,
    CLEANUP_JOB_STATUS_DISABLED,
    CLEANUP_JOB_STATUS_ENABLED,
    CLEANUP_RUN_STATUS_FAILED,
    CLEANUP_RUN_STATUS_SUCCESS,
    DEFAULT_CLEANUP_LIMIT,
    DEFAULT_CLEANUP_JOB_INTERVAL_MINUTES,
    ERROR_CODE_INVALID_REQUEST,
    HTTP_STATUS_BAD_REQUEST,
)
from backend.src.storage import get_connection
from backend.src.services.tasks.task_recovery import stop_running_task_records
from backend.src.services.knowledge.knowledge_governance import (
    auto_deprecate_low_quality_knowledge,
    dedupe_and_merge_skills,
    rollback_skill_to_previous_version,
    rollback_tool_to_previous_version,
    rollback_knowledge_from_run,
    validate_and_fix_skill_tags,
)

router = APIRouter()

_scheduler_started = False
_scheduler_lock = threading.Lock()

_CLEANUP_TABLES = {
    "chat_messages": "created_at",
    "llm_records": "created_at",
    "tool_call_records": "created_at",
    "search_records": "created_at",
    "eval_records": "created_at",
    "eval_criteria_records": "created_at",
    "agent_review_records": "created_at",
    "tasks": "created_at",
    "task_steps": "created_at",
    "task_outputs": "created_at",
    "task_runs": "created_at",
}


@router.post("/maintenance/stop-running")
def maintenance_stop_running() -> dict:
    """
    终止遗留的 running 状态（用于应用退出/崩溃后的自恢复）。

    典型场景：Electron 退出时 kill 掉 uvicorn，导致 tasks/task_runs/task_steps 的 finally 没机会把状态从 running 改回去。
    """
    permission = ensure_write_permission()
    if permission:
        return permission
    return {"result": stop_running_task_records(reason="maintenance_api")}


@router.post("/maintenance/knowledge/rollback")
def maintenance_knowledge_rollback(payload: MaintenanceKnowledgeRollbackRequest) -> dict:
    """
    知识治理：一键回滚/废弃某次 run 产生的知识（skills/tools）。

    典型场景：
    - 某次 run 沉淀了错误的技能/工具，导致后续检索与规划被污染；
    - 需要快速把这次 run 的知识降级为 deprecated/abandoned 或工具 rejected。
    """
    permission = ensure_write_permission()
    if permission:
        return permission
    return rollback_knowledge_from_run(
        run_id=int(payload.run_id),
        dry_run=bool(payload.dry_run) if payload.dry_run is not None else False,
        include_skills=bool(payload.include_skills) if payload.include_skills is not None else True,
        include_tools=bool(payload.include_tools) if payload.include_tools is not None else True,
        draft_skill_target_status=str(payload.draft_skill_target_status or "").strip().lower()
        or "abandoned",
        approved_skill_target_status=str(payload.approved_skill_target_status or "").strip().lower()
        or "deprecated",
        tool_target_status=str(payload.tool_target_status or "").strip().lower() or "rejected",
        reason=str(payload.reason or "").strip() or None,
    )


@router.post("/maintenance/knowledge/auto-deprecate")
def maintenance_knowledge_auto_deprecate(payload: MaintenanceKnowledgeAutoDeprecateRequest) -> dict:
    """
    知识治理：按“最近成功率/复用验证”信号自动废弃低质量知识（skills/tools）。
    """
    permission = ensure_write_permission()
    if permission:
        return permission

    try:
        since_days = int(payload.since_days) if payload.since_days is not None else 30
    except Exception:
        since_days = 30
    try:
        min_calls = int(payload.min_calls) if payload.min_calls is not None else 3
    except Exception:
        min_calls = 3
    try:
        threshold = float(payload.success_rate_threshold) if payload.success_rate_threshold is not None else 0.3
    except Exception:
        threshold = 0.3

    return auto_deprecate_low_quality_knowledge(
        since_days=int(since_days),
        min_calls=int(min_calls),
        success_rate_threshold=float(threshold),
        dry_run=bool(payload.dry_run) if payload.dry_run is not None else False,
        include_skills=bool(payload.include_skills) if payload.include_skills is not None else True,
        include_tools=bool(payload.include_tools) if payload.include_tools is not None else True,
        reason=str(payload.reason or "").strip() or None,
    )


@router.post("/maintenance/knowledge/rollback-version")
def maintenance_knowledge_rollback_version(payload: MaintenanceKnowledgeRollbackVersionRequest) -> dict:
    """
    知识治理：一键回滚到上一版本（skills/tools）。
    """
    permission = ensure_write_permission()
    if permission:
        return permission

    kind = str(payload.kind or "").strip().lower()
    rid = int(payload.id)
    dry_run = bool(payload.dry_run) if payload.dry_run is not None else False
    reason = str(payload.reason or "").strip() or None
    if kind == "skill":
        return rollback_skill_to_previous_version(skill_id=int(rid), dry_run=dry_run, reason=reason)
    if kind == "tool":
        return rollback_tool_to_previous_version(tool_id=int(rid), dry_run=dry_run, reason=reason)
    return error_response(code=ERROR_CODE_INVALID_REQUEST, message="invalid_kind", status_code=HTTP_STATUS_BAD_REQUEST)


@router.post("/maintenance/knowledge/validate-tags")
def maintenance_knowledge_validate_tags(payload: MaintenanceKnowledgeValidateTagsRequest) -> dict:
    """
    知识治理：校验/修复 skills_items.tags。
    """
    permission = ensure_write_permission()
    if permission:
        return permission

    dry_run = bool(payload.dry_run) if payload.dry_run is not None else True
    fix = bool(payload.fix) if payload.fix is not None else False
    strict_keys = bool(payload.strict_keys) if payload.strict_keys is not None else False
    include_draft = bool(payload.include_draft) if payload.include_draft is not None else True
    try:
        limit = int(payload.limit) if payload.limit is not None else 5000
    except Exception:
        limit = 5000

    return validate_and_fix_skill_tags(
        dry_run=dry_run,
        fix=fix,
        strict_keys=strict_keys,
        include_draft=include_draft,
        limit=int(limit),
    )


@router.post("/maintenance/knowledge/dedupe-skills")
def maintenance_knowledge_dedupe_skills(payload: MaintenanceKnowledgeDedupeSkillsRequest) -> dict:
    """
    知识治理：去重 + 版本合并（同 scope/name）。
    """
    permission = ensure_write_permission()
    if permission:
        return permission

    dry_run = bool(payload.dry_run) if payload.dry_run is not None else True
    include_draft = bool(payload.include_draft) if payload.include_draft is not None else True
    merge_across_domains = bool(payload.merge_across_domains) if payload.merge_across_domains is not None else False
    reason = str(payload.reason or "").strip() or None

    return dedupe_and_merge_skills(
        dry_run=dry_run,
        include_draft=include_draft,
        merge_across_domains=merge_across_domains,
        reason=reason,
    )


def _ensure_interval_column() -> None:
    with get_connection() as conn:
        columns = [
            row["name"] for row in conn.execute("PRAGMA table_info(cleanup_jobs)").fetchall()
        ]
        if "interval_minutes" not in columns:
            conn.execute("ALTER TABLE cleanup_jobs ADD COLUMN interval_minutes INTEGER")


def _resolve_cutoff(retention_days: Optional[int], before: Optional[str]) -> Optional[str]:
    if before:
        return before
    if retention_days is None:
        return None
    now_dt = datetime.fromisoformat(now_iso().replace("Z", "+00:00"))
    cutoff_dt = now_dt - timedelta(days=retention_days)
    return cutoff_dt.isoformat().replace("+00:00", "Z")


def _ensure_archive_table(table_name: str) -> None:
    archive_table = f"archive_{table_name}"
    with get_connection() as conn:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {archive_table} AS SELECT * FROM {table_name} WHERE 0"
        )
        columns = [
            row["name"]
            for row in conn.execute(f"PRAGMA table_info({archive_table})").fetchall()
        ]
        if "archived_at" not in columns:
            conn.execute(f"ALTER TABLE {archive_table} ADD COLUMN archived_at TEXT")


def _fetch_ids(
    table_name: str,
    time_column: str,
    cutoff: str,
    limit: Optional[int],
) -> List[int]:
    limit_value = limit or DEFAULT_CLEANUP_LIMIT
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT id FROM {table_name} WHERE {time_column} <= ? ORDER BY id ASC LIMIT ?",
            (cutoff, limit_value),
        ).fetchall()
    return [row["id"] for row in rows]


def _archive_rows(
    table_name: str, ids: List[int], archived_at: str
) -> Tuple[int, str]:
    if not ids:
        return 0, f"archive_{table_name}"
    _ensure_archive_table(table_name)
    archive_table = f"archive_{table_name}"
    with get_connection() as conn:
        columns = [
            row["name"]
            for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        ]
        columns = [name for name in columns if name != "archived_at"]
        placeholders = ",".join(["?"] * len(ids))
        column_list = ", ".join(columns)
        conn.execute(
            f"INSERT INTO {archive_table} ({column_list}, archived_at) "
            f"SELECT {column_list}, ? FROM {table_name} WHERE id IN ({placeholders})",
            [archived_at, *ids],
        )
        conn.execute(
            f"DELETE FROM {table_name} WHERE id IN ({placeholders})",
            ids,
        )
    return len(ids), archive_table


def _delete_rows(table_name: str, ids: List[int]) -> int:
    if not ids:
        return 0
    placeholders = ",".join(["?"] * len(ids))
    with get_connection() as conn:
        conn.execute(
            f"DELETE FROM {table_name} WHERE id IN ({placeholders})",
            ids,
        )
    return len(ids)


def _serialize_job_row(row) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "status": row["status"],
        "mode": row["mode"],
        "tables": json.loads(row["tables"]) if row["tables"] else [],
        "retention_days": row["retention_days"],
        "before": row["before"],
        "limit": row["limit_value"],
        "interval_minutes": row["interval_minutes"],
        "last_run_at": row["last_run_at"],
        "next_run_at": row["next_run_at"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _create_job_run(job_id: int, run_at: str) -> int:
    with get_connection() as conn:
        cursor = conn.execute(
            "INSERT INTO cleanup_job_runs (job_id, status, run_at, finished_at, summary, detail) VALUES (?, ?, ?, ?, ?, ?)",
            (job_id, CLEANUP_RUN_STATUS_SUCCESS, run_at, None, None, None),
        )
        return cursor.lastrowid


def _update_job_run(run_id: int, status: str, summary: str, detail: Optional[str]) -> None:
    finished_at = now_iso()
    with get_connection() as conn:
        conn.execute(
            "UPDATE cleanup_job_runs SET status = ?, finished_at = ?, summary = ?, detail = ? WHERE id = ?",
            (status, finished_at, summary, detail, run_id),
        )


def _advance_job_schedule(job_row, run_at: str) -> Optional[str]:
    """
    先推进 next_run_at（避免调度器阻塞/重复触发）。

    说明：
    - 原实现是在 job 执行完成后更新 next_run_at，若改为异步调度会出现“下一轮扫描仍认为任务到期”的重复触发；
    - 因此把 schedule 推进放到启动线程前同步完成。
    """
    try:
        interval_minutes = int(job_row["interval_minutes"])
    except Exception:
        interval_minutes = DEFAULT_CLEANUP_JOB_INTERVAL_MINUTES
    next_run = _compute_next_run(job_row["next_run_at"], interval_minutes)
    with get_connection() as conn:
        conn.execute(
            "UPDATE cleanup_jobs SET last_run_at = ?, next_run_at = ?, updated_at = ? WHERE id = ?",
            (run_at, next_run, now_iso(), job_row["id"]),
        )
    return next_run


def _run_job_once(job_row, run_at: str) -> None:
    run_id = _create_job_run(job_row["id"], run_at)
    payload = MaintenanceCleanupRequest(
        mode=job_row["mode"],
        tables=json.loads(job_row["tables"]) if job_row["tables"] else None,
        retention_days=job_row["retention_days"],
        before=job_row["before"],
        limit=job_row["limit_value"],
        dry_run=False,
    )
    try:
        result = _cleanup_execute(payload)
        summary = json.dumps(result.get("summary", {}), ensure_ascii=False)
        detail = json.dumps(result.get("items", []), ensure_ascii=False)
        _update_job_run(run_id, CLEANUP_RUN_STATUS_SUCCESS, summary, detail)
    except Exception as exc:
        _update_job_run(
            run_id,
            CLEANUP_RUN_STATUS_FAILED,
            "cleanup_failed",
            f"{exc}",
        )


def _run_job_async(job_row) -> None:
    run_at = now_iso()
    try:
        _advance_job_schedule(job_row, run_at)
    except Exception:
        # schedule 推进失败不应阻塞清理执行：最多导致下一轮仍会触发，再由 job_run 记录体现异常。
        pass
    thread = threading.Thread(target=_run_job_once, args=(job_row, run_at), daemon=True)
    thread.start()


def _compute_next_run(current_next: Optional[str], interval_minutes: int) -> str:
    try:
        interval_minutes = int(interval_minutes)
    except Exception:
        interval_minutes = DEFAULT_CLEANUP_JOB_INTERVAL_MINUTES
    if interval_minutes <= 0:
        interval_minutes = DEFAULT_CLEANUP_JOB_INTERVAL_MINUTES
    now_dt = datetime.fromisoformat(now_iso().replace("Z", "+00:00"))
    base = now_dt
    if current_next:
        try:
            base = datetime.fromisoformat(current_next.replace("Z", "+00:00"))
        except ValueError:
            base = now_dt
    next_dt = base + timedelta(minutes=interval_minutes)
    # 避免 next_run_at 仍落在过去导致调度器“追赶式无限运行”
    if next_dt <= now_dt:
        next_dt = now_dt + timedelta(minutes=interval_minutes)
    return next_dt.isoformat().replace("+00:00", "Z")


def _scheduler_loop() -> None:
    while True:
        now = datetime.fromisoformat(now_iso().replace("Z", "+00:00"))
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM cleanup_jobs WHERE status = ? AND next_run_at IS NOT NULL",
                (CLEANUP_JOB_STATUS_ENABLED,),
            ).fetchall()
        for row in rows:
            try:
                next_run = datetime.fromisoformat(row["next_run_at"].replace("Z", "+00:00"))
            except ValueError:
                next_run = now
            if next_run <= now:
                _run_job_async(row)
        time.sleep(5)


def _ensure_scheduler_started() -> None:
    global _scheduler_started
    with _scheduler_lock:
        if _scheduler_started:
            return
        _ensure_interval_column()
        _scheduler_started = True
        thread = threading.Thread(target=_scheduler_loop, daemon=True)
        thread.start()


@router.post("/maintenance/cleanup")
def cleanup_records(payload: MaintenanceCleanupRequest) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    return _cleanup_execute(payload)


def _cleanup_execute(payload: MaintenanceCleanupRequest) -> dict:
    mode = payload.mode or CLEANUP_MODE_DELETE
    if mode not in {CLEANUP_MODE_DELETE, CLEANUP_MODE_ARCHIVE}:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            "非法清理模式",
            HTTP_STATUS_BAD_REQUEST,
        )
    cutoff = _resolve_cutoff(payload.retention_days, payload.before)
    if not cutoff:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            "缺少保留天数或截止时间",
            HTTP_STATUS_BAD_REQUEST,
        )
    tables = payload.tables or list(_CLEANUP_TABLES.keys())
    invalid_tables = [name for name in tables if name not in _CLEANUP_TABLES]
    if invalid_tables:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            f"不支持的表: {', '.join(invalid_tables)}",
            HTTP_STATUS_BAD_REQUEST,
        )
    limit = payload.limit or DEFAULT_CLEANUP_LIMIT
    dry_run = bool(payload.dry_run)
    archived_at = now_iso()
    results: List[Dict] = []
    for table_name in tables:
        ids = _fetch_ids(table_name, _CLEANUP_TABLES[table_name], cutoff, limit)
        if dry_run:
            results.append(
                {
                    "table": table_name,
                    "mode": mode,
                    "cutoff": cutoff,
                    "count": len(ids),
                    "archived_table": f"archive_{table_name}"
                    if mode == CLEANUP_MODE_ARCHIVE
                    else None,
                }
            )
            continue
        if mode == CLEANUP_MODE_ARCHIVE:
            count, archive_table = _archive_rows(table_name, ids, archived_at)
            results.append(
                {
                    "table": table_name,
                    "mode": mode,
                    "cutoff": cutoff,
                    "count": count,
                    "archived_table": archive_table,
                }
            )
        else:
            count = _delete_rows(table_name, ids)
            results.append(
                {
                    "table": table_name,
                    "mode": mode,
                    "cutoff": cutoff,
                    "count": count,
                }
            )
    return {
        "summary": {
            "mode": mode,
            "cutoff": cutoff,
            "dry_run": dry_run,
            "limit": limit,
        },
        "items": results,
    }


@router.post("/maintenance/jobs")
def create_cleanup_job(payload: CleanupJobCreate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    mode = payload.mode or CLEANUP_MODE_DELETE
    if mode not in {CLEANUP_MODE_DELETE, CLEANUP_MODE_ARCHIVE}:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            "非法清理模式",
            HTTP_STATUS_BAD_REQUEST,
        )
    tables = payload.tables or list(_CLEANUP_TABLES.keys())
    invalid_tables = [name for name in tables if name not in _CLEANUP_TABLES]
    if invalid_tables:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            f"不支持的表: {', '.join(invalid_tables)}",
            HTTP_STATUS_BAD_REQUEST,
        )
    interval_minutes = (
        payload.interval_minutes
        if payload.interval_minutes is not None
        else DEFAULT_CLEANUP_JOB_INTERVAL_MINUTES
    )
    if int(interval_minutes) <= 0:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            "interval_minutes 必须为正整数",
            HTTP_STATUS_BAD_REQUEST,
        )
    retention_days = payload.retention_days
    before = payload.before
    if retention_days is None and not before:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            "缺少保留天数或截止时间",
            HTTP_STATUS_BAD_REQUEST,
        )
    created_at = now_iso()
    updated_at = created_at
    status = (
        CLEANUP_JOB_STATUS_ENABLED
        if payload.enabled is None or payload.enabled
        else CLEANUP_JOB_STATUS_DISABLED
    )
    next_run_at = _compute_next_run(None, interval_minutes)
    with get_connection() as conn:
        cursor = conn.execute(
            "INSERT INTO cleanup_jobs (name, status, mode, tables, retention_days, before, limit_value, last_run_at, next_run_at, created_at, updated_at, interval_minutes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                payload.name,
                status,
                mode,
                json.dumps(tables),
                retention_days,
                before,
                payload.limit,
                None,
                next_run_at,
                created_at,
                updated_at,
                interval_minutes,
            ),
        )
        job_id = cursor.lastrowid
        row = conn.execute(
            "SELECT * FROM cleanup_jobs WHERE id = ?", (job_id,)
        ).fetchone()
    _ensure_scheduler_started()
    return {"job": _serialize_job_row(row)}


@router.get("/maintenance/jobs")
def list_cleanup_jobs() -> dict:
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM cleanup_jobs ORDER BY id ASC").fetchall()
    return {"items": [_serialize_job_row(row) for row in rows]}


@router.get("/maintenance/jobs/{job_id}")
def get_cleanup_job(job_id: int) -> dict:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM cleanup_jobs WHERE id = ?", (job_id,)).fetchone()
    if not row:
        return error_response(ERROR_CODE_INVALID_REQUEST, "任务不存在", HTTP_STATUS_BAD_REQUEST)
    return {"job": _serialize_job_row(row)}


@router.patch("/maintenance/jobs/{job_id}")
def update_cleanup_job(job_id: int, payload: CleanupJobUpdate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    fields = []
    params: List = []
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM cleanup_jobs WHERE id = ?", (job_id,)).fetchone()
        if not row:
            return error_response(ERROR_CODE_INVALID_REQUEST, "任务不存在", HTTP_STATUS_BAD_REQUEST)
        if payload.name is not None:
            fields.append("name = ?")
            params.append(payload.name)
        if payload.mode is not None:
            if payload.mode not in {CLEANUP_MODE_DELETE, CLEANUP_MODE_ARCHIVE}:
                return error_response(
                    ERROR_CODE_INVALID_REQUEST,
                    "非法清理模式",
                    HTTP_STATUS_BAD_REQUEST,
                )
            fields.append("mode = ?")
            params.append(payload.mode)
        if payload.tables is not None:
            invalid_tables = [name for name in payload.tables if name not in _CLEANUP_TABLES]
            if invalid_tables:
                return error_response(
                    ERROR_CODE_INVALID_REQUEST,
                    f"不支持的表: {', '.join(invalid_tables)}",
                    HTTP_STATUS_BAD_REQUEST,
                )
            fields.append("tables = ?")
            params.append(json.dumps(payload.tables))
        if payload.retention_days is not None:
            fields.append("retention_days = ?")
            params.append(payload.retention_days)
        if payload.before is not None:
            fields.append("before = ?")
            params.append(payload.before)
        if payload.limit is not None:
            fields.append("limit_value = ?")
            params.append(payload.limit)
        if payload.interval_minutes is not None:
            if int(payload.interval_minutes) <= 0:
                return error_response(
                    ERROR_CODE_INVALID_REQUEST,
                    "interval_minutes 必须为正整数",
                    HTTP_STATUS_BAD_REQUEST,
                )
            fields.append("interval_minutes = ?")
            params.append(payload.interval_minutes)
            fields.append("next_run_at = ?")
            params.append(_compute_next_run(None, payload.interval_minutes))
        if payload.enabled is not None:
            fields.append("status = ?")
            params.append(
                CLEANUP_JOB_STATUS_ENABLED if payload.enabled else CLEANUP_JOB_STATUS_DISABLED
            )
        if fields:
            fields.append("updated_at = ?")
            params.append(now_iso())
            params.append(job_id)
            conn.execute(
                f"UPDATE cleanup_jobs SET {', '.join(fields)} WHERE id = ?",
                params,
            )
        row = conn.execute("SELECT * FROM cleanup_jobs WHERE id = ?", (job_id,)).fetchone()
    _ensure_scheduler_started()
    return {"job": _serialize_job_row(row)}


@router.get("/maintenance/jobs/{job_id}/runs")
def list_cleanup_job_runs(job_id: int, limit: int = DEFAULT_CLEANUP_LIMIT) -> dict:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM cleanup_job_runs WHERE job_id = ? ORDER BY id DESC LIMIT ?",
            (job_id, limit),
        ).fetchall()
    return {
        "items": [
            {
                "id": row["id"],
                "job_id": row["job_id"],
                "status": row["status"],
                "run_at": row["run_at"],
                "finished_at": row["finished_at"],
                "summary": row["summary"],
                "detail": row["detail"],
            }
            for row in rows
        ]
    }


@router.post("/maintenance/jobs/{job_id}/run")
def run_cleanup_job(job_id: int) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM cleanup_jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
    if not row:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            "任务不存在",
            HTTP_STATUS_BAD_REQUEST,
        )
    _ensure_scheduler_started()
    _run_job_async(row)
    return {"job": _serialize_job_row(row), "queued": True}
