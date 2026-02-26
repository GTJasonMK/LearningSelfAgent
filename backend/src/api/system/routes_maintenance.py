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
from backend.src.api.utils import clamp_page_limit, error_response, now_iso, require_write_permission
from backend.src.common.sql import in_clause_placeholders
from backend.src.common.utils import parse_json_list
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


def _invalid_mode_error():
    return error_response(
        ERROR_CODE_INVALID_REQUEST,
        "非法清理模式",
        HTTP_STATUS_BAD_REQUEST,
    )


def _missing_cutoff_error():
    return error_response(
        ERROR_CODE_INVALID_REQUEST,
        "缺少保留天数或截止时间",
        HTTP_STATUS_BAD_REQUEST,
    )


def _unsupported_tables_error(invalid_tables: List[str]):
    return error_response(
        ERROR_CODE_INVALID_REQUEST,
        f"不支持的表: {', '.join(invalid_tables)}",
        HTTP_STATUS_BAD_REQUEST,
    )


def _normalize_cleanup_mode(mode: Optional[str]) -> Tuple[Optional[str], Optional[dict]]:
    mode_value = mode or CLEANUP_MODE_DELETE
    if mode_value not in {CLEANUP_MODE_DELETE, CLEANUP_MODE_ARCHIVE}:
        return None, _invalid_mode_error()
    return mode_value, None


def _invalid_cleanup_tables(tables: List[str]) -> List[str]:
    return [name for name in tables if name not in _CLEANUP_TABLES]


def _normalize_cleanup_tables(
    tables: Optional[List[str]],
) -> Tuple[Optional[List[str]], Optional[dict]]:
    table_list = tables or list(_CLEANUP_TABLES.keys())
    invalid_tables = _invalid_cleanup_tables(table_list)
    if invalid_tables:
        return None, _unsupported_tables_error(invalid_tables)
    return table_list, None


def _payload_bool(value: Optional[bool], *, default: bool) -> bool:
    return bool(value) if value is not None else bool(default)


def _payload_int(value: Optional[int], *, default: int) -> int:
    try:
        return int(value) if value is not None else int(default)
    except Exception:
        return int(default)


def _payload_float(value: Optional[float], *, default: float) -> float:
    try:
        return float(value) if value is not None else float(default)
    except Exception:
        return float(default)


def _payload_text(
    value: Optional[str],
    *,
    default: Optional[str] = None,
    lower: bool = False,
) -> Optional[str]:
    text = str(value or "").strip()
    if lower:
        text = text.lower()
    if text:
        return text
    return default


def _invalid_positive_int_error(field_name: str) -> dict:
    return error_response(
        ERROR_CODE_INVALID_REQUEST,
        f"{field_name} 必须为正整数",
        HTTP_STATUS_BAD_REQUEST,
    )


def _invalid_kind_error() -> dict:
    return error_response(
        code=ERROR_CODE_INVALID_REQUEST,
        message="invalid_kind",
        status_code=HTTP_STATUS_BAD_REQUEST,
    )


def _cleanup_job_not_found_error() -> dict:
    return error_response(
        ERROR_CODE_INVALID_REQUEST,
        "任务不存在",
        HTTP_STATUS_BAD_REQUEST,
    )


def _resolve_positive_int(
    value: Optional[int],
    *,
    default: int,
    field_name: str,
) -> Tuple[Optional[int], Optional[dict]]:
    parsed = _payload_int(value, default=default)
    if parsed <= 0:
        return None, _invalid_positive_int_error(field_name)
    return parsed, None


@router.post("/maintenance/stop-running")
@require_write_permission
def maintenance_stop_running() -> dict:
    """
    终止遗留的 running 状态（用于应用退出/崩溃后的自恢复）。

    典型场景：Electron 退出时 kill 掉 uvicorn，导致 tasks/task_runs/task_steps 的 finally 没机会把状态从 running 改回去。
    """
    return {"result": stop_running_task_records(reason="maintenance_api")}


@router.post("/maintenance/knowledge/rollback")
@require_write_permission
def maintenance_knowledge_rollback(payload: MaintenanceKnowledgeRollbackRequest) -> dict:
    """
    知识治理：一键回滚/废弃某次 run 产生的知识（skills/tools）。

    典型场景：
    - 某次 run 沉淀了错误的技能/工具，导致后续检索与规划被污染；
    - 需要快速把这次 run 的知识降级为 deprecated/abandoned 或工具 rejected。
    """
    return rollback_knowledge_from_run(
        run_id=int(payload.run_id),
        dry_run=_payload_bool(payload.dry_run, default=False),
        include_skills=_payload_bool(payload.include_skills, default=True),
        include_tools=_payload_bool(payload.include_tools, default=True),
        draft_skill_target_status=_payload_text(
            payload.draft_skill_target_status,
            default="abandoned",
            lower=True,
        )
        or "abandoned",
        approved_skill_target_status=_payload_text(
            payload.approved_skill_target_status,
            default="deprecated",
            lower=True,
        )
        or "deprecated",
        tool_target_status=_payload_text(payload.tool_target_status, default="rejected", lower=True)
        or "rejected",
        reason=_payload_text(payload.reason),
    )


@router.post("/maintenance/knowledge/auto-deprecate")
@require_write_permission
def maintenance_knowledge_auto_deprecate(payload: MaintenanceKnowledgeAutoDeprecateRequest) -> dict:
    """
    知识治理：按“最近成功率/复用验证”信号自动废弃低质量知识（skills/tools）。
    """
    since_days = _payload_int(payload.since_days, default=30)
    min_calls = _payload_int(payload.min_calls, default=3)
    threshold = _payload_float(payload.success_rate_threshold, default=0.3)

    return auto_deprecate_low_quality_knowledge(
        since_days=int(since_days),
        min_calls=int(min_calls),
        success_rate_threshold=float(threshold),
        dry_run=_payload_bool(payload.dry_run, default=False),
        include_skills=_payload_bool(payload.include_skills, default=True),
        include_tools=_payload_bool(payload.include_tools, default=True),
        reason=_payload_text(payload.reason),
    )


@router.post("/maintenance/knowledge/rollback-version")
@require_write_permission
def maintenance_knowledge_rollback_version(payload: MaintenanceKnowledgeRollbackVersionRequest) -> dict:
    """
    知识治理：一键回滚到上一版本（skills/tools）。
    """
    kind = _payload_text(payload.kind, default="", lower=True) or ""
    rid = int(payload.id)
    dry_run = _payload_bool(payload.dry_run, default=False)
    reason = _payload_text(payload.reason)
    if kind == "skill":
        return rollback_skill_to_previous_version(skill_id=int(rid), dry_run=dry_run, reason=reason)
    if kind == "tool":
        return rollback_tool_to_previous_version(tool_id=int(rid), dry_run=dry_run, reason=reason)
    return _invalid_kind_error()


@router.post("/maintenance/knowledge/validate-tags")
@require_write_permission
def maintenance_knowledge_validate_tags(payload: MaintenanceKnowledgeValidateTagsRequest) -> dict:
    """
    知识治理：校验/修复 skills_items.tags。
    """
    dry_run = _payload_bool(payload.dry_run, default=True)
    fix = _payload_bool(payload.fix, default=False)
    strict_keys = _payload_bool(payload.strict_keys, default=False)
    include_draft = _payload_bool(payload.include_draft, default=True)
    limit = _payload_int(payload.limit, default=5000)

    return validate_and_fix_skill_tags(
        dry_run=dry_run,
        fix=fix,
        strict_keys=strict_keys,
        include_draft=include_draft,
        limit=limit,
    )


@router.post("/maintenance/knowledge/dedupe-skills")
@require_write_permission
def maintenance_knowledge_dedupe_skills(payload: MaintenanceKnowledgeDedupeSkillsRequest) -> dict:
    """
    知识治理：去重 + 版本合并（同 scope/name）。
    """
    dry_run = _payload_bool(payload.dry_run, default=True)
    include_draft = _payload_bool(payload.include_draft, default=True)
    merge_across_domains = _payload_bool(payload.merge_across_domains, default=False)
    reason = _payload_text(payload.reason)

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
        placeholders = in_clause_placeholders(ids)
        if not placeholders:
            return 0, archive_table
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
    placeholders = in_clause_placeholders(ids)
    if not placeholders:
        return 0
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
        "tables": parse_json_list(row["tables"]),
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
        tables=parse_json_list(job_row["tables"]) or None,
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
@require_write_permission
def cleanup_records(payload: MaintenanceCleanupRequest) -> dict:
    return _cleanup_execute(payload)


def _cleanup_execute(payload: MaintenanceCleanupRequest) -> dict:
    mode, mode_error = _normalize_cleanup_mode(payload.mode)
    if mode_error:
        return mode_error
    cutoff = _resolve_cutoff(payload.retention_days, payload.before)
    if not cutoff:
        return _missing_cutoff_error()
    tables, tables_error = _normalize_cleanup_tables(payload.tables)
    if tables_error:
        return tables_error
    limit = payload.limit or DEFAULT_CLEANUP_LIMIT
    dry_run = _payload_bool(payload.dry_run, default=False)
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
@require_write_permission
def create_cleanup_job(payload: CleanupJobCreate) -> dict:
    mode, mode_error = _normalize_cleanup_mode(payload.mode)
    if mode_error:
        return mode_error
    tables, tables_error = _normalize_cleanup_tables(payload.tables)
    if tables_error:
        return tables_error
    interval_minutes, interval_error = _resolve_positive_int(
        payload.interval_minutes,
        default=DEFAULT_CLEANUP_JOB_INTERVAL_MINUTES,
        field_name="interval_minutes",
    )
    if interval_error:
        return interval_error
    retention_days = payload.retention_days
    before = payload.before
    if retention_days is None and not before:
        return _missing_cutoff_error()
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
        return _cleanup_job_not_found_error()
    return {"job": _serialize_job_row(row)}


@router.patch("/maintenance/jobs/{job_id}")
@require_write_permission
def update_cleanup_job(job_id: int, payload: CleanupJobUpdate) -> dict:
    fields = []
    params: List = []
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM cleanup_jobs WHERE id = ?", (job_id,)).fetchone()
        if not row:
            return _cleanup_job_not_found_error()
        if payload.name is not None:
            fields.append("name = ?")
            params.append(payload.name)
        if payload.mode is not None:
            _, mode_error = _normalize_cleanup_mode(payload.mode)
            if mode_error:
                return mode_error
            fields.append("mode = ?")
            params.append(payload.mode)
        if payload.tables is not None:
            invalid_tables = _invalid_cleanup_tables(payload.tables)
            if invalid_tables:
                return _unsupported_tables_error(invalid_tables)
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
            interval_minutes, interval_error = _resolve_positive_int(
                payload.interval_minutes,
                default=DEFAULT_CLEANUP_JOB_INTERVAL_MINUTES,
                field_name="interval_minutes",
            )
            if interval_error:
                return interval_error
            fields.append("interval_minutes = ?")
            params.append(interval_minutes)
            fields.append("next_run_at = ?")
            params.append(_compute_next_run(None, interval_minutes))
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
    safe_limit = clamp_page_limit(limit, default=DEFAULT_CLEANUP_LIMIT)
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM cleanup_job_runs WHERE job_id = ? ORDER BY id DESC LIMIT ?",
            (job_id, safe_limit),
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
@require_write_permission
def run_cleanup_job(job_id: int) -> dict:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM cleanup_jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
    if not row:
        return _cleanup_job_not_found_error()
    _ensure_scheduler_started()
    _run_job_async(row)
    return {"job": _serialize_job_row(row), "queued": True}
