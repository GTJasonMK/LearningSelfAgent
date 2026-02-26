from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

from backend.src.common.utils import now_iso
from backend.src.repositories.repo_conn import provide_connection


def create_task_run_event(
    *,
    task_id: int,
    run_id: int,
    session_key: Optional[str],
    event_id: str,
    event_type: str,
    payload: Any,
    created_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[int]:
    """
    写入 run 事件日志；若 event_id 已存在则忽略（幂等）。
    """
    event_key = str(event_id or "").strip()
    if not event_key:
        return None
    event_type_text = str(event_type or "").strip() or "unknown"
    created = str(created_at or "").strip() or now_iso()
    payload_text = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    with provide_connection(conn) as inner:
        cursor = inner.execute(
            """
            INSERT OR IGNORE INTO task_run_events
            (task_id, run_id, session_key, event_id, event_type, payload, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(task_id),
                int(run_id),
                str(session_key or "").strip() or None,
                event_key,
                event_type_text,
                str(payload_text or ""),
                created,
            ),
        )
        if cursor.rowcount <= 0:
            return None
        return int(cursor.lastrowid)


def list_task_run_events(
    *,
    run_id: int,
    after_event_id: Optional[str] = None,
    limit: int = 200,
    conn: Optional[sqlite3.Connection] = None,
) -> list[sqlite3.Row]:
    rid = int(run_id)
    lim = max(1, min(2000, int(limit)))
    after_id = str(after_event_id or "").strip()
    with provide_connection(conn) as inner:
        if not after_id:
            return list(
                inner.execute(
                    """
                    SELECT id, task_id, run_id, session_key, event_id, event_type, payload, created_at
                    FROM task_run_events
                    WHERE run_id = ?
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (rid, lim),
                ).fetchall()
            )

        anchor = inner.execute(
            "SELECT id FROM task_run_events WHERE run_id = ? AND event_id = ? LIMIT 1",
            (rid, after_id),
        ).fetchone()
        if anchor is None:
            # after_event_id 失效（例如事件被裁剪/游标跨会话）时，
            # 回放应优先返回“最新窗口”，避免前端在有限批次内只拿到旧事件。
            latest_rows = list(
                inner.execute(
                    """
                    SELECT id, task_id, run_id, session_key, event_id, event_type, payload, created_at
                    FROM task_run_events
                    WHERE run_id = ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (rid, lim),
                ).fetchall()
            )
            latest_rows.reverse()
            return latest_rows

        return list(
            inner.execute(
                """
                SELECT id, task_id, run_id, session_key, event_id, event_type, payload, created_at
                FROM task_run_events
                WHERE run_id = ? AND id > ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (rid, int(anchor["id"]), lim),
            ).fetchall()
        )
