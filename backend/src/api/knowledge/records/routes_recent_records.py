from typing import List

from fastapi import APIRouter

from backend.src.common.serializers import task_from_row
from backend.src.common.utils import truncate_text
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    DEFAULT_PAGE_OFFSET,
    STREAM_RESULT_PREVIEW_MAX_CHARS,
)
from backend.src.storage import get_connection

router = APIRouter()


def _truncate_preview(text: str, max_chars: int = STREAM_RESULT_PREVIEW_MAX_CHARS) -> str:
    return truncate_text(text, max_chars)


@router.get("/records/recent")
def list_recent_records(limit: int = 50, offset: int = DEFAULT_PAGE_OFFSET) -> dict:
    """
    最近动态（跨任务聚合）。

    说明：
    - 用于前端主面板 Dashboard 展示“日志/动态”，让用户随时看到 Agent/系统做了什么。
    - 数据来源于现有表（run/step/output/llm/tool/memory/skill/agent_review），不引入额外日志系统。
    """
    if offset < 0:
        offset = 0
    if limit <= 0:
        limit = DEFAULT_PAGE_LIMIT
    if limit > DEFAULT_PAGE_LIMIT:
        limit = DEFAULT_PAGE_LIMIT

    query = """
    SELECT event_type, event_id, timestamp, task_id, run_id, ref_id, title, status, summary, detail
    FROM (
        SELECT
            'run' AS event_type,
            r.id AS event_id,
            COALESCE(r.started_at, r.created_at) AS timestamp,
            r.task_id AS task_id,
            r.id AS run_id,
            NULL AS ref_id,
            NULL AS title,
            r.status AS status,
            r.summary AS summary,
            NULL AS detail
        FROM task_runs r

        UNION ALL

        SELECT
            'step' AS event_type,
            s.id AS event_id,
            COALESCE(s.started_at, s.created_at) AS timestamp,
            s.task_id AS task_id,
            s.run_id AS run_id,
            NULL AS ref_id,
            s.title AS title,
            s.status AS status,
            NULL AS summary,
            s.detail AS detail
        FROM task_steps s

        UNION ALL

        SELECT
            'output' AS event_type,
            o.id AS event_id,
            o.created_at AS timestamp,
            o.task_id AS task_id,
            o.run_id AS run_id,
            NULL AS ref_id,
            NULL AS title,
            NULL AS status,
            o.output_type AS summary,
            o.content AS detail
        FROM task_outputs o

        UNION ALL

        SELECT
            'llm' AS event_type,
            l.id AS event_id,
            COALESCE(l.started_at, l.created_at) AS timestamp,
            l.task_id AS task_id,
            l.run_id AS run_id,
            NULL AS ref_id,
            NULL AS title,
            l.status AS status,
            l.model AS summary,
            l.response AS detail
        FROM llm_records l

        UNION ALL

        SELECT
            'tool' AS event_type,
            tr.id AS event_id,
            tr.created_at AS timestamp,
            tr.task_id AS task_id,
            tr.run_id AS run_id,
            tr.tool_id AS ref_id,
            ti.name AS title,
            tr.reuse_status AS status,
            tr.input AS summary,
            tr.output AS detail
        FROM tool_call_records tr
        LEFT JOIN tools_items ti ON ti.id = tr.tool_id

        UNION ALL

        SELECT
            'memory' AS event_type,
            m.id AS event_id,
            m.created_at AS timestamp,
            m.task_id AS task_id,
            NULL AS run_id,
            NULL AS ref_id,
            NULL AS title,
            m.memory_type AS status,
            NULL AS summary,
            m.content AS detail
        FROM memory_items m

        UNION ALL

        SELECT
            'skill' AS event_type,
            s.id AS event_id,
            s.created_at AS timestamp,
            s.task_id AS task_id,
            NULL AS run_id,
            NULL AS ref_id,
            s.name AS title,
            s.category AS status,
            s.version AS summary,
            s.description AS detail
        FROM skills_items s

        UNION ALL

        SELECT
            'agent_review' AS event_type,
            a.id AS event_id,
            a.created_at AS timestamp,
            a.task_id AS task_id,
            a.run_id AS run_id,
            NULL AS ref_id,
            NULL AS title,
            a.status AS status,
            a.summary AS summary,
            a.issues AS detail
        FROM agent_review_records a
    )
    ORDER BY timestamp DESC
    LIMIT ? OFFSET ?
    """

    with get_connection() as conn:
        rows = conn.execute(query, (int(limit), int(offset))).fetchall()

        task_ids = [int(r["task_id"]) for r in rows if r["task_id"] is not None]
        task_titles = {}
        if task_ids:
            placeholders = ",".join(["?"] * len(task_ids))
            task_rows = conn.execute(
                f"SELECT id, title FROM tasks WHERE id IN ({placeholders})",
                task_ids,
            ).fetchall()
            task_titles = {int(r["id"]): str(r["title"] or "") for r in task_rows}

    items = []
    for row in rows:
        task_id = row["task_id"]
        task_title = task_titles.get(int(task_id)) if task_id is not None else None
        items.append(
            {
                "type": row["event_type"],
                "id": int(row["event_id"]),
                "timestamp": row["timestamp"],
                "task_id": int(task_id) if task_id is not None else None,
                "task_title": task_title,
                "run_id": int(row["run_id"]) if row["run_id"] is not None else None,
                "ref_id": int(row["ref_id"]) if row["ref_id"] is not None else None,
                "title": _truncate_preview(str(row["title"] or "")),
                "status": _truncate_preview(str(row["status"] or ""), 80),
                "summary": _truncate_preview(str(row["summary"] or "")),
                "detail": _truncate_preview(str(row["detail"] or "")),
            }
        )
    return {"items": items}
