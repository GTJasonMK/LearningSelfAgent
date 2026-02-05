import json
import logging
from typing import Dict, List, Optional

from backend.src.common.utils import now_iso
from backend.src.constants import (
    RUN_STATUS_DONE,
    RUN_STATUS_WAITING,
    AGENT_REVIEW_DISTILL_STATUS_ALLOW,
    TOOL_APPROVAL_STATUS_APPROVED,
    TOOL_APPROVAL_STATUS_DRAFT,
    TOOL_METADATA_APPROVAL_KEY,
)
from backend.src.repositories.tool_call_records_repo import list_tool_calls_with_tool_name_by_run
from backend.src.repositories.tools_repo import get_tool, update_tool
from backend.src.services.debug.debug_output import write_task_debug_output
from backend.src.services.skills.tool_skill_autogen import autogen_tool_skill_from_call
from backend.src.services.tools.tools_store import publish_tool_file
from backend.src.storage import get_connection

logger = logging.getLogger(__name__)


def _safe_write_debug(
    *,
    task_id: int,
    run_id: int,
    message: str,
    data: Optional[dict] = None,
    level: str = "debug",
) -> None:
    """
    工具审批链路的调试输出不应影响主链路：失败时降级为 logger.exception。
    """
    try:
        write_task_debug_output(
            task_id=int(task_id),
            run_id=int(run_id),
            message=message,
            data=data if isinstance(data, dict) else None,
            level=level,
        )
    except Exception:
        logger.exception("write_task_debug_output failed: %s", message)


def _load_tool_metadata(row) -> Dict:
    if not row or not row["metadata"]:
        return {}
    try:
        meta = json.loads(row["metadata"])
    except Exception:
        return {}
    return meta if isinstance(meta, dict) else {}


def approve_draft_tools_from_run(
    *,
    task_id: int,
    run_id: int,
    run_status: str,
    review_id: int,
    review_status: str,
    distill_status: Optional[str] = None,
    allow_waiting_feedback: bool = False,
    model: Optional[str] = None,
) -> dict:
    """
    将本次 run 中“新创建且处于 draft”的工具，升级为 approved，并（可选）沉淀 tool skill。

    约束（MVP）：
    - 默认只在 run_status=done 且 review_status=pass 时自动批准；
    - 若 allow_waiting_feedback=True，则允许在 run_status=waiting（确认满意度等待）且 review_status=pass 时批准；
    - distill_status 非 allow 时不自动批准（允许 pass 但不沉淀）；
    - 工具是否为“本 run 创建”：metadata.approval.created_run_id == run_id 且 status==draft。
    """
    normalized_run_status = str(run_status or "").strip()
    normalized_review_status = str(review_status or "").strip().lower()
    if normalized_review_status != "pass":
        return {"ok": True, "skipped": True, "reason": f"review_status_{normalized_review_status}", "approved_tools": []}
    normalized_distill_status = str(distill_status or "").strip().lower()
    if normalized_distill_status and normalized_distill_status != AGENT_REVIEW_DISTILL_STATUS_ALLOW:
        return {
            "ok": True,
            "skipped": True,
            "reason": f"distill_status_{normalized_distill_status}",
            "approved_tools": [],
        }
    if normalized_run_status == RUN_STATUS_DONE:
        pass
    elif normalized_run_status == RUN_STATUS_WAITING and bool(allow_waiting_feedback):
        pass
    else:
        return {"ok": True, "skipped": True, "reason": "run_not_done", "approved_tools": []}

    rows = list_tool_calls_with_tool_name_by_run(run_id=int(run_id), limit=200)
    if not rows:
        return {"ok": True, "skipped": True, "reason": "no_tool_calls", "approved_tools": []}

    # tool_id -> example (input/output)
    examples: Dict[int, Dict[str, str]] = {}
    tool_ids: List[int] = []
    for row in rows:
        try:
            tid = int(row["tool_id"])
        except Exception:
            continue
        if tid <= 0:
            continue
        if tid not in tool_ids:
            tool_ids.append(tid)
        # 取第一个样例即可（通常是“自测”）
        if tid not in examples:
            examples[tid] = {"input": str(row["input"] or ""), "output": str(row["output"] or "")}

    approved_tools: List[dict] = []
    approved_ids: List[int] = []
    approved_skill_ids: List[int] = []
    approved_at = now_iso()

    with get_connection() as conn:
        for tid in tool_ids:
            tool_row = get_tool(tool_id=int(tid), conn=conn)
            if not tool_row:
                continue
            meta = _load_tool_metadata(tool_row)
            approval = meta.get(TOOL_METADATA_APPROVAL_KEY)
            if not isinstance(approval, dict):
                continue
            status = str(approval.get("status") or "").strip().lower()
            if status != TOOL_APPROVAL_STATUS_DRAFT:
                continue
            created_run_id = approval.get("created_run_id")
            try:
                created_run_id = int(created_run_id) if created_run_id is not None else None
            except Exception:
                created_run_id = None
            if created_run_id != int(run_id):
                continue

            approval["status"] = TOOL_APPROVAL_STATUS_APPROVED
            approval["approved_at"] = approved_at
            approval["approved_review_id"] = int(review_id)
            meta[TOOL_METADATA_APPROVAL_KEY] = approval

            update_tool(
                tool_id=int(tid),
                name=None,
                description=None,
                version=None,
                metadata=meta,
                change_notes="Eval 通过自动批准",
                updated_at=approved_at,
                conn=conn,
            )
            approved_ids.append(int(tid))
            approved_tools.append({"tool_id": int(tid), "name": tool_row["name"], "approved_at": approved_at})

            # 工具“灵魂存档”：批准状态也要落盘（失败不阻塞）
            try:
                publish_tool_file(int(tid), conn=conn)
            except Exception:
                pass

    # 额外沉淀：批准后再总结 tool skill（失败不阻塞）
    for tid in approved_ids:
        ex = examples.get(int(tid)) or {}
        try:
            resp = autogen_tool_skill_from_call(
                tool_id=int(tid),
                tool_input=str(ex.get("input") or ""),
                tool_output=str(ex.get("output") or ""),
                task_id=int(task_id),
                run_id=int(run_id),
                model=model,
            )
            if isinstance(resp, dict) and resp.get("ok") and resp.get("skill_id") is not None:
                try:
                    approved_skill_ids.append(int(resp.get("skill_id")))
                except Exception:
                    pass
            else:
                # 技能沉淀失败也要留痕，否则用户会看到“工具已批准但没有技能卡”且无法定位原因。
                _safe_write_debug(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    message="tool.skill_autogen.skipped",
                    data={
                        "tool_id": int(tid),
                        "resp": resp if isinstance(resp, dict) else {"value": str(resp)},
                    },
                    level="warning",
                )
        except Exception:
            _safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="tool.skill_autogen.failed",
                data={"tool_id": int(tid)},
                level="warning",
            )
            continue

    if approved_ids:
        _safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="tool.approval.applied",
            data={"review_id": int(review_id), "approved_tool_ids": approved_ids, "skill_ids": approved_skill_ids},
            level="info",
        )

    return {"ok": True, "approved_tools": approved_tools, "skill_ids": approved_skill_ids}
