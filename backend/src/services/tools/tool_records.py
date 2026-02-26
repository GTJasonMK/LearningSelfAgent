from typing import Any

from backend.src.common.app_error_utils import invalid_request_error, not_found_error
from backend.src.common.serializers import tool_call_from_row
from backend.src.common.utils import dump_model, now_iso, parse_json_dict
from backend.src.constants import (
    DEFAULT_TOOL_VERSION,
    ERROR_MESSAGE_INVALID_STATUS,
    ERROR_MESSAGE_TOOL_NOT_FOUND,
    ERROR_MESSAGE_TOOL_REQUIRED,
    SQL_BOOL_FALSE,
    SQL_BOOL_TRUE,
    TOOL_APPROVAL_STATUS_DRAFT,
    TOOL_METADATA_APPROVAL_KEY,
    TOOL_REUSE_STATUS_FAIL,
    TOOL_REUSE_STATUS_PASS,
    TOOL_REUSE_STATUS_UNKNOWN,
    TOOL_VERSION_CHANGE_NOTE_AUTO,
)
from backend.src.repositories.tool_call_records_repo import (
    ToolCallRecordCreateParams,
    create_tool_call_record,
    get_tool_call_record,
)
from backend.src.repositories.tools_repo import (
    ToolCreateParams,
    create_tool,
    get_tool,
    get_tool_by_name,
    update_tool,
    update_tool_last_used_at,
)
from backend.src.services.common.coerce import to_int, to_text
from backend.src.services.tools.tools_store import publish_tool_file
from backend.src.storage import get_connection


TOOL_REUSE_ALLOWED_STATUSES = frozenset(
    {
        TOOL_REUSE_STATUS_PASS,
        TOOL_REUSE_STATUS_FAIL,
        TOOL_REUSE_STATUS_UNKNOWN,
    }
)


def _update_tool_partial(
    *,
    tool_id: int,
    conn,
    description: Any = None,
    version: Any = None,
    metadata: Any = None,
    change_notes: Any = None,
    updated_at: str,
) -> None:
    update_tool(
        tool_id=to_int(tool_id),
        name=None,
        description=description,
        version=version,
        metadata=metadata,
        change_notes=change_notes,
        updated_at=updated_at,
        conn=conn,
    )

def is_valid_tool_reuse_status(value, *, allow_none: bool = False) -> bool:
    if value is None:
        return bool(allow_none)
    return value in TOOL_REUSE_ALLOWED_STATUSES


def empty_tool_reuse_status_counts() -> dict:
    return {
        TOOL_REUSE_STATUS_PASS: 0,
        TOOL_REUSE_STATUS_FAIL: 0,
        TOOL_REUSE_STATUS_UNKNOWN: 0,
    }


def create_tool_record(payload: Any) -> dict:
    """
    写入一条 tool_call_records 记录，并在需要时自动创建/更新 tools_items（同步）。

    说明：
    - API 层的权限校验由路由负责；
    - Agent 执行链路也会复用该函数（避免直接调用 async 路由函数导致 coroutine 泄漏）。
    """
    data = dump_model(payload)
    if not is_valid_tool_reuse_status(data.get("reuse_status"), allow_none=True):
        raise invalid_request_error(ERROR_MESSAGE_INVALID_STATUS)

    reuse_flag = data.get("reuse")
    if reuse_flag is None and data.get("skill_id") is not None:
        reuse_flag = True
    reuse_status = data.get("reuse_status")
    if reuse_status is None and reuse_flag:
        reuse_status = TOOL_REUSE_STATUS_UNKNOWN

    created_at = now_iso()
    reuse_value = SQL_BOOL_TRUE if reuse_flag else SQL_BOOL_FALSE

    with get_connection() as conn:
        tool_changed = False
        tool_id = data.get("tool_id")
        tool_row = None
        tool_metadata_obj = data.get("tool_metadata")
        tool_metadata_value = (
            tool_metadata_obj
            if isinstance(tool_metadata_obj, dict)
            else ({"raw": tool_metadata_obj} if tool_metadata_obj is not None else None)
        )

        if tool_id is not None:
            tool_row = get_tool(tool_id=to_int(tool_id), conn=conn)
            if not tool_row:
                raise not_found_error(ERROR_MESSAGE_TOOL_NOT_FOUND)
        else:
            tool_name = to_text(data.get("tool_name")).strip()
            if not tool_name:
                raise invalid_request_error(ERROR_MESSAGE_TOOL_REQUIRED)
            tool_row = get_tool_by_name(name=tool_name, conn=conn)
            if tool_row:
                tool_id = to_int(tool_row["id"])
            else:
                tool_created_at = now_iso()
                tool_version = to_text(data.get("tool_version") or DEFAULT_TOOL_VERSION)
                tool_description = to_text(data.get("tool_description") or "自动生成工具")
                # 新工具注册策略（MVP）：
                # - 若来自 agent（run_id 存在），先标记为 draft，待 Eval 通过后再进入“可复用工具清单”；
                # - 若非 agent 创建（无 run_id），默认视为已批准（不写 approval 字段也视为 approved）。
                meta_for_create = tool_metadata_value
                if meta_for_create is None:
                    meta_for_create = {}
                if not isinstance(meta_for_create, dict):
                    meta_for_create = {"raw": meta_for_create}
                if data.get("run_id") is not None:
                    approval = meta_for_create.get(TOOL_METADATA_APPROVAL_KEY)
                    if not isinstance(approval, dict):
                        approval = {}
                    # 安全约束（逻辑一致性）：Agent 创建的新工具必须先进入 draft，
                    # 不允许由模型“自称已批准”，避免未验证工具污染全局工具清单。
                    approval["status"] = TOOL_APPROVAL_STATUS_DRAFT
                    approval["created_at"] = tool_created_at
                    try:
                        approval["created_run_id"] = to_int(data.get("run_id"))
                    except Exception:
                        pass
                    if data.get("task_id") is not None:
                        try:
                            approval["created_task_id"] = to_int(data.get("task_id"))
                        except Exception:
                            pass
                    meta_for_create[TOOL_METADATA_APPROVAL_KEY] = approval

                tool_id = create_tool(
                    ToolCreateParams(
                        name=tool_name,
                        description=tool_description,
                        version=tool_version,
                        metadata=meta_for_create if meta_for_create else None,
                        last_used_at=tool_created_at,
                        created_at=tool_created_at,
                        updated_at=tool_created_at,
                    ),
                    conn=conn,
                )
                # 与旧逻辑一致：新建工具不再做“兜底更新/合并”，直接用创建时的字段即可
                tool_row = None
                tool_changed = True

        # 兜底更新工具描述/版本（补齐自动生成工具的信息）
        if tool_id is not None and tool_row:
            if data.get("tool_description"):
                if not tool_row["description"] or tool_row["description"] == "自动生成工具":
                    _update_tool_partial(
                        tool_id=tool_id,
                        conn=conn,
                        description=to_text(data.get("tool_description")),
                        updated_at=created_at,
                    )
                    tool_changed = True
            if data.get("tool_version") and data.get("tool_version") != tool_row["version"]:
                _update_tool_partial(
                    tool_id=tool_id,
                    conn=conn,
                    version=to_text(data.get("tool_version")),
                    change_notes=TOOL_VERSION_CHANGE_NOTE_AUTO,
                    updated_at=created_at,
                )
                tool_changed = True

        # 合并工具 metadata（尽量保留历史字段）
        if tool_id is not None and isinstance(tool_metadata_obj, dict) and tool_row:
            existing_metadata = tool_row["metadata"]
            merged = parse_json_dict(existing_metadata) or {}
            # Agent 执行链路：不允许模型通过 tool_metadata 篡改审批状态。
            # 说明：
            # - 新工具在创建时已强制写入 approval.status=draft；
            # - 旧工具的 approval 只能在评估通过后的后处理阶段升级为 approved；
            # - 若允许执行阶段覆盖 approval，可能导致“未验证工具进入 tools_hint”，进而影响后续任务稳定性。
            if data.get("run_id") is not None and TOOL_METADATA_APPROVAL_KEY in tool_metadata_obj:
                filtered = dict(tool_metadata_obj)
                filtered.pop(TOOL_METADATA_APPROVAL_KEY, None)
                merged.update(filtered)
            else:
                merged.update(tool_metadata_obj)
            _update_tool_partial(
                tool_id=tool_id,
                conn=conn,
                metadata=merged,
                updated_at=created_at,
            )
            tool_changed = True

        # 工具“灵魂存档”：尽力落盘工具文件（失败不阻塞工具调用统计）
        if tool_id is not None and tool_changed:
            try:
                publish_tool_file(to_int(tool_id), conn=conn)
            except Exception:
                # 不 raise：避免因为落盘失败导致工具调用链路整体失败
                pass

        record_id, _ = create_tool_call_record(
            ToolCallRecordCreateParams(
                tool_id=to_int(tool_id),
                task_id=data.get("task_id"),
                skill_id=data.get("skill_id"),
                run_id=data.get("run_id"),
                reuse=to_int(reuse_value),
                reuse_status=reuse_status,
                reuse_notes=data.get("reuse_notes"),
                input=to_text(data.get("input")),
                output=to_text(data.get("output")),
                created_at=created_at,
            ),
            conn=conn,
        )
        update_tool_last_used_at(tool_id=to_int(tool_id), last_used_at=created_at, conn=conn)
        row = get_tool_call_record(record_id=to_int(record_id), conn=conn)
    return {"record": tool_call_from_row(row)}
