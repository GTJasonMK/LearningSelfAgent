from __future__ import annotations

from typing import List, Optional

from backend.src.common.utils import extract_json_object, truncate_text
from backend.src.constants import AGENT_TASK_FEEDBACK_STEP_TITLE, RUN_STATUS_WAITING


def is_selftest_title(title: str) -> bool:
    """
    判断 step.title 是否为“自测/验证”类步骤。
    """
    text = str(title or "")
    lowered = text.lower()
    return (
        "自测" in text
        or "selftest" in lowered
        or "self-test" in lowered
        or "verify" in lowered
        or "smoke" in lowered
    )


def extract_tool_name_from_tool_call_step(title: str, payload_preview: object) -> str:
    """
    从 tool_call 步骤中提取 tool_name。

    优先级：
    1) detail.payload.tool_name
    2) title 前缀：tool_call:<tool_name> ...
    """
    try:
        if isinstance(payload_preview, dict):
            name = str(payload_preview.get("tool_name") or "").strip()
            if name:
                return name
    except Exception:
        pass

    raw = str(title or "").strip()
    if not raw:
        return ""

    lowered = raw.lower()
    for sep in (":", "："):
        prefix = f"tool_call{sep}"
        if lowered.startswith(prefix):
            rest = raw[len(prefix) :].strip()
            if not rest:
                return ""
            token = rest.split()[0].strip()
            if len(token) >= 2 and ((token[0] == token[-1] == '"') or (token[0] == token[-1] == "'")):
                token = token[1:-1].strip()
            return token

    return ""


def find_unverified_text_output(output_rows: List[dict]) -> Optional[dict]:
    """
    检测是否存在“未验证草稿”文本输出。

    返回：
    - None：未命中
    - dict: {"output_id": int|None, "content_preview": str}
    """
    markers = (
        "【未验证草稿】",
        "[证据引用]\n- 无",
        "无（建议补齐 step/tool/artifact 证据",
    )

    for row in output_rows or []:
        if not row:
            continue
        output_type = str(row["output_type"] or "").strip().lower() if "output_type" in row.keys() else ""
        if output_type != "text":
            continue
        content = str(row["content"] or "") if "content" in row.keys() else ""
        if not content:
            continue
        if not any(marker in content for marker in markers):
            continue

        output_id = None
        try:
            if row["id"] is not None:
                output_id = int(row["id"])
        except Exception:
            output_id = None

        return {
            "output_id": output_id,
            "content_preview": truncate_text(content, 260),
        }

    return None


def allow_tool_approval_on_waiting_feedback(run_row: Optional[dict]) -> bool:
    try:
        run_status_value = str(run_row["status"] or "").strip() if run_row else ""
        if run_status_value != RUN_STATUS_WAITING:
            return False
        state_obj = extract_json_object(run_row["agent_state"] or "") if run_row else None
        paused = state_obj.get("paused") if isinstance(state_obj, dict) else None
        step_title = str(paused.get("step_title") or "").strip() if isinstance(paused, dict) else ""
        return step_title == AGENT_TASK_FEEDBACK_STEP_TITLE
    except Exception:
        return False
