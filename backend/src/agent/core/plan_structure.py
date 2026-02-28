from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

from backend.src.actions.registry import normalize_action_type
from backend.src.constants import AGENT_PLAN_BRIEF_MAX_CHARS


_PLAN_STATUS_ALLOWED = {
    "pending",
    "planned",
    "running",
    "waiting",
    "done",
    "failed",
    "skipped",
}


_PLAN_STEP_KIND_ALLOWED = {
    "user_prompt",
    "task_feedback",
    "tool_call",
    "shell_command",
    "task_output",
    "llm_call",
    "file_write",
    "file_read",
    "file_append",
    "file_list",
    "file_delete",
    "http_request",
    "json_parse",
    "memory_write",
}

# 合法的状态转移表：key 为当前状态，value 为允许的目标状态集合。
# 未在此表中的转移将被静默拒绝并记录警告日志。
_VALID_TRANSITIONS: dict[str, set[str]] = {
    "pending":  {"planned", "running", "waiting", "done", "skipped", "failed"},
    "planned":  {"running", "waiting", "done", "skipped", "failed"},
    "running":  {"done", "failed", "waiting", "skipped"},
    "waiting":  {"running", "done", "failed"},
    "done":     set(),  # 终态，不允许再转移
    "failed":   {"running", "planned"},  # 允许 replan 后重新执行
    "skipped":  {"running", "planned"},  # 允许 replan 后重新激活
}


def _fallback_brief_from_title(title: str, max_len: int = AGENT_PLAN_BRIEF_MAX_CHARS) -> str:
    value = str(title or "").strip()
    if not value:
        return ""
    for prefix in (
        "tool_call:",
        "tool_call：",
        "llm_call:",
        "llm_call：",
        "task_output:",
        "task_output：",
        "shell_command:",
        "shell_command：",
    ):
        if value.startswith(prefix):
            value = value[len(prefix) :].strip()
            break
    for sep in (" ", "：", ":"):
        if sep in value:
            value = value.split(sep, 1)[0].strip()
            break
    if len(value) > max_len:
        value = value[:max_len]
    return value


def _normalize_allow_list(raw_allow: object) -> List[str]:
    values: List[object]
    if raw_allow is None:
        values = []
    elif isinstance(raw_allow, list):
        values = list(raw_allow)
    else:
        values = [raw_allow]

    result: List[str] = []
    for item in values:
        normalized = normalize_action_type(str(item or ""))
        if not normalized:
            continue
        if normalized in result:
            continue
        result.append(normalized)
    return result


def _normalize_step_kind(raw_kind: object, *, allow: List[str], title: str) -> str:
    direct = str(raw_kind or "").strip()
    if direct:
        normalized = normalize_action_type(direct)
        if normalized:
            return normalized
        lowered = direct.lower()
        if lowered in _PLAN_STEP_KIND_ALLOWED:
            return lowered
    if len(allow or []) == 1:
        only = str((allow or [])[0] or "").strip().lower()
        if only:
            return only
    title_text = str(title or "").strip()
    match = re.match(r"^([a-zA-Z_][a-zA-Z0-9_]*)\s*[:：]", title_text)
    if match:
        prefix = str(match.group(1) or "").strip()
        normalized_prefix = normalize_action_type(prefix)
        if normalized_prefix:
            return normalized_prefix
    return ""


def _normalize_prompt_payload(raw_prompt: object) -> Optional[Dict[str, Any]]:
    if not isinstance(raw_prompt, dict):
        return None
    question = str(raw_prompt.get("question") or "").strip()
    if not question:
        return None
    payload: Dict[str, Any] = {"question": question}
    kind = str(raw_prompt.get("kind") or "").strip()
    if kind:
        payload["kind"] = kind
    choices = raw_prompt.get("choices")
    if isinstance(choices, list) and choices:
        payload["choices"] = list(choices)
    return payload


def _prompt_from_user_prompt_title(title: str) -> Optional[Dict[str, Any]]:
    text = str(title or "").strip()
    if not text:
        return None
    match = re.match(r"^user_prompt\s*[:：]\s*(.+)$", text, re.IGNORECASE)
    if not match:
        return None
    question = str(match.group(1) or "").strip()
    if not question:
        return None
    return {"question": question}


def _normalize_artifacts(values: Optional[Iterable[object]]) -> List[str]:
    out: List[str] = []
    for item in values or []:
        value = str(item or "").strip()
        if not value:
            continue
        value = value.replace("\\", "/")
        if value in out:
            continue
        out.append(value)
    return out


def _normalize_status(value: object) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return "pending"
    if raw in _PLAN_STATUS_ALLOWED:
        return raw
    return "pending"


@dataclass
class PlanStep:
    id: int
    title: str
    brief: str
    allow: List[str]
    status: str = "pending"
    kind: str = ""
    prompt: Optional[Dict[str, Any]] = None

    def to_item(self) -> dict:
        item = {
            "id": int(self.id),
            "title": str(self.title),
            "brief": str(self.brief),
            "allow": list(self.allow or []),
            "status": str(self.status),
        }
        kind_value = str(self.kind or "").strip()
        if kind_value:
            item["kind"] = kind_value
        prompt_value = _normalize_prompt_payload(self.prompt)
        if prompt_value:
            item["prompt"] = prompt_value
        return item


@dataclass
class PlanStructure:
    steps: List[PlanStep]
    artifacts: List[str]

    @classmethod
    def from_agent_plan_payload(cls, plan_obj: dict) -> "PlanStructure":
        if not isinstance(plan_obj, dict):
            return cls(steps=[], artifacts=[])

        titles_raw = plan_obj.get("titles")
        items_raw = plan_obj.get("items")
        allows_raw = plan_obj.get("allows")
        artifacts_raw = plan_obj.get("artifacts")

        titles: List[str] = [str(t).strip() for t in (titles_raw or []) if str(t).strip()] if isinstance(titles_raw, list) else []
        items: List[dict] = [dict(it) for it in (items_raw or []) if isinstance(it, dict)] if isinstance(items_raw, list) else []
        allows: List[List[str]] = []
        if isinstance(allows_raw, list):
            for value in allows_raw:
                allows.append(_normalize_allow_list(value))

        if not titles and items:
            for item in items:
                title = str(item.get("title") or "").strip()
                if title:
                    titles.append(title)

        steps: List[PlanStep] = []
        for idx, title in enumerate(titles, start=1):
            item = items[idx - 1] if idx - 1 < len(items) and isinstance(items[idx - 1], dict) else {}
            brief = str(item.get("brief") or "").strip() or _fallback_brief_from_title(title)
            allow = allows[idx - 1] if idx - 1 < len(allows) else _normalize_allow_list(item.get("allow"))
            status = _normalize_status(item.get("status"))
            kind = _normalize_step_kind(item.get("kind"), allow=list(allow or []), title=title)
            prompt = _normalize_prompt_payload(item.get("prompt"))
            steps.append(
                PlanStep(
                    id=idx,
                    title=title,
                    brief=brief,
                    allow=list(allow or []),
                    status=status,
                    kind=kind,
                    prompt=prompt,
                )
            )

        if not steps and items:
            for idx, item in enumerate(items, start=1):
                title = str(item.get("title") or "").strip()
                if not title:
                    continue
                brief = str(item.get("brief") or "").strip() or _fallback_brief_from_title(title)
                allow = _normalize_allow_list(item.get("allow"))
                status = _normalize_status(item.get("status"))
                kind = _normalize_step_kind(item.get("kind"), allow=list(allow or []), title=title)
                prompt = _normalize_prompt_payload(item.get("prompt"))
                steps.append(
                    PlanStep(
                        id=idx,
                        title=title,
                        brief=brief,
                        allow=allow,
                        status=status,
                        kind=kind,
                        prompt=prompt,
                    )
                )

        normalized_artifacts = _normalize_artifacts(artifacts_raw if isinstance(artifacts_raw, list) else [])
        plan = cls(steps=steps, artifacts=normalized_artifacts)
        plan.validate()
        return plan

    @classmethod
    def from_legacy(
        cls,
        *,
        plan_titles: List[str],
        plan_items: List[dict],
        plan_allows: List[List[str]],
        plan_artifacts: List[str],
    ) -> "PlanStructure":
        titles = [str(title or "").strip() for title in (plan_titles or []) if str(title or "").strip()]
        items = [dict(item) for item in (plan_items or []) if isinstance(item, dict)]
        allows = [list(allow or []) for allow in (plan_allows or [])]

        steps: List[PlanStep] = []
        for idx, title in enumerate(titles, start=1):
            item = items[idx - 1] if idx - 1 < len(items) else {}
            brief = str(item.get("brief") or "").strip() or _fallback_brief_from_title(title)
            allow = _normalize_allow_list(allows[idx - 1] if idx - 1 < len(allows) else item.get("allow"))
            status = _normalize_status(item.get("status"))
            kind = _normalize_step_kind(item.get("kind"), allow=list(allow or []), title=title)
            prompt = _normalize_prompt_payload(item.get("prompt"))
            steps.append(
                PlanStep(
                    id=idx,
                    title=title,
                    brief=brief,
                    allow=list(allow or []),
                    status=status,
                    kind=kind,
                    prompt=prompt,
                )
            )

        artifacts = _normalize_artifacts(plan_artifacts or [])
        plan = cls(steps=steps, artifacts=artifacts)
        plan.validate()
        return plan

    def validate(self) -> None:
        for idx, step in enumerate(self.steps, start=1):
            title = str(step.title or "").strip()
            if not title:
                raise ValueError(f"plan.steps[{idx}].title 不能为空")
            step.title = title

            brief = str(step.brief or "").strip()
            if not brief:
                brief = _fallback_brief_from_title(title)
            if len(brief) > int(AGENT_PLAN_BRIEF_MAX_CHARS):
                brief = brief[: int(AGENT_PLAN_BRIEF_MAX_CHARS)]
            step.brief = brief

            step.allow = _normalize_allow_list(step.allow)
            step.status = _normalize_status(step.status)
            step.kind = _normalize_step_kind(step.kind, allow=list(step.allow or []), title=title)
            step.prompt = _normalize_prompt_payload(step.prompt)
            if step.kind == "user_prompt" and not step.prompt:
                step.prompt = _prompt_from_user_prompt_title(title)
            if step.kind not in {"user_prompt", "task_feedback"} and step.prompt:
                # 仅交互类步骤维护 prompt 结构，避免语义漂移。
                step.prompt = None
            step.id = idx

        self.artifacts = _normalize_artifacts(self.artifacts)

    def clone(self) -> "PlanStructure":
        return PlanStructure(
            steps=[
                PlanStep(
                    id=int(step.id),
                    title=str(step.title),
                    brief=str(step.brief),
                    allow=list(step.allow or []),
                    status=str(step.status or "pending"),
                    kind=str(step.kind or ""),
                    prompt=dict(step.prompt) if isinstance(step.prompt, dict) else None,
                )
                for step in (self.steps or [])
            ],
            artifacts=list(self.artifacts or []),
        )

    def to_legacy_lists(self) -> Tuple[List[str], List[dict], List[List[str]], List[str]]:
        self.validate()
        titles: List[str] = []
        items: List[dict] = []
        allows: List[List[str]] = []
        for step in self.steps:
            titles.append(str(step.title))
            allows.append(list(step.allow or []))
            item = {
                "id": int(step.id),
                "title": str(step.title),
                "brief": str(step.brief),
                "allow": list(step.allow or []),
                "status": str(step.status),
            }
            kind_value = str(step.kind or "").strip()
            if kind_value:
                item["kind"] = kind_value
            prompt_value = _normalize_prompt_payload(step.prompt)
            if prompt_value:
                item["prompt"] = prompt_value
            items.append(item)
        return titles, items, allows, list(self.artifacts or [])

    def to_agent_plan_payload(self) -> dict:
        titles, items, allows, artifacts = self.to_legacy_lists()
        return {
            "titles": titles,
            "items": items,
            "allows": allows,
            "artifacts": artifacts,
        }

    # ---- 变更 API（替代直接操作 legacy lists）----

    @property
    def step_count(self) -> int:
        return len(self.steps)

    def get_step(self, idx: int) -> Optional[PlanStep]:
        """0-based 索引获取步骤。"""
        if 0 <= idx < len(self.steps):
            return self.steps[idx]
        return None

    def set_step_status(self, idx: int, status: str) -> None:
        """
        设置指定步骤状态（0-based 索引）。

        遵循状态转移表 _VALID_TRANSITIONS：非法转移将被静默拒绝并记录警告日志。
        """
        if not (0 <= idx < len(self.steps)):
            return
        new_status = _normalize_status(status)
        current = self.steps[idx].status
        if current == new_status:
            return
        allowed_targets = _VALID_TRANSITIONS.get(current)
        if allowed_targets is not None and new_status not in allowed_targets:
            logger.warning(
                "plan_step[%d] 非法状态转移: %s -> %s（已忽略）",
                idx, current, new_status,
            )
            return
        self.steps[idx].status = new_status

    def mark_running_as_done(self) -> None:
        """将所有 running 步骤标记为 done（每次进入新步骤前调用）。"""
        for step in self.steps:
            if step.status == "running":
                step.status = "done"

    def insert_steps(self, at_idx: int, new_steps: List[PlanStep]) -> None:
        """在指定位置插入步骤并重编号。"""
        for i, step in enumerate(new_steps):
            self.steps.insert(at_idx + i, step)
        self._renumber()

    def replace_from(self, merged: "PlanStructure") -> None:
        """整体替换（用于 replan 合并后的结果回写）。"""
        self.steps = list(merged.steps)
        self.artifacts = list(merged.artifacts)
        self._renumber()

    def _renumber(self) -> None:
        """重编号步骤 ID。"""
        for idx, step in enumerate(self.steps, start=1):
            step.id = idx

    def get_titles_json(self) -> str:
        """返回标题列表的 JSON（用于 prompt）。"""
        return json.dumps([s.title for s in self.steps], ensure_ascii=False)

    def get_items_payload(self) -> List[dict]:
        """返回 plan_items 格式的 payload（用于 SSE 推送）。"""
        return [step.to_item() for step in self.steps]

    def get_titles(self) -> List[str]:
        """返回标题列表。"""
        return [str(step.title) for step in self.steps]
