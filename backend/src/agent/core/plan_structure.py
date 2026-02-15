from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

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

    def to_item(self) -> dict:
        return {
            "id": int(self.id),
            "title": str(self.title),
            "brief": str(self.brief),
            "allow": list(self.allow or []),
            "status": str(self.status),
        }


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
            steps.append(
                PlanStep(
                    id=idx,
                    title=title,
                    brief=brief,
                    allow=list(allow or []),
                    status=status,
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
                steps.append(PlanStep(id=idx, title=title, brief=brief, allow=allow, status=status))

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
            steps.append(
                PlanStep(
                    id=idx,
                    title=title,
                    brief=brief,
                    allow=list(allow or []),
                    status=status,
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
            items.append(
                {
                    "id": int(step.id),
                    "title": str(step.title),
                    "brief": str(step.brief),
                    "allow": list(step.allow or []),
                    "status": str(step.status),
                }
            )
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
        """设置指定步骤状态（0-based 索引）。"""
        if 0 <= idx < len(self.steps):
            self.steps[idx].status = _normalize_status(status)

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
