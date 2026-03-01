# -*- coding: utf-8 -*-
"""CLI 流式会话执行与回放补齐。"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from backend.src.cli.commands.stream_render import RenderOutcome, render_stream_event
from backend.src.cli.output import print_sse_status
from backend.src.cli.sse import SseEvent

_TERMINAL_RUN_STATUSES = {"done", "failed", "stopped", "cancelled"}


def _coerce_positive_int(value: object) -> Optional[int]:
    try:
        num = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if num <= 0:
        return None
    return num


def _normalize_run_status(value: object) -> str:
    return str(value or "").strip().lower()


@dataclass
class StreamSessionResult:
    seen_done: bool = False
    seen_error: bool = False
    saw_business_state_event: bool = False
    last_run_status: str = ""
    last_run_id: Optional[int] = None
    last_task_id: Optional[int] = None
    last_event_id: str = ""
    replay_applied: int = 0
    seen_event_ids: set[str] = field(default_factory=set)
    last_run_status_by_run: dict[str, str] = field(default_factory=dict)
    last_need_input_by_run: dict[str, str] = field(default_factory=dict)
    seen_structural_keys: set[str] = field(default_factory=set)

    def update_from_payload(self, payload: Dict[str, Any]) -> None:
        event_id = str(payload.get("event_id") or "").strip()
        if event_id:
            self.last_event_id = event_id
            self.seen_event_ids.add(event_id)
        run_id = _coerce_positive_int(payload.get("run_id"))
        if run_id is not None:
            self.last_run_id = run_id
        task_id = _coerce_positive_int(payload.get("task_id"))
        if task_id is not None:
            self.last_task_id = task_id
        if str(payload.get("type") or "").strip() == "run_status":
            normalized = _normalize_run_status(payload.get("status"))
            if normalized:
                self.saw_business_state_event = True
                self.last_run_status = normalized
        if str(payload.get("type") or "").strip() == "need_input":
            self.saw_business_state_event = True
        if str(payload.get("type") or "").strip() in {"done", "stream_end"}:
            if str(payload.get("run_status") or "").strip():
                self.saw_business_state_event = True
            self.seen_done = True

    @staticmethod
    def _run_task_key(payload: Dict[str, Any]) -> str:
        run_id = _coerce_positive_int(payload.get("run_id"))
        task_id = _coerce_positive_int(payload.get("task_id"))
        run_part = str(run_id or 0)
        task_part = str(task_id or 0)
        return f"{task_part}:{run_part}"

    def should_process_payload(self, payload: Dict[str, Any]) -> bool:
        event_id = str(payload.get("event_id") or "").strip()
        if event_id:
            if event_id in self.seen_event_ids:
                return False
            self.seen_event_ids.add(event_id)

        payload_type = str(payload.get("type") or "").strip().lower()
        run_task_key = self._run_task_key(payload)

        if payload_type == "run_status":
            status = _normalize_run_status(payload.get("status"))
            if not status:
                return True
            prev = str(self.last_run_status_by_run.get(run_task_key) or "").strip().lower()
            if prev and prev == status:
                return False
            self.last_run_status_by_run[run_task_key] = status
            return True

        if payload_type == "need_input":
            nested = payload.get("payload")
            nested_payload = nested if isinstance(nested, dict) else {}
            kind = str(payload.get("kind") or nested_payload.get("kind") or "").strip()
            prompt_token = str(
                payload.get("prompt_token")
                or payload.get("promptToken")
                or nested_payload.get("prompt_token")
                or ""
            ).strip()
            question = str(payload.get("question") or nested_payload.get("question") or "").strip()
            marker = prompt_token or question
            if not marker:
                return True
            next_marker = f"{kind}:{marker}"
            prev = str(self.last_need_input_by_run.get(run_task_key) or "")
            if prev and prev == next_marker:
                return False
            self.last_need_input_by_run[run_task_key] = next_marker
            return True

        if payload_type in {"run_created", "done", "stream_end"}:
            dedup_key = f"{payload_type}:{run_task_key}"
            if dedup_key in self.seen_structural_keys:
                return False
            self.seen_structural_keys.add(dedup_key)
            return True

        return True


def _make_sse_event_from_payload(payload: Dict[str, Any]) -> SseEvent:
    event_name = "message"
    payload_type = str(payload.get("type") or "").strip()
    if payload_type in {"done", "stream_end"}:
        event_name = "done"
    elif payload_type == "error":
        event_name = "error"
    return SseEvent(
        event=event_name,
        data=json.dumps(payload, ensure_ascii=False),
        json_data=payload,
    )


def _apply_event(
    *,
    event: SseEvent,
    output_json: bool,
    done_message: str,
    result: StreamSessionResult,
) -> RenderOutcome:
    if isinstance(event.json_data, dict):
        if not result.should_process_payload(event.json_data):
            return "skip"
    outcome = render_stream_event(event, output_json, done_message=done_message)
    if outcome == "done":
        result.seen_done = True
    elif outcome == "error":
        result.seen_error = True

    if isinstance(event.json_data, dict):
        result.update_from_payload(event.json_data)
    return outcome


def _replay_agent_run_events(
    *,
    client: Any,
    run_id: int,
    after_event_id: Optional[str],
    output_json: bool,
    done_message: str,
    result: StreamSessionResult,
    limit: int = 200,
    max_batches: int = 4,
) -> int:
    cursor = str(after_event_id or "").strip() or None
    applied = 0
    for _ in range(max_batches):
        params: Dict[str, Any] = {"limit": int(limit)}
        if cursor:
            params["after_event_id"] = cursor
        response = client.get(f"/agent/runs/{int(run_id)}/events", params=params)
        items = response.get("items") if isinstance(response, dict) else None
        if not isinstance(items, list) or not items:
            break

        progressed = False
        for item in items:
            if not isinstance(item, dict):
                continue
            payload = item.get("payload")
            if not isinstance(payload, dict):
                continue
            event_id = str(payload.get("event_id") or item.get("event_id") or "").strip()
            if event_id:
                if event_id == cursor:
                    progressed = True
                    continue
                if event_id in result.seen_event_ids:
                    cursor = event_id
                    progressed = True
                    continue
                cursor = event_id
                progressed = True
            event = _make_sse_event_from_payload(payload)
            outcome = _apply_event(
                event=event,
                output_json=output_json,
                done_message=done_message,
                result=result,
            )
            if outcome != "skip":
                applied += 1

        if len(items) < int(limit):
            break
        if not progressed:
            break
    return int(applied)


def run_stream_session(
    *,
    client: Any,
    path: str,
    payload: Dict[str, Any],
    output_json: bool,
    done_message: str,
    enable_agent_replay: bool = False,
) -> StreamSessionResult:
    """
    执行 CLI 流式会话，并在必要时进行 agent 事件回放补齐。

    说明：
    - 优先按实时流事件渲染；
    - 当出现流错误或缺失 done/stream_end 时，若可定位 run_id 则尝试回放事件日志；
    - 与前端保持一致：若只收到 done 但未见业务状态事件(run_status/need_input)，也会触发回放补齐；
    - 回放成功补齐后，不再把本次会话标记为 transport error。
    """
    result = StreamSessionResult()
    for event in client.stream_post(path, json_data=payload):
        _apply_event(
            event=event,
            output_json=output_json,
            done_message=done_message,
            result=result,
        )

    need_replay = bool(enable_agent_replay) and (
        result.seen_error
        or not result.seen_done
        or (result.seen_done and not result.saw_business_state_event)
    )
    if need_replay and result.last_run_id is not None:
        applied = _replay_agent_run_events(
            client=client,
            run_id=int(result.last_run_id),
            after_event_id=result.last_event_id or None,
            output_json=output_json,
            done_message=done_message,
            result=result,
        )
        result.replay_applied = int(applied)
        if applied > 0:
            # 与前端 stream replay 对齐：补齐到结构化事件后，清除 transport 层错误标记。
            result.seen_error = False
            if not output_json:
                print_sse_status("回放", f"已补齐 {applied} 条事件", "dim")

    if not result.seen_done and result.last_run_status in _TERMINAL_RUN_STATUSES:
        result.seen_done = True

    return result
