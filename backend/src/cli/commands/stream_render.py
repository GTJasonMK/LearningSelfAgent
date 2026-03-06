# -*- coding: utf-8 -*-
"""CLI 流式事件渲染工具。"""

from __future__ import annotations

from typing import Literal
from typing import Any

from rich.markup import escape as rich_escape

from backend.src.cli.output import (
    console,
    print_error,
    print_json,
    print_sse_delta,
    print_sse_status,
)


def _status_style(status: str) -> str:
    value = str(status or "").strip().lower()
    if value == "done":
        return "green"
    if value == "running":
        return "yellow"
    if value == "waiting":
        return "magenta"
    if value in {"failed", "stopped"}:
        return "red"
    return "cyan"


RenderOutcome = Literal["done", "error", "other", "skip"]


def _safe(value: object) -> str:
    return rich_escape(str(value or ""))


def _resolve_done_status_message(*, run_status: str, done_message: str) -> tuple[str, str, str]:
    status = str(run_status or "").strip().lower()
    if status == "done":
        return "完成", done_message, "green"
    if status == "waiting":
        return "等待", "等待输入", "magenta"
    if status == "failed":
        return "失败", "执行失败", "red"
    if status == "stopped":
        return "停止", "执行已停止", "red"
    if status == "cancelled":
        return "停止", "执行已取消", "red"
    return "完成", done_message, "green"


def render_stream_event(
    event: Any,
    output_json: bool,
    *,
    done_message: str,
    last_run_status: str = "",
) -> RenderOutcome:
    """统一渲染 SSE 事件并返回事件结果类型。"""
    def _format_structured_error(data: dict) -> str:
        code = str(data.get("error_code") or data.get("code") or "").strip()
        phase = str(data.get("phase") or "").strip()
        msg = str(data.get("error_message") or data.get("message") or "").strip()
        prefix_parts = []
        if code:
            prefix_parts.append(code)
        if phase:
            prefix_parts.append(phase)
        prefix = f"[{'|'.join(prefix_parts)}] " if prefix_parts else ""
        return f"{prefix}{msg}".strip() or str(data)

    if output_json and getattr(event, "json_data", None):
        print_json(event.json_data)
        data = getattr(event, "json_data", None)
        event_name = str(getattr(event, "event", "") or "").strip().lower()
        event_type = str(data.get("type") or "").strip().lower() if isinstance(data, dict) else ""
        if event_name == "error" or event_type == "error":
            return "error"
        if event_name == "done" or event_type in {"done", "stream_end"}:
            return "done"
        return "other"

    if getattr(event, "event", "") == "error":
        msg = ""
        data = getattr(event, "json_data", None)
        if isinstance(data, dict):
            msg = _format_structured_error(data)
        print_error(msg or str(getattr(event, "data", "") or ""))
        return "error"

    data = getattr(event, "json_data", None)
    if getattr(event, "event", "") == "done" or (
        isinstance(data, dict) and str(data.get("type") or "").strip() in {"done", "stream_end"}
    ):
        terminal_status = ""
        if isinstance(data, dict):
            terminal_status = str(data.get("run_status") or data.get("status") or "").strip().lower()
        if not terminal_status:
            terminal_status = str(last_run_status or "").strip().lower()
        label, message, style = _resolve_done_status_message(
            run_status=terminal_status,
            done_message=done_message,
        )
        print_sse_status(label, message, style)
        return "done"

    if not isinstance(data, dict):
        raw = str(getattr(event, "data", "") or "")
        if raw.strip():
            print_sse_delta(raw)
        return "other"

    event_type = str(data.get("type") or "")

    if event_type == "error":
        print_error(_format_structured_error(data))
        return "error"

    if "delta" in data:
        print_sse_delta(str(data.get("delta") or ""))
        return "other"

    if event_type == "run_created":
        print_sse_status("创建", f"任务 #{data.get('task_id', '')} 运行 #{data.get('run_id', '')}")
        return "other"

    if event_type == "run_status":
        status = str(data.get("status") or "")
        print_sse_status("状态", status, _status_style(status))
        return "other"

    if event_type == "plan":
        items = data.get("items", [])
        if isinstance(items, list) and items:
            print_sse_status("规划", f"{len(items)} 个步骤")
            for item in items:
                if not isinstance(item, dict):
                    continue
                brief = str(item.get("brief") or item.get("title") or "").strip()
                console.print(f"  [dim]{item.get('id', '')}.[/dim] {_safe(brief)}")
        return "other"

    if event_type == "plan_delta":
        changes = data.get("changes", [])
        if isinstance(changes, list):
            for change in changes:
                if not isinstance(change, dict):
                    continue
                step_id = change.get("id") or change.get("step_order") or ""
                status = str(change.get("status") or "")
                title = str(change.get("title") or change.get("brief") or "")
                summary = f"{step_id}. {title} [{status}]".strip()
                print_sse_status("步骤", summary, _status_style(status))
        return "other"

    if event_type == "strategy_update":
        reason = str(data.get("reason") or "").strip()
        fingerprint = str(data.get("strategy_fingerprint") or "").strip()
        attempt_index = data.get("attempt_index")
        switched = bool(data.get("switched"))
        summary = (
            f"attempt={attempt_index} "
            f"fingerprint={fingerprint[:12] or '-'} "
            f"reason={reason or '-'} "
            f"{'switched' if switched else 'updated'}"
        ).strip()
        print_sse_status("策略", summary, "blue")
        return "other"

    if event_type == "progress_update":
        score = data.get("score")
        prev = data.get("previous_score")
        improved = bool(data.get("improved"))
        streak = data.get("no_progress_streak")
        reason = str(data.get("reason") or "").strip()
        summary = (
            f"score={score} prev={prev} "
            f"{'improved' if improved else 'no-improve'} "
            f"streak={streak} reason={reason or '-'}"
        ).strip()
        print_sse_status("进展", summary, "cyan")
        return "other"

    if event_type == "search_progress":
        stage = str(data.get("stage") or "").strip()
        query = str(data.get("query") or "").strip()
        url = str(data.get("url") or "").strip()
        host = str(data.get("host") or "").strip()
        message = str(data.get("message") or "").strip()
        error_code = str(data.get("error_code") or "").strip()
        parts = [part for part in [stage, query, host, error_code, message or url] if part]
        print_sse_status("搜索", " | ".join(parts) or "搜索处理中", "cyan")
        return "other"

    if event_type == "search_candidates":
        total = data.get("total_candidates")
        print_sse_status("候选", f"发现候选 {total}", "blue")
        candidates = data.get("candidates")
        if isinstance(candidates, list):
            for idx, item in enumerate(candidates, start=1):
                if not isinstance(item, dict):
                    continue
                host = str(item.get("host") or "").strip()
                url = str(item.get("url") or "").strip()
                score = item.get("initial_score")
                console.print(
                    f"  [dim]{idx}.[/dim] {_safe(host or '-')} score={_safe(score)} {_safe(url)}"
                )
        return "other"

    if event_type == "search_selected":
        selected = data.get("selected") if isinstance(data.get("selected"), dict) else {}
        host = str(selected.get("host") or "").strip()
        url = str(selected.get("url") or "").strip()
        score = selected.get("score")
        evidence = ", ".join([str(item) for item in (selected.get("evidence") or [])[:4]])
        summary = f"{host or '-'} score={score} {url}".strip()
        if evidence:
            summary += f" | {evidence}"
        print_sse_status("命中", summary, "green")
        return "other"

    if event_type == "search_rejected":
        total = data.get("total_rejected")
        print_sse_status("拒绝", f"拒绝候选 {total}", "yellow")
        rejected = data.get("rejected")
        if isinstance(rejected, list):
            for idx, item in enumerate(rejected, start=1):
                if not isinstance(item, dict):
                    continue
                host = str(item.get("host") or "").strip()
                reason = str(item.get("reason") or "").strip()
                detail = str(item.get("detail") or "").strip()
                console.print(
                    f"  [dim]{idx}.[/dim] {_safe(host or '-')} {_safe(reason)} {_safe(detail)}".rstrip()
                )
        return "other"

    if event_type == "step_progress":
        step_order = data.get("step_order")
        phase = str(data.get("phase") or "").strip()
        status = str(data.get("status") or "").strip()
        action_type = str(data.get("action_type") or "").strip()
        message = str(data.get("message") or "").strip()
        elapsed_ms = data.get("elapsed_ms")
        attempt = data.get("attempt")
        total_attempts = data.get("total_attempts")
        parts = []
        if step_order not in (None, ""):
            parts.append(f"#{step_order}")
        if phase:
            parts.append(phase)
        if status:
            parts.append(status)
        if action_type:
            parts.append(action_type)
        if attempt not in (None, ""):
            if total_attempts not in (None, ""):
                parts.append(f"attempt={attempt}/{total_attempts}")
            else:
                parts.append(f"attempt={attempt}")
        if elapsed_ms not in (None, ""):
            parts.append(f"elapsed_ms={elapsed_ms}")
        if message:
            parts.append(message)
        print_sse_status("阶段", " | ".join(parts) or "步骤处理中", "blue")
        return "other"

    if event_type == "unreachable_proof":
        failure_class = str(data.get("failure_class") or "").strip()
        reason = str(data.get("reason") or "").strip()
        proof_id = str(data.get("proof_id") or "").strip()
        summary = f"class={failure_class or '-'} reason={reason or '-'} proof={proof_id[:12] or '-'}"
        print_sse_status("证明", summary, "red")
        return "other"

    if event_type == "need_input":
        question = str(data.get("question") or data.get("message") or "智能体需要你的输入")
        print_sse_status("输入", question, "magenta")
        choices = data.get("choices")
        if isinstance(choices, list) and choices:
            for idx, choice in enumerate(choices, start=1):
                if not isinstance(choice, dict):
                    continue
                label = str(choice.get("label") or choice.get("value") or "").strip()
                value = str(choice.get("value") or "").strip()
                if value and value != label:
                    console.print(f"  [dim]{idx}.[/dim] {_safe(label)} [dim]=> {_safe(value)}[/dim]")
                else:
                    console.print(f"  [dim]{idx}.[/dim] {_safe(label)}")
        token = str(data.get("prompt_token") or "").strip()
        if token:
            console.print(f"[dim]prompt_token: {_safe(token)}[/dim]")
        session_key = str(data.get("session_key") or "").strip()
        if session_key:
            console.print(f"[dim]session_key: {_safe(session_key)}[/dim]")
        return "other"

    if event_type == "review":
        verdict = str(data.get("verdict") or "")
        print_sse_status("审查", f"审查结论: {verdict}", "blue")
        return "other"

    if event_type == "step_error":
        code = str(data.get("code") or "").strip()
        message = str(data.get("message") or "").strip()
        non_retriable = bool(data.get("non_retriable_failure"))
        style = "red" if non_retriable else "yellow"
        summary = f"[{code}] {message}" if code else message
        print_sse_status("步骤失败", summary or "步骤执行失败", style)
        return "other"

    if event_type == "step_warning":
        step_order = data.get("step_order")
        tool = str(data.get("tool") or "").strip()
        title = str(data.get("title") or "").strip()
        primary_warning = str(data.get("primary_warning") or "").strip()
        attempt_count = data.get("attempt_count")
        failed_attempt_count = data.get("failed_attempt_count")
        successful_attempt_count = data.get("successful_attempt_count")
        protocol_source = str(data.get("protocol_source") or "").strip()
        fallback_used = bool(data.get("fallback_used"))

        parts = []
        if step_order not in (None, ""):
            parts.append(f"#{step_order}")
        if tool:
            parts.append(tool)
        elif title:
            parts.append(title)
        if primary_warning:
            parts.append(primary_warning)
        detail = " | ".join(part for part in parts if part).strip()
        print_sse_status("步骤告警", detail or "步骤存在告警", "yellow")

        detail_parts = []
        if attempt_count not in (None, ""):
            detail_parts.append(f"attempts={attempt_count}")
        if failed_attempt_count not in (None, "") or successful_attempt_count not in (None, ""):
            detail_parts.append(
                f"failed={failed_attempt_count if failed_attempt_count not in (None, '') else 0}"
                f" success={successful_attempt_count if successful_attempt_count not in (None, '') else 0}"
            )
        if fallback_used:
            detail_parts.append("fallback=yes")
        if protocol_source:
            detail_parts.append(f"source={protocol_source}")
        if detail_parts:
            print_sse_status("告警细节", " ".join(detail_parts), "dim")
        return "other"

    if event_type == "memory_item":
        content = str(data.get("content") or "")[:50]
        print_sse_status("记忆", f"已沉淀: {content}", "green")
        return "other"

    return "other"
