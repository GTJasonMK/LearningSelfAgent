# -*- coding: utf-8 -*-
"""
终端输出格式化。

支持两种模式：
- rich 模式（默认）：使用表格/面板渲染
- JSON 模式（--json）：原始 JSON 输出，便于管道/脚本消费
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.markup import escape as rich_escape
from rich.panel import Panel
from rich.table import Table

console = Console()

# 状态颜色映射
_STATUS_COLORS = {
    "done": "green",
    "running": "yellow",
    "queued": "cyan",
    "planned": "cyan",
    "pending": "dim",
    "failed": "red",
    "stopped": "red",
    "waiting": "magenta",
    "skipped": "dim",
    "approved": "green",
    "draft": "yellow",
    "deprecated": "red",
}


def _status_style(status: str) -> str:
    return _STATUS_COLORS.get(str(status).lower(), "white")


def print_json(data: Any) -> None:
    """JSON 格式输出。"""
    console.print_json(json.dumps(data, ensure_ascii=False, default=str))


def print_error(message: str, code: Optional[str] = None) -> None:
    """错误信息输出。"""
    title = f"错误 [{code}]" if code else "错误"
    console.print(Panel(message, title=title, border_style="red"))


def print_success(message: str) -> None:
    """成功信息输出。"""
    console.print(f"[green]{message}[/green]")


def print_warning(message: str) -> None:
    """警告信息输出。"""
    console.print(f"[yellow]{message}[/yellow]")


def print_sse_delta(text: str) -> None:
    """SSE 增量文本输出（打字机效果）。"""
    sys.stdout.write(text)
    sys.stdout.flush()


def print_sse_status(label: str, message: str, style: str = "cyan") -> None:
    """SSE 状态行输出。"""
    safe_label = rich_escape(str(label))
    safe_message = rich_escape(str(message))
    console.print(f"[{style}][{safe_label}][/{style}] {safe_message}")


# ── 任务相关 ──


def print_tasks_table(tasks: List[Dict[str, Any]]) -> None:
    """表格形式展示任务列表。"""
    if not tasks:
        print_warning("暂无任务")
        return
    table = Table(title="任务列表")
    table.add_column("ID", style="cyan", justify="right", width=6)
    table.add_column("标题", style="white", min_width=20)
    table.add_column("状态", width=10)
    table.add_column("创建时间", style="dim", width=19)
    for t in tasks:
        s = str(t.get("status", ""))
        table.add_row(
            str(t.get("id", "")),
            str(t.get("title", "")),
            f"[{_status_style(s)}]{s}[/{_status_style(s)}]",
            str(t.get("created_at", ""))[:19],
        )
    console.print(table)


def print_task_detail(task: Dict[str, Any]) -> None:
    """面板形式展示任务详情。"""
    lines = [
        f"ID:     {task.get('id', '')}",
        f"标题:   {task.get('title', '')}",
        f"状态:   {task.get('status', '')}",
        f"创建:   {str(task.get('created_at', ''))[:19]}",
    ]
    if task.get("expectation_id"):
        lines.append(f"期望ID: {task['expectation_id']}")
    console.print(Panel("\n".join(lines), title="任务详情", border_style="cyan"))


# ── 记忆相关 ──


def print_memory_table(items: List[Dict[str, Any]]) -> None:
    """表格形式展示记忆列表。"""
    if not items:
        print_warning("暂无记忆项")
        return
    table = Table(title="记忆列表")
    table.add_column("ID", style="cyan", justify="right", width=6)
    table.add_column("内容", style="white", min_width=30, max_width=60)
    table.add_column("类型", width=10)
    table.add_column("标签", style="dim", max_width=20)
    table.add_column("创建时间", style="dim", width=19)
    for item in items:
        content = str(item.get("content", ""))
        # 截断过长内容
        if len(content) > 60:
            content = content[:57] + "..."
        tags = item.get("tags")
        tags_str = ", ".join(tags) if isinstance(tags, list) else str(tags or "")
        if len(tags_str) > 20:
            tags_str = tags_str[:17] + "..."
        table.add_row(
            str(item.get("id", "")),
            content,
            str(item.get("memory_type", "")),
            tags_str,
            str(item.get("created_at", ""))[:19],
        )
    console.print(table)


def print_memory_detail(item: Dict[str, Any]) -> None:
    """面板形式展示单个记忆项详情。"""
    tags = item.get("tags")
    tags_str = ", ".join(tags) if isinstance(tags, list) else str(tags or "")
    lines = [
        f"ID:     {item.get('id', '')}",
        f"类型:   {item.get('memory_type', '')}",
        f"标签:   {tags_str}",
        f"创建:   {str(item.get('created_at', ''))[:19]}",
    ]
    if item.get("task_id"):
        lines.append(f"任务ID: {item['task_id']}")
    lines.append("")
    lines.append(str(item.get("content", "")))
    console.print(Panel("\n".join(lines), title="记忆详情", border_style="green"))


# ── 技能相关 ──


def print_skills_table(items: List[Dict[str, Any]]) -> None:
    """表格形式展示技能列表。"""
    if not items:
        print_warning("暂无技能")
        return
    table = Table(title="技能列表")
    table.add_column("ID", style="cyan", justify="right", width=6)
    table.add_column("名称", style="white", min_width=20)
    table.add_column("类别", width=15)
    table.add_column("状态", width=10)
    for item in items:
        s = str(item.get("status", ""))
        table.add_row(
            str(item.get("id", "")),
            str(item.get("name", "")),
            str(item.get("category", "")),
            f"[{_status_style(s)}]{s}[/{_status_style(s)}]",
        )
    console.print(table)


def print_skill_detail(item: Dict[str, Any]) -> None:
    """面板形式展示技能详情。"""
    lines = [
        f"ID:     {item.get('id', '')}",
        f"名称:   {item.get('name', '')}",
        f"类别:   {item.get('category', '')}",
        f"状态:   {item.get('status', '')}",
        f"版本:   {item.get('version', '')}",
    ]
    desc = item.get("description")
    if desc:
        lines.append("")
        lines.append(str(desc))
    steps = item.get("steps")
    if isinstance(steps, list) and steps:
        lines.append("")
        lines.append("步骤:")
        for i, step in enumerate(steps, 1):
            lines.append(f"  {i}. {step}")
    console.print(Panel("\n".join(lines), title="技能详情", border_style="blue"))


# ── 知识图谱相关 ──


def print_graph_nodes_table(nodes: List[Dict[str, Any]]) -> None:
    """表格形式展示图谱节点。"""
    if not nodes:
        print_warning("暂无图谱节点")
        return
    table = Table(title="图谱节点")
    table.add_column("ID", style="cyan", justify="right", width=6)
    table.add_column("标签", style="white", min_width=20)
    table.add_column("类型", width=15)
    for node in nodes:
        table.add_row(
            str(node.get("id", "")),
            str(node.get("label", "")),
            str(node.get("node_type", "")),
        )
    console.print(table)


def print_graph_edges_table(edges: List[Dict[str, Any]]) -> None:
    """表格形式展示图谱边。"""
    if not edges:
        print_warning("暂无图谱边")
        return
    table = Table(title="图谱边")
    table.add_column("ID", style="cyan", justify="right", width=6)
    table.add_column("源节点", justify="right", width=8)
    table.add_column("关系", style="white", min_width=15)
    table.add_column("目标节点", justify="right", width=8)
    table.add_column("置信度", width=8)
    for edge in edges:
        conf = edge.get("confidence")
        conf_str = f"{conf:.2f}" if conf is not None else ""
        table.add_row(
            str(edge.get("id", "")),
            str(edge.get("source", "")),
            str(edge.get("relation", "")),
            str(edge.get("target", "")),
            conf_str,
        )
    console.print(table)


# ── 搜索相关 ──


def print_search_results(results: Dict[str, Any]) -> None:
    """分组展示统一搜索结果。"""
    memory_items = results.get("memory", [])
    skill_items = results.get("skills", [])
    graph = results.get("graph", {})
    graph_nodes = graph.get("nodes", []) if isinstance(graph, dict) else []

    has_results = False
    for section_key, section_title in [
        ("memory", "记忆"),
        ("skills", "技能"),
        ("graph_nodes", "图谱节点"),
    ]:
        if section_key == "memory":
            items = memory_items
        elif section_key == "skills":
            items = skill_items
        else:
            items = graph_nodes
        if not items:
            continue
        has_results = True
        console.print(f"\n[bold]{section_title}[/bold] ({len(items)} 条)")
        for item in items:
            name = item.get("content") or item.get("name") or item.get("label") or ""
            if len(str(name)) > 80:
                name = str(name)[:77] + "..."
            item_id = item.get("id", "")
            console.print(f"  [cyan]#{item_id}[/cyan] {name}")
    if not has_results:
        print_warning("未找到匹配结果")


# ── 聊天相关 ──


def print_chat_messages(messages: List[Dict[str, Any]]) -> None:
    """展示聊天消息列表。"""
    if not messages:
        print_warning("暂无聊天消息")
        return
    for msg in messages:
        role = str(msg.get("role", "")).upper()
        content = str(msg.get("content", ""))
        ts = str(msg.get("created_at", ""))[:19]
        role_style = "green" if role == "ASSISTANT" else "cyan"
        console.print(f"[dim]{ts}[/dim] [{role_style}]{role}[/{role_style}]: {content}")


# ── 通用 ──


def print_summary(data: Dict[str, Any], title: str) -> None:
    """面板形式展示汇总信息。"""
    lines = []
    for key, value in data.items():
        lines.append(f"{key}: {value}")
    console.print(Panel("\n".join(lines), title=title, border_style="cyan"))
