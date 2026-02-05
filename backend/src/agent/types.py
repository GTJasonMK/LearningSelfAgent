# -*- coding: utf-8 -*-
"""
Agent 公共数据结构定义。

提供强类型定义，消除 List[dict] 的滥用，提高代码可维护性。
"""

from dataclasses import dataclass, field
from typing import List, Optional, Any, Dict


@dataclass
class PlanItem:
    """
    计划步骤项。

    用于表示执行计划中的单个步骤。

    Attributes:
        id: 步骤序号（从 1 开始）
        title: 完整步骤标题，包含动作类型前缀（如 "tool_call:web_fetch 抓取数据"）
        brief: 简短描述（用于 UI 展示，通常 <= 20 字符）
        allow: 该步骤允许执行的动作类型列表
        status: 执行状态（pending/running/done/failed/waiting/planned/skipped）
    """
    id: int
    brief: str
    status: str = "pending"
    title: str = ""
    allow: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式（用于与现有代码兼容）"""
        result = {
            "id": self.id,
            "brief": self.brief,
            "status": self.status,
        }
        if self.title:
            result["title"] = self.title
        if self.allow:
            result["allow"] = self.allow
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PlanItem":
        """从字典创建实例"""
        return cls(
            id=data.get("id", 0),
            title=data.get("title", ""),
            brief=data.get("brief", ""),
            allow=data.get("allow", []),
            status=data.get("status", "pending"),
        )


@dataclass
class StepResult:
    """
    步骤执行结果。

    用于记录单个步骤的执行情况。

    Attributes:
        step_order: 步骤序号
        title: 步骤标题
        action_type: 执行的动作类型
        observation: 执行观测结果
        success: 是否成功
        error: 错误信息（仅当 success=False 时）
    """
    step_order: int
    title: str
    action_type: str
    observation: str
    success: bool
    error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "step_order": self.step_order,
            "title": self.title,
            "action_type": self.action_type,
            "observation": self.observation,
            "success": self.success,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StepResult":
        """从字典创建实例"""
        return cls(
            step_order=data.get("step_order", 0),
            title=data.get("title", ""),
            action_type=data.get("action_type", ""),
            observation=data.get("observation", ""),
            success=data.get("success", False),
            error=data.get("error", ""),
        )


@dataclass
class RetrievalContext:
    """
    知识检索上下文。

    存储从各知识源检索到的内容。

    Attributes:
        graph_nodes: 图谱节点列表
        memories: 记忆项列表
        skills: 技能列表
        domain_ids: 匹配的领域 ID 列表
        tools: 可用工具列表
    """
    graph_nodes: List[Dict[str, Any]] = field(default_factory=list)
    memories: List[Dict[str, Any]] = field(default_factory=list)
    skills: List[Dict[str, Any]] = field(default_factory=list)
    domain_ids: List[str] = field(default_factory=list)
    tools: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        """检查检索结果是否为空"""
        return (
            not self.graph_nodes
            and not self.memories
            and not self.skills
            and not self.tools
        )

    @property
    def skill_count(self) -> int:
        """技能数量"""
        return len(self.skills)

    @property
    def graph_count(self) -> int:
        """图谱节点数量"""
        return len(self.graph_nodes)

    @property
    def memory_count(self) -> int:
        """记忆数量"""
        return len(self.memories)


@dataclass
class ActionPayload:
    """
    动作载荷基类。

    所有动作类型的 payload 都应继承此类或使用此结构。

    Attributes:
        type: 动作类型
        payload: 动作具体参数
    """
    type: str
    payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "type": self.type,
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ActionPayload":
        """从字典创建实例"""
        return cls(
            type=data.get("type", ""),
            payload=data.get("payload", {}),
        )


@dataclass
class PlanPatch:
    """
    计划修正补丁。

    用于在 ReAct 循环中动态修改计划。

    Attributes:
        step_index: 要修改的步骤索引（必须是下一步）
        title: 新标题（可选）
        brief: 新简述（可选）
        allow: 新的允许动作类型（可选）
        insert_steps: 要插入的新步骤列表（可选）
        artifacts_add: 要追加的文件路径（可选）
        reason: 修改原因
    """
    step_index: int
    title: Optional[str] = None
    brief: Optional[str] = None
    allow: Optional[List[str]] = None
    insert_steps: Optional[List[Dict[str, Any]]] = None
    artifacts_add: Optional[List[str]] = None
    reason: str = ""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional["PlanPatch"]:
        """从字典创建实例，如果 data 为 None 则返回 None"""
        if not data:
            return None
        return cls(
            step_index=data.get("step_index", 0),
            title=data.get("title"),
            brief=data.get("brief"),
            allow=data.get("allow"),
            insert_steps=data.get("insert_steps"),
            artifacts_add=data.get("artifacts_add"),
            reason=data.get("reason", ""),
        )


@dataclass
class ExecutionState:
    """
    执行状态快照。

    用于跟踪 Agent 执行过程中的状态。

    Attributes:
        task_id: 任务 ID
        run_id: 执行尝试 ID
        current_step: 当前步骤索引（0-based）
        plan_items: 计划步骤列表
        artifacts: 预期产物文件路径列表
        observations: 已完成的观测结果列表
        status: 整体执行状态
    """
    task_id: int
    run_id: int
    current_step: int = 0
    plan_items: List[PlanItem] = field(default_factory=list)
    artifacts: List[str] = field(default_factory=list)
    observations: List[str] = field(default_factory=list)
    status: str = "running"

    @property
    def total_steps(self) -> int:
        """总步骤数"""
        return len(self.plan_items)

    @property
    def is_last_step(self) -> bool:
        """是否为最后一步"""
        return self.current_step >= self.total_steps - 1

    def get_current_plan_item(self) -> Optional[PlanItem]:
        """获取当前步骤项"""
        if 0 <= self.current_step < len(self.plan_items):
            return self.plan_items[self.current_step]
        return None


# 类型别名（用于渐进式迁移）
PlanItemDict = Dict[str, Any]
PlanItemList = List[Dict[str, Any]]
