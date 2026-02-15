from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from backend.src.constants import AGENT_EXPERIMENT_DIR_REL


def _normalize_dict(value: object) -> Dict:
    return dict(value) if isinstance(value, dict) else {}


def _normalize_step_order(value: object) -> int:
    try:
        normalized = int(value or 1)
    except Exception:
        normalized = 1
    if normalized < 1:
        normalized = 1
    return int(normalized)


def _normalize_observations(value: object) -> List[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


@dataclass
class AgentContextPolicy:
    enforce_task_output_evidence: bool = True
    enforce_shell_script_dependency: bool = True
    disallow_complex_python_c: bool = True
    auto_rewrite_complex_python_c: bool = True
    enforce_json_parse_recent_source: bool = True
    enforce_csv_artifact_quality: bool = True
    enforce_csv_artifact_quality_hard_fail: bool = True


@dataclass
class AgentRunContext:
    mode: str = ""
    message: str = ""
    model: str = ""
    parameters: Dict = field(default_factory=dict)
    max_steps: Optional[int] = None
    workdir: str = ""
    tools_hint: str = ""
    skills_hint: str = ""
    solutions_hint: str = ""
    memories_hint: str = ""
    graph_hint: str = ""
    task_feedback_asked: bool = False
    last_user_input: Optional[str] = None
    last_user_prompt: Optional[str] = None
    step_order: int = 1
    paused: Optional[dict] = None
    observations: List[str] = field(default_factory=list)
    context: Dict = field(default_factory=dict)
    stage: str = ""
    stage_at: str = ""
    policy_config: AgentContextPolicy = field(default_factory=AgentContextPolicy)
    extras: Dict = field(default_factory=dict)

    @staticmethod
    def _known_keys() -> set[str]:
        return {
            "mode",
            "message",
            "model",
            "parameters",
            "max_steps",
            "workdir",
            "tools_hint",
            "skills_hint",
            "solutions_hint",
            "memories_hint",
            "graph_hint",
            "task_feedback_asked",
            "last_user_input",
            "last_user_prompt",
            "step_order",
            "paused",
            "observations",
            "context",
            "stage",
            "stage_at",
        }

    @classmethod
    def from_agent_state(
        cls,
        state_obj: Optional[dict],
        *,
        mode: Optional[str] = None,
        message: Optional[str] = None,
        model: Optional[str] = None,
        parameters: Optional[dict] = None,
        max_steps: Optional[int] = None,
        workdir: Optional[str] = None,
        tools_hint: Optional[str] = None,
        skills_hint: Optional[str] = None,
        solutions_hint: Optional[str] = None,
        memories_hint: Optional[str] = None,
        graph_hint: Optional[str] = None,
    ) -> "AgentRunContext":
        state = _normalize_dict(state_obj)
        if mode is not None:
            state["mode"] = str(mode)
        if message is not None:
            state["message"] = str(message)
        if model is not None:
            state["model"] = str(model)
        if parameters is not None:
            state["parameters"] = _normalize_dict(parameters)
        if max_steps is not None:
            state["max_steps"] = max_steps
        if workdir is not None:
            state["workdir"] = str(workdir)
        if tools_hint is not None:
            state["tools_hint"] = str(tools_hint)
        if skills_hint is not None:
            state["skills_hint"] = str(skills_hint)
        if solutions_hint is not None:
            state["solutions_hint"] = str(solutions_hint)
        if memories_hint is not None:
            state["memories_hint"] = str(memories_hint)
        if graph_hint is not None:
            state["graph_hint"] = str(graph_hint)

        known_keys = cls._known_keys()
        extras = {key: value for key, value in state.items() if key not in known_keys}

        raw_max_steps = state.get("max_steps")
        try:
            normalized_max_steps = int(raw_max_steps) if raw_max_steps is not None else None
        except Exception:
            normalized_max_steps = None

        run_ctx = cls(
            mode=str(state.get("mode") or ""),
            message=str(state.get("message") or ""),
            model=str(state.get("model") or ""),
            parameters=_normalize_dict(state.get("parameters")),
            max_steps=normalized_max_steps,
            workdir=str(state.get("workdir") or ""),
            tools_hint=str(state.get("tools_hint") or ""),
            skills_hint=str(state.get("skills_hint") or ""),
            solutions_hint=str(state.get("solutions_hint") or ""),
            memories_hint=str(state.get("memories_hint") or ""),
            graph_hint=str(state.get("graph_hint") or ""),
            task_feedback_asked=bool(state.get("task_feedback_asked")),
            last_user_input=state.get("last_user_input"),
            last_user_prompt=state.get("last_user_prompt"),
            step_order=_normalize_step_order(state.get("step_order")),
            paused=dict(state.get("paused")) if isinstance(state.get("paused"), dict) else None,
            observations=_normalize_observations(state.get("observations")),
            context=_normalize_dict(state.get("context")),
            stage=str(state.get("stage") or ""),
            stage_at=str(state.get("stage_at") or ""),
            extras=extras,
        )
        run_ctx.ensure_defaults()
        return run_ctx

    def ensure_defaults(self) -> None:
        self.mode = str(self.mode or "")
        self.message = str(self.message or "")
        self.model = str(self.model or "")
        self.parameters = _normalize_dict(self.parameters)
        if self.max_steps is not None:
            try:
                self.max_steps = int(self.max_steps)
            except Exception:
                self.max_steps = None
        self.workdir = str(self.workdir or "")
        self.tools_hint = str(self.tools_hint or "")
        self.skills_hint = str(self.skills_hint or "")
        self.solutions_hint = str(self.solutions_hint or "")
        self.memories_hint = str(self.memories_hint or "")
        self.graph_hint = str(self.graph_hint or "")
        self.task_feedback_asked = bool(self.task_feedback_asked)
        self.step_order = _normalize_step_order(self.step_order)
        self.paused = dict(self.paused) if isinstance(self.paused, dict) else None
        self.observations = _normalize_observations(self.observations)
        self.context = _normalize_dict(self.context)
        self.policy_config = AgentContextPolicy(
            enforce_task_output_evidence=bool(
                self.context.get("enforce_task_output_evidence", self.policy_config.enforce_task_output_evidence)
            ),
            enforce_shell_script_dependency=bool(
                self.context.get("enforce_shell_script_dependency", self.policy_config.enforce_shell_script_dependency)
            ),
            disallow_complex_python_c=bool(
                self.context.get("disallow_complex_python_c", self.policy_config.disallow_complex_python_c)
            ),
            auto_rewrite_complex_python_c=bool(
                self.context.get("auto_rewrite_complex_python_c", self.policy_config.auto_rewrite_complex_python_c)
            ),
            enforce_json_parse_recent_source=bool(
                self.context.get("enforce_json_parse_recent_source", self.policy_config.enforce_json_parse_recent_source)
            ),
            enforce_csv_artifact_quality=bool(
                self.context.get("enforce_csv_artifact_quality", self.policy_config.enforce_csv_artifact_quality)
            ),
            enforce_csv_artifact_quality_hard_fail=bool(
                self.context.get(
                    "enforce_csv_artifact_quality_hard_fail",
                    self.policy_config.enforce_csv_artifact_quality_hard_fail,
                )
            ),
        )
        self.context.setdefault("last_llm_response", None)
        self.context.setdefault("latest_parse_input_text", None)
        self.context.setdefault("agent_workspace_rel", AGENT_EXPERIMENT_DIR_REL)
        self._sync_policy_to_context()
        self.extras = _normalize_dict(self.extras)

    def set_stage(self, stage: str, stage_at: str) -> None:
        self.stage = str(stage or "").strip()
        self.stage_at = str(stage_at or "").strip()

    @property
    def state(self) -> Dict:
        """
        兼容层：保留历史 `run_ctx.state` 读取方式。
        """
        return self.to_agent_state()

    @state.setter
    def state(self, value: dict) -> None:
        self.merge_state_overrides(value)

    def set_hints(
        self,
        *,
        tools_hint: Optional[str] = None,
        skills_hint: Optional[str] = None,
        solutions_hint: Optional[str] = None,
        memories_hint: Optional[str] = None,
        graph_hint: Optional[str] = None,
    ) -> None:
        if tools_hint is not None:
            self.tools_hint = str(tools_hint)
        if skills_hint is not None:
            self.skills_hint = str(skills_hint)
        if solutions_hint is not None:
            self.solutions_hint = str(solutions_hint)
        if memories_hint is not None:
            self.memories_hint = str(memories_hint)
        if graph_hint is not None:
            self.graph_hint = str(graph_hint)

    @property
    def policy(self) -> AgentContextPolicy:
        return AgentContextPolicy(
            enforce_task_output_evidence=bool(self.policy_config.enforce_task_output_evidence),
            enforce_shell_script_dependency=bool(self.policy_config.enforce_shell_script_dependency),
            disallow_complex_python_c=bool(self.policy_config.disallow_complex_python_c),
            auto_rewrite_complex_python_c=bool(self.policy_config.auto_rewrite_complex_python_c),
            enforce_json_parse_recent_source=bool(self.policy_config.enforce_json_parse_recent_source),
            enforce_csv_artifact_quality=bool(self.policy_config.enforce_csv_artifact_quality),
            enforce_csv_artifact_quality_hard_fail=bool(self.policy_config.enforce_csv_artifact_quality_hard_fail),
        )

    def set_policy(self, policy: AgentContextPolicy) -> None:
        self.policy_config = AgentContextPolicy(
            enforce_task_output_evidence=bool(policy.enforce_task_output_evidence),
            enforce_shell_script_dependency=bool(policy.enforce_shell_script_dependency),
            disallow_complex_python_c=bool(policy.disallow_complex_python_c),
            auto_rewrite_complex_python_c=bool(policy.auto_rewrite_complex_python_c),
            enforce_json_parse_recent_source=bool(policy.enforce_json_parse_recent_source),
            enforce_csv_artifact_quality=bool(policy.enforce_csv_artifact_quality),
            enforce_csv_artifact_quality_hard_fail=bool(policy.enforce_csv_artifact_quality_hard_fail),
        )
        self._sync_policy_to_context()

    def _sync_policy_to_context(self) -> None:
        self.context.update(
            {
                "enforce_task_output_evidence": bool(self.policy_config.enforce_task_output_evidence),
                "enforce_shell_script_dependency": bool(self.policy_config.enforce_shell_script_dependency),
                "disallow_complex_python_c": bool(self.policy_config.disallow_complex_python_c),
                "auto_rewrite_complex_python_c": bool(self.policy_config.auto_rewrite_complex_python_c),
                "enforce_json_parse_recent_source": bool(self.policy_config.enforce_json_parse_recent_source),
                "enforce_csv_artifact_quality": bool(self.policy_config.enforce_csv_artifact_quality),
                "enforce_csv_artifact_quality_hard_fail": bool(self.policy_config.enforce_csv_artifact_quality_hard_fail),
            }
        )

    def set_extra(self, key: str, value: Any) -> None:
        normalized_key = str(key or "").strip()
        if not normalized_key:
            return
        if normalized_key in self._known_keys():
            setattr(self, normalized_key, value)
            return
        self.extras[normalized_key] = value

    def merge_state_overrides(self, overrides: Optional[dict]) -> None:
        if not isinstance(overrides, dict) or not overrides:
            return
        for key, value in overrides.items():
            self.set_extra(str(key), value)
        self.ensure_defaults()

    def to_agent_state(self) -> Dict:
        self.ensure_defaults()
        payload = {
            "mode": str(self.mode),
            "message": str(self.message),
            "model": str(self.model),
            "parameters": dict(self.parameters or {}),
            "max_steps": self.max_steps,
            "workdir": str(self.workdir),
            "tools_hint": str(self.tools_hint),
            "skills_hint": str(self.skills_hint),
            "solutions_hint": str(self.solutions_hint),
            "memories_hint": str(self.memories_hint),
            "graph_hint": str(self.graph_hint),
            "task_feedback_asked": bool(self.task_feedback_asked),
            "last_user_input": self.last_user_input,
            "last_user_prompt": self.last_user_prompt,
            "step_order": int(self.step_order),
            "paused": dict(self.paused) if isinstance(self.paused, dict) else None,
            "observations": list(self.observations or []),
            "context": dict(self.context or {}),
            "stage": str(self.stage or ""),
            "stage_at": str(self.stage_at or ""),
        }
        payload.update(dict(self.extras or {}))
        return payload
