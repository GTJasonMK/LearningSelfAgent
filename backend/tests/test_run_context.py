import unittest

from backend.src.agent.core.run_context import AgentContextPolicy, AgentRunContext
from backend.src.constants import AGENT_EXPERIMENT_DIR_REL


class TestAgentRunContext(unittest.TestCase):
    def test_from_agent_state_fills_required_defaults(self):
        ctx = AgentRunContext.from_agent_state(
            None,
            mode="do",
            message="hello",
            model="test-model",
            parameters={"temperature": 0.2},
            max_steps=12,
            workdir="/tmp/workspace",
            tools_hint="tool-a",
            skills_hint="skill-a",
            solutions_hint="solution-a",
            memories_hint="memory-a",
            graph_hint="graph-a",
        )
        state = ctx.to_agent_state()

        self.assertEqual(state.get("mode"), "do")
        self.assertEqual(state.get("message"), "hello")
        self.assertEqual(state.get("model"), "test-model")
        self.assertEqual(state.get("max_steps"), 12)
        self.assertEqual(state.get("workdir"), "/tmp/workspace")
        self.assertEqual(state.get("tools_hint"), "tool-a")
        self.assertEqual(state.get("skills_hint"), "skill-a")
        self.assertEqual(state.get("solutions_hint"), "solution-a")
        self.assertEqual(state.get("memories_hint"), "memory-a")
        self.assertEqual(state.get("graph_hint"), "graph-a")
        self.assertEqual(state.get("step_order"), 1)
        self.assertEqual(state.get("task_feedback_asked"), False)

        context = state.get("context") or {}
        self.assertEqual(context.get("agent_workspace_rel"), AGENT_EXPERIMENT_DIR_REL)
        self.assertEqual(context.get("enforce_task_output_evidence"), True)
        self.assertEqual(context.get("enforce_shell_script_dependency"), True)
        self.assertEqual(context.get("enforce_csv_artifact_quality_hard_fail"), True)
        self.assertIsNone(context.get("last_llm_response"))

    def test_accessors_normalize_invalid_runtime_fields(self):
        ctx = AgentRunContext.from_agent_state({"context": "invalid", "observations": "invalid"})
        self.assertIsInstance(ctx.context, dict)
        self.assertEqual(ctx.context.get("latest_parse_input_text"), None)
        self.assertEqual(ctx.observations, [])

        ctx.observations.append("step:a")
        ctx.set_stage("execute", "2026-02-15T00:00:00Z")
        state = ctx.to_agent_state()

        self.assertEqual(state.get("stage"), "execute")
        self.assertEqual(state.get("stage_at"), "2026-02-15T00:00:00Z")
        self.assertEqual(state.get("observations"), ["step:a"])

    def test_typed_properties_and_policy(self):
        ctx = AgentRunContext.from_agent_state({"mode": "do", "step_order": "0"})
        self.assertEqual(ctx.mode, "do")
        self.assertEqual(ctx.step_order, 1)

        ctx.mode = "think"
        ctx.message = "msg"
        ctx.model = "m1"
        ctx.workdir = "/tmp/w"
        ctx.step_order = 4
        ctx.paused = {"step_order": 3}
        ctx.set_hints(tools_hint="t", skills_hint="s", solutions_hint="sol", memories_hint="mem", graph_hint="g")
        ctx.set_policy(AgentContextPolicy(enforce_task_output_evidence=False, disallow_complex_python_c=False))

        state = ctx.to_agent_state()
        self.assertEqual(state.get("mode"), "think")
        self.assertEqual(state.get("message"), "msg")
        self.assertEqual(state.get("model"), "m1")
        self.assertEqual(state.get("workdir"), "/tmp/w")
        self.assertEqual(state.get("step_order"), 4)
        self.assertEqual(state.get("paused", {}).get("step_order"), 3)
        self.assertEqual(state.get("tools_hint"), "t")
        self.assertEqual(state.get("skills_hint"), "s")
        self.assertEqual(state.get("solutions_hint"), "sol")
        self.assertEqual(state.get("memories_hint"), "mem")
        self.assertEqual(state.get("graph_hint"), "g")
        self.assertFalse(bool(state.get("context", {}).get("enforce_task_output_evidence")))
        self.assertFalse(bool(state.get("context", {}).get("disallow_complex_python_c")))


if __name__ == "__main__":
    unittest.main()
