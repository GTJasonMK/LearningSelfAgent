import unittest


class TestSystemPromptFormatting(unittest.TestCase):
    def test_system_prompts_can_format_without_keyerror(self):
        from backend.src.prompt.system_prompts import load_system_prompt

        # skill_classify
        classify = load_system_prompt("skill_classify")
        self.assertIsInstance(classify, str)
        text = classify.format(categories="- misc", skill="{}")
        self.assertTrue(text)

        # skill_from_tool
        from_tool = load_system_prompt("skill_from_tool")
        self.assertIsInstance(from_tool, str)
        text = from_tool.format(tool="{}")
        self.assertTrue(text)

        # agent_evaluate
        evaluate = load_system_prompt("agent_evaluate")
        self.assertIsInstance(evaluate, str)
        text = evaluate.format(
            skill_categories="- misc",
            pass_threshold=80,
            distill_threshold=90,
            user_note="(无)",
            task_title="t",
            run_meta="{}",
            plan="{}",
            steps="[]",
            outputs="[]",
            tool_calls="[]",
        )
        self.assertTrue(text)

    def test_agent_react_step_prompt_template_can_format_without_keyerror(self):
        from backend.src.constants import AGENT_REACT_STEP_PROMPT_TEMPLATE

        text = AGENT_REACT_STEP_PROMPT_TEMPLATE.format(
            now="2026-02-14T00:00:00Z",
            workdir=".",
            agent_workspace="backend/.agent/workspace",
            message="test",
            plan="[\"step1\"]",
            step_index=1,
            step_title="step1",
            allowed_actions="tool_call",
            observations="- none",
            graph="(无)",
            tools="(无)",
            skills="(无)",
            memories="(无)",
            output_style="简洁输出",
            action_types_line="tool_call,task_output",
        )
        self.assertIn("user_prompt", text)
        self.assertIn("{label,value}", text)


if __name__ == "__main__":
    unittest.main()
