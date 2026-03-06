import os
import tempfile
import unittest
from unittest.mock import Mock

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.react_error_handler import handle_action_invalid
from backend.src.constants import RUN_STATUS_FAILED


class TestReactActionInvalidBudget(unittest.TestCase):
    def test_action_invalid_exceeds_budget_fail_fast_without_replan(self):
        old_value = os.environ.get("AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX")
        os.environ["AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX"] = "1"
        try:
            plan = PlanStructure.from_legacy(
                plan_titles=["步骤1", "步骤2"],
                plan_items=[
                    {"id": 1, "title": "步骤1", "status": "running"},
                    {"id": 2, "title": "步骤2", "status": "pending"},
                ],
                plan_allows=[["file_write"], ["task_output"]],
                plan_artifacts=[],
            )

            def _unexpected_replan(**_kwargs):
                raise AssertionError("repeat budget exceeded 后不应继续 replan")

            gen = handle_action_invalid(
                task_id=1,
                run_id=1,
                step_order=1,
                idx=0,
                title="file_write:tmp.py",
                message="m",
                workdir=".",
                model="gpt-5.2",
                react_params={"temperature": 0.2},
                tools_hint="(无)",
                skills_hint="(无)",
                memories_hint="(无)",
                graph_hint="(无)",
                action_validate_error="LLM call timeout after 20s",
                last_action_text=None,
                plan_struct=plan,
                agent_state={},
                context={},
                observations=[],
                max_steps_limit=10,
                run_replan_and_merge=_unexpected_replan,
                safe_write_debug=Mock(),
            )

            events = []
            result = ("", None)
            try:
                while True:
                    events.append(next(gen))
            except StopIteration as stop:
                result = stop.value

            self.assertEqual(result[0], RUN_STATUS_FAILED)
            self.assertIsNone(result[1])
            self.assertTrue(
                any("停止自动重规划并终止本轮执行" in str(msg) for msg in events),
                "应输出 fail-fast 提示",
            )
        finally:
            if old_value is None:
                os.environ.pop("AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX", None)
            else:
                os.environ["AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX"] = old_value

    def test_action_invalid_transient_retries_current_step(self):
        old_value = os.environ.get("AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX")
        os.environ["AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX"] = "3"
        try:
            plan = PlanStructure.from_legacy(
                plan_titles=["步骤1", "步骤2"],
                plan_items=[
                    {"id": 1, "title": "步骤1", "status": "running"},
                    {"id": 2, "title": "步骤2", "status": "pending"},
                ],
                plan_allows=[["file_write"], ["task_output"]],
                plan_artifacts=[],
            )

            gen = handle_action_invalid(
                task_id=1,
                run_id=1,
                step_order=1,
                idx=0,
                title="file_write:tmp.py",
                message="m",
                workdir=".",
                model="gpt-5.2",
                react_params={"temperature": 0.2},
                tools_hint="(无)",
                skills_hint="(无)",
                memories_hint="(无)",
                graph_hint="(无)",
                action_validate_error="LLM call timeout after 30s",
                last_action_text=None,
                plan_struct=plan,
                agent_state={},
                context={},
                observations=[],
                max_steps_limit=10,
                run_replan_and_merge=Mock(return_value=None),
                safe_write_debug=Mock(),
            )

            events = []
            result = ("", None)
            try:
                while True:
                    events.append(next(gen))
            except StopIteration as stop:
                result = stop.value

            self.assertEqual(result[0], "")
            self.assertEqual(result[1], 0)
            self.assertTrue(
                any("重试当前步骤" in str(msg) for msg in events),
                "transient action_invalid 应重试当前步骤，而不是跳到下一步",
            )
        finally:
            if old_value is None:
                os.environ.pop("AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX", None)
            else:
                os.environ["AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX"] = old_value

    def test_action_invalid_transient_repeat_updates_switch_constraints(self):
        old_value = os.environ.get("AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX")
        os.environ["AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX"] = "5"
        try:
            plan = PlanStructure.from_legacy(
                plan_titles=["步骤1", "步骤2"],
                plan_items=[
                    {"id": 1, "title": "步骤1", "status": "running"},
                    {"id": 2, "title": "步骤2", "status": "pending"},
                ],
                plan_allows=[["file_write"], ["task_output"]],
                plan_artifacts=[],
            )

            agent_state = {}
            observations = []

            for _ in range(2):
                context = {}
                gen = handle_action_invalid(
                    task_id=1,
                    run_id=1,
                    step_order=1,
                    idx=0,
                    title="file_write:tmp.py",
                    message="m",
                    workdir=".",
                    model="gpt-5.2",
                    react_params={"temperature": 0.2},
                    tools_hint="(无)",
                    skills_hint="(无)",
                    memories_hint="(无)",
                    graph_hint="(无)",
                    action_validate_error="LLM call timeout after 60s",
                    last_action_text=None,
                    plan_struct=plan,
                    agent_state=agent_state,
                    context={},
                    observations=observations,
                    max_steps_limit=10,
                    run_replan_and_merge=Mock(return_value=None),
                    safe_write_debug=Mock(),
                )

                result = ("", None)
                try:
                    while True:
                        next(gen)
                except StopIteration as stop:
                    result = stop.value
                self.assertEqual(result[0], "")
                self.assertEqual(result[1], 0)

            constraints = agent_state.get("execution_constraints") or {}
            self.assertGreaterEqual(int(constraints.get("prefer_compact_action_prompt_until_step") or 0), 1)
            self.assertGreaterEqual(int(constraints.get("require_grounded_script_file_write_until_step") or 0), 1)
            self.assertGreaterEqual(int(constraints.get("prefer_action_path_switch_until_step") or 0), 1)
            self.assertTrue(any("REPAIR_HINT:" in str(item) for item in observations))
        finally:
            if old_value is None:
                os.environ.pop("AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX", None)
            else:
                os.environ["AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX"] = old_value

    def test_action_invalid_script_missing_auto_materializes_and_retries(self):
        old_value = os.environ.get("AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX")
        os.environ["AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX"] = "1"
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                missing_script = os.path.join(tmpdir, "validate_missing.py")
                plan = PlanStructure.from_legacy(
                    plan_titles=["步骤1"],
                    plan_items=[{"id": 1, "title": "步骤1", "status": "running"}],
                    plan_allows=[["shell_command"]],
                    plan_artifacts=[],
                )

                action_text = {
                    "action": {
                        "type": "shell_command",
                        "payload": {
                            "script": missing_script,
                            "command": ["python", missing_script],
                            "workdir": tmpdir,
                        },
                    }
                }

                context = {}
                gen = handle_action_invalid(
                    task_id=1,
                    run_id=1,
                    step_order=1,
                    idx=0,
                    title="shell_command:自测校验",
                    message="m",
                    workdir=tmpdir,
                    model="gpt-5.2",
                    react_params={"temperature": 0.2},
                    tools_hint="(无)",
                    skills_hint="(无)",
                    memories_hint="(无)",
                    graph_hint="(无)",
                    action_validate_error=(
                        f"[code=script_missing] shell_command 引用脚本不存在：{missing_script}（请先 file_write 再执行）"
                    ),
                    last_action_text=str(action_text).replace("'", "\""),
                    plan_struct=plan,
                    agent_state={},
                    context=context,
                    observations=[],
                    max_steps_limit=10,
                    run_replan_and_merge=Mock(return_value=None),
                    safe_write_debug=Mock(),
                )

                events = []
                result = ("", None)
                try:
                    while True:
                        events.append(next(gen))
                except StopIteration as stop:
                    result = stop.value

                self.assertEqual(result[0], RUN_STATUS_FAILED)
                self.assertIsNone(result[1])
                self.assertFalse(os.path.exists(missing_script))
                self.assertFalse(any("已自动创建" in str(msg) for msg in events))
                auto_bound_paths = list(context.get("shell_dependency_auto_bind_paths") or [])
                self.assertNotIn(missing_script, auto_bound_paths)
        finally:
            if old_value is None:
                os.environ.pop("AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX", None)
            else:
                os.environ["AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX"] = old_value

    def test_action_invalid_script_missing_allows_project_root_path(self):
        old_value = os.environ.get("AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX")
        os.environ["AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX"] = "1"
        try:
            with tempfile.TemporaryDirectory(dir=os.getcwd()) as root_tmp:
                workdir = os.path.join(root_tmp, "workdir")
                os.makedirs(workdir, exist_ok=True)
                missing_script = os.path.join(root_tmp, "scripts", "validate_root_scope.py")
                plan = PlanStructure.from_legacy(
                    plan_titles=["步骤1"],
                    plan_items=[{"id": 1, "title": "步骤1", "status": "running"}],
                    plan_allows=[["shell_command"]],
                    plan_artifacts=[],
                )

                gen = handle_action_invalid(
                    task_id=1,
                    run_id=1,
                    step_order=1,
                    idx=0,
                    title="shell_command:自测校验",
                    message="m",
                    workdir=workdir,
                    model="gpt-5.2",
                    react_params={"temperature": 0.2},
                    tools_hint="(无)",
                    skills_hint="(无)",
                    memories_hint="(无)",
                    graph_hint="(无)",
                    action_validate_error=(
                        f"[code=script_missing] shell_command 引用脚本不存在：{missing_script}（请先 file_write 再执行）"
                    ),
                    last_action_text=(
                        '{'
                        '"action":{"type":"shell_command","payload":{"script":"'
                        + missing_script.replace("\\", "\\\\")
                        + '","command":["python","'
                        + missing_script.replace("\\", "\\\\")
                        + '"]}}'
                        '}'
                    ),
                    plan_struct=plan,
                    agent_state={},
                    context={},
                    observations=[],
                    max_steps_limit=10,
                    run_replan_and_merge=Mock(return_value=None),
                    safe_write_debug=Mock(),
                )

                result = ("", None)
                try:
                    while True:
                        next(gen)
                except StopIteration as stop:
                    result = stop.value

                self.assertEqual(result[0], RUN_STATUS_FAILED)
                self.assertIsNone(result[1])
                self.assertFalse(os.path.exists(missing_script))
        finally:
            if old_value is None:
                os.environ.pop("AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX", None)
            else:
                os.environ["AGENT_REACT_ACTION_INVALID_REPEAT_FAILURE_MAX"] = old_value

    def test_file_write_action_invalid_replan_failed_should_not_skip_next_step(self):
        plan = PlanStructure.from_legacy(
            plan_titles=["file_write:backend/.agent/workspace/parse_web_table.py", "步骤2"],
            plan_items=[
                {"id": 1, "title": "file_write:backend/.agent/workspace/parse_web_table.py", "status": "running"},
                {"id": 2, "title": "步骤2", "status": "pending"},
            ],
            plan_allows=[["file_write"], ["task_output"]],
            plan_artifacts=[],
        )

        replan_calls = {"n": 0}

        def _replan_none(**kwargs):
            replan_calls["n"] += 1
            if False:
                yield kwargs
            return None

        gen = handle_action_invalid(
            task_id=1,
            run_id=1,
            step_order=1,
            idx=0,
            title="file_write:backend/.agent/workspace/parse_web_table.py",
            message="m",
            workdir=".",
            model="gpt-5.2",
            react_params={"temperature": 0.2},
            tools_hint="(无)",
            skills_hint="(无)",
            memories_hint="(无)",
            graph_hint="(无)",
            action_validate_error="malformed action envelope",
            last_action_text=None,
            plan_struct=plan,
            agent_state={},
            context={},
            observations=[],
            max_steps_limit=10,
            run_replan_and_merge=_replan_none,
            safe_write_debug=Mock(),
        )

        events = []
        result = ("", None)
        try:
            while True:
                events.append(next(gen))
        except StopIteration as stop:
            result = stop.value

        self.assertEqual(replan_calls["n"], 1)
        self.assertEqual(result[0], RUN_STATUS_FAILED)
        self.assertIsNone(result[1])
        self.assertTrue(
            any("动作生成失败且无法恢复" in str(msg) or "关键产物步骤未恢复" in str(msg) for msg in events),
            "file_write 产物步骤 action_invalid 在 replan 无效时应直接失败，而不是跳到下一步",
        )


if __name__ == "__main__":
    unittest.main()
