import unittest

from backend.src.agent.core.plan_structure import PlanStructure


def _make_plan_struct(plan_titles, plan_allows, plan_artifacts=None):
    """测试辅助：从标题/允许列表快速构建 PlanStructure。"""
    plan_items = [{"id": i + 1, "brief": "", "status": "pending"} for i in range(len(plan_titles))]
    return PlanStructure.from_legacy(
        plan_titles=list(plan_titles),
        plan_items=plan_items,
        plan_allows=[list(a) for a in plan_allows],
        plan_artifacts=list(plan_artifacts or []),
    )


class TestThinkParallelDependencyMap(unittest.TestCase):
    """回归：think_parallel_loop 依赖图构建的健壮性与推断能力。"""

    def test_dependency_infers_file_read_depends_on_file_write_when_artifacts_empty(self):
        from backend.src.agent.runner.think_parallel_loop import _build_dependency_map

        plan_titles = [
            "file_write:out.txt 写入结果",
            "file_read:out.txt 读取结果",
            "task_output 输出结果",
        ]
        plan_allows = [["file_write"], ["file_read"], ["task_output"]]

        dep_map = _build_dependency_map(
            plan_struct=_make_plan_struct(plan_titles, plan_allows),
            dependencies=None,
        )

        self.assertEqual(dep_map[0], [])
        self.assertEqual(dep_map[1], [0])
        # task_output 门闩：必须依赖所有前置步骤
        self.assertEqual(dep_map[2], [0, 1])

    def test_dependency_picks_latest_previous_file_write_for_same_path(self):
        from backend.src.agent.runner.think_parallel_loop import _build_dependency_map

        plan_titles = [
            "file_write:out.txt 写入 v1",
            "file_write:out.txt 写入 v2",
            "file_read:out.txt 读取结果",
        ]
        plan_allows = [["file_write"], ["file_write"], ["file_read"]]

        dep_map = _build_dependency_map(
            plan_struct=_make_plan_struct(plan_titles, plan_allows),
            dependencies=None,
        )

        self.assertEqual(dep_map[0], [])
        self.assertEqual(dep_map[1], [])
        # file_read 应依赖最近一次写入（step 1），而不是更早的 step 0
        self.assertEqual(dep_map[2], [1])

    def test_dependency_makes_file_read_depend_on_file_append_latest_modifier(self):
        """
        回归：file_append 也会修改文件内容，后续 file_read 必须依赖 append（否则并行会产生竞态）。
        """
        from backend.src.agent.runner.think_parallel_loop import _build_dependency_map

        dep_map = _build_dependency_map(
            plan_struct=_make_plan_struct(
                [
                    "file_write:out.txt 写入 v1",
                    "file_append:out.txt 追加 v2",
                    "file_read:out.txt 读取结果",
                ],
                [["file_write"], ["file_append"], ["file_read"]],
            ),
            dependencies=None,
        )

        self.assertEqual(dep_map[0], [])
        self.assertEqual(dep_map[1], [0])
        self.assertEqual(dep_map[2], [1])

    def test_dependency_ignores_out_of_range_step_index_in_dependencies(self):
        from backend.src.agent.runner.think_parallel_loop import _build_dependency_map

        dep_map = _build_dependency_map(
            plan_struct=_make_plan_struct(
                ["task_output 输出结果"],
                [["task_output"]],
            ),
            dependencies=[{"step_index": 999, "depends_on": [0]}],
        )

        self.assertEqual(dep_map, [[]])

    def test_dependency_normalizes_1_based_indices_in_llm_dependencies(self):
        """
        回归：LLM 可能把 dependencies 输出成 1-based（人类习惯），需要在并行调度前归一化为 0-based。
        """
        from backend.src.agent.runner.think_parallel_loop import _build_dependency_map

        dep_map = _build_dependency_map(
            plan_struct=_make_plan_struct(
                ["步骤1", "步骤2", "步骤3"],
                [["tool_call"], ["tool_call"], ["tool_call"]],
            ),
            dependencies=[
                {"from_step": 1, "to_step": 3, "reason": "1-based from/to"},
                {"step_index": 3, "depends_on": [1, 2], "reason": "1-based depends_on"},
            ],
        )

        self.assertEqual(dep_map[0], [])
        self.assertEqual(dep_map[1], [])
        # step3（index=2）应依赖 step1/step2（index=0,1）
        self.assertEqual(dep_map[2], [0, 1])


if __name__ == "__main__":
    unittest.main()
