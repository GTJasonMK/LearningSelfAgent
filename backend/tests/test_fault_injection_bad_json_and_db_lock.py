import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch


class TestFaultInjectionBadJsonAndDbLock(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        os.environ["AGENT_DB_PATH"] = os.path.join(self._tmpdir.name, "agent_fault_injection.db")
        os.environ["AGENT_PROMPT_ROOT"] = os.path.join(self._tmpdir.name, "prompt")
        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
        except Exception:
            pass
        self._tmpdir.cleanup()

    def test_knowledge_sufficiency_handles_bad_json(self):
        from backend.src.agent.retrieval import _assess_knowledge_sufficiency

        with patch("backend.src.agent.retrieval.call_openai", return_value=("not a json", None, None)):
            result = _assess_knowledge_sufficiency(
                message="task",
                skills=[],
                graph_nodes=[],
                memories=[],
                model="fake",
                parameters={"temperature": 0},
            )
        self.assertTrue(result.sufficient)
        self.assertEqual(result.suggestion, "proceed")

    def test_persist_loop_state_handles_db_locked(self):
        from backend.src.agent.runner import react_state_manager as sm
        from backend.src.agent.core.plan_structure import PlanStructure

        plan_struct = PlanStructure.from_legacy(
            plan_titles=["a"],
            plan_items=[{"id": 1, "brief": "a", "status": "running"}],
            plan_allows=[["llm_call"]],
            plan_artifacts=[],
        )

        with patch(
            "backend.src.agent.runner.react_state_manager.update_task_run",
            side_effect=sqlite3.OperationalError("database is locked"),
        ):
            ok = sm.persist_loop_state(
                run_id=1,
                plan_struct=plan_struct,
                agent_state={},
                step_order=1,
                observations=[],
                context={},
                paused=None,
                status=None,
                force=True,
            )
        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()

