import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


class TestTaskPostprocessMemory(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp.name) / "agent_test.db"
        os.environ["AGENT_DB_PATH"] = str(db_path)
        os.environ["AGENT_PROMPT_ROOT"] = str(Path(self._tmp.name) / "prompt")

        import backend.src.storage as storage
        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
            self._tmp.cleanup()
        except Exception:
            pass

    def test_postprocess_writes_auto_memory_item_for_done_run(self):
        from backend.src.constants import RUN_STATUS_DONE
        from backend.src.constants import AUTO_SKILL_SUFFIX, DEFAULT_SKILL_VERSION
        from backend.src.services.tasks.task_postprocess import postprocess_task_run
        from backend.src.storage import get_connection

        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("写一个测试文件", "done", created_at, None, created_at, created_at),
            )
            task_id = cursor.lastrowid
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (task_id, "done", "agent_command_react", created_at, created_at, created_at, created_at),
            )
            run_id = cursor.lastrowid

            # 结果输出（用于自动记忆兜底）
            conn.execute(
                "INSERT INTO task_outputs (task_id, run_id, output_type, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (task_id, run_id, "text", "最终结果：已生成文件 test.txt", created_at),
            )

            # 预先插入同名 skill，避免 postprocess 触发 classify_and_publish_skill（单测不应依赖外部 LLM）
            skill_name = f"写一个测试文件-{AUTO_SKILL_SUFFIX}#{run_id}"
            conn.execute(
                "INSERT INTO skills_items (name, created_at, description, scope, category, tags, triggers, aliases, source_path, prerequisites, inputs, outputs, steps, failure_modes, validation, version, task_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    skill_name,
                    created_at,
                    "preseed",
                    f"task:{task_id}",
                    None,
                    "[]",
                    "[]",
                    "[]",
                    None,
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    DEFAULT_SKILL_VERSION,
                    task_id,
                ),
            )

            task_row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()

        postprocess_task_run(task_row=task_row, task_id=int(task_id), run_id=int(run_id), run_status=RUN_STATUS_DONE)

        with get_connection() as conn:
            count = conn.execute("SELECT COUNT(*) AS c FROM memory_items").fetchone()["c"]
            row = conn.execute(
                "SELECT * FROM memory_items ORDER BY id DESC LIMIT 1"
            ).fetchone()

        self.assertEqual(count, 1)
        self.assertIsNotNone(row)
        self.assertIn("写一个测试文件", row["content"])
        self.assertIn("最终结果", row["content"])
        self.assertIn(f"run:{run_id}", row["tags"])


if __name__ == "__main__":
    unittest.main()
