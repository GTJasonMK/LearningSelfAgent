import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


class TestRunSolutionAutogen(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_test.db")
        self._prompt_root = Path(self._tmpdir.name) / "prompt"

        os.environ["AGENT_DB_PATH"] = self._db_path
        os.environ["AGENT_PROMPT_ROOT"] = str(self._prompt_root)
        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
        except Exception:
            pass
        self._tmpdir.cleanup()

    def _now_iso(self):
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def test_autogen_solution_from_run_creates_solution_skill_and_publishes_file(self):
        from backend.src.storage import get_connection
        from backend.src.services.skills.run_solution_autogen import autogen_solution_from_run

        now = self._now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at) VALUES (?, ?, ?)",
                ("写一个测试文件", "done", now),
            )
            task_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_plan, agent_state) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "done",
                    "agent_command_react",
                    now,
                    now,
                    now,
                    now,
                    json.dumps({"titles": ["file_write:a.txt"], "artifacts": ["a.txt"]}, ensure_ascii=False),
                    json.dumps({"message": "写一个测试文件", "mode": "do", "domain_ids": ["file.write"], "skill_ids": [1]}, ensure_ascii=False),
                ),
            )
            run_id = int(cursor.lastrowid)
            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "file_write:a.txt 写入文件",
                    "done",
                    json.dumps({"type": "file_write", "payload": {"path": "a.txt", "content": "hi"}}, ensure_ascii=False),
                    None,
                    None,
                    1,
                    now,
                    now,
                    1,
                    now,
                    now,
                ),
            )

        result = autogen_solution_from_run(task_id=task_id, run_id=run_id, force=False)
        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("status"), "created")
        self.assertIsNotNone(result.get("skill_id"))
        self.assertTrue(result.get("source_path"))

        skill_id = int(result["skill_id"])
        source_path = str(result["source_path"])

        with get_connection() as conn:
            row = conn.execute("SELECT * FROM skills_items WHERE id = ?", (skill_id,)).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["skill_type"], "solution")
        self.assertEqual(row["status"], "approved")
        self.assertEqual(int(row["source_task_id"]), task_id)
        self.assertEqual(int(row["source_run_id"]), run_id)
        self.assertEqual(row["domain_id"], "file.write")

        target = (self._prompt_root / "skills" / Path(source_path)).resolve()
        self.assertTrue(target.exists())

    def test_autogen_solution_from_run_upgrades_existing_draft_solution_in_place(self):
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill
        from backend.src.services.skills.skills_publish import publish_skill_file
        from backend.src.storage import get_connection
        from backend.src.services.skills.run_solution_autogen import autogen_solution_from_run

        now = self._now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at) VALUES (?, ?, ?)",
                ("写一个测试文件", "done", now),
            )
            task_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_plan, agent_state) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "done",
                    "agent_command_react",
                    now,
                    now,
                    now,
                    now,
                    json.dumps({"titles": ["file_write:a.txt"], "artifacts": ["a.txt"]}, ensure_ascii=False),
                    json.dumps({"message": "写一个测试文件", "mode": "do", "domain_ids": ["file.write"], "skill_ids": [1]}, ensure_ascii=False),
                ),
            )
            run_id = int(cursor.lastrowid)
            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "file_write:a.txt 写入文件",
                    "done",
                    json.dumps({"type": "file_write", "payload": {"path": "a.txt", "content": "hi"}}, ensure_ascii=False),
                    None,
                    None,
                    1,
                    now,
                    now,
                    1,
                    now,
                    now,
                ),
            )

        draft_solution_id = create_skill(
            SkillCreateParams(
                name="草稿方案",
                description="draft",
                scope=f"solution:draft:run:{int(run_id)}",
                category="solution",
                tags=["solution", "draft_solution", f"run:{int(run_id)}"],
                inputs=["写一个测试文件"],
                outputs=["old.txt"],
                steps=[{"title": "task_output:old", "allow": ["task_output"]}],
                version="0.1.0",
                task_id=int(task_id),
                domain_id="file.write",
                skill_type="solution",
                status="draft",
                source_task_id=int(task_id),
                source_run_id=int(run_id),
                created_at=now,
            )
        )
        source_path, publish_err = publish_skill_file(int(draft_solution_id))
        self.assertIsNone(publish_err)
        self.assertTrue(str(source_path or "").strip())

        # 模拟规划阶段已把草稿方案注入 agent_state.solution_ids（真实链路会发生）
        with get_connection() as conn:
            conn.execute(
                "UPDATE task_runs SET agent_state = ? WHERE id = ?",
                (
                    json.dumps(
                        {
                            "message": "写一个测试文件",
                            "mode": "do",
                            "domain_ids": ["file.write"],
                            "skill_ids": [1],
                            "solution_ids": [int(draft_solution_id)],
                        },
                        ensure_ascii=False,
                    ),
                    int(run_id),
                ),
            )

        # 覆盖并升级：应复用同一条 skills_items 记录，而不是新插入
        result = autogen_solution_from_run(task_id=task_id, run_id=run_id, force=False)
        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("status"), "upgraded")
        self.assertEqual(int(result.get("skill_id")), int(draft_solution_id))

        with get_connection() as conn:
            row = conn.execute("SELECT * FROM skills_items WHERE id = ?", (int(draft_solution_id),)).fetchone()
            count_row = conn.execute(
                "SELECT COUNT(*) AS c FROM skills_items WHERE skill_type = 'solution' AND source_run_id = ?",
                (int(run_id),),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(str(row["status"] or ""), "approved")
        self.assertEqual(int(count_row["c"]), 1)

        tags = json.loads(row["tags"] or "[]")
        self.assertNotIn(f"ref_solution:{int(draft_solution_id)}", tags)

        steps = json.loads(row["steps"] or "[]")
        titles = []
        for s in steps or []:
            if isinstance(s, dict):
                titles.append(str(s.get("title") or ""))
            else:
                titles.append(str(s))
        self.assertTrue(any("file_write:a.txt" in t for t in titles))

        # 文件也应被覆盖更新
        target = (self._prompt_root / "skills" / Path(row["source_path"])).resolve()
        self.assertTrue(target.exists())
        text = target.read_text(encoding="utf-8")
        self.assertIn("file_write:a.txt", text)

    def test_autogen_solution_from_run_force_updates_existing_solution_in_place(self):
        from backend.src.storage import get_connection
        from backend.src.services.skills.run_solution_autogen import autogen_solution_from_run

        now = self._now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at) VALUES (?, ?, ?)",
                ("写一个测试文件", "done", now),
            )
            task_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_plan, agent_state) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "done",
                    "agent_command_react",
                    now,
                    now,
                    now,
                    now,
                    json.dumps({"titles": ["file_write:a.txt"], "artifacts": ["a.txt"]}, ensure_ascii=False),
                    json.dumps({"message": "写一个测试文件", "mode": "do", "domain_ids": ["file.write"], "skill_ids": [1]}, ensure_ascii=False),
                ),
            )
            run_id = int(cursor.lastrowid)
            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "file_write:a.txt 写入文件",
                    "done",
                    json.dumps({"type": "file_write", "payload": {"path": "a.txt", "content": "hi"}}, ensure_ascii=False),
                    None,
                    None,
                    1,
                    now,
                    now,
                    1,
                    now,
                    now,
                ),
            )

        first = autogen_solution_from_run(task_id=task_id, run_id=run_id, force=False)
        self.assertTrue(first.get("ok"))
        self.assertEqual(first.get("status"), "created")
        skill_id = int(first.get("skill_id"))

        second = autogen_solution_from_run(task_id=task_id, run_id=run_id, force=True)
        self.assertTrue(second.get("ok"))
        self.assertEqual(second.get("status"), "updated")
        self.assertEqual(int(second.get("skill_id")), int(skill_id))

        with get_connection() as conn:
            count_row = conn.execute(
                "SELECT COUNT(*) AS c FROM skills_items WHERE skill_type = 'solution' AND source_run_id = ?",
                (int(run_id),),
            ).fetchone()
        self.assertEqual(int(count_row["c"]), 1)

    def test_autogen_solution_from_run_skips_non_agent_run(self):
        from backend.src.storage import get_connection
        from backend.src.services.skills.run_solution_autogen import autogen_solution_from_run

        now = self._now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at) VALUES (?, ?, ?)",
                ("普通任务不应生成方案", "done", now),
            )
            task_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (task_id, "done", "manual_run", now, now, now, now),
            )
            run_id = int(cursor.lastrowid)

        result = autogen_solution_from_run(task_id=task_id, run_id=run_id, force=False)
        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("status"), "skipped_not_agent_run")
        self.assertEqual(int(result.get("run_id")), int(run_id))

        with get_connection() as conn:
            row = conn.execute(
                "SELECT id FROM skills_items WHERE skill_type = 'solution' AND source_run_id = ?",
                (int(run_id),),
            ).fetchone()
        self.assertIsNone(row)


if __name__ == "__main__":
    unittest.main()
