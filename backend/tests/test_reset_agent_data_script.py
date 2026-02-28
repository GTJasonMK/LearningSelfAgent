import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path


class TestResetAgentDataScript(unittest.TestCase):
    def test_reset_agent_data_clears_non_preserved_tables_and_prompt_files(self):
        """
        回归：reset_agent_data 应该
        - 仅保留 config_store，清空包括 chat_messages 在内的业务数据
        - 清空 FTS 索引（delete-all），避免“主表为空但 FTS 仍残留”
        - 清理 prompt_root 下的业务 md 文件（保留 README 与系统 skill）
        - 删除 backend/.agent 工作目录
        """
        from backend.src.common.utils import now_iso
        from backend.src.constants import DB_ENV_VAR, PROMPT_ENV_VAR
        from backend.src import storage
        import scripts.reset_agent_data as reset_script

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()

            db_path = root / "agent.db"
            prompt_root = root / "prompt"
            agent_dir = root / "backend" / ".agent" / "workspace"
            agent_dir.mkdir(parents=True, exist_ok=True)
            (agent_dir / "tmp.txt").write_text("x", encoding="utf-8")

            # prompt_root 结构：README 保留，其它 md 清理
            for rel in (
                "memory/README.md",
                "memory/foo.md",
                "memory/sub/bar.md",
                "tools/README.md",
                "tools/t1.md",
                "tools/keep_tool.md",
                "skills/README.md",
                "skills/s1.md",
                "skills/misc/curated.md",
                "skills/system/core.md",
                "skills/misc/system_meta.md",
                "graph/nodes/README.md",
                "graph/nodes/n1.md",
                "graph/edges/README.md",
                "graph/edges/e1.md",
                "memory/.trash/old.md",
            ):
                p = prompt_root / rel
                p.parent.mkdir(parents=True, exist_ok=True)
                if rel == "tools/t1.md":
                    # 模拟 Agent 执行阶段创建的 draft 工具文件（reset 时应清理）
                    p.write_text(
                        "---\n"
                        + json.dumps(
                            {
                                "id": 2,
                                "name": "t1",
                                "description": "draft tool",
                                "version": "0.1.0",
                                "metadata": {"approval": {"status": "draft"}},
                            },
                            ensure_ascii=False,
                        )
                        + "\n---\n\n",
                        encoding="utf-8",
                    )
                elif rel == "tools/keep_tool.md":
                    # 非 draft 工具应被保留（避免 reset 清空系统资源）
                    p.write_text(
                        "---\n"
                        + json.dumps(
                            {
                                "id": 1,
                                "name": "keep_tool",
                                "description": "approved tool",
                                "version": "0.1.0",
                                "metadata": {"exec": {"type": "shell", "args": ["echo", "{input}"]}},
                            },
                            ensure_ascii=False,
                        )
                        + "\n---\n\n",
                        encoding="utf-8",
                    )
                elif rel == "skills/system/core.md":
                    # 按路径判定的系统 skill：reset 后应保留
                    p.write_text(
                        "---\n"
                        "name: system_core_skill\n"
                        "description: core\n"
                        "category: system.core\n"
                        "tags: [system]\n"
                        "---\n\n",
                        encoding="utf-8",
                    )
                elif rel == "skills/misc/system_meta.md":
                    # 按元数据判定的系统 skill：即使不在 skills/system 目录也应保留
                    p.write_text(
                        "---\n"
                        "name: system_meta_skill\n"
                        "description: meta\n"
                        "category: misc\n"
                        "scope: system.global\n"
                        "tags: [domain:system]\n"
                        "---\n\n",
                        encoding="utf-8",
                    )
                elif rel == "skills/s1.md":
                    # 自动生成 skill：reset 后应删除
                    p.write_text(
                        "---\n"
                        "name: auto_skill_generated\n"
                        "description: generated\n"
                        "category: misc\n"
                        "source_task_id: 101\n"
                        "source_run_id: 202\n"
                        "tags: [task:101, run:202]\n"
                        "---\n\n",
                        encoding="utf-8",
                    )
                elif rel == "skills/misc/curated.md":
                    # 手工维护 skill：无 generated 标记，reset 后应保留
                    p.write_text(
                        "---\n"
                        "name: curated_skill\n"
                        "description: curated\n"
                        "category: misc\n"
                        "tags: [manual]\n"
                        "---\n\n",
                        encoding="utf-8",
                    )
                else:
                    p.write_text(f"file:{rel}", encoding="utf-8")

            # 用临时 DB 初始化 schema
            old_db = os.environ.get(DB_ENV_VAR)
            old_prompt = os.environ.get(PROMPT_ENV_VAR)
            os.environ[DB_ENV_VAR] = str(db_path)
            os.environ[PROMPT_ENV_VAR] = str(prompt_root)
            try:
                storage.reset_db_cache()
                storage.init_db()

                with storage.get_connection() as con:
                    cur = con.cursor()
                    # 保留表：仅 config_store；chat_messages 应被清空
                    cur.execute("UPDATE config_store SET llm_model=? WHERE id=1", ("test-model",))
                    cur.execute(
                        "INSERT INTO chat_messages (role, content, created_at) VALUES (?, ?, ?)",
                        ("user", "hi", now_iso()),
                    )
                    # 非保留表：插入一些数据，确保 reset 会清空
                    cur.execute(
                        "INSERT INTO tasks (title, status, created_at) VALUES (?, ?, ?)",
                        ("t1", "done", now_iso()),
                    )
                    cur.execute(
                        "INSERT INTO memory_items (content, created_at) VALUES (?, ?)",
                        ("m1", now_iso()),
                    )
                    cur.execute(
                        "INSERT INTO skills_items (name, created_at, scope) VALUES (?, ?, ?)",
                        ("s1", now_iso(), "global"),
                    )

                # 确认 FTS 有行（由 trigger 写入）
                con2 = sqlite3.connect(db_path)
                cur2 = con2.cursor()
                mem_fts_before = int(cur2.execute("SELECT COUNT(*) FROM memory_items_fts").fetchone()[0])
                skill_fts_before = int(cur2.execute("SELECT COUNT(*) FROM skills_items_fts").fetchone()[0])
                con2.close()
                self.assertGreater(mem_fts_before, 0)
                self.assertGreater(skill_fts_before, 0)

                # 将 reset_script 的 PROJECT_ROOT 指向临时目录，避免清理真实仓库
                reset_script.PROJECT_ROOT = root
                output_buffer = StringIO()
                with redirect_stdout(output_buffer):
                    reset_script.main()
                reset_output = output_buffer.getvalue()

                # DB：仅保留 config_store
                con3 = sqlite3.connect(db_path)
                cur3 = con3.cursor()
                self.assertEqual(int(cur3.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]), 0)
                self.assertEqual(int(cur3.execute("SELECT COUNT(*) FROM memory_items").fetchone()[0]), 0)
                # skills_items：保留系统 skill（从保留的 skills 文件同步回 DB）
                self.assertGreaterEqual(int(cur3.execute("SELECT COUNT(*) FROM skills_items").fetchone()[0]), 1)
                self.assertEqual(
                    int(cur3.execute("SELECT COUNT(*) FROM skills_items WHERE name = ?", ("s1",)).fetchone()[0]),
                    0,
                )
                self.assertGreaterEqual(
                    int(
                        cur3.execute(
                            "SELECT COUNT(*) FROM skills_items WHERE name IN (?, ?)",
                            ("system_core_skill", "system_meta_skill"),
                        ).fetchone()[0]
                    ),
                    2,
                )
                self.assertEqual(
                    int(cur3.execute("SELECT COUNT(*) FROM skills_items WHERE name = ?", ("auto_skill_generated",)).fetchone()[0]),
                    0,
                )
                self.assertEqual(
                    int(cur3.execute("SELECT COUNT(*) FROM skills_items WHERE name = ?", ("curated_skill",)).fetchone()[0]),
                    1,
                )
                # config 保留；chat_messages 清空
                self.assertGreaterEqual(int(cur3.execute("SELECT COUNT(*) FROM config_store").fetchone()[0]), 1)
                self.assertEqual(int(cur3.execute("SELECT COUNT(*) FROM chat_messages").fetchone()[0]), 0)
                # FTS 索引清空
                self.assertEqual(int(cur3.execute("SELECT COUNT(*) FROM memory_items_fts").fetchone()[0]), 0)
                # skills FTS 会由系统 skill 回灌触发重建，不再强制为 0
                self.assertGreaterEqual(int(cur3.execute("SELECT COUNT(*) FROM skills_items_fts").fetchone()[0]), 1)
                con3.close()

                # backend/.agent 被删
                self.assertFalse((root / "backend" / ".agent").exists())

                # prompt_root 清理：README 保留，其它 md 删除
                self.assertTrue((prompt_root / "memory" / "README.md").exists())
                self.assertFalse((prompt_root / "memory" / "foo.md").exists())
                self.assertFalse((prompt_root / "memory" / "sub" / "bar.md").exists())
                self.assertTrue((prompt_root / "skills" / "README.md").exists())
                self.assertFalse((prompt_root / "skills" / "s1.md").exists())
                self.assertTrue((prompt_root / "skills" / "misc" / "curated.md").exists())
                self.assertTrue((prompt_root / "skills" / "system" / "core.md").exists())
                self.assertTrue((prompt_root / "skills" / "misc" / "system_meta.md").exists())
                self.assertFalse((prompt_root / "memory" / ".trash").exists())
                # tools：仅 draft 删除，非 draft 保留
                self.assertTrue((prompt_root / "tools" / "README.md").exists())
                self.assertFalse((prompt_root / "tools" / "t1.md").exists())
                self.assertTrue((prompt_root / "tools" / "keep_tool.md").exists())

                # skills 判定日志可观测：每个 skill 给出 KEEP/DELETE 原因
                self.assertIn("memory 判定：", reset_output)
                self.assertIn("KEEP memory/README.md [readme]", reset_output)
                self.assertIn("DELETE memory/foo.md [memory_data]", reset_output)
                self.assertIn("DELETE memory/sub/bar.md [memory_data]", reset_output)
                self.assertIn("tools 判定：", reset_output)
                self.assertIn("KEEP tools/README.md [readme]", reset_output)
                self.assertIn("DELETE tools/t1.md [draft_tool]", reset_output)
                self.assertIn("KEEP tools/keep_tool.md [approved_or_builtin]", reset_output)
                self.assertIn("skills 判定：", reset_output)
                self.assertIn("DELETE skills/s1.md [generated_non_system]", reset_output)
                self.assertIn("KEEP skills/misc/curated.md [non_generated_or_curated]", reset_output)
                self.assertIn("KEEP skills/system/core.md [system_skill]", reset_output)
            finally:
                if old_db is None:
                    os.environ.pop(DB_ENV_VAR, None)
                else:
                    os.environ[DB_ENV_VAR] = old_db
                if old_prompt is None:
                    os.environ.pop(PROMPT_ENV_VAR, None)
                else:
                    os.environ[PROMPT_ENV_VAR] = old_prompt

    def test_reset_agent_data_rebuilds_when_config_store_missing(self):
        """
        回归：当旧库缺少 config_store（典型于旧版本/半清理库）时，
        reset_agent_data 应能自动走“备份 + 重建”路径，避免卡死在 no such table。
        """
        from backend.src.constants import DB_ENV_VAR, PROMPT_ENV_VAR
        from backend.src import storage
        import scripts.reset_agent_data as reset_script

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()

            db_path = root / "agent.db"
            prompt_root = root / "prompt"
            prompt_root.mkdir(parents=True, exist_ok=True)

            # 构造一个“缺少 config_store”的旧库，只建一个 chat_messages 表
            con = sqlite3.connect(db_path)
            cur = con.cursor()
            cur.executescript(
                """
                CREATE TABLE IF NOT EXISTS chat_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                INSERT INTO chat_messages (role, content, created_at) VALUES ('user', 'hi', '2020-01-01T00:00:00Z');
                """
            )
            con.commit()
            con.close()

            old_db = os.environ.get(DB_ENV_VAR)
            old_prompt = os.environ.get(PROMPT_ENV_VAR)
            os.environ[DB_ENV_VAR] = str(db_path)
            os.environ[PROMPT_ENV_VAR] = str(prompt_root)
            try:
                storage.reset_db_cache()

                # 将 reset_script 的 PROJECT_ROOT 指向临时目录，避免清理真实仓库
                reset_script.PROJECT_ROOT = root
                reset_script.main()

                # 新库应包含 config_store，并且数据为空（重建后的库）
                con2 = sqlite3.connect(db_path)
                cur2 = con2.cursor()
                tables = {r[0] for r in cur2.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                self.assertIn("config_store", tables)
                self.assertIn("chat_messages", tables)
                self.assertEqual(int(cur2.execute("SELECT COUNT(*) FROM chat_messages").fetchone()[0]), 0)
                con2.close()
            finally:
                if old_db is None:
                    os.environ.pop(DB_ENV_VAR, None)
                else:
                    os.environ[DB_ENV_VAR] = old_db
                if old_prompt is None:
                    os.environ.pop(PROMPT_ENV_VAR, None)
                else:
                    os.environ[PROMPT_ENV_VAR] = old_prompt


if __name__ == "__main__":
    unittest.main()
