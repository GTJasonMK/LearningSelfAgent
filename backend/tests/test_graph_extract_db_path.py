import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


class TestGraphExtractDbPath(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._db1 = Path(self._tmp.name) / "db1.db"
        self._db2 = Path(self._tmp.name) / "db2.db"
        self._prompt_root = Path(self._tmp.name) / "prompt"

        os.environ["AGENT_PROMPT_ROOT"] = str(self._prompt_root)

        import backend.src.storage as storage

        os.environ["AGENT_DB_PATH"] = str(self._db1)
        storage.init_db()

        os.environ["AGENT_DB_PATH"] = str(self._db2)
        storage.init_db()

        # 默认回到 db1（用于本用例入队）
        os.environ["AGENT_DB_PATH"] = str(self._db1)

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
        except Exception:
            pass
        self._tmp.cleanup()

    def test_process_item_uses_item_db_path_not_current_env(self):
        import backend.src.services.graph.graph_extract as graph_extract
        from backend.src.constants import GRAPH_EXTRACT_STATUS_DONE
        from backend.src.storage import get_connection

        # 避免其它用例污染：清空内存队列，并确保不会启动无限 worker 线程
        try:
            graph_extract._graph_extract_queue.clear()
        except Exception:
            pass

        with patch("backend.src.services.graph.graph_extract._start_graph_extractor", return_value=None):
            extract_id = graph_extract._enqueue_graph_extraction(1, 1, "evidence")

        # 切换到另一个 DB：若实现错误（用默认 env DB），将导致写入 db2
        os.environ["AGENT_DB_PATH"] = str(self._db2)

        item = graph_extract._graph_extract_queue.popleft()
        self.assertEqual(item[0], str(self._db1))

        db_path, extract_id2, task_id, run_id, content = item
        self.assertEqual(int(extract_id2), int(extract_id))

        fake_graph_json = json.dumps(
            {
                "graph": {
                    "nodes": [{"label": "A"}, {"label": "B"}],
                    "edges": [{"source": "A", "target": "B", "relation": "rel"}],
                }
            },
            ensure_ascii=False,
        )

        with patch(
            "backend.src.services.graph.graph_extract.call_openai",
            return_value=(fake_graph_json, None, None),
        ):
            graph_extract._process_graph_extract_item(
                db_path=str(db_path),
                extract_id=int(extract_id2),
                task_id=int(task_id),
                run_id=int(run_id),
                content=str(content),
            )

        with get_connection(db_path=str(self._db1)) as conn:
            nodes = conn.execute("SELECT COUNT(*) AS c FROM graph_nodes").fetchone()["c"]
            edges = conn.execute("SELECT COUNT(*) AS c FROM graph_edges").fetchone()["c"]
            status = conn.execute(
                "SELECT status FROM graph_extract_tasks WHERE id = ?",
                (int(extract_id),),
            ).fetchone()["status"]

        self.assertEqual(int(nodes), 2)
        self.assertEqual(int(edges), 1)
        self.assertEqual(str(status), GRAPH_EXTRACT_STATUS_DONE)

        # db2 不应被写入任何图谱/抽取任务
        with get_connection(db_path=str(self._db2)) as conn:
            nodes2 = conn.execute("SELECT COUNT(*) AS c FROM graph_nodes").fetchone()["c"]
            edges2 = conn.execute("SELECT COUNT(*) AS c FROM graph_edges").fetchone()["c"]
            tasks2 = conn.execute("SELECT COUNT(*) AS c FROM graph_extract_tasks").fetchone()["c"]

        self.assertEqual(int(nodes2), 0)
        self.assertEqual(int(edges2), 0)
        self.assertEqual(int(tasks2), 0)


if __name__ == "__main__":
    unittest.main()

