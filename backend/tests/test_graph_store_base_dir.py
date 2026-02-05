import json
import os
import tempfile
import unittest
from pathlib import Path


class TestGraphStoreBaseDir(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._db_path = Path(self._tmp.name) / "agent_test.db"
        self._prompt_root = Path(self._tmp.name) / "prompt"
        os.environ["AGENT_DB_PATH"] = str(self._db_path)
        os.environ["AGENT_PROMPT_ROOT"] = str(self._prompt_root)

        import backend.src.storage as storage

        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
        except Exception:
            pass
        self._tmp.cleanup()

    def _write_graph_markdown(self, path: Path, meta: dict) -> None:
        text = "---\n" + json.dumps(meta, ensure_ascii=False) + "\n---\n\n"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def test_sync_graph_from_files_respects_base_dir(self):
        from backend.src.services.graph.graph_store import sync_graph_from_files
        from backend.src.storage import get_connection

        base_dir = Path(self._tmp.name) / "custom_graph"
        self._write_graph_markdown(base_dir / "nodes" / "1.md", {"id": 1, "label": "A"})
        self._write_graph_markdown(base_dir / "nodes" / "2.md", {"id": 2, "label": "B"})
        self._write_graph_markdown(
            base_dir / "edges" / "1.md",
            {"id": 1, "source": 1, "target": 2, "relation": "rel"},
        )

        result = sync_graph_from_files(base_dir=base_dir, prune=False)

        self.assertEqual(result.get("inserted_nodes"), 2)
        self.assertEqual(result.get("inserted_edges"), 1)

        with get_connection() as conn:
            nodes = conn.execute("SELECT COUNT(*) AS c FROM graph_nodes").fetchone()["c"]
            edges = conn.execute("SELECT COUNT(*) AS c FROM graph_edges").fetchone()["c"]
        self.assertEqual(int(nodes), 2)
        self.assertEqual(int(edges), 1)

    def test_sync_graph_export_db_to_files_uses_base_dir(self):
        from backend.src.common.utils import now_iso
        from backend.src.services.graph.graph_store import sync_graph_from_files
        from backend.src.storage import get_connection

        base_dir = Path(self._tmp.name) / "export_graph"
        created_at = now_iso()

        with get_connection() as conn:
            conn.execute(
                "INSERT INTO graph_nodes (id, label, created_at, node_type, attributes, task_id, evidence) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (1, "A", created_at, None, None, None, "test"),
            )

        result = sync_graph_from_files(base_dir=base_dir, prune=True)
        self.assertEqual(result.get("mode"), "export_db_to_files")

        self.assertTrue((base_dir / "nodes" / "1.md").exists())
        self.assertFalse((self._prompt_root / "graph" / "nodes" / "1.md").exists())


if __name__ == "__main__":
    unittest.main()

