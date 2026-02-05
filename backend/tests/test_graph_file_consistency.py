import os
import tempfile
import unittest
from pathlib import Path


class TestGraphFileConsistency(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["AGENT_DB_PATH"] = str(Path(self._tmp.name) / "agent_test.db")
        os.environ["AGENT_PROMPT_ROOT"] = str(Path(self._tmp.name) / "prompt")

        import backend.src.storage as storage

        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
        except Exception:
            pass
        self._tmp.cleanup()

    def test_publish_and_delete_graph_node_cascades_files(self):
        from backend.src.common.utils import now_iso
        from backend.src.services.graph.graph_delete import delete_graph_node_strong
        from backend.src.services.graph.graph_store import (
            graph_edge_file_path,
            graph_node_file_path,
            publish_graph_edge_file,
            publish_graph_node_file,
        )
        from backend.src.storage import get_connection

        created_at = now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO graph_nodes (label, created_at, node_type, attributes, task_id, evidence) VALUES (?, ?, ?, ?, ?, ?)",
                ("A", created_at, "concept", None, None, "test"),
            )
            a_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO graph_nodes (label, created_at, node_type, attributes, task_id, evidence) VALUES (?, ?, ?, ?, ?, ?)",
                ("B", created_at, "concept", None, None, "test"),
            )
            b_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO graph_edges (source, target, relation, created_at, confidence, evidence) VALUES (?, ?, ?, ?, ?, ?)",
                (a_id, b_id, "related_to", created_at, None, "test"),
            )
            e_id = int(cursor.lastrowid)

            n_info = publish_graph_node_file(a_id, conn=conn)
            self.assertTrue(n_info.get("ok"))
            e_info = publish_graph_edge_file(e_id, conn=conn)
            self.assertTrue(e_info.get("ok"))

        self.assertTrue(graph_node_file_path(a_id).exists())
        self.assertTrue(graph_edge_file_path(e_id).exists())

        delete_graph_node_strong(a_id)

        self.assertFalse(graph_node_file_path(a_id).exists())
        self.assertFalse(graph_edge_file_path(e_id).exists())
        with get_connection() as conn:
            node = conn.execute("SELECT * FROM graph_nodes WHERE id = ?", (int(a_id),)).fetchone()
            edge = conn.execute("SELECT * FROM graph_edges WHERE id = ?", (int(e_id),)).fetchone()
        self.assertIsNone(node)
        self.assertIsNone(edge)


if __name__ == "__main__":
    unittest.main()

