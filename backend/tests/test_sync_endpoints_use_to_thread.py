import os
import tempfile
import unittest
from unittest.mock import patch


class TestSyncEndpointsUseToThread(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_test.db")
        os.environ["AGENT_DB_PATH"] = self._db_path
        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
        except Exception:
            pass
        self._tmpdir.cleanup()

    async def test_sync_memory_uses_to_thread(self):
        from backend.src.api.knowledge.memory.routes_items import sync_memory
        from backend.src.services.memory.memory_store import sync_memory_from_files

        calls = []

        async def fake_to_thread(fn, *args, **kwargs):
            calls.append((fn, args, kwargs))
            return {"fn": getattr(fn, "__name__", ""), "args": list(args), "kwargs": kwargs}

        with patch("backend.src.api.knowledge.memory.routes_items.asyncio.to_thread", fake_to_thread):
            resp = await sync_memory()

        self.assertEqual(len(calls), 1)
        self.assertIs(calls[0][0], sync_memory_from_files)
        self.assertEqual(calls[0][1], (None,))
        self.assertEqual(calls[0][2], {"prune": True})
        self.assertEqual(resp["result"]["fn"], "sync_memory_from_files")

    async def test_sync_graph_uses_to_thread(self):
        from backend.src.api.knowledge.memory.routes_graph import sync_graph
        from backend.src.services.graph.graph_store import sync_graph_from_files

        calls = []

        async def fake_to_thread(fn, *args, **kwargs):
            calls.append((fn, args, kwargs))
            return {"fn": getattr(fn, "__name__", ""), "args": list(args), "kwargs": kwargs}

        with patch("backend.src.api.knowledge.memory.routes_graph.asyncio.to_thread", fake_to_thread):
            resp = await sync_graph()

        self.assertEqual(len(calls), 1)
        self.assertIs(calls[0][0], sync_graph_from_files)
        self.assertEqual(calls[0][1], (None,))
        self.assertEqual(calls[0][2], {"prune": True})
        self.assertEqual(resp["result"]["fn"], "sync_graph_from_files")

    async def test_sync_skills_uses_to_thread(self):
        from backend.src.api.knowledge.routes_skills import sync_skills
        from backend.src.services.skills.skills_sync import sync_skills_from_files

        calls = []

        async def fake_to_thread(fn, *args, **kwargs):
            calls.append((fn, args, kwargs))
            return {"fn": getattr(fn, "__name__", ""), "args": list(args), "kwargs": kwargs}

        with patch("backend.src.api.knowledge.routes_skills.asyncio.to_thread", fake_to_thread):
            resp = await sync_skills()

        self.assertEqual(len(calls), 1)
        self.assertIs(calls[0][0], sync_skills_from_files)
        self.assertEqual(calls[0][1], ())
        self.assertEqual(calls[0][2], {})
        self.assertEqual(resp["result"]["fn"], "sync_skills_from_files")

    async def test_sync_tools_uses_to_thread(self):
        from backend.src.api.knowledge.routes_tools import sync_tools
        from backend.src.services.tools.tools_store import sync_tools_from_files

        calls = []

        async def fake_to_thread(fn, *args, **kwargs):
            calls.append((fn, args, kwargs))
            return {"fn": getattr(fn, "__name__", ""), "args": list(args), "kwargs": kwargs}

        with patch("backend.src.api.knowledge.routes_tools.asyncio.to_thread", fake_to_thread):
            resp = await sync_tools()

        self.assertEqual(len(calls), 1)
        self.assertIs(calls[0][0], sync_tools_from_files)
        self.assertEqual(calls[0][1], ())
        self.assertEqual(calls[0][2], {"prune": True})
        self.assertEqual(resp["result"]["fn"], "sync_tools_from_files")


if __name__ == "__main__":
    unittest.main()

