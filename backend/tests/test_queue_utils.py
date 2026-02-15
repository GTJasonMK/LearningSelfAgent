import asyncio
import unittest

from backend.src.agent.runner.queue_utils import make_queue_emit


class TestQueueUtils(unittest.TestCase):
    def test_make_queue_emit_pushes_non_empty_message(self):
        out_q: "asyncio.Queue[str]" = asyncio.Queue()
        emit = make_queue_emit(out_q)
        emit("hello")
        self.assertEqual(out_q.get_nowait(), "hello")

    def test_make_queue_emit_ignores_empty_message(self):
        out_q: "asyncio.Queue[str]" = asyncio.Queue()
        emit = make_queue_emit(out_q)
        emit("")
        self.assertTrue(out_q.empty())


if __name__ == "__main__":
    unittest.main()
