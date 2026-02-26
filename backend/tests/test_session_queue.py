import asyncio
import os
import unittest


class TestSessionQueue(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from backend.src.agent.runner.session_queue import reset_stream_queue_state_for_tests

        await reset_stream_queue_state_for_tests()

    async def asyncTearDown(self):
        from backend.src.agent.runner.session_queue import reset_stream_queue_state_for_tests

        await reset_stream_queue_state_for_tests()

    async def test_same_session_is_serialized(self):
        from backend.src.agent.runner.session_queue import acquire_stream_queue_ticket

        first = await acquire_stream_queue_ticket(session_key="sess_a", timeout_seconds=2)
        acquired_second = {"done": False}
        holder = {"ticket": None}

        async def _acquire_second():
            ticket = await acquire_stream_queue_ticket(session_key="sess_a", timeout_seconds=2)
            holder["ticket"] = ticket
            acquired_second["done"] = True

        task = asyncio.create_task(_acquire_second())
        await asyncio.sleep(0.05)
        self.assertFalse(acquired_second["done"])

        await first.release()
        await asyncio.wait_for(task, timeout=1.0)
        self.assertTrue(acquired_second["done"])
        if holder["ticket"] is not None:
            await holder["ticket"].release()

    async def test_global_limit_blocks_cross_session(self):
        from backend.src.agent.runner.session_queue import acquire_stream_queue_ticket, reset_stream_queue_state_for_tests

        old = os.getenv("AGENT_STREAM_GLOBAL_CONCURRENCY")
        os.environ["AGENT_STREAM_GLOBAL_CONCURRENCY"] = "1"
        try:
            await reset_stream_queue_state_for_tests()
            first = await acquire_stream_queue_ticket(session_key="sess_1", timeout_seconds=2)
            blocked = {"done": False}
            holder = {"ticket": None}

            async def _acquire_other():
                ticket = await acquire_stream_queue_ticket(session_key="sess_2", timeout_seconds=2)
                holder["ticket"] = ticket
                blocked["done"] = True

            task = asyncio.create_task(_acquire_other())
            await asyncio.sleep(0.05)
            self.assertFalse(blocked["done"])

            await first.release()
            await asyncio.wait_for(task, timeout=1.0)
            self.assertTrue(blocked["done"])
            if holder["ticket"] is not None:
                await holder["ticket"].release()
        finally:
            if old is None:
                os.environ.pop("AGENT_STREAM_GLOBAL_CONCURRENCY", None)
            else:
                os.environ["AGENT_STREAM_GLOBAL_CONCURRENCY"] = old

    async def test_global_queue_is_fifo_fair(self):
        from backend.src.agent.runner.session_queue import acquire_stream_queue_ticket, reset_stream_queue_state_for_tests

        old = os.getenv("AGENT_STREAM_GLOBAL_CONCURRENCY")
        os.environ["AGENT_STREAM_GLOBAL_CONCURRENCY"] = "1"
        try:
            await reset_stream_queue_state_for_tests()
            first = await acquire_stream_queue_ticket(session_key="sess_head", timeout_seconds=2)
            order = []
            holders = []

            async def _acquire(name: str):
                ticket = await acquire_stream_queue_ticket(session_key=name, timeout_seconds=2)
                order.append(name)
                holders.append(ticket)

            t2 = asyncio.create_task(_acquire("sess_2"))
            t3 = asyncio.create_task(_acquire("sess_3"))
            await asyncio.sleep(0.05)
            self.assertEqual(order, [])

            await first.release()
            await asyncio.wait_for(t2, timeout=1.0)
            self.assertEqual(order, ["sess_2"])
            await holders[0].release()
            await asyncio.wait_for(t3, timeout=1.0)
            self.assertEqual(order, ["sess_2", "sess_3"])
            await holders[1].release()
        finally:
            if old is None:
                os.environ.pop("AGENT_STREAM_GLOBAL_CONCURRENCY", None)
            else:
                os.environ["AGENT_STREAM_GLOBAL_CONCURRENCY"] = old

    async def test_acquire_timeout_does_not_leak_global_slot(self):
        from backend.src.agent.runner.session_queue import acquire_stream_queue_ticket, reset_stream_queue_state_for_tests

        old = os.getenv("AGENT_STREAM_GLOBAL_CONCURRENCY")
        os.environ["AGENT_STREAM_GLOBAL_CONCURRENCY"] = "1"
        try:
            await reset_stream_queue_state_for_tests()
            first = await acquire_stream_queue_ticket(session_key="sess_timeout_1", timeout_seconds=2)
            with self.assertRaises(asyncio.TimeoutError):
                await acquire_stream_queue_ticket(session_key="sess_timeout_2", timeout_seconds=0.01)
            await first.release()

            second = await acquire_stream_queue_ticket(session_key="sess_timeout_2", timeout_seconds=2)
            await second.release()
        finally:
            if old is None:
                os.environ.pop("AGENT_STREAM_GLOBAL_CONCURRENCY", None)
            else:
                os.environ["AGENT_STREAM_GLOBAL_CONCURRENCY"] = old


if __name__ == "__main__":
    unittest.main()
