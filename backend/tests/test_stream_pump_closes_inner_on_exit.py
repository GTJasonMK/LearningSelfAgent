import asyncio
import threading
import time
import unittest


class TestStreamPumpClosesInnerOnExit(unittest.IsolatedAsyncioTestCase):
    async def test_consumer_exit_closes_inner_generator(self):
        """
        回归：pump_sync_generator 在消费者提前结束（例如 SSE 客户端断开）时，
        必须尽快 close 同步 generator，避免后台线程继续产出导致队列增长/线程空转。
        """
        from backend.src.agent.runner.stream_pump import pump_sync_generator

        closed = threading.Event()

        def inner():
            try:
                i = 0
                while True:
                    i += 1
                    yield f"msg{i}"
                    time.sleep(0.05)
            finally:
                closed.set()

        async def consume_one_msg():
            async for kind, _payload in pump_sync_generator(
                inner=inner(),
                label="test_pump_close",
                poll_interval_seconds=1,
                idle_timeout_seconds=5,
            ):
                if kind == "msg":
                    return

        await consume_one_msg()

        # 等待后台线程触发 inner.close() 的 finally 执行
        for _ in range(100):
            if closed.is_set():
                break
            await asyncio.sleep(0.01)

        self.assertTrue(closed.is_set())


if __name__ == "__main__":
    unittest.main()

