import os
import sys
import tempfile
import unittest
from unittest.mock import patch


class _ImmediateTimer:
    def __init__(self, _interval, fn, args=None, kwargs=None):
        self._fn = fn
        self._args = args or ()
        self._kwargs = kwargs or {}

    def start(self):
        self._fn(*self._args, **self._kwargs)


class TestUpdateRestart(unittest.TestCase):
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

    def test_restart_update_stops_running_and_uses_python_m_uvicorn_for_cli(self):
        """
        回归：
        - 重启前应调用 stop_running_task_records 收敛 running/waiting；
        - 当 sys.argv[0] == 'uvicorn'（不是文件路径）时，应改为 python -m uvicorn 形式重启。
        """
        from backend.src.api.system.routes_update import restart_update

        popen_calls = []
        stop_calls = []

        def _fake_popen(cmd, **kwargs):
            popen_calls.append({"cmd": cmd, "kwargs": kwargs})
            return object()

        def _fake_stop_running_task_records(*, reason: str):
            stop_calls.append(reason)
            return {"ok": True}

        with (
            patch("backend.src.api.system.routes_update.threading.Timer", _ImmediateTimer),
            patch("backend.src.api.system.routes_update.subprocess.Popen", side_effect=_fake_popen),
            patch("backend.src.api.system.routes_update.stop_running_task_records", side_effect=_fake_stop_running_task_records),
            patch("backend.src.api.system.routes_update.os._exit", lambda _code: None),
            patch.object(sys, "argv", ["uvicorn", "backend.src.main:app", "--host", "127.0.0.1"]),
        ):
            resp = restart_update()

        self.assertIn("update", resp)
        self.assertEqual(stop_calls, ["update_restart"])
        self.assertEqual(len(popen_calls), 1)
        self.assertEqual(
            popen_calls[0]["cmd"],
            [sys.executable, "-m", "uvicorn", "backend.src.main:app", "--host", "127.0.0.1"],
        )


if __name__ == "__main__":
    unittest.main()
