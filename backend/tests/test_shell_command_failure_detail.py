import unittest


class TestShellCommandFailureDetail(unittest.TestCase):
    def test_prefers_real_error_line_over_deprecation_warning(self):
        from backend.src.actions.handlers.shell_command import _build_shell_failure_detail

        detail = _build_shell_failure_detail(
            stdout="",
            stderr=(
                "DeprecationWarning: datetime.utcnow() is deprecated\n"
                "today = dt.datetime.utcnow().date()\n"
                "ERROR: failed to fetch a usable gold series"
            ),
            returncode=2,
        )
        self.assertEqual(detail, "ERROR: failed to fetch a usable gold series")

    def test_falls_back_to_returncode_when_no_output(self):
        from backend.src.actions.handlers.shell_command import _build_shell_failure_detail

        detail = _build_shell_failure_detail(stdout="", stderr="", returncode=127)
        self.assertEqual(detail, "127")

    def test_prefers_last_exception_line_over_traceback_header(self):
        from backend.src.actions.handlers.shell_command import _build_shell_failure_detail

        detail = _build_shell_failure_detail(
            stdout="",
            stderr=(
                "DeprecationWarning: datetime.utcnow() is deprecated\n"
                "Traceback (most recent call last):\n"
                "  File \"x.py\", line 1, in <module>\n"
                "    main()\n"
                "ValueError: Invalid isoformat string: '--fx'\n"
            ),
            returncode=1,
        )
        self.assertEqual(detail, "ValueError: Invalid isoformat string: '--fx'")


if __name__ == "__main__":
    unittest.main()
