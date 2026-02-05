import unittest


class TestOutputFormat(unittest.TestCase):
    def test_format_visible_result_always_prefixes_result_tag(self):
        from backend.src.services.output.output_format import format_visible_result
        from backend.src.constants import STREAM_TAG_RESULT

        self.assertEqual(
            format_visible_result("hello"),
            f"{STREAM_TAG_RESULT}\nhello",
        )

    def test_format_visible_result_no_duplicate_when_already_tagged(self):
        from backend.src.services.output.output_format import format_visible_result
        from backend.src.constants import STREAM_TAG_RESULT

        raw = f"{STREAM_TAG_RESULT}\nhello"
        self.assertEqual(format_visible_result(raw), raw)

    def test_format_visible_result_empty(self):
        from backend.src.services.output.output_format import format_visible_result

        self.assertEqual(format_visible_result(""), "")
        self.assertEqual(format_visible_result(None), "")


if __name__ == "__main__":
    unittest.main()
