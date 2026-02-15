import unittest


class TestExtractJsonObject(unittest.TestCase):
    def test_extracts_plain_json_object(self):
        from backend.src.common.utils import extract_json_object

        obj = extract_json_object('{"a": 1, "b": 2}')
        self.assertEqual(obj, {"a": 1, "b": 2})

    def test_extracts_from_json_code_fence(self):
        from backend.src.common.utils import extract_json_object

        text = "前缀\n```json\n{\"a\": 1, \"b\": 2}\n```\n后缀"
        obj = extract_json_object(text)
        self.assertEqual(obj, {"a": 1, "b": 2})

    def test_extracts_first_balanced_object_when_multiple_present(self):
        from backend.src.common.utils import extract_json_object

        text = "prefix {\"a\": 1} middle {\"b\": 2} suffix"
        obj = extract_json_object(text)
        self.assertEqual(obj, {"a": 1})

    def test_returns_none_for_non_object_json(self):
        from backend.src.common.utils import extract_json_object

        self.assertIsNone(extract_json_object("[1, 2, 3]"))


if __name__ == "__main__":
    unittest.main()

