import unittest


class TestJsonParseHandler(unittest.TestCase):
    def test_json_parse_extracts_from_code_fence(self):
        from backend.src.actions.handlers.json_parse import execute_json_parse

        result, err = execute_json_parse(
            {"text": "前缀\\n```json\\n{\"a\": 1, \"b\": 2}\\n```\\n后缀"}
        )
        self.assertIsNone(err)
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("picked"), False)
        self.assertEqual(result.get("value"), {"a": 1, "b": 2})

    def test_json_parse_extracts_array_from_mixed_text(self):
        from backend.src.actions.handlers.json_parse import execute_json_parse

        result, err = execute_json_parse({"text": "prefix [1, 2, 3] suffix"})
        self.assertIsNone(err)
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("value"), [1, 2, 3])

    def test_json_parse_pick_keys(self):
        from backend.src.actions.handlers.json_parse import execute_json_parse

        result, err = execute_json_parse(
            {"text": "prefix {\"a\": 1, \"b\": 2} suffix", "pick_keys": ["b"]}
        )
        self.assertIsNone(err)
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("picked"), True)
        self.assertEqual(result.get("value"), {"b": 2})



    def test_json_parse_requires_recent_source_for_large_structured_text(self):
        from backend.src.actions.handlers.json_parse import execute_json_parse

        text_value = '{"status":"success","data":[' + ','.join(str(i) for i in range(60)) + ']}'
        with self.assertRaises(ValueError) as ctx:
            execute_json_parse({"text": text_value})
        self.assertIn("来源", str(ctx.exception))

    def test_json_parse_accepts_recent_source_binding(self):
        from backend.src.actions.handlers.json_parse import execute_json_parse

        text_value = '{"status":"success","data":[' + ','.join(str(i) for i in range(60)) + ']}'
        result, err = execute_json_parse(
            {"text": text_value},
            context={"latest_parse_input_text": text_value, "enforce_json_parse_recent_source": True},
        )
        self.assertIsNone(err)
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("picked"), False)

    def test_json_parse_allows_small_json_even_when_recent_source_differs(self):
        from backend.src.actions.handlers.json_parse import execute_json_parse

        result, err = execute_json_parse(
            {"text": "{\"a\": 1}"},
            context={"latest_parse_input_text": "{\"b\": 2}", "enforce_json_parse_recent_source": True},
        )
        self.assertIsNone(err)
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("value"), {"a": 1})

if __name__ == "__main__":
    unittest.main()
