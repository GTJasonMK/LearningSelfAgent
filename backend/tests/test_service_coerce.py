import unittest

from backend.src.services.common.coerce import (
    to_int,
    to_int_list,
    to_int_or_default,
    to_non_empty_optional_text,
    to_non_empty_texts,
    to_optional_int,
    to_optional_text,
    to_text,
)


class TestServiceCoerce(unittest.TestCase):
    def test_numeric_and_optional_numeric(self):
        self.assertEqual(3, to_int("3"))
        self.assertEqual(7, to_int_or_default("7", default=1))
        self.assertEqual(5, to_int_or_default(None, default=5))
        self.assertEqual(9, to_optional_int("9"))
        self.assertIsNone(to_optional_int(None))

    def test_text_helpers(self):
        self.assertEqual("abc", to_text("abc"))
        self.assertEqual("", to_text(None))
        self.assertEqual("x", to_optional_text("x"))
        self.assertIsNone(to_optional_text(None))
        self.assertEqual("ok", to_non_empty_optional_text("  ok  "))
        self.assertIsNone(to_non_empty_optional_text("   "))

    def test_sequence_helpers(self):
        self.assertEqual(["a", "b"], to_non_empty_texts(["a", " ", "b"]))
        self.assertEqual([1, 2], to_int_list([1, "2"]))
        self.assertEqual([1, 3], to_int_list([1, "x", 3], ignore_errors=True))


if __name__ == "__main__":
    unittest.main()
