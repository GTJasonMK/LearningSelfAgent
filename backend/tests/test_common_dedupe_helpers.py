import tempfile
import unittest
from pathlib import Path


class TestCommonDedupeHelpers(unittest.TestCase):
    def test_build_json_frontmatter_markdown_and_discover_files(self):
        from backend.src.common.utils import (
            build_json_frontmatter_markdown,
            discover_markdown_files,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            kept = root / "note.md"
            readme = root / "README.md"
            hidden = root / ".trash" / "deleted.md"
            hidden.parent.mkdir(parents=True, exist_ok=True)

            kept.write_text("# hello\n", encoding="utf-8")
            readme.write_text("# readme\n", encoding="utf-8")
            hidden.write_text("# hidden\n", encoding="utf-8")

            files = discover_markdown_files(root)
            self.assertEqual(files, [kept])

            text = build_json_frontmatter_markdown({"a": 1, "b": "x"}, body="content")
            self.assertIn("\n{\n", text)
            self.assertTrue(text.startswith("---\n"))
            self.assertTrue(text.endswith("content\n"))

    def test_path_within_root(self):
        from backend.src.common.path_utils import is_path_within_root

        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as other_tmp:
            root = Path(tmp)
            inside = root / "a" / "file.txt"
            outside = Path(other_tmp) / "outside.txt"

            inside.parent.mkdir(parents=True, exist_ok=True)
            inside.write_text("ok", encoding="utf-8")
            outside.write_text("no", encoding="utf-8")

            self.assertTrue(is_path_within_root(inside, root))
            self.assertFalse(is_path_within_root(outside, root))

    def test_python_code_helpers(self):
        from backend.src.common.python_code import (
            can_compile_python_source,
            has_risky_inline_control_flow,
            normalize_python_c_source,
        )

        source = "a=1; with open('x.txt', 'w', encoding='utf-8') as f: f.write('x')"
        normalized = normalize_python_c_source(source)
        self.assertTrue(can_compile_python_source(normalized))

        risky = "for i in range(2): print(i); if True: print('x')"
        self.assertTrue(has_risky_inline_control_flow(risky))


if __name__ == "__main__":
    unittest.main()
