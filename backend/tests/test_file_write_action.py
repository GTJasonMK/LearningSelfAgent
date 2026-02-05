import os
import tempfile
import unittest


class TestFileWriteAction(unittest.TestCase):
    def test_file_write_creates_file(self):
        from backend.src.actions.file_write import write_text_file

        with tempfile.TemporaryDirectory() as tmp:
            old_cwd = os.getcwd()
            os.chdir(tmp)
            try:
                result = write_text_file(path="out/demo.txt", content="hello", encoding="utf-8")
                self.assertIsInstance(result, dict)
                self.assertTrue(os.path.exists(os.path.join(tmp, "out", "demo.txt")))
                with open(os.path.join(tmp, "out", "demo.txt"), "r", encoding="utf-8") as f:
                    self.assertEqual(f.read(), "hello")
            finally:
                os.chdir(old_cwd)


if __name__ == "__main__":
    unittest.main()
