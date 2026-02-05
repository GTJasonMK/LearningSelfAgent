import unittest


class TestFileWriteTitlePathCoercion(unittest.TestCase):
    def test_coerce_uses_title_path_when_it_looks_like_a_path(self):
        from backend.src.agent.plan_utils import coerce_file_write_payload_path_from_title

        payload = {"path": "a.py", "content": "x"}
        patched = coerce_file_write_payload_path_from_title("file_write:test/demo.txt 写入文件", payload)
        self.assertEqual(patched.get("path"), "test/demo.txt")

    def test_coerce_keeps_payload_path_when_title_is_not_a_path(self):
        """
        回归：避免把 file_write:编写/生成 等自然语言当成文件名覆盖 payload.path，
        否则会导致脚本写入到错误文件，后续执行再找不到真正的脚本路径。
        """
        from backend.src.agent.plan_utils import coerce_file_write_payload_path_from_title

        payload = {"path": "gold_price_fetcher.py", "content": "print('ok')"}
        patched = coerce_file_write_payload_path_from_title("file_write:编写 Python 脚本获取黄金价格", payload)
        self.assertEqual(patched.get("path"), "gold_price_fetcher.py")


if __name__ == "__main__":
    unittest.main()

