import os
import tempfile
import unittest


class TestFileWriteScriptQualityGuard(unittest.TestCase):
    def test_reject_placeholder_python_script(self):
        from backend.src.actions.handlers.file_write import execute_file_write

        content = (
            '#!/usr/bin/env python3\n'
            '"""Minimal executable skeleton."""\n\n'
            'def main():\n'
            '    print("Gold price parser started.")\n'
            '    # TODO: Implement parsing logic\n'
            '    print("Gold price parser finished.")\n\n'
            'if __name__ == "__main__":\n'
            '    main()\n'
        )

        with tempfile.TemporaryDirectory() as tmp:
            prev = os.getcwd()
            try:
                os.chdir(tmp)
                result, error = execute_file_write(
                    {"path": "parse_gold_price.py", "content": content},
                    context={},
                )
            finally:
                os.chdir(prev)

        self.assertIsNone(result)
        self.assertIn("file_write_placeholder_script", str(error or ""))

    def test_reject_instruction_only_wrapper_script(self):
        from backend.src.actions.handlers.file_write import execute_file_write

        content = (
            'import sys\n\n'
            'def main():\n'
            '    print("请运行具体解析脚本")\n'
            '    print("python parse_sge.py")\n'
            '    return 0\n\n'
            'if __name__ == "__main__":\n'
            '    sys.exit(main())\n'
        )

        with tempfile.TemporaryDirectory() as tmp:
            prev = os.getcwd()
            try:
                os.chdir(tmp)
                result, error = execute_file_write(
                    {"path": "router.py", "content": content},
                    context={},
                )
            finally:
                os.chdir(prev)

        self.assertIsNone(result)
        self.assertIn("file_write_placeholder_script", str(error or ""))

    def test_reject_script_with_placeholder_stub_functions(self):
        from backend.src.actions.handlers.file_write import execute_file_write

        content = (
            'import requests\n\n'
            'def fetch_page():\n'
            '    resp = requests.get("https://example.com", timeout=5)\n'
            '    return resp.text\n\n'
            'def parse_page(html):\n'
            '    # 这里需要根据实际页面结构编写解析逻辑\n'
            '    return []\n\n'
            'def main():\n'
            '    html = fetch_page()\n'
            '    rows = parse_page(html)\n'
            '    print(rows)\n\n'
            'if __name__ == "__main__":\n'
            '    main()\n'
        )

        with tempfile.TemporaryDirectory() as tmp:
            prev = os.getcwd()
            try:
                os.chdir(tmp)
                result, error = execute_file_write(
                    {"path": "fetch_gold_price.py", "content": content},
                    context={},
                )
            finally:
                os.chdir(prev)

        self.assertIsNone(result)
        self.assertIn("file_write_placeholder_script", str(error or ""))
        self.assertIn("占位函数", str(error or ""))

    def test_allow_substantive_python_script(self):
        from backend.src.actions.handlers.file_write import execute_file_write

        content = (
            'import csv\n'
            'import sys\n\n'
            'def main():\n'
            '    raw = sys.stdin.read().strip()\n'
            '    rows = []\n'
            '    for line in raw.splitlines():\n'
            '        day, price = line.split()\n'
            '        rows.append({"date": day, "price": float(price)})\n'
            '    writer = csv.DictWriter(sys.stdout, fieldnames=["date", "price"])\n'
            '    writer.writeheader()\n'
            '    for row in rows:\n'
            '        writer.writerow(row)\n\n'
            'if __name__ == "__main__":\n'
            '    main()\n'
        )

        with tempfile.TemporaryDirectory() as tmp:
            prev = os.getcwd()
            try:
                os.chdir(tmp)
                result, error = execute_file_write(
                    {"path": "parse_gold_price.py", "content": content},
                    context={},
                )
            finally:
                os.chdir(prev)

        self.assertIsNone(error)
        self.assertIsInstance(result, dict)
        self.assertTrue(str(result.get("path") or "").endswith("parse_gold_price.py"))

    def test_allow_real_script_even_with_todo_comment(self):
        from backend.src.actions.handlers.file_write import execute_file_write

        content = (
            'import json\n'
            'import sys\n\n'
            'def main():\n'
            '    raw = sys.stdin.read()\n'
            '    data = {"length": len(raw)}\n'
            '    # TODO: 支持更多字段\n'
            '    print(json.dumps(data, ensure_ascii=False))\n\n'
            'if __name__ == "__main__":\n'
            '    main()\n'
        )

        with tempfile.TemporaryDirectory() as tmp:
            prev = os.getcwd()
            try:
                os.chdir(tmp)
                result, error = execute_file_write(
                    {"path": "measure.py", "content": content},
                    context={},
                )
            finally:
                os.chdir(prev)

        self.assertIsNone(error)
        self.assertIsInstance(result, dict)


    def test_reject_assumed_response_shape_script(self):
        from backend.src.actions.handlers.file_write import execute_file_write

        content = (
            'import json\n'
            'import sys\n\n'
            'def parse_payload(text):\n'
            '    data = json.loads(text)\n'
            '    # 假设数据结构为 {"result": {"data": [...]}}，实际结构需根据观测调整\n'
            '    items = data.get("result", {}).get("data", [])\n'
            '    return items\n\n'
            'def main():\n'
            '    print(parse_payload(sys.stdin.read()))\n\n'
            'if __name__ == "__main__":\n'
            '    main()\n'
        )

        with tempfile.TemporaryDirectory() as tmp:
            prev = os.getcwd()
            try:
                os.chdir(tmp)
                result, error = execute_file_write(
                    {"path": "parse_sina.py", "content": content},
                    context={},
                )
            finally:
                os.chdir(prev)

        self.assertIsNone(result)
        self.assertIn("file_write_placeholder_script", str(error or ""))
        self.assertIn("假设数据结构", str(error or ""))


    def test_reject_json_only_parser_against_html_sample(self):
        from backend.src.actions.handlers.file_write import execute_file_write

        content = (
            'import json\n'
            'import sys\n\n'
            'def main():\n'
            '    text = sys.stdin.read()\n'
            '    data = json.loads(text)\n'
            '    print(data)\n\n'
            'if __name__ == "__main__":\n'
            '    main()\n'
        )

        with tempfile.TemporaryDirectory() as tmp:
            prev = os.getcwd()
            try:
                os.chdir(tmp)
                result, error = execute_file_write(
                    {"path": "parse_html_as_json.py", "content": content},
                    context={"latest_parse_input_text": "<!DOCTYPE html><html><body><table><tr><td>680</td></tr></table></body></html>"},
                )
            finally:
                os.chdir(prev)

        self.assertIsNone(result)
        self.assertIn("file_write_placeholder_script", str(error or ""))
        self.assertIn("当前真实样本是 HTML", str(error or ""))

    def test_reject_expected_format_placeholder_script(self):
        from backend.src.actions.handlers.file_write import execute_file_write

        content = (
            'import re\n\n'
            'def parse(text):\n'
            '    # 期望格式：每行都包含日期和价格\n'
            '    rows = []\n'
            '    for line in text.splitlines():\n'
            '        if re.search(r"\\d{4}-\\d{2}-\\d{2}", line):\n'
            '            rows.append(line)\n'
            '    return rows\n'
        )

        with tempfile.TemporaryDirectory() as tmp:
            prev = os.getcwd()
            try:
                os.chdir(tmp)
                result, error = execute_file_write(
                    {"path": "parse_expected.py", "content": content},
                    context={},
                )
            finally:
                os.chdir(prev)

        self.assertIsNone(result)
        self.assertIn("file_write_placeholder_script", str(error or ""))
        self.assertIn("期望格式", str(error or ""))


if __name__ == "__main__":
    unittest.main()
