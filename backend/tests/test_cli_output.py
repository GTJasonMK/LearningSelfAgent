import unittest
from unittest.mock import patch


class _FakeBuffer:
    def __init__(self):
        self.parts = []

    def write(self, value):
        self.parts.append(value)

    def flush(self):
        return None


class _FakeStdout:
    def __init__(self, encoding: str):
        self.encoding = encoding
        self.parts = []
        self.buffer = _FakeBuffer()

    def write(self, s):
        self.parts.append(str(s))
        return len(str(s))

    def flush(self):
        return None

    def getvalue(self):
        direct = ''.join(self.parts)
        if direct:
            return direct
        return b''.join(self.buffer.parts).decode(self.encoding, errors='replace')


class TestCliOutput(unittest.TestCase):
    def test_print_json_escapes_non_utf_stdout(self):
        from backend.src.cli.output import print_json

        fake = _FakeStdout('gbk')
        with patch('backend.src.cli.output.sys.stdout', fake):
            print_json({'message': '黄金价格', 'bad': '\ufffd'})

        text = fake.getvalue()
        self.assertIn('\\u9ec4\\u91d1\\u4ef7\\u683c', text)
        self.assertIn('\\ufffd', text)

    def test_print_json_preserves_utf_stdout(self):
        from backend.src.cli.output import print_json

        fake = _FakeStdout('utf-8')
        with patch('backend.src.cli.output.sys.stdout', fake):
            print_json({'message': '黄金价格'})

        self.assertIn('黄金价格', fake.getvalue())


if __name__ == '__main__':
    unittest.main()
