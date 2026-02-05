import unittest
from unittest.mock import patch


class TestHttpRequestHandler(unittest.TestCase):
    def test_http_request_respects_max_bytes(self):
        from backend.src.actions.handlers.http_request import execute_http_request
        from backend.src.constants import HTTP_REQUEST_DEFAULT_TIMEOUT_MS

        created = {"timeout": None, "iter_calls": 0, "read_calls": 0}

        class FakeResponse:
            def __init__(self):
                self.url = "http://example.test/"
                self.status_code = 200
                self.headers = {"content-type": "text/plain"}
                self.encoding = "utf-8"
                self._chunks = [b"hello", b"world"]

            def iter_bytes(self):
                created["iter_calls"] += 1
                for chunk in self._chunks:
                    yield chunk

            def read(self):
                created["read_calls"] += 1
                return b"".join(self._chunks)

        class FakeStream:
            def __init__(self, resp):
                self._resp = resp

            def __enter__(self):
                return self._resp

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeClient:
            def __init__(self, timeout=None):
                created["timeout"] = timeout

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def stream(self, method, url, headers=None, params=None, data=None, json=None, follow_redirects=True):
                _ = method, url, headers, params, data, json, follow_redirects
                return FakeStream(FakeResponse())

        with patch("backend.src.actions.handlers.http_request.httpx.Client", FakeClient):
            result, error_message = execute_http_request({"url": "http://example.test/", "max_bytes": 7})

        self.assertIsNone(error_message)
        self.assertEqual(result["bytes"], 7)
        self.assertEqual(result["content"], "hellowo")
        self.assertEqual(created["iter_calls"], 1)
        self.assertEqual(created["read_calls"], 0)
        self.assertEqual(created["timeout"], float(HTTP_REQUEST_DEFAULT_TIMEOUT_MS) / 1000)

    def test_http_request_without_max_bytes_uses_read(self):
        from backend.src.actions.handlers.http_request import execute_http_request

        created = {"iter_calls": 0, "read_calls": 0}

        class FakeResponse:
            def __init__(self):
                self.url = "http://example.test/"
                self.status_code = 200
                self.headers = {"content-type": "text/plain"}
                self.encoding = "utf-8"
                self._chunks = [b"hello", b"world"]

            def iter_bytes(self):
                created["iter_calls"] += 1
                for chunk in self._chunks:
                    yield chunk

            def read(self):
                created["read_calls"] += 1
                return b"".join(self._chunks)

        class FakeStream:
            def __init__(self, resp):
                self._resp = resp

            def __enter__(self):
                return self._resp

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeClient:
            def __init__(self, timeout=None):
                _ = timeout

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def stream(self, method, url, headers=None, params=None, data=None, json=None, follow_redirects=True):
                _ = method, url, headers, params, data, json, follow_redirects
                return FakeStream(FakeResponse())

        with patch("backend.src.actions.handlers.http_request.httpx.Client", FakeClient):
            result, error_message = execute_http_request({"url": "http://example.test/"})

        self.assertIsNone(error_message)
        self.assertEqual(result["bytes"], 10)
        self.assertEqual(result["content"], "helloworld")
        self.assertEqual(created["iter_calls"], 0)
        self.assertEqual(created["read_calls"], 1)


if __name__ == "__main__":
    unittest.main()
