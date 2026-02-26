import unittest
from unittest.mock import patch


class TestHttpRequestHandler(unittest.TestCase):
    def test_http_request_missing_httpx_dependency_returns_error(self):
        from backend.src.actions.handlers.http_request import execute_http_request

        class MissingClient:
            def __init__(self, *_args, **_kwargs):
                raise ModuleNotFoundError("No module named 'httpx'")

        with patch("backend.src.actions.handlers.http_request.httpx.Client", MissingClient):
            result, error_message = execute_http_request({"url": "http://example.test/"})

        self.assertIsNone(result)
        self.assertIn("http_request 执行失败", str(error_message))
        self.assertIn("httpx", str(error_message))

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

    def test_http_request_fallback_urls_use_next_source_when_primary_fails(self):
        from backend.src.actions.handlers.http_request import execute_http_request

        class FakeResponse:
            def __init__(self, url, status_code, body):
                self.url = url
                self.status_code = status_code
                self.headers = {"content-type": "text/plain"}
                self.encoding = "utf-8"
                self._body = body

            def iter_bytes(self):
                yield self._body

            def read(self):
                return self._body

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
                _ = method, headers, params, data, json, follow_redirects
                if str(url) == "http://primary.test/fail":
                    return FakeStream(FakeResponse("http://primary.test/fail", 503, b"primary down"))
                return FakeStream(FakeResponse("http://mirror.test/ok", 200, b"mirror ok"))

        with patch("backend.src.actions.handlers.http_request.httpx.Client", FakeClient):
            result, error_message = execute_http_request(
                {
                    "url": "http://primary.test/fail",
                    "fallback_urls": ["http://mirror.test/ok"],
                }
            )

        self.assertIsNone(error_message)
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("status_code"), 200)
        self.assertEqual(result.get("url"), "http://mirror.test/ok")
        self.assertEqual(result.get("source_url"), "http://mirror.test/ok")
        self.assertEqual(result.get("source_index"), 1)
        self.assertEqual(result.get("source_count"), 2)

    def test_http_request_fallback_urls_return_aggregate_error_when_all_fail(self):
        from backend.src.actions.handlers.http_request import execute_http_request

        class FakeResponse:
            def __init__(self, url, status_code, body):
                self.url = url
                self.status_code = status_code
                self.headers = {"content-type": "text/plain"}
                self.encoding = "utf-8"
                self._body = body

            def iter_bytes(self):
                yield self._body

            def read(self):
                return self._body

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
                _ = method, headers, params, data, json, follow_redirects
                if str(url) == "http://primary.test/fail":
                    return FakeStream(FakeResponse("http://primary.test/fail", 503, b"primary down"))
                return FakeStream(FakeResponse("http://backup.test/missing", 404, b"not found"))

        with patch("backend.src.actions.handlers.http_request.httpx.Client", FakeClient):
            result, error_message = execute_http_request(
                {
                    "url": "http://primary.test/fail",
                    "fallback_urls": ["http://backup.test/missing"],
                }
            )

        self.assertIsNone(result)
        self.assertIn("候选源全部失败", str(error_message))
        self.assertIn("http://primary.test/fail", str(error_message))
        self.assertIn("http://backup.test/missing", str(error_message))

    def test_http_request_prioritizes_cross_host_sources_before_same_host_backup(self):
        from backend.src.actions.handlers.http_request import execute_http_request

        calls = []

        class FakeResponse:
            def __init__(self, url, status_code, body):
                self.url = url
                self.status_code = status_code
                self.headers = {"content-type": "text/plain"}
                self.encoding = "utf-8"
                self._body = body

            def iter_bytes(self):
                yield self._body

            def read(self):
                return self._body

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
                _ = method, headers, params, data, json, follow_redirects
                calls.append(str(url))
                if str(url) == "http://same.test/primary":
                    return FakeStream(FakeResponse("http://same.test/primary", 404, b"missing"))
                if str(url) == "http://same.test/backup":
                    return FakeStream(FakeResponse("http://same.test/backup", 200, b"backup ok"))
                return FakeStream(FakeResponse("http://mirror.test/ok", 200, b"mirror ok"))

        with patch("backend.src.actions.handlers.http_request.httpx.Client", FakeClient):
            result, error_message = execute_http_request(
                {
                    "url": "http://same.test/primary",
                    "fallback_urls": ["http://same.test/backup", "http://mirror.test/ok"],
                }
            )

        self.assertIsNone(error_message)
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("status_code"), 200)
        self.assertEqual(result.get("source_url"), "http://mirror.test/ok")
        self.assertEqual(calls, ["http://same.test/primary", "http://mirror.test/ok"])

    def test_http_request_skips_same_host_after_host_level_failure(self):
        from backend.src.actions.handlers.http_request import execute_http_request

        calls = []

        class FakeResponse:
            def __init__(self, url, status_code, body):
                self.url = url
                self.status_code = status_code
                self.headers = {"content-type": "text/plain"}
                self.encoding = "utf-8"
                self._body = body

            def iter_bytes(self):
                yield self._body

            def read(self):
                return self._body

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
                _ = method, headers, params, data, json, follow_redirects
                calls.append(str(url))
                if str(url) == "http://down.test/a":
                    raise RuntimeError("Could not resolve host: down.test")
                return FakeStream(FakeResponse("http://down.test/b", 200, b"ok"))

        with patch("backend.src.actions.handlers.http_request.httpx.Client", FakeClient):
            result, error_message = execute_http_request(
                {
                    "url": "http://down.test/a",
                    "fallback_urls": ["http://down.test/b"],
                }
            )

        self.assertIsNone(result)
        self.assertIn("候选源全部失败", str(error_message))
        self.assertIn("跳过同 host 备选源", str(error_message))
        self.assertEqual(calls, ["http://down.test/a"])


    def test_http_request_strict_business_success_blocks_success_false_payload(self):
        from backend.src.actions.handlers.http_request import execute_http_request

        class FakeResponse:
            def __init__(self):
                self.url = "http://example.test/"
                self.status_code = 200
                self.headers = {"content-type": "application/json"}
                self.encoding = "utf-8"

            def iter_bytes(self):
                yield b'{"success":false,"error":{"statusCode":102,"message":"invalid key"}}'

            def read(self):
                return b'{"success":false,"error":{"statusCode":102,"message":"invalid key"}}'

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

        self.assertIsNone(result)
        self.assertIn("http_request 业务失败", str(error_message))
        self.assertIn("code=102", str(error_message))

    def test_http_request_strict_status_code_blocks_4xx_by_default(self):
        from backend.src.actions.handlers.http_request import execute_http_request

        class FakeResponse:
            def __init__(self):
                self.url = "http://example.test/blocked"
                self.status_code = 403
                self.headers = {"content-type": "text/plain"}
                self.encoding = "utf-8"

            def iter_bytes(self):
                yield b"Forbidden"

            def read(self):
                return b"Forbidden"

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
            result, error_message = execute_http_request({"url": "http://example.test/blocked"})

        self.assertIsNone(result)
        self.assertIn("HTTP 403", str(error_message))
        self.assertIn("Forbidden", str(error_message))

    def test_http_request_strict_status_code_can_be_disabled(self):
        from backend.src.actions.handlers.http_request import execute_http_request

        class FakeResponse:
            def __init__(self):
                self.url = "http://example.test/blocked"
                self.status_code = 403
                self.headers = {"content-type": "text/plain"}
                self.encoding = "utf-8"

            def iter_bytes(self):
                yield b"Forbidden"

            def read(self):
                return b"Forbidden"

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
            result, error_message = execute_http_request({"url": "http://example.test/blocked", "strict_status_code": False})

        self.assertIsNone(error_message)
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("status_code"), 403)

if __name__ == "__main__":
    unittest.main()
