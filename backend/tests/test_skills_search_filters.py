import os
import tempfile
import unittest

try:
    import httpx  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    httpx = None


class TestSkillsSearchFilters(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        if httpx is None:
            self.skipTest("httpx 未安装，跳过需要 ASGI 客户端的测试")

        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_test.db")

        os.environ["AGENT_DB_PATH"] = self._db_path
        os.environ["AGENT_PROMPT_ROOT"] = os.path.join(self._tmpdir.name, "prompt")
        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
        except Exception:
            pass
        self._tmpdir.cleanup()

    async def test_skills_search_supports_skill_type_and_status_filters(self):
        from backend.src.common.utils import now_iso
        from backend.src.main import create_app
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill

        create_skill(SkillCreateParams(name="methodology#1", category="dev", created_at=now_iso()))
        create_skill(
            SkillCreateParams(
                name="solution#1",
                category="dev",
                skill_type="solution",
                status="approved",
                created_at=now_iso(),
            )
        )
        create_skill(
            SkillCreateParams(
                name="solution#draft",
                category="dev",
                skill_type="solution",
                status="draft",
                created_at=now_iso(),
            )
        )

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/skills/search?skill_type=solution")
            self.assertEqual(resp.status_code, 200)
            data = resp.json()
            self.assertEqual(int(data.get("total") or 0), 2)
            items = data.get("items") or []
            self.assertTrue(all(it.get("skill_type") == "solution" for it in items))

            resp2 = await client.get("/api/skills/search?skill_type=solution&status=approved")
            self.assertEqual(resp2.status_code, 200)
            data2 = resp2.json()
            self.assertEqual(int(data2.get("total") or 0), 1)
            items2 = data2.get("items") or []
            self.assertEqual(len(items2), 1)
            self.assertEqual(items2[0].get("skill_type"), "solution")
            self.assertEqual(str(items2[0].get("status") or "").lower(), "approved")

    async def test_skills_catalog_includes_type_and_status_stats(self):
        from backend.src.common.utils import now_iso
        from backend.src.main import create_app
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill

        create_skill(SkillCreateParams(name="methodology#1", category="dev", created_at=now_iso()))
        create_skill(
            SkillCreateParams(
                name="solution#1",
                category="dev",
                skill_type="solution",
                status="approved",
                created_at=now_iso(),
            )
        )
        create_skill(
            SkillCreateParams(
                name="solution#draft",
                category="dev",
                skill_type="solution",
                status="draft",
                created_at=now_iso(),
            )
        )

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/skills/catalog")
            self.assertEqual(resp.status_code, 200)
            data = resp.json()

        types = {it.get("skill_type"): int(it.get("count") or 0) for it in (data.get("skill_types") or [])}
        self.assertEqual(types.get("methodology"), 1)
        self.assertEqual(types.get("solution"), 2)

        statuses = {it.get("status"): int(it.get("count") or 0) for it in (data.get("statuses") or [])}
        self.assertEqual(statuses.get("approved"), 2)
        self.assertEqual(statuses.get("draft"), 1)


if __name__ == "__main__":
    unittest.main()

