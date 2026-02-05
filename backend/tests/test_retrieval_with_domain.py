"""
测试领域筛选检索功能。
"""
import os
import tempfile
import unittest


class TestRetrievalWithDomain(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_domain_test.db")

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

    def test_list_domain_candidates(self):
        """测试读取领域候选集。"""
        from backend.src.agent.retrieval import _list_domain_candidates

        # 预定义领域应存在
        candidates = _list_domain_candidates(limit=20)
        self.assertTrue(candidates, "应返回领域候选集")

        # 检查返回格式
        domain_ids = [c.get("domain_id") for c in candidates]
        self.assertIn("data", domain_ids)
        self.assertIn("misc", domain_ids)

    def test_format_domain_candidates_for_prompt(self):
        """测试领域候选集格式化。"""
        from backend.src.agent.retrieval import _format_domain_candidates_for_prompt

        # 空列表
        result = _format_domain_candidates_for_prompt([])
        self.assertEqual(result, "(无)")

        # 有内容
        items = [
            {"domain_id": "test.domain", "name": "测试领域", "description": "描述", "keywords": ["kw1"]},
            {"domain_id": "other", "name": "其他", "description": None, "keywords": None},
        ]
        result = _format_domain_candidates_for_prompt(items)
        self.assertIn("test.domain", result)
        self.assertIn("测试领域", result)
        self.assertIn("描述", result)
        self.assertIn("other", result)

    def test_list_skill_candidates_by_domains(self):
        """测试按领域筛选技能候选集。"""
        from backend.src.storage import get_connection
        from backend.src.repositories.agent_retrieval_repo import list_skill_candidates_by_domains
        from backend.src.common.utils import now_iso

        with get_connection() as conn:
            created_at = now_iso()
            # 插入 data 领域的技能
            conn.execute(
                "INSERT INTO skills_items (name, created_at, description, scope, category, tags, triggers, aliases, source_path, prerequisites, inputs, outputs, steps, failure_modes, validation, version, task_id, domain_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "data_cleaner",
                    created_at,
                    "数据清洗工具",
                    "数据处理",
                    "data",
                    '["data","clean"]',
                    '["清洗"]',
                    "[]",
                    None,
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "0.1.0",
                    None,
                    "data.clean",
                ),
            )
            # 插入 data.collect 子领域的技能
            conn.execute(
                "INSERT INTO skills_items (name, created_at, description, scope, category, tags, triggers, aliases, source_path, prerequisites, inputs, outputs, steps, failure_modes, validation, version, task_id, domain_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "data_collector",
                    created_at,
                    "数据采集工具",
                    "数据采集",
                    "data",
                    '["data","collect"]',
                    '["采集"]',
                    "[]",
                    None,
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "0.1.0",
                    None,
                    "data.collect",
                ),
            )
            # 插入 web 领域的技能
            conn.execute(
                "INSERT INTO skills_items (name, created_at, description, scope, category, tags, triggers, aliases, source_path, prerequisites, inputs, outputs, steps, failure_modes, validation, version, task_id, domain_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "web_scraper",
                    created_at,
                    "网页抓取工具",
                    "网络爬虫",
                    "web",
                    '["web","scrape"]',
                    '["抓取"]',
                    "[]",
                    None,
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "0.1.0",
                    None,
                    "web.scrape",
                ),
            )

        # 按 data 领域筛选（应匹配 data.clean 和 data.collect）
        items = list_skill_candidates_by_domains(domain_ids=["data"], limit=10)
        names = [it.get("name") for it in items]
        self.assertIn("data_cleaner", names)
        self.assertIn("data_collector", names)
        self.assertNotIn("web_scraper", names)

        # 按 data.clean 精确筛选
        items = list_skill_candidates_by_domains(domain_ids=["data.clean"], limit=10)
        names = [it.get("name") for it in items]
        self.assertIn("data_cleaner", names)
        self.assertNotIn("data_collector", names)
        self.assertNotIn("web_scraper", names)

        # 按多个领域筛选
        items = list_skill_candidates_by_domains(domain_ids=["data.clean", "web"], limit=10)
        names = [it.get("name") for it in items]
        self.assertIn("data_cleaner", names)
        self.assertIn("web_scraper", names)

    def test_list_skill_candidates_with_domain_filter(self):
        """测试 _list_skill_candidates 的领域筛选参数。"""
        from backend.src.storage import get_connection
        from backend.src.agent.retrieval import _list_skill_candidates
        from backend.src.common.utils import now_iso

        with get_connection() as conn:
            created_at = now_iso()
            # 插入不同领域的技能
            conn.execute(
                "INSERT INTO skills_items (name, created_at, description, scope, category, tags, triggers, aliases, source_path, prerequisites, inputs, outputs, steps, failure_modes, validation, version, task_id, domain_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "finance_analyzer",
                    created_at,
                    "金融分析工具",
                    "金融分析",
                    "finance",
                    '["finance","analyze"]',
                    '["分析"]',
                    "[]",
                    None,
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "0.1.0",
                    None,
                    "finance.stock",
                ),
            )
            conn.execute(
                "INSERT INTO skills_items (name, created_at, description, scope, category, tags, triggers, aliases, source_path, prerequisites, inputs, outputs, steps, failure_modes, validation, version, task_id, domain_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "file_converter",
                    created_at,
                    "文件转换工具",
                    "文件操作",
                    "file",
                    '["file","convert"]',
                    '["转换"]',
                    "[]",
                    None,
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "0.1.0",
                    None,
                    "file.convert",
                ),
            )

        # 无领域筛选：返回所有
        items = _list_skill_candidates(limit=10, domain_ids=None)
        names = [it.get("name") for it in items]
        self.assertIn("finance_analyzer", names)
        self.assertIn("file_converter", names)

        # 有领域筛选：只返回匹配的
        items = _list_skill_candidates(limit=10, domain_ids=["finance"])
        names = [it.get("name") for it in items]
        self.assertIn("finance_analyzer", names)
        self.assertNotIn("file_converter", names)

    def test_list_skill_candidates_excludes_solutions_by_default(self):
        """技能检索默认只返回 methodology（不应混入 solution）。"""
        from backend.src.agent.retrieval import _list_skill_candidates
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill

        _ = create_skill(
            SkillCreateParams(
                name="method_skill",
                description="methodology skill",
                domain_id="data",
                skill_type="methodology",
                status="approved",
            )
        )
        _ = create_skill(
            SkillCreateParams(
                name="solution_skill",
                description="solution skill",
                domain_id="data",
                skill_type="solution",
                status="approved",
            )
        )

        items = _list_skill_candidates(limit=10, domain_ids=["data"])
        names = [it.get("name") for it in items]
        self.assertIn("method_skill", names)
        self.assertNotIn("solution_skill", names)

    def test_list_skill_candidates_by_domains_with_fts(self):
        """测试领域筛选结合 FTS 搜索。"""
        from backend.src.storage import get_connection
        from backend.src.repositories.agent_retrieval_repo import list_skill_candidates_by_domains
        from backend.src.services.search.fts_search import fts_table_exists
        from backend.src.common.utils import now_iso

        with get_connection() as conn:
            # 如果 FTS 不可用则跳过
            if not fts_table_exists(conn, "skills_items_fts"):
                self.skipTest("FTS5 not available")

            created_at = now_iso()
            # 插入技能
            conn.execute(
                "INSERT INTO skills_items (name, created_at, description, scope, category, tags, triggers, aliases, source_path, prerequisites, inputs, outputs, steps, failure_modes, validation, version, task_id, domain_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "stock_recommender",
                    created_at,
                    "股票推荐系统",
                    "金融分析",
                    "finance",
                    '["stock","recommend"]',
                    '["推荐","股票"]',
                    "[]",
                    None,
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "0.1.0",
                    None,
                    "finance.stock",
                ),
            )
            conn.execute(
                "INSERT INTO skills_items (name, created_at, description, scope, category, tags, triggers, aliases, source_path, prerequisites, inputs, outputs, steps, failure_modes, validation, version, task_id, domain_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "stock_price_fetcher",
                    created_at,
                    "股票价格获取",
                    "数据采集",
                    "data",
                    '["stock","price"]',
                    '["获取","价格"]',
                    "[]",
                    None,
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "0.1.0",
                    None,
                    "data.collect",
                ),
            )

        # 按领域 + 关键词筛选
        debug = {}
        items = list_skill_candidates_by_domains(
            domain_ids=["finance"],
            limit=10,
            query_text="股票推荐",
            debug=debug,
        )
        names = [it.get("name") for it in items]
        # 应返回 finance 领域的股票相关技能
        self.assertIn("stock_recommender", names)
        # 不应返回 data 领域的技能
        self.assertNotIn("stock_price_fetcher", names)

    def test_empty_domain_list_fallback_to_all(self):
        """测试空领域列表回退到返回所有技能（不按领域筛选）。"""
        from backend.src.storage import get_connection
        from backend.src.repositories.agent_retrieval_repo import list_skill_candidates_by_domains
        from backend.src.common.utils import now_iso

        with get_connection() as conn:
            created_at = now_iso()
            conn.execute(
                "INSERT INTO skills_items (name, created_at, description, scope, category, tags, triggers, aliases, source_path, prerequisites, inputs, outputs, steps, failure_modes, validation, version, task_id, domain_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "test_skill",
                    created_at,
                    "测试技能",
                    None,
                    "misc",
                    '["test"]',
                    "[]",
                    "[]",
                    None,
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "0.1.0",
                    None,
                    "misc",
                ),
            )

        # 空领域列表应回退到无筛选模式，返回所有技能
        items = list_skill_candidates_by_domains(domain_ids=[], limit=10)
        names = [it.get("name") for it in items]
        self.assertIn("test_skill", names)

    def test_domain_prefix_matching(self):
        """测试领域前缀匹配逻辑。"""
        from backend.src.storage import get_connection
        from backend.src.repositories.agent_retrieval_repo import list_skill_candidates_by_domains
        from backend.src.common.utils import now_iso

        with get_connection() as conn:
            created_at = now_iso()
            # 插入层级领域的技能
            for domain_id, name in [
                ("dev", "dev_tool"),
                ("dev.test", "dev_test_tool"),
                ("dev.test.unit", "dev_unit_test_tool"),
                ("dev.build", "dev_build_tool"),
            ]:
                conn.execute(
                    "INSERT INTO skills_items (name, created_at, description, scope, category, tags, triggers, aliases, source_path, prerequisites, inputs, outputs, steps, failure_modes, validation, version, task_id, domain_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        name,
                        created_at,
                        f"{name} 描述",
                        None,
                        "dev",
                        '[]',
                        "[]",
                        "[]",
                        None,
                        "[]",
                        "[]",
                        "[]",
                        "[]",
                        "[]",
                        "[]",
                        "0.1.0",
                        None,
                        domain_id,
                    ),
                )

        # dev 应匹配所有 dev.* 子领域
        items = list_skill_candidates_by_domains(domain_ids=["dev"], limit=10)
        names = [it.get("name") for it in items]
        self.assertIn("dev_tool", names)
        self.assertIn("dev_test_tool", names)
        self.assertIn("dev_unit_test_tool", names)
        self.assertIn("dev_build_tool", names)

        # dev.test 应匹配 dev.test 和 dev.test.unit
        items = list_skill_candidates_by_domains(domain_ids=["dev.test"], limit=10)
        names = [it.get("name") for it in items]
        self.assertIn("dev_test_tool", names)
        self.assertIn("dev_unit_test_tool", names)
        self.assertNotIn("dev_tool", names)
        self.assertNotIn("dev_build_tool", names)

        # dev.test.unit 应只匹配自身
        items = list_skill_candidates_by_domains(domain_ids=["dev.test.unit"], limit=10)
        names = [it.get("name") for it in items]
        self.assertIn("dev_unit_test_tool", names)
        self.assertNotIn("dev_test_tool", names)


if __name__ == "__main__":
    unittest.main()
