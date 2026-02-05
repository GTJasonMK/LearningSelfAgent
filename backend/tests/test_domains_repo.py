"""
测试领域系统 CRUD 操作。
"""
import os
import tempfile
import unittest
from pathlib import Path


class TestDomainsRepo(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp.name) / "agent_test.db"
        os.environ["AGENT_DB_PATH"] = str(db_path)
        os.environ["AGENT_PROMPT_ROOT"] = str(Path(self._tmp.name) / "prompt")

        import backend.src.storage as storage

        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
            self._tmp.cleanup()
        except Exception:
            pass

    def test_create_and_get_domain(self):
        """测试创建和获取领域。"""
        from backend.src.repositories.domains_repo import (
            DomainCreateParams,
            create_domain,
            get_domain,
        )

        params = DomainCreateParams(
            domain_id="test.create",
            name="测试创建",
            description="测试创建领域",
            keywords=["test", "create"],
        )
        domain_pk = create_domain(params)
        self.assertIsNotNone(domain_pk)
        self.assertGreater(domain_pk, 0)

        # 按 domain_id 获取
        row = get_domain(domain_id="test.create")
        self.assertIsNotNone(row)
        self.assertEqual(row["domain_id"], "test.create")
        self.assertEqual(row["name"], "测试创建")
        self.assertEqual(row["description"], "测试创建领域")
        self.assertEqual(row["status"], "active")

        # 按 id 获取
        row_by_id = get_domain(id=domain_pk)
        self.assertIsNotNone(row_by_id)
        self.assertEqual(row_by_id["domain_id"], "test.create")

    def test_create_child_domain(self):
        """测试创建子领域。"""
        from backend.src.repositories.domains_repo import (
            DomainCreateParams,
            create_domain,
            get_domain,
            list_child_domains,
        )

        # 创建父领域
        parent_params = DomainCreateParams(
            domain_id="parent",
            name="父领域",
        )
        create_domain(parent_params)

        # 创建子领域
        child_params = DomainCreateParams(
            domain_id="parent.child",
            name="子领域",
            parent_id="parent",
        )
        create_domain(child_params)

        # 验证父领域
        parent_row = get_domain(domain_id="parent")
        self.assertIsNotNone(parent_row)
        self.assertIsNone(parent_row["parent_id"])

        # 验证子领域
        child_row = get_domain(domain_id="parent.child")
        self.assertIsNotNone(child_row)
        self.assertEqual(child_row["parent_id"], "parent")

        # 列出子领域
        children = list_child_domains(parent_id="parent")
        self.assertEqual(len(children), 1)
        self.assertEqual(children[0]["domain_id"], "parent.child")

    def test_list_domains_with_filter(self):
        """测试按条件筛选领域列表。"""
        from backend.src.repositories.domains_repo import (
            DomainCreateParams,
            DomainUpdateParams,
            create_domain,
            update_domain,
            list_domains,
        )

        # 创建多个领域
        create_domain(DomainCreateParams(domain_id="a", name="A领域"))
        create_domain(DomainCreateParams(domain_id="b", name="B领域"))
        create_domain(DomainCreateParams(domain_id="a.sub", name="A子领域", parent_id="a"))

        # 列出所有领域（不含预定义的）
        all_domains = list_domains()
        custom_domains = [d for d in all_domains if d["domain_id"] in ["a", "b", "a.sub"]]
        self.assertEqual(len(custom_domains), 3)

        # 按 parent_id 筛选：空字符串表示一级领域
        top_level = list_domains(parent_id="")
        top_level_ids = [d["domain_id"] for d in top_level]
        self.assertIn("a", top_level_ids)
        self.assertIn("b", top_level_ids)
        self.assertNotIn("a.sub", top_level_ids)

        # 按 parent_id 筛选：指定父领域
        children = list_domains(parent_id="a")
        self.assertEqual(len(children), 1)
        self.assertEqual(children[0]["domain_id"], "a.sub")

        # 按 status 筛选
        update_domain(domain_id="b", params=DomainUpdateParams(status="deprecated"))
        active_domains = list_domains(status="active")
        active_ids = [d["domain_id"] for d in active_domains]
        self.assertIn("a", active_ids)
        self.assertNotIn("b", active_ids)

    def test_update_domain(self):
        """测试更新领域。"""
        from backend.src.repositories.domains_repo import (
            DomainCreateParams,
            DomainUpdateParams,
            create_domain,
            update_domain,
            get_domain,
        )

        create_domain(DomainCreateParams(
            domain_id="update.test",
            name="更新测试",
            description="原始描述",
        ))

        # 更新名称和描述
        result = update_domain(
            domain_id="update.test",
            params=DomainUpdateParams(
                name="更新后名称",
                description="更新后描述",
                keywords=["new", "keywords"],
            ),
        )
        self.assertTrue(result)

        # 验证更新
        row = get_domain(domain_id="update.test")
        self.assertEqual(row["name"], "更新后名称")
        self.assertEqual(row["description"], "更新后描述")

        # 更新状态
        update_domain(
            domain_id="update.test",
            params=DomainUpdateParams(status="deprecated"),
        )
        row = get_domain(domain_id="update.test")
        self.assertEqual(row["status"], "deprecated")

    def test_delete_domain(self):
        """测试删除领域。"""
        from backend.src.repositories.domains_repo import (
            DomainCreateParams,
            create_domain,
            delete_domain,
            get_domain,
        )

        create_domain(DomainCreateParams(domain_id="delete.test", name="删除测试"))

        # 确认存在
        row = get_domain(domain_id="delete.test")
        self.assertIsNotNone(row)

        # 删除
        result = delete_domain(domain_id="delete.test")
        self.assertTrue(result)

        # 确认已删除
        row = get_domain(domain_id="delete.test")
        self.assertIsNone(row)

        # 再次删除应返回 False
        result = delete_domain(domain_id="delete.test")
        self.assertFalse(result)

    def test_skill_count_operations(self):
        """测试技能计数增减。"""
        from backend.src.repositories.domains_repo import (
            DomainCreateParams,
            create_domain,
            get_domain,
            increment_skill_count,
            decrement_skill_count,
        )

        create_domain(DomainCreateParams(domain_id="count.test", name="计数测试"))

        # 初始计数为 0
        row = get_domain(domain_id="count.test")
        self.assertEqual(row["skill_count"], 0)

        # 增加计数
        increment_skill_count(domain_id="count.test")
        increment_skill_count(domain_id="count.test")
        row = get_domain(domain_id="count.test")
        self.assertEqual(row["skill_count"], 2)

        # 减少计数
        decrement_skill_count(domain_id="count.test")
        row = get_domain(domain_id="count.test")
        self.assertEqual(row["skill_count"], 1)

        # 减少到 0 后不会变负
        decrement_skill_count(domain_id="count.test")
        decrement_skill_count(domain_id="count.test")
        row = get_domain(domain_id="count.test")
        self.assertEqual(row["skill_count"], 0)

    def test_search_domains_by_keyword(self):
        """测试按关键词搜索领域。"""
        from backend.src.repositories.domains_repo import (
            DomainCreateParams,
            create_domain,
            search_domains_by_keyword,
        )

        create_domain(DomainCreateParams(
            domain_id="search.test1",
            name="数据分析",
            description="数据处理和分析",
            keywords=["data", "analysis"],
        ))
        create_domain(DomainCreateParams(
            domain_id="search.test2",
            name="网络爬虫",
            description="网页抓取",
            keywords=["web", "crawler"],
        ))

        # 搜索名称
        results = search_domains_by_keyword(keyword="数据")
        domain_ids = [r["domain_id"] for r in results]
        self.assertIn("search.test1", domain_ids)
        self.assertNotIn("search.test2", domain_ids)

        # 搜索描述
        results = search_domains_by_keyword(keyword="抓取")
        domain_ids = [r["domain_id"] for r in results]
        self.assertIn("search.test2", domain_ids)

        # 搜索关键词
        results = search_domains_by_keyword(keyword="analysis")
        domain_ids = [r["domain_id"] for r in results]
        self.assertIn("search.test1", domain_ids)

    def test_get_domain_with_children(self):
        """测试获取领域及其子领域。"""
        from backend.src.repositories.domains_repo import (
            DomainCreateParams,
            create_domain,
            get_domain_with_children,
        )

        create_domain(DomainCreateParams(domain_id="tree", name="树根"))
        create_domain(DomainCreateParams(domain_id="tree.branch1", name="分支1", parent_id="tree"))
        create_domain(DomainCreateParams(domain_id="tree.branch2", name="分支2", parent_id="tree"))
        create_domain(DomainCreateParams(domain_id="tree.branch1.leaf", name="叶子", parent_id="tree.branch1"))
        create_domain(DomainCreateParams(domain_id="other", name="其他"))

        # 获取 tree 及其子领域
        results = get_domain_with_children(domain_id="tree")
        domain_ids = [r["domain_id"] for r in results]
        self.assertIn("tree", domain_ids)
        self.assertIn("tree.branch1", domain_ids)
        self.assertIn("tree.branch2", domain_ids)
        self.assertIn("tree.branch1.leaf", domain_ids)
        self.assertNotIn("other", domain_ids)

        # 获取 tree.branch1 及其子领域
        results = get_domain_with_children(domain_id="tree.branch1")
        domain_ids = [r["domain_id"] for r in results]
        self.assertIn("tree.branch1", domain_ids)
        self.assertIn("tree.branch1.leaf", domain_ids)
        self.assertNotIn("tree", domain_ids)
        self.assertNotIn("tree.branch2", domain_ids)

    def test_predefined_domains_exist(self):
        """测试预定义领域是否已初始化。"""
        from backend.src.repositories.domains_repo import get_domain, count_domains

        # 应至少存在预定义领域
        total = count_domains()
        self.assertGreater(total, 0)

        # 检查几个预定义领域
        data_domain = get_domain(domain_id="data")
        self.assertIsNotNone(data_domain)
        self.assertEqual(data_domain["name"], "数据处理")

        misc_domain = get_domain(domain_id="misc")
        self.assertIsNotNone(misc_domain)
        self.assertEqual(misc_domain["name"], "未分类")


if __name__ == "__main__":
    unittest.main()
