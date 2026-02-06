import os
import tempfile
import unittest


class TestKnowledgeGovernanceSkillLifecycle(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        os.environ["AGENT_DB_PATH"] = os.path.join(self._tmpdir.name, "agent_knowledge_gov.db")
        os.environ["AGENT_PROMPT_ROOT"] = os.path.join(self._tmpdir.name, "prompt")
        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
        except Exception:
            pass
        self._tmpdir.cleanup()

    def test_validate_and_fix_skill_tags_updates_db(self):
        from backend.src.common.utils import parse_json_list
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill, get_skill
        from backend.src.services.knowledge.knowledge_governance import validate_and_fix_skill_tags

        skill_id = create_skill(
            SkillCreateParams(
                name="tag_test_skill",
                description="",
                scope="scope:tag_test",
                category="misc",
                tags=[
                    "PLAIN",
                    "plain",
                    "task:001",
                    "run:-2",
                    "mode:THINK",
                    "unknown:xx",
                ],
                triggers=[],
                aliases=[],
                prerequisites=[],
                inputs=[],
                outputs=[],
                steps=[],
                failure_modes=[],
                validation=[],
                version="0.1.0",
                task_id=None,
                domain_id="misc",
                skill_type="methodology",
                status="approved",
                source_task_id=None,
                source_run_id=None,
            )
        )

        preview = validate_and_fix_skill_tags(dry_run=True, fix=False, strict_keys=False, include_draft=True, limit=100)
        self.assertTrue(preview.get("ok"))
        self.assertEqual(preview.get("matched"), 1)
        self.assertEqual(preview.get("changed"), 1)

        applied = validate_and_fix_skill_tags(dry_run=False, fix=True, strict_keys=False, include_draft=True, limit=100)
        self.assertTrue(applied.get("ok"))
        self.assertEqual(applied.get("matched"), 1)
        self.assertEqual(applied.get("changed"), 1)

        row = get_skill(skill_id=int(skill_id))
        self.assertIsNotNone(row)
        tags = parse_json_list(row["tags"])
        self.assertIn("plain", tags)
        self.assertIn("task:1", tags)
        self.assertIn("mode:think", tags)
        self.assertIn("unknown:xx", tags)
        self.assertNotIn("run:-2", tags)
        self.assertNotIn("PLAIN", tags)

    def test_dedupe_and_merge_skills_merges_and_marks_duplicates(self):
        from backend.src.common.utils import parse_json_list
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill, get_skill
        from backend.src.services.knowledge.knowledge_governance import dedupe_and_merge_skills

        dup_scope = "scope:dup_test"
        skill_id_1 = create_skill(
            SkillCreateParams(
                name="dup_skill",
                description="desc1",
                scope=dup_scope,
                category="misc",
                tags=["a"],
                triggers=[],
                aliases=[],
                prerequisites=[],
                inputs=[],
                outputs=[],
                steps=["s1"],
                failure_modes=[],
                validation=[],
                version="0.1.0",
                task_id=None,
                domain_id="misc",
                skill_type="methodology",
                status="approved",
                source_task_id=None,
                source_run_id=None,
            )
        )
        skill_id_2 = create_skill(
            SkillCreateParams(
                name="dup_skill_newer",
                description="",  # canonical desc 为空时应从 duplicate 补齐
                scope=dup_scope,
                category="misc",
                tags=["b"],
                triggers=[],
                aliases=[],
                prerequisites=[],
                inputs=[],
                outputs=[],
                steps=["s2"],
                failure_modes=[],
                validation=[],
                version="0.1.0",
                task_id=None,
                domain_id="misc",
                skill_type="methodology",
                status="approved",
                source_task_id=None,
                source_run_id=None,
            )
        )

        result = dedupe_and_merge_skills(dry_run=False, include_draft=True, merge_across_domains=False, reason="test")
        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("merged"), 1)
        self.assertEqual(result.get("marked_duplicates"), 1)

        canonical = get_skill(skill_id=int(skill_id_2))
        duplicate = get_skill(skill_id=int(skill_id_1))
        self.assertIsNotNone(canonical)
        self.assertIsNotNone(duplicate)

        self.assertEqual(str(canonical["version"] or ""), "0.1.1")
        self.assertEqual(str(canonical["description"] or ""), "desc1")
        self.assertEqual(str(duplicate["status"] or ""), "deprecated")

        tags = parse_json_list(canonical["tags"])
        steps = parse_json_list(canonical["steps"])
        self.assertEqual(tags[:2], ["b", "a"])
        self.assertEqual(steps[:2], ["s2", "s1"])

    def test_rollback_skill_to_previous_version_restores_snapshot(self):
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill, get_skill, update_skill
        from backend.src.services.knowledge.knowledge_governance import rollback_skill_to_previous_version

        skill_id = create_skill(
            SkillCreateParams(
                name="rollback_skill",
                description="v1",
                scope="scope:rollback_test",
                category="misc",
                tags=["a"],
                triggers=[],
                aliases=[],
                prerequisites=[],
                inputs=[],
                outputs=[],
                steps=["s1"],
                failure_modes=[],
                validation=[],
                version="0.1.0",
                task_id=None,
                domain_id="misc",
                skill_type="methodology",
                status="approved",
                source_task_id=None,
                source_run_id=None,
            )
        )

        _ = update_skill(
            skill_id=int(skill_id),
            description="v2",
            version="0.1.1",
            change_notes="test_update",
        )
        updated = get_skill(skill_id=int(skill_id))
        self.assertEqual(str(updated["description"] or ""), "v2")
        self.assertEqual(str(updated["version"] or ""), "0.1.1")

        out = rollback_skill_to_previous_version(skill_id=int(skill_id), dry_run=False, reason="test_rollback")
        self.assertTrue(out.get("ok"))
        restored = get_skill(skill_id=int(skill_id))
        self.assertEqual(str(restored["description"] or ""), "v1")
        self.assertEqual(str(restored["version"] or ""), "0.1.0")

