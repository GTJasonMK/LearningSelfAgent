import ast
import unittest
from pathlib import Path


class TestAgentLayerImportBoundaries(unittest.TestCase):
    def test_agent_layer_must_not_import_repositories_directly(self):
        root = Path("backend/src/agent")
        violations = []

        for file_path in root.rglob("*.py"):
            module_name = "backend.src." + ".".join(file_path.with_suffix("").parts[2:])
            tree = ast.parse(file_path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                targets = []
                if isinstance(node, ast.Import):
                    targets = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom):
                    imported_module = node.module or ""
                    if node.level:
                        base_parts = module_name.split(".")[:-node.level]
                        imported_module = ".".join(base_parts + ([imported_module] if imported_module else []))
                    targets = [imported_module]
                for target in targets:
                    if str(target or "").startswith("backend.src.repositories"):
                        violations.append(f"{file_path}:{target}")

        self.assertEqual([], violations)


if __name__ == "__main__":
    unittest.main()
