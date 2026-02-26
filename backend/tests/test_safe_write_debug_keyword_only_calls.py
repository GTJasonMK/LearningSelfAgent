import ast
import unittest
from pathlib import Path


class TestSafeWriteDebugKeywordOnlyCalls(unittest.TestCase):
    def test_safe_write_debug_calls_use_keyword_only_contract(self):
        """
        回归：safe_write_debug/_safe_write_debug 的公共契约为
        (task_id, run_id, *, message=..., data=..., level=...)
        禁止第三个及以上位置参数，避免运行时 TypeError 打断主链路。
        """
        root = Path(__file__).resolve().parents[1] / "src"
        violations = []

        for path in root.rglob("*.py"):
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except Exception:
                continue

            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                name = None
                if isinstance(node.func, ast.Name):
                    name = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    name = node.func.attr
                if name in {"safe_write_debug", "_safe_write_debug"} and len(node.args) > 2:
                    rel = path.relative_to(root.parent)
                    violations.append(f"{rel}:{node.lineno} (positional_args={len(node.args)})")

        self.assertEqual(
            violations,
            [],
            "发现 safe_write_debug/_safe_write_debug 位置参数误用：\n" + "\n".join(violations),
        )


if __name__ == "__main__":
    unittest.main()
