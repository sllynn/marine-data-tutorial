"""An enabled anomaly config must be refused before the runner resets outputs."""
import ast
from pathlib import Path
import unittest


class RunnerGuardTests(unittest.TestCase):
    def test_enabled_anomalies_never_reach_setup(self):
        path = Path(__file__).resolve().parents[1] / "ais_pipeline/runner.py"
        tree = ast.parse(path.read_text())
        run = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run")
        calls = []
        scope = {
            "load_config": lambda _: {"anomalies": [{"kind": "fields"}]},
            "setup": lambda *args: calls.append("reset"),
        }
        exec(compile(ast.Module(body=[run], type_ignores=[]), str(path), "exec"), scope)
        with self.assertRaisesRegex(ValueError, "No outputs were reset"):
            scope["run"](None, None, "catalog.schema", "config.yaml")
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
