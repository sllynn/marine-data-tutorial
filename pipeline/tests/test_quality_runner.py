"""Reject invalid overrides and spatial references before the runner resets outputs."""
import ast
from pathlib import Path
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]


def function(name, module):
    tree = ast.parse((ROOT / "ais_pipeline" / module).read_text())
    return next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)


class QualityRunnerTests(unittest.TestCase):
    def settings(self, config):
        scope = {}
        exec(compile(ast.Module([function("settings", "quality.py")], type_ignores=[]), "quality.py", "exec"), scope)
        return scope["settings"](config)

    def test_default_reference_is_global(self):
        self.assertEqual(self.settings({}), {"land_reference": "global"})
        self.assertEqual(self.settings({"quality": {"land_reference": "none"}}), {"land_reference": "none"})

    def test_bad_reference_options_fail(self):
        for options in ({"projected_srid": 4326}, {"projected_srid": 32631},
                        {"land_wkt": "POLYGON EMPTY"}, {"land_reference": "hawaii"}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.settings({"quality": options})

    def test_override_revalidation_happens_before_reset(self):
        calls = []
        def reject(config):
            self.assertEqual(config["reports_per_vessel"], 5)
            raise ValueError("scenario outside overridden interval")
        scope = dict(load_config=lambda _: {"vessels": 12, "reports_per_vessel": 60},
                     validate_scenarios=reject, setup=lambda *args: calls.append("reset"))
        exec(compile(ast.Module([function("run", "runner.py")], type_ignores=[]), "runner.py", "exec"), scope)
        with self.assertRaisesRegex(ValueError, "scenario outside"):
            scope["run"](None, None, "catalog.schema", "config.yaml", reports=5)
        self.assertEqual(calls, [])

    def test_bad_quality_config_happens_before_reset(self):
        calls = []
        def reject(config):
            raise ValueError("bad projection")
        scope = dict(load_config=lambda _: {"vessels": 12, "reports_per_vessel": 60},
                     validate_scenarios=lambda _: None,
                     quality=SimpleNamespace(settings=reject),
                     setup=lambda *args: calls.append("reset"))
        exec(compile(ast.Module([function("run", "runner.py")], type_ignores=[]), "runner.py", "exec"), scope)
        with self.assertRaisesRegex(ValueError, "bad projection"):
            scope["run"](None, None, "catalog.schema", "config.yaml")
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
