"""Numeric/fixture checks extracted from the teaching notebook without starting Spark.

Development validation separately checks native SQL and saved batch results on Databricks.
"""
import ast
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
from pyproj import Transformer

TO_GEOGRAPHIC = Transformer.from_crs(32605, 4326, always_xy=True)

NOTEBOOK = Path(__file__).resolve().parents[2] / "notebooks/00_position_quality_and_anomalies.py"
FIXTURE_NOTEBOOK = NOTEBOOK.parent / "resources/ais_anomaly_fixtures.py"
PURE_FUNCTIONS = {
    "fixture_observations",
    "fit_circle", "window_members_group",
}
CONSTANTS = {
    "FIXTURE_START_S", "UTM_SRID", "FIXTURE_EASTING_M", "FIXTURE_NORTHING_M",
    "COAST_EASTING_M", "COAST_NORTHING_M", "COASTAL_TOLERANCE_M",
    "COVERAGE_WKT", "MAX_EPISODE_S",
    "WINDOW_STEP_S", "FUTURE_TOLERANCE_S", "MAX_MOTION_KNOTS", "MIN_WINDOW_S",
    "WINDOW_MEMBER_SCHEMA", "WINDOW_MEMBER_COLUMNS",
}


def load_pure_namespace():
    nodes = []
    for source in (FIXTURE_NOTEBOOK, NOTEBOOK):
        for node in ast.parse(source.read_text()).body:
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                names = [a.name for a in node.names] if isinstance(node, ast.Import) else [node.module]
                if all(n in {"numpy", "pandas", "hashlib", "json", "math", "datetime", "pyproj", "circle_fit"} for n in names):
                    nodes.append(node)
            elif isinstance(node, ast.Assign):
                targets = {n.id for target in node.targets for n in ast.walk(target) if isinstance(n, ast.Name)}
                if targets and targets <= CONSTANTS:
                    nodes.append(node)
            elif isinstance(node, ast.FunctionDef) and node.name in PURE_FUNCTIONS:
                nodes.append(node)
    namespace = {}
    exec(compile(ast.Module(nodes, type_ignores=[]), str(NOTEBOOK), "exec"), namespace)
    return namespace


class QualityNotebookTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.n = load_pure_namespace()
        cls.rows, cls.truth = cls.n["fixture_observations"]()
        # Independent PROJ reference for local numeric tests. The notebook uses Spatial SQL.
        for row in cls.rows:
            lon, lat = TO_GEOGRAPHIC.transform(row["x_m"], row["y_m"])
            if row["LON"] is None:
                row["LON"] = lon
            if row["LAT"] is None:
                row["LAT"] = lat

    def records(self, label):
        distinct = {r["obs_id"]: r for r in self.rows if self.truth[r["obs_id"]]["label"] == label}
        return sorted(distinct.values(), key=lambda r: r["BaseDateTime"])

    def fit(self, rows):
        xy = np.array([[r["x_m"], r["y_m"]] for r in rows])
        return self.n["fit_circle"](xy[:, 0], xy[:, 1],
                                    [r["BaseDateTime"].timestamp() for r in rows],
                                    [r["LON"] for r in rows], [r["LAT"] for r in rows])

    def test_fast_circle_geometry_and_units_independent_of_reported_speed(self):
        fast = self.records("fast_circle")
        result = self.fit(fast)
        self.assertTrue(result["fast_circle"])
        self.assertAlmostEqual(result["radius_m"], 5000.0, places=5)
        self.assertGreater(result["median_implied_knots"], 83)
        self.assertLess(result["median_implied_knots"], 85)
        self.assertEqual(result, self.fit([{**r, "SOG": None} for r in fast]))

    def test_connected_scenarios_remain_detectable_amid_baseline_reports(self):
        from ais_pipeline.generator.fleet import load_config, vessel_reports
        from ais_pipeline.generator.routing import Leg
        config = load_config(NOTEBOOK.parents[1] / "pipeline/config/fleet-anomalies.yaml")
        routes = [[Leg([(1.3, 51.1), (1.8, 51)], [0, 38]),
                   Leg([(1.8, 51), (1.3, 51.1)], [0, 38])]]
        project = Transformer.from_crs(4326, config["quality"]["projected_srid"], always_xy=True)
        for vessel, column in ((2, "fast_circle"), (4, "position_unchanged_despite_reported_speed")):
            with self.subTest(vessel=vessel):
                frame = pd.DataFrame(vessel_reports(vessel, config, routes)).drop_duplicates("obs_id")
                frame["x_m"], frame["y_m"] = project.transform(frame.LON, frame.LAT)
                frame["sog_known"] = True
                members = self.n["window_members_group"](frame)
                self.assertTrue(members[column].any(), f"No full detector window for vessel {vessel}")

    def test_slow_circle_is_geometric_candidate_not_fast(self):
        result = self.fit(self.records("slow_circle"))
        self.assertTrue(result["circle_candidate"])
        self.assertFalse(result["fast_circle"])
        self.assertAlmostEqual(result["radius_m"], 100.0, places=5)
        self.assertGreater(result["median_implied_knots"], 0.67)
        self.assertLess(result["median_implied_knots"], 0.69)

    def test_partial_arc_and_degenerate_line_are_not_a_circle_finding(self):
        angle = np.linspace(0, np.pi, 15)
        t = np.arange(15) * 60
        x, y = 5000 * np.cos(angle), 5000 * np.sin(angle)
        ll = np.array([TO_GEOGRAPHIC.transform(a + self.n["FIXTURE_EASTING_M"], b + self.n["FIXTURE_NORTHING_M"]) for a, b in zip(x, y)])
        result = self.n["fit_circle"](x, y, t, ll[:, 0], ll[:, 1])
        self.assertFalse(result["circle_candidate"])
        line = np.arange(15) * 100.0
        result = self.n["fit_circle"](line, line * 2, t, line * 0, line * 0 + 51)
        self.assertEqual(result["fit_status"], "not_assessed")

    def test_noisy_clockwise_circle_retains_evidence(self):
        rng = np.random.default_rng(42)
        angle = np.linspace(1.2, 1.2 - 4 * np.pi, 25)
        x = 3000 + 5000 * np.cos(angle) + rng.normal(0, 5, len(angle))
        y = -2000 + 5000 * np.sin(angle) + rng.normal(0, 5, len(angle))
        ll = np.array([TO_GEOGRAPHIC.transform(a + self.n["FIXTURE_EASTING_M"], b + self.n["FIXTURE_NORTHING_M"]) for a, b in zip(x, y)])
        result = self.n["fit_circle"](x, y, np.arange(25) * 60, ll[:, 0], ll[:, 1])
        self.assertTrue(result["fast_circle"])
        self.assertGreater(result["angular_consistency"], 0.99)

    def frame(self, label):
        rows = self.records(label)
        frame = pd.DataFrame(rows)
        xy = [(r["x_m"], r["y_m"]) for r in rows]
        frame["x_m"], frame["y_m"] = zip(*xy)
        frame["event_time_s"] = [int(r["BaseDateTime"].timestamp()) for r in rows]
        frame["sog_known"] = True
        return frame

    def test_window_membership_is_row_order_invariant(self):
        frame = self.frame("fast_circle")
        calculate = self.n["window_members_group"]
        a = calculate(frame).sort_values(["window_id", "obs_id"]).reset_index(drop=True)
        b = calculate(frame.sample(frac=1, random_state=7)).sort_values(["window_id", "obs_id"]).reset_index(drop=True)
        pd.testing.assert_frame_equal(a, b)
        self.assertTrue(a.fast_circle.any())
        self.assertFalse(a.duplicated(["window_id", "obs_id"]).any())
        self.assertTrue(((a.window_end_s - a.window_start_s) == 30 * 60).all())

    def test_last_report_on_window_boundary_is_included(self):
        frame = self.frame("slow_circle")
        result = self.n["window_members_group"](frame)
        self.assertEqual(set(result.obs_id), set(frame.obs_id))
        last_id = frame.sort_values("event_time_s").iloc[-1].obs_id
        self.assertTrue((result.loc[result.obs_id == last_id, "window_end_s"] >
                         frame.event_time_s.max()).all())

    def test_repeated_timestamps_leave_circle_unassessed(self):
        frame = self.frame("fast_circle")
        frame.loc[1, "event_time_s"] = frame.loc[0, "event_time_s"]
        result = self.n["fit_circle"](
            frame.x_m, frame.y_m, frame.event_time_s, frame.LON, frame.LAT)
        self.assertEqual(result["fit_status"], "not_assessed")
        self.assertFalse(result["fast_circle"])

    def test_unchanged_position_check_does_not_flag_moored_control(self):
        calculate = self.n["window_members_group"]
        self.assertTrue(calculate(self.frame("position_unchanged_despite_reported_speed")).position_unchanged_despite_reported_speed.any())
        self.assertFalse(calculate(self.frame("moored")).position_unchanged_despite_reported_speed.any())

    def test_fixture_truth_is_separate_and_receiver_copies_consistent(self):
        rows, truth = self.n["fixture_observations"]()
        self.assertEqual(len(truth), len({r["obs_id"] for r in rows}))
        self.assertEqual(len(rows), len({r["reception_id"] for r in rows}))
        self.assertTrue(all("label" not in r and "vessel_id" not in r and "scenario_id" not in r for r in rows))
        copies = [r for r in rows if r["obs_id"] == rows[0]["obs_id"]]
        self.assertEqual(len(copies), 2)
        self.assertEqual({r["source"] for r in copies}, {"receiver-1", "receiver-2"})
        self.assertEqual(copies[0]["LON"], copies[1]["LON"])
        self.assertEqual(copies[0]["LAT"], copies[1]["LAT"])

    def test_source_cells_do_not_hide_python_or_validation_in_markdown(self):
        # A SOURCE notebook can import successfully while treating code following %md
        # as prose. Guard the execution boundaries and keep job exits out of the tutorial.
        code_cells = []
        for source in (FIXTURE_NOTEBOOK, NOTEBOOK):
            for cell in source.read_text().split("# COMMAND ----------"):
                if "# MAGIC %md" in cell:
                    statements = [line for line in cell.splitlines()
                                  if line.strip() and not line.startswith("#")]
                    self.assertEqual(statements, [])
                else:
                    code_cells.append(cell)
        self.assertFalse(any("dbutils.notebook.exit(" in c for c in code_cells))

    def test_numeric_detectors_do_not_read_fixture_truth(self):
        for name in ("fit_circle", "window_members_group"):
            function = next(n for n in ast.parse(NOTEBOOK.read_text()).body
                            if isinstance(n, ast.FunctionDef) and n.name == name)
            names = {n.id for n in ast.walk(function) if isinstance(n, ast.Name)}
            self.assertFalse(names & {"EXPECTATIONS", "LABEL_IDS", "RECEPTIONS"})


if __name__ == "__main__":
    unittest.main()
