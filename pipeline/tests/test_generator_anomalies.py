"""Generator contracts checked without treating simulation labels as detector input."""
import copy
import hashlib
import json
import math
import unittest
from pathlib import Path

from ais_pipeline.generator.anomalies import validate_scenarios
from ais_pipeline.generator.fleet import load_config, vessel_reports, vessel_events, vessel_truth
from ais_pipeline.generator.routing import Leg
from ais_pipeline.generator.world import haversine_km


CONFIG = Path(__file__).resolve().parents[1] / "config/fleet.yaml"


class AnomalyGeneratorTests(unittest.TestCase):
    def setUp(self):
        self.config = load_config(CONFIG)
        self.config.update(vessels=12, reports_per_vessel=90, dropout_percent=12,
                           duplicate_percent=30, lateness_percent=35)
        self.routes = [[Leg([(1.3, 51.1), (1.8, 51)], [0, 38]),
                        Leg([(1.8, 51), (1.3, 51.1)], [0, 38])]]

    def scenario(self, kind, **values):
        return dict(id="example", type=kind, vessels=[0], start_tick=10, end_tick=20, **values)

    def configured(self, scenario, **values):
        return {**self.config, "anomalies": [scenario], **values}

    def rows(self, config, index=0):
        return list(vessel_reports(index, config, self.routes))

    def test_disabled_output_matches_pre_anomaly_implementation(self):
        # Captured before refactoring vessel_reports, using these pinned fake legs,
        # config and datetime serializer. This does not compare code to itself.
        rows = [r for i in (0, 3, 7) for r in self.rows(self.config, i)]
        digest = hashlib.sha256(json.dumps(rows, default=str, sort_keys=True,
                                           separators=(",", ":")).encode()).hexdigest()
        self.assertEqual(len(rows), 292)
        self.assertEqual(digest, "f3123fc8d51e549ce39acf591013db1786e5d3b6427b695a3f2d92ef1e540235")
        self.assertEqual(self.rows(self.config), self.rows({**self.config, "anomalies": []}))

    def test_scenario_isolation_determinism_and_separate_truth(self):
        s = self.scenario("inland", land_bounds=[1.0, 51.2, 1.2, 51.4], jitter_metres=500)
        c = self.configured(s)
        self.assertEqual(self.rows(c), self.rows(c))
        # Added delivery metadata is observable; remove it for baseline equality.
        without_received = lambda rows: [{k: v for k, v in r.items() if k != "received_at"} for r in rows]
        self.assertEqual(self.rows(self.config, 3), without_received(self.rows(c, 3)))
        outside = lambda rows: [r for r in without_received(rows)
                                if not self.config["start_epoch"] + 600 <= r["event_time_s"] <= self.config["start_epoch"] + 1200]
        self.assertEqual(outside(self.rows(self.config)), outside(self.rows(c)))
        for row in self.rows(c):
            self.assertFalse({"scenario_id", "scenario_type", "vessel_index", "baseline_lon", "tick"} & row.keys())
            if self.config["start_epoch"] + 600 <= row["event_time_s"] <= self.config["start_epoch"] + 1200:
                self.assertTrue(1.0 < row["LON"] < 1.2 and 51.2 < row["LAT"] < 51.4)
        ledger = list(vessel_truth(0, c, self.routes))
        self.assertEqual(len(ledger), c["reports_per_vessel"])
        self.assertTrue(any(r["scenario_id"] == "example" for r in ledger))

    def test_gap_preserves_later_delivery_and_includes_missing_ticks(self):
        c = self.configured(self.scenario("gap"))
        baseline = self.rows(self.config)
        actual = [{k: v for k, v in r.items() if k != "received_at"} for r in self.rows(c)]
        expected = [r for r in baseline if not self.config["start_epoch"] + 600 <= r["event_time_s"] <= self.config["start_epoch"] + 1200]
        self.assertEqual(actual, expected)
        events = list(vessel_events(0, c, self.routes))
        for receptions, truth in events[10:21]:
            self.assertEqual(receptions, ())
            self.assertTrue(truth["scenario_dropped"])
            self.assertIsNone(truth["obs_id"])
        self.assertEqual(sum(len(rs) for rs, _ in events), len(actual))

    def test_duplicates_share_perturbed_observation_and_immutable_delivery(self):
        c = self.configured(self.scenario("future_time", offset_seconds=7200),
                            duplicate_percent=100, dropout_percent=0)
        events = list(vessel_events(0, c, self.routes))
        for receptions, truth in events:
            self.assertEqual(len(receptions), 2)
            a, b = receptions
            self.assertEqual({k: v for k, v in a.items() if k not in ("source", "reception_id")},
                             {k: v for k, v in b.items() if k not in ("source", "reception_id")})
            self.assertNotEqual(a["reception_id"], b["reception_id"])
            self.assertEqual(a["event_time_s"], int(a["BaseDateTime"].timestamp()))
            self.assertEqual(truth["reception_ids"], [a["reception_id"], b["reception_id"]])
            baseline_time = int(truth["baseline_time"].timestamp())
            if 10 <= truth["tick"] <= 20:
                self.assertEqual(a["event_time_s"] - baseline_time, 7200)
                self.assertLess(a["received_at"], a["BaseDateTime"])

    def test_circle_radius_timing_and_motion_modes(self):
        s = self.scenario("circle", center_lon=1.55, center_lat=50.95,
                          radius_metres=5000, period_seconds=720, motion_mode="constant",
                          reported_sog_knots=16)
        s.update(start_tick=0, end_tick=24)
        c = self.configured(s, duplicate_percent=0, dropout_percent=0)
        rows = self.rows(c)[:25]
        for row in rows:
            radius = haversine_km((1.55, 50.95), (row["LON"], row["LAT"])) * 1000
            self.assertAlmostEqual(radius, 5000, delta=0.02)
            self.assertEqual(row["SOG"], 16)
        step_knots = haversine_km((rows[0]["LON"], rows[0]["LAT"]),
                                  (rows[1]["LON"], rows[1]["LAT"])) * 1000 / 60 * 3600 / 1852
        self.assertTrue(83 < step_knots < 85)
        self.assertEqual((rows[0]["LON"], rows[0]["LAT"]), (rows[12]["LON"], rows[12]["LAT"]))
        consistent = copy.deepcopy(c)
        consistent["anomalies"][0]["motion_mode"] = "consistent"
        expected = 2 * math.pi * 5000 / 720 * 3600 / 1852
        self.assertAlmostEqual(self.rows(consistent)[0]["SOG"], expected)

    def test_frozen_jump_and_null_or_nonfinite_fields(self):
        c = self.configured(self.scenario("frozen"), duplicate_percent=0, dropout_percent=0)
        rows = self.rows(c)
        self.assertEqual(len({(r["LON"], r["LAT"]) for r in rows[10:21]}), 1)
        self.assertTrue(all(r["SOG"] == 16 for r in rows[10:21]))
        c["anomalies"] = [self.scenario("fields", values={"LAT": None, "LON": float("nan"), "Heading": 511})]
        rows = self.rows(c)
        self.assertIsNone(rows[10]["LAT"])
        self.assertTrue(math.isnan(rows[10]["LON"]))
        self.assertTrue(rows[10]["obs_id"])
        jump = self.scenario("jump", lon=2, lat=52)
        jump["end_tick"] = 10
        c["anomalies"] = [jump]
        rows = self.rows(c)
        self.assertEqual((rows[10]["LON"], rows[10]["LAT"]), (2, 52))
        self.assertNotEqual(rows[11]["LAT"], 52)

    def test_invalid_configuration_is_rejected(self):
        good = self.scenario("circle", center_lon=1, center_lat=51, radius_metres=5000, period_seconds=720)
        mutations = [dict(good, typo=True), dict(good, type="unknown"), dict(good, vessels=[999]),
                     dict(good, start_tick=True), dict(good, end_tick=1000), dict(good, clockwise="yes"),
                     dict(good, radius_metres=-1), dict(good, motion_mode="guess"), dict(good, vessels=[0, 0])]
        for scenario in mutations:
            with self.subTest(scenario=scenario), self.assertRaises(ValueError):
                validate_scenarios(self.configured(scenario))
        with self.assertRaises(ValueError):
            validate_scenarios({**self.config, "anomalies": [good, dict(good, id="overlap")]})
        with self.assertRaises(ValueError):
            validate_scenarios(self.configured(self.scenario("fields", values={"vessel_id": 42})))

    def test_example_config_loads(self):
        c = load_config(CONFIG.with_name("fleet-anomalies.yaml"))
        self.assertGreaterEqual(len(c["anomalies"]), 7)


if __name__ == "__main__":
    unittest.main()
