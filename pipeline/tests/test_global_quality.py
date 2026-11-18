"""Worldwide regressions independent of Spark and of the Hawaiian tutorial fixture."""
import csv
import gzip
import json
import unittest
from pathlib import Path

import h3
import numpy as np
import pandas as pd
from pyproj import Geod
from shapely import Point, from_wkt

from ais_pipeline.global_windows import project_window, window_members_group

DATA = Path(__file__).resolve().parents[1] / "ais_pipeline/data"
GEOD = Geod(ellps="WGS84")


def records(name):
    csv.field_size_limit(64 * 1024 * 1024)
    with gzip.open(DATA / name, "rt") as stream:
        return list(csv.DictReader(stream))


class GlobalQualityTests(unittest.TestCase):
    def circle(self, lon, lat, radius=5000, period=720, sog=16):
        times = np.arange(25) * 60
        bearings = times / period * 360
        longitudes, latitudes, _ = GEOD.fwd(np.full(25, lon), np.full(25, lat), bearings, np.full(25, radius))
        return pd.DataFrame(dict(obs_id=[str(i) for i in times], MMSI="1", event_time_s=times,
                                 window_end_s=1500, LON=longitudes, LAT=latitudes, SOG=sog, sog_known=True))

    def test_fast_circles_in_both_hemispheres_poles_and_dateline(self):
        for lon, lat in ((-155, 20), (1.5, 51), (151, -34), (18, -34),
                         (-70, -54), (179.999, 0), (-179.999, -10),
                         (0, 89.99), (80, -89.99), (0, 90), (0, -90), (6, 45)):
            with self.subTest(lon=lon, lat=lat):
                result = window_members_group(self.circle(lon, lat))
                self.assertTrue(result.fast_circle.all())
                self.assertEqual(set(result.projection_status), {"projected"})
                self.assertLess(abs(result.radius_m.iloc[0] - 5000), 1)

    def test_slow_circles_and_frozen_controls_worldwide(self):
        for lon, lat in ((179.9999, 70), (-120, -50), (20, 89.99), (160, -89.99)):
            with self.subTest(lon=lon, lat=lat):
                slow = window_members_group(self.circle(lon, lat, radius=100, period=720, sog=0.5))
                self.assertTrue(slow.circle_candidate.all())
                self.assertFalse(slow.fast_circle.any())
                frozen = self.circle(lon, lat)
                frozen["LON"], frozen["LAT"] = lon, lat
                self.assertTrue(window_members_group(frozen).position_unchanged_despite_reported_speed.all())
                frozen["SOG"] = 0
                self.assertFalse(window_members_group(frozen).position_unchanged_despite_reported_speed.any())

    def test_distant_reports_are_preserved_without_a_local_fit(self):
        group = self.circle(0, 0)
        group.loc[12, ["LON", "LAT"]] = [179.99, 0]
        result = window_members_group(group)
        self.assertEqual(set(result.obs_id), set(group.obs_id))
        self.assertFalse(result.window_evaluable.any())
        self.assertFalse(result.fast_circle.any())
        self.assertEqual(set(result.projection_status), {"nonlocal_extent"})

    def test_projected_crs_is_selected_for_each_window(self):
        *_, north = project_window([30, 30.001], [89.9, 89.9])
        self.assertEqual(north, "projected")
        a = window_members_group(self.circle(179.999, 50))
        b = window_members_group(self.circle(-70, -50))
        self.assertNotEqual(a.projection_definition.iloc[0], b.projection_definition.iloc[0])


class GlobalLandTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.polygons = {int(r["polygon_id"]): from_wkt(r["wkt"]) for r in records("global_land.csv.gz")}
        cls.cells = {}
        for row in records("global_land_cells.csv.gz"):
            cls.cells.setdefault(int(row["cell"]), []).append(int(row["polygon_id"]))
        cls.coast = {int(r["cell"]): from_wkt(r["wkt"]) for r in records("global_coast_cells.csv.gz")}

    def inside(self, lon, lat):
        cell = int(h3.latlng_to_cell(lat, lon, 3), 16)
        return any(self.polygons[i].intersects(Point(lon, lat)) for i in self.cells.get(cell, []))

    def test_land_reference_covers_all_continents_and_polar_caps(self):
        for lon, lat in ((-100, 40), (-60, -10), (15, 0), (100, 40),
                         (135, -25), (0, -89), (40, -90), (-40, 80), (-155.5, 19.6),
                         (179.99, 66), (-179.99, 66)):
            with self.subTest(lon=lon, lat=lat):
                self.assertTrue(self.inside(lon, lat))
        for lon, lat in ((-150, 0), (0, 0), (179.999, 0), (-179.999, 0), (0, 90), (0, -60)):
            with self.subTest(lon=lon, lat=lat):
                self.assertFalse(self.inside(lon, lat))

    def test_coast_neighbours_cover_boundary_points_worldwide(self):
        # Independently query a reproducible sample of original source vertices.
        for polygon in list(self.polygons.values())[::37]:
            lon, lat = polygon.exterior.coords[len(polygon.exterior.coords) // 2]
            if abs(lon) == 180 or lat == -90:
                continue
            candidates = [self.coast[int(c, 16)] for c in h3.grid_disk(h3.latlng_to_cell(lat, lon, 3), 2)
                          if int(c, 16) in self.coast]
            self.assertTrue(candidates, (lon, lat))
            self.assertLess(min(c.distance(Point(lon, lat)) for c in candidates), 1e-5)

    def test_artificial_pole_and_dateline_cuts_are_not_coast(self):
        for geometry in self.coast.values():
            for line in geometry.geoms:
                for a, b in zip(line.coords, list(line.coords)[1:]):
                    self.assertFalse(abs(a[0]) == 180 and a[0] == b[0])
                    self.assertFalse(a[1] == b[1] == -90)
        # The real Antarctic coastline is far from the South Pole.
        neighbours = h3.grid_disk(h3.latlng_to_cell(-90, 40, 3), 2)
        self.assertFalse(any(int(c, 16) in self.coast for c in neighbours))

    def test_reference_files_are_checksummed_and_geographic(self):
        import hashlib
        manifest = json.loads((DATA / "global_land_source.json").read_text())
        for name, expected in manifest["files"].items():
            self.assertEqual(hashlib.sha256((DATA / name).read_bytes()).hexdigest(), expected)
        for geometry in self.polygons.values():
            self.assertTrue(geometry.is_valid)
            west, south, east, north = geometry.bounds
            self.assertTrue(-180 <= west <= east <= 180 and -90 <= south <= north <= 90)


if __name__ == "__main__":
    unittest.main()
