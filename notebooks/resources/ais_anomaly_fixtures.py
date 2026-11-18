# Databricks notebook source
# MAGIC %md
# MAGIC # Prepare the AIS anomaly tutorial data
# MAGIC Run this helper from notebook 00 with `%run`. It uses that notebook's catalog,
# MAGIC schema and coastline path to prepare synthetic reports. It overwrites only
# MAGIC `fixture_receptions`, which contains all reports for the batch analysis.

# COMMAND ----------

# FIGURE_FIXTURE_START
import hashlib
import json
import math
from datetime import datetime, timezone

FIXTURE_START_S = int(datetime(2026, 11, 1, tzinfo=timezone.utc).timestamp())
UTM_SRID = 32605  # WGS 84 / UTM zone 5N, covering the island of Hawaiʻi.
FIXTURE_EASTING_M = 150000.0
FIXTURE_NORTHING_M = 2175000.0
COASTAL_TOLERANCE_M = 150.0
COVERAGE_WKT = "POLYGON ((130000 2070000,330000 2070000,330000 2260000,130000 2260000,130000 2070000))"
# One point on the bundled, millimetre-rounded coastline, near Kailua-Kona.
COAST_EASTING_M = 181207.288
COAST_NORTHING_M = 2178784.553


def fixture_observations():
    """Build synthetic UTM positions; Spatial SQL converts them to AIS longitude/latitude."""
    records, expectations = [], {}

    def add(label, mmsi, minute, x, y, sog=16.0, **overrides):
        event_s = FIXTURE_START_S + int(minute * 60)
        row = dict(MMSI=str(mmsi), BaseDateTime=datetime.fromtimestamp(event_s, timezone.utc),
                   x_m=FIXTURE_EASTING_M + x, y_m=FIXTURE_NORTHING_M + y,
                   LON=None, LAT=None, SOG=sog, COG=90.0, Heading=90.0,
                   VesselName=None, IMO=None, CallSign=None, VesselType=70, Status=0,
                   Length=150.0, Width=24.0, Draft=7.5, Cargo=70, TransceiverClass="A",
                   source="receiver-1", received_time_s=FIXTURE_START_S + 35 * 60)
        row.update(overrides)
        key = [row["MMSI"], str(row["BaseDateTime"]), row["x_m"], row["y_m"], row["LON"], row["LAT"]]
        row["obs_id"] = hashlib.sha256(json.dumps(key, allow_nan=True).encode()).hexdigest()[:20]
        row["reception_id"] = row["obs_id"] + "-1"
        records.append(row)
        expectations[row["obs_id"]] = dict(label=label)
        return row

    for minute in range(25):
        angle = 2 * math.pi * minute / 12
        add("fast_circle", 235000001, minute, -5000 + 5000 * math.cos(angle),
            5000 * math.sin(angle), 16.0)
    for minute in range(31):
        angle = 2 * math.pi * minute / 30
        add("slow_circle", 235000002, minute, 9000 + 100 * math.cos(angle),
            100 * math.sin(angle), 2 * math.pi * 100 / (30 * 60) * 3600 / 1852)
    for minute in range(0, 31, 5):
        add("ordinary_water", 235000003, minute, 500 + minute * 10, -3500, 0.4)
        add("moored", 235000004, minute, 1000, 4000, 0.0)
        add("position_unchanged_despite_reported_speed", 235000005, minute, 1200, 4500, 16.0)
    # These positions are measured against the bundled Hawaiʻi coastline.
    for label, easting, northing in [
        ("deep_inland", 240000, 2180000),
        ("nearshore", COAST_EASTING_M - 50, COAST_NORTHING_M),
        ("boundary", COAST_EASTING_M, COAST_NORTHING_M),
        ("offshore_water", 170000, 2180000),
        ("coastal_water", COAST_EASTING_M - 1000, COAST_NORTHING_M),
        ("outside_mask", 400000, 2175000),
    ]:
        add(label, 235000010 + len(records), 5,
            easting - FIXTURE_EASTING_M, northing - FIXTURE_NORTHING_M, 0.0)
    add("jump_before", 235000006, 0, 0, -4500)
    add("isolated_jump", 235000006, 10, 20000, -4500)
    add("jump_after", 235000006, 20, 400, -4500)
    add("unavailable_position", 235000008, 5, 0, 0, LAT=91.0, LON=181.0)
    add("nonfinite_position", 235000009, 5, 0, 0, LON=float("nan"))
    add("future_time", 235000011, 10000, 0, 0)
    add("missing_motion", 235000012, 5, 0, 0, sog=None, COG=360.0, Heading=511.0)
    add("null_timestamp", 235000013, 5, 0, 0, BaseDateTime=None)
    duplicate = {**records[0], "source": "receiver-2", "reception_id": records[0]["obs_id"] + "-2"}
    records.append(duplicate)
    return records, expectations
# FIGURE_FIXTURE_END

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {PREFIX}")
RAW_SCHEMA = """MMSI string, BaseDateTime timestamp, LON double, LAT double,
SOG double, COG double, Heading double, VesselName string, IMO string, CallSign string,
VesselType int, Status int, Length double, Width double, Draft double, Cargo int,
TransceiverClass string, source string, received_time_s long,
obs_id string, reception_id string"""

# COMMAND ----------

import csv

if not COASTLINE_CSV.is_file():
    raise FileNotFoundError(
        f"Missing {COASTLINE_CSV}. Copy notebooks/data/hawaii_land_utm.csv beside this "
        "notebook in a data/ folder, or set coastline_csv to its Workspace/Volume path.")
with COASTLINE_CSV.open(newline="") as stream:
    coastline_rows = list(csv.DictReader(stream))
if len(coastline_rows) != 1 or int(coastline_rows[0]["srid"]) != UTM_SRID:
    raise ValueError("Expected one Hawaiʻi land polygon in EPSG:32605.")
LAND_WKT = coastline_rows[0]["wkt"]
land_info = spark.sql(
    "SELECT ST_IsValid(ST_GeomFromText(:wkt,32605)) AS valid, "
    "ST_AsGeoJSON(ST_GeomFromText(:wkt,32605)) AS geojson", args={"wkt": LAND_WKT}).first()
assert land_info.valid
LAND_PLOT_RINGS = json.loads(land_info.geojson)["coordinates"]

FIXTURE_POSITIONS, EXPECTATIONS = fixture_observations()
# LON/LAT are normally null in the UTM fixture; explicit values inject invalid reports.
# The generated projected coordinates never enter the AIS input table or its detectors.
fixture_frame = spark.createDataFrame(FIXTURE_POSITIONS, RAW_SCHEMA + ", x_m double, y_m double")
fixture_frame.createOrReplaceTempView("quality_fixture_utm")
geographic = spark.sql(f"""
  SELECT *, ST_Transform(ST_SetSRID(ST_Point(x_m,y_m),{UTM_SRID}),4326) AS geographic_point
  FROM quality_fixture_utm
""")
reported = (geographic.withColumn("LON", F.coalesce("LON", F.expr("ST_X(geographic_point)")))
            .withColumn("LAT", F.coalesce("LAT", F.expr("ST_Y(geographic_point)"))))
reported.select(*spark.createDataFrame([], RAW_SCHEMA).columns).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    table("fixture_receptions"))
FIXTURE_BY_ID = {r["obs_id"]: r for r in FIXTURE_POSITIONS}
LABEL_IDS = {}
for obs_id, truth in EXPECTATIONS.items():
    LABEL_IDS.setdefault(truth["label"], []).append(obs_id)
print(f"{len(FIXTURE_POSITIONS)} receptions; {len(EXPECTATIONS)} distinct observations")

