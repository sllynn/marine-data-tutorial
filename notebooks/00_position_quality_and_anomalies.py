# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "circle-fit==0.2.1",
#   "pyproj==3.7.2",
# ]
# ///
# MAGIC %md
# MAGIC # Checking AIS positions for errors and unusual movement
# MAGIC
# MAGIC ## What are we computing?
# MAGIC We check AIS coordinates, timestamps and movement before building vessel tracks.
# MAGIC The checks flag positions on land, isolated jumps, fast circular tracks and repeated
# MAGIC positions with inconsistent speed reports. We keep the original reports alongside
# MAGIC the results of each check.
# MAGIC
# MAGIC ## What data do we use?
# MAGIC A helper notebook prepares a small test dataset with known errors and normal vessel movement
# MAGIC for comparison. It uses a bundled outline of the island of Hawaiʻi,
# MAGIC fast and slow circles, a jump, missing values and a moored vessel.
# MAGIC Some reports are received through more than one receiver.
# MAGIC We assume all reports for the period have arrived and analyse them in one batch.
# MAGIC The expected results are stored separately and used to check the output.
# MAGIC Copy the bundled `data/hawaii_land_utm.csv` alongside this notebook.
# MAGIC The first code cell installs `circle-fit==0.2.1` and `pyproj==3.7.2`.
# MAGIC
# MAGIC ## How will we compute it?
# MAGIC 1. Check coordinates and timestamps before using them in calculations.
# MAGIC 2. Use spatial SQL to check for positions on land and calculate speed between reports.
# MAGIC 3. Fit circles to reports in overlapping time windows using grouped Pandas tasks.
# MAGIC 4. Save the observations with quality labels and reasons for downstream analysis.
# MAGIC
# MAGIC ## What does the output look like?
# MAGIC `quality_observations` stores one row per observation, with measurements and a result
# MAGIC for each rule: `flagged`, `passed` or `not_assessed`. We display the circle-window
# MAGIC results and the observations used in each window without saving additional tables.
# MAGIC An observation can appear in several overlapping windows, so several flagged windows
# MAGIC may describe the same period of movement.
# MAGIC Every observation remains in the results table. `input_evaluable` indicates whether
# MAGIC its coordinates and timestamp can be used in spatial and movement calculations.
# MAGIC `quality_status`, `flags` and `rule_results` describe the findings for downstream users.
# MAGIC
# MAGIC **Run all overwrites this example's two tables:** `fixture_receptions` and
# MAGIC `quality_observations`. Use a dedicated schema and run one copy at a time in that schema.
# MAGIC Select serverless environment 6 and enter your catalog.
# MAGIC
# MAGIC ## What's next?
# MAGIC
# MAGIC * Notebook 1 explores how to separate vessel tracks when different vessels report the same MMSI, and matches those tracks to vessel registry records.
# MAGIC * Notebook 2 calculates port and berth visits. Each notebook runs independently.

# COMMAND ----------

# MAGIC %pip install circle-fit==0.2.1 pyproj==3.7.2

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd
from pyproj import Geod
from circle_fit import taubinSVD
from pyspark.sql import functions as F

MAX_EPISODE_S = 30 * 60  # Length of each circle-fitting window, in seconds.
WINDOW_STEP_S = 5 * 60  # Start a new, overlapping window every five minutes.
FUTURE_TOLERANCE_S = 2 * 60  # Example clock-skew allowance; requires trusted reception metadata.
MAX_MOTION_KNOTS = 50.0  # Implied-speed threshold for isolated jumps and fast circles.
MIN_WINDOW_S = 12 * 60  # Minimum observation span for circle and unchanged-position checks.

spark.conf.set("spark.sql.session.timeZone", "UTC")  # Interpret and display timestamps in UTC.
# The CSV supplies the land geometry used for the land checks.
dbutils.widgets.text("coastline_csv", "data/hawaii_land_utm.csv", "Bundled coastline CSV path")
# Results are written to two tables in this catalog and dedicated schema.
dbutils.widgets.text("catalog", "", "Catalog (required)")
dbutils.widgets.text("schema", "ais_anomalies_tutorial", "Dedicated tutorial schema")

# COMMAND ----------

COASTLINE_CSV = Path(dbutils.widgets.get("coastline_csv")).resolve()
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
if not CATALOG or not SCHEMA:
    raise ValueError("Set an explicit catalog and dedicated tutorial schema before Run all.")
if any("`" in name or "/" in name for name in (CATALOG, SCHEMA)):
    raise ValueError("Catalog/schema must not contain backticks or slashes.")
PREFIX = f"`{CATALOG}`.`{SCHEMA}`"

def table(name):
    return f"{PREFIX}.`{name}`"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understand the input fields and test data
# MAGIC Coordinates are WGS 84 longitude and latitude in degrees. `BaseDateTime` is the reported
# MAGIC observation time in UTC. `received_time_s` is reception metadata added by our fixture
# MAGIC generator; it is not transmitted in an AIS position report. Some providers supply a
# MAGIC reception timestamp, but many AIS datasets do not include one. This example assumes
# MAGIC that timestamp is trustworthy and allows two minutes for clock differences when checking
# MAGIC for future-dated observations. Adapt or omit that check if your feed lacks this metadata.
# MAGIC The synthetic example starts at 00:00 UTC on 1 November 2026. `obs_id` identifies an observation;
# MAGIC `reception_id` distinguishes copies received from different receivers.
# MAGIC The helper writes the prepared reports to `fixture_receptions`. The table below contains
# MAGIC all reports for the analysis period, ready to process in one batch.
# MAGIC Expected results are kept separately and used only in assertions and figure labels.
# MAGIC
# MAGIC The companion CSV contains the island of Hawaiʻi from [Natural Earth's public-domain
# MAGIC 1:10m land dataset](https://www.naturalearthdata.com/downloads/10m-physical-vectors/10m-land/),
# MAGIC stored as WKT in **WGS 84 / UTM zone 5N (EPSG:32605)**. Easting and northing are in metres.
# MAGIC We create synthetic report positions in that CRS and use `ST_Transform` to convert them
# MAGIC to WGS 84 longitude/latitude (EPSG:4326). The analysis transforms those reported coordinates
# MAGIC back to UTM for land distances and circle fitting. `ST_SetSRID` declares the source CRS;
# MAGIC `ST_Transform` changes the coordinates. In EPSG:4326 we use X=longitude, Y=latitude.
# MAGIC
# MAGIC The land outline is generalised cartography. The 150 m coastal band is a teaching threshold,
# MAGIC not an estimate of its accuracy. Detailed harbour, inland-water or navigation work needs
# MAGIC a more suitable land dataset. This example covers only the island of Hawaiʻi and a defined
# MAGIC surrounding rectangle; positions outside that rectangle remain unassessed by the land check.
# MAGIC Source, licence and reproduction instructions are in `data/README.md`.

# COMMAND ----------

# MAGIC %run ./resources/ais_anomaly_fixtures

# COMMAND ----------

spark.table(table("fixture_receptions")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Check coordinates, timestamps and optional fields
# MAGIC For this feed, longitude 181° and latitude 91° mean the position is unavailable.
# MAGIC SOG 102.3, COG 360 and heading 511 also mean unavailable; some providers convert these
# MAGIC values to null. A position can still be usable when its speed, course or heading is missing.
# MAGIC SOG 102.2 means at least that speed, so it cannot be treated as an exact measurement.
# MAGIC `(0,0)` is a valid coordinate. Check how your provider represents each field.
# MAGIC
# MAGIC We compare timestamps with reception time and exclude invalid inputs from spatial and
# MAGIC movement calculations. All observations remain available in the saved results.

# COMMAND ----------

def field_checks(receptions):
    # A position fingerprint can be repeated with disagreeing motion attributes.
    # Keep all receptions in the input table, choose a representative consistently, and flag conflicts.
    identity = F.to_json(F.struct("MMSI", "BaseDateTime", "LON", "LAT"))
    attributes = F.to_json(F.struct("SOG", "COG", "Heading"))
    representative = F.struct(
        "MMSI", "BaseDateTime", "LON", "LAT", "SOG", "COG", "Heading"
    )
    grouped = (
        receptions.groupBy("obs_id")
        .agg(
            F.min(representative).alias("reported"),
            F.min("received_time_s").alias("first_received_s"),
            F.sort_array(F.collect_set("source")).alias("sources"),
            (F.countDistinct(identity) > 1).alias("identity_payload_conflict"),
            (F.countDistinct(attributes) > 1).alias("motion_payload_conflict"),
        )
        .select(
            "obs_id",
            "reported.*",
            "first_received_s",
            "sources",
            "identity_payload_conflict",
            "motion_payload_conflict",
        )
    )

    check_results = (
        grouped.withColumn("event_time_s", F.unix_timestamp("BaseDateTime"))
        .withColumn(
            "coordinate_valid",
            F.coalesce(
                F.col("LON").between(-180, 180)
                & F.col("LAT").between(-90, 90)
                & ~F.isnan("LON")
                & ~F.isnan("LAT"),
                F.lit(False),
            ),
        )
        .withColumn(
            "coordinate_unavailable",
            F.coalesce(
                F.col("LON").isNull()
                | F.col("LAT").isNull()
                | (F.col("LON") == 181)
                | (F.col("LAT") == 91),
                F.lit(False),
            ),
        )
        .withColumn(
            "time_valid",
            F.coalesce(
                F.col("event_time_s").isNotNull()
                & F.col("first_received_s").isNotNull()
                & (
                    F.col("event_time_s")
                    <= F.col("first_received_s") + FUTURE_TOLERANCE_S
                ),
                F.lit(False),
            ),
        )
        .withColumn(
            "input_evaluable",
            F.col("coordinate_valid")
            & F.col("time_valid")
            & ~F.col("identity_payload_conflict"),
        )
        .withColumn(
            "sog_known",
            F.coalesce(
                F.col("SOG").between(0, 102.2)
                & ~F.isnan("SOG")
                & ~F.col("motion_payload_conflict"),
                F.lit(False),
            ),
        )
        .withColumn(
            "cog_known",
            F.coalesce(
                (F.col("COG") >= 0)
                & (F.col("COG") < 360)
                & ~F.isnan("COG")
                & ~F.col("motion_payload_conflict"),
                F.lit(False),
            ),
        )
        .withColumn(
            "heading_known",
            F.coalesce(
                (F.col("Heading") >= 0)
                & (F.col("Heading") < 360)
                & ~F.isnan("Heading")
                & ~F.col("motion_payload_conflict"),
                F.lit(False),
            ),
        )
    )

    return check_results

inputs = spark.table(table("fixture_receptions")).transform(field_checks)

display(
    inputs.select(
        "obs_id",
        "MMSI",
        "BaseDateTime",
        "sources",
        "coordinate_valid",
        "time_valid",
        "sog_known",
        "cog_known",
        "heading_known",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Check for positions on land and isolated jumps
# MAGIC ### Check positions against land
# MAGIC We flag a position as inland when it is inside the land polygon and more than 150 m
# MAGIC from the boundary. Positions within 150 m on either side of the boundary are treated as
# MAGIC uncertain. Both the land polygon and the projected points use EPSG:32605.
# MAGIC We construct each WGS 84 point once and project it for the land checks and circle fitting.

# COMMAND ----------

def land_checks(inputs):
    inputs.createOrReplaceTempView("quality_checked_inputs")
    return spark.sql(f"""
      WITH positions AS (
        SELECT *, ST_SetSRID(ST_Point(LON, LAT), 4326) AS geo_point
        FROM quality_checked_inputs
        WHERE input_evaluable
      ), projected AS (
        SELECT *, ST_Transform(geo_point, {UTM_SRID}) AS local_point,
               ST_GeomFromText('{LAND_WKT}', {UTM_SRID}) AS land,
               ST_GeomFromText('{COVERAGE_WKT}', {UTM_SRID}) AS coverage
        FROM positions
      )
      SELECT obs_id, MMSI, event_time_s, geo_point,
             ST_X(local_point) AS x_m, ST_Y(local_point) AS y_m,
             ST_Intersects(coverage, local_point) AS mask_covered,
             ST_Contains(land, local_point) AS inside_land,
             ST_Distance(ST_Boundary(land), local_point) AS shoreline_distance_m
      FROM projected
    """)

land_positions = inputs.transform(land_checks)
display(land_positions.select("obs_id", "inside_land", "shoreline_distance_m", "mask_covered"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare movement between neighbouring reports
# MAGIC We calculate implied speed by dividing distance by elapsed observation time and
# MAGIC converting metres per second to knots. Here we use `ST_DistanceSpheroid` on the WGS 84
# MAGIC ellipsoid. Section 4 uses `pyproj.Geod.inv` for the equivalent calculation inside Pandas.
# MAGIC An isolated jump requires excessive speed from both neighbours, while travelling
# MAGIC directly between those neighbours would be plausible.
# MAGIC
# MAGIC Each neighbour is a structure containing its point and timestamp.
# MAGIC Ordering by time means elapsed times cannot be negative. `try_divide` returns null
# MAGIC for a zero time difference; missing neighbours also produce null speeds.
# MAGIC The isolated-jump check remains unassessed when any of its three speeds is null.

# COMMAND ----------

def motion_checks(positions):
    positions.createOrReplaceTempView("land_checked_positions")
    return spark.sql(f"""
      WITH neighbours AS (
        SELECT *,
          lag(named_struct('point', geo_point, 'time_s', event_time_s)) OVER w AS prev,
          lead(named_struct('point', geo_point, 'time_s', event_time_s)) OVER w AS next
        FROM land_checked_positions
        WINDOW w AS (PARTITION BY MMSI ORDER BY event_time_s, obs_id)
      ), speeds AS (
        SELECT *,
          try_divide(ST_DistanceSpheroid(prev.point, geo_point),
                     event_time_s - prev.time_s) * 3600 / 1852 AS prev_knots,
          try_divide(ST_DistanceSpheroid(geo_point, next.point),
                     next.time_s - event_time_s) * 3600 / 1852 AS next_knots,
          try_divide(ST_DistanceSpheroid(prev.point, next.point),
                     next.time_s - prev.time_s) * 3600 / 1852 AS bypass_knots
        FROM neighbours
      )
      SELECT obs_id, x_m, y_m, mask_covered, inside_land, shoreline_distance_m,
        prev_knots, next_knots, bypass_knots,
        CASE WHEN prev_knots IS NULL OR next_knots IS NULL
               OR bypass_knots IS NULL THEN NULL ELSE
          prev_knots > {MAX_MOTION_KNOTS} AND next_knots > {MAX_MOTION_KNOTS}
          AND bypass_knots <= {MAX_MOTION_KNOTS} END AS isolated_jump
      FROM speeds
    """)

metrics = land_positions.transform(motion_checks)
display(metrics.select("obs_id",
                       "prev_knots", "next_knots", "bypass_knots", "isolated_jump"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Detect circular movement over 30-minute windows
# MAGIC The fast-circle example has a radius of 5 km and completes a rotation every 12 minutes,
# MAGIC with reports one minute apart. It reports SOG of 16 knots, but the distances between
# MAGIC positions imply about 84 knots. The comparison circle has a radius of 100 m and takes
# MAGIC 30 minutes to complete a rotation. Its reported and calculated speeds are about 0.68 knots.
# MAGIC We chose both examples to test the code; their dimensions do not come from real incidents.
# MAGIC
# MAGIC We use `circle_fit.taubinSVD` to fit a circle to the UTM coordinates in each
# MAGIC 30-minute window, starting a new window every 5 minutes. It returns the centre,
# MAGIC radius and RMS radial error. We calculate speed separately from the reports and timestamps.
# MAGIC Each grouped Pandas task calculates the radius, fit error, angular coverage, direction
# MAGIC of movement and median implied speed. A geometric circle candidate needs at least
# MAGIC 12 reports spanning 12 minutes, 270° of coverage, consistent movement around the circle
# MAGIC and a fit error below 10% of its radius. Collinear or poorly conditioned samples cannot
# MAGIC be fitted reliably. A circle with a radius over 2 km and median implied speed over
# MAGIC 50 knots also gets a `fast_circle` flag. The geometry flag includes smaller, slower circles.
# MAGIC We also check for **position unchanged despite reported speed**: at least three reports
# MAGIC spanning 12 minutes, a coordinate spread below 5 m, and usable reported speeds all above
# MAGIC 5 knots. This can indicate stale positions or inconsistent speed reports.
# MAGIC
# MAGIC Short or sparse sequences can remain unassessed. Adjust these thresholds for your vessels
# MAGIC and reporting frequency before using them on real data.
# MAGIC
# MAGIC We run this check before notebook 1's 30-knot limit for linking positions, which would
# MAGIC split the fast circle into separate track fragments. We group by reported MMSI for this
# MAGIC initial check.
# MAGIC

# COMMAND ----------

def fit_circle(x, y, times, lon, lat):
    """Pure numeric evidence; no fixture labels, expected outcomes or vessel truth."""
    x, y, times = np.asarray(x, float), np.asarray(y, float), np.asarray(times, float)
    result = dict(radius_m=None, radial_residual_ratio=None, angular_coverage_deg=None,
                  angular_consistency=None, median_implied_knots=None, circle_candidate=False,
                  fast_circle=False, fit_status="not_assessed")
    if len(x) < 12 or times[-1] - times[0] < MIN_WINDOW_S or np.any(np.diff(times) <= 0):
        return result
    coordinates = np.column_stack((x, y))
    centred = coordinates - coordinates.mean(axis=0)
    if np.linalg.matrix_rank(centred) < 2 or np.linalg.cond(centred) > 1e8:
        return result
    try:
        center_x, center_y, radius, error = taubinSVD(coordinates)
    except np.linalg.LinAlgError:
        return result
    if radius <= 0 or not np.isfinite([center_x, center_y, radius, error]).all():
        return result
    residual = float(error / radius)
    angles = np.arctan2(y - center_y, x - center_x)
    ordered_angles = np.sort(np.mod(angles, 2 * np.pi))
    gaps = np.diff(np.r_[ordered_angles, ordered_angles[0] + 2 * np.pi])
    coverage = float(np.degrees(2 * np.pi - gaps.max()))
    changes = np.diff(np.unwrap(angles))
    moving_changes = changes[np.abs(changes) > 1e-6]
    consistency = float(max(np.mean(moving_changes > 0), np.mean(moving_changes < 0))) if len(moving_changes) else 0.0
    _, _, distances = Geod(ellps="WGS84").inv(
        np.asarray(lon)[:-1], np.asarray(lat)[:-1],
        np.asarray(lon)[1:], np.asarray(lat)[1:])
    speed = float(np.median(distances / np.diff(times) * 3600 / 1852))
    candidate = bool(residual < 0.1 and coverage >= 270 and consistency >= 0.8)
    result.update(radius_m=radius, radial_residual_ratio=residual, angular_coverage_deg=coverage,
                  angular_consistency=consistency, median_implied_knots=speed,
                  circle_candidate=candidate, fast_circle=bool(candidate and radius > 2000 and speed > MAX_MOTION_KNOTS),
                  fit_status="assessed")
    return result


WINDOW_MEMBER_SCHEMA = """window_id string, MMSI string, window_start_s long, window_end_s long,
obs_id string, sample_count long, observed_span_s long, radius_m double, radial_residual_ratio double,
angular_coverage_deg double, angular_consistency double, median_implied_knots double,
circle_candidate boolean, fast_circle boolean, position_unchanged_despite_reported_speed boolean, fit_status string"""
WINDOW_MEMBER_COLUMNS = [f.strip().split()[0] for f in WINDOW_MEMBER_SCHEMA.replace("\n", " ").split(",")]


def window_members_group(pdf):
    """A bounded tutorial group. Larger feeds should pre-group bounded temporal windows."""
    pdf = pdf.sort_values(["event_time_s", "obs_id"], kind="mergesort")
    output = []
    if len(pdf) == 0:
        return pd.DataFrame(columns=WINDOW_MEMBER_COLUMNS)
    first_end = (int(pdf.event_time_s.min()) // WINDOW_STEP_S + 1) * WINDOW_STEP_S
    # Include the last report even when it falls exactly on a five-minute boundary.
    last_end = (int(pdf.event_time_s.max()) // WINDOW_STEP_S + 1) * WINDOW_STEP_S
    for end in range(first_end, last_end + 1, WINDOW_STEP_S):
        start = end - MAX_EPISODE_S
        w = pdf[(pdf.event_time_s >= start) & (pdf.event_time_s < end)]
        if len(w) == 0:
            continue
        fit = fit_circle(w.x_m, w.y_m, w.event_time_s, w.LON, w.LAT)
        position_unchanged = bool(len(w) >= 3 and w.event_time_s.max() - w.event_time_s.min() >= MIN_WINDOW_S
                      and np.hypot(w.x_m.max() - w.x_m.min(), w.y_m.max() - w.y_m.min()) < 5
                      and w.sog_known.all() and (w.SOG > 5).all())
        window_id = hashlib.sha256(f"{w.MMSI.iloc[0]}|{start}|{end}".encode()).hexdigest()[:24]
        for obs_id in w.obs_id:
            output.append(dict(window_id=window_id, MMSI=str(w.MMSI.iloc[0]), window_start_s=int(start),
                               window_end_s=int(end), obs_id=obs_id, sample_count=int(len(w)),
                               observed_span_s=int(w.event_time_s.max() - w.event_time_s.min()),
                               position_unchanged_despite_reported_speed=position_unchanged, **fit))
    return pd.DataFrame(output, columns=WINDOW_MEMBER_COLUMNS)

enriched = inputs.join(metrics, "obs_id", "inner")
members = enriched.groupBy("MMSI").applyInPandas(window_members_group, WINDOW_MEMBER_SCHEMA)
windows = members.drop("obs_id").dropDuplicates(["window_id"])
display(windows.where("circle_candidate OR position_unchanged_despite_reported_speed"))
# Each membership row identifies an observation used in one window.
display(members.select("window_id", "obs_id", "window_start_s", "window_end_s"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Save the results of each check
# MAGIC We join the field checks, spatial measurements and window flags by `obs_id` and save
# MAGIC one row per observation in `quality_observations`. Each rule records `flagged`, `passed`
# MAGIC or `not_assessed`, together with its reason. When several windows include an observation,
# MAGIC its summary records whether any of those windows flagged it.
# MAGIC The input table keeps every reception, including receiver copies.

# COMMAND ----------

def rule_struct(name, outcome, reason):
    return F.struct(
        F.lit(name).alias("rule"), outcome.alias("outcome"), reason.alias("reason")
    )


def observation_decisions(inputs, metrics, members):
    episode_signals = members.groupBy("obs_id").agg(
        F.max(F.col("circle_candidate").cast("int"))
        .cast("boolean")
        .alias("circle_candidate"),
        F.max(F.col("fast_circle").cast("int")).cast("boolean").alias("fast_circle"),
        F.max(F.col("position_unchanged_despite_reported_speed").cast("int"))
        .cast("boolean")
        .alias("position_unchanged_despite_reported_speed"),
        F.max((F.col("fit_status") == "assessed").cast("int"))
        .cast("boolean")
        .alias("circle_assessed"),
        F.max(
            (
                (F.col("observed_span_s") >= MIN_WINDOW_S)
                & (F.col("sample_count") >= 3)
            ).cast("int")
        )
        .cast("boolean")
        .alias("unchanged_position_assessed"),
    )
    d = inputs.join(metrics, "obs_id", "left").join(episode_signals, "obs_id", "left")

    def outcome(assessed, flagged):
        return (
            F.when(~assessed, "not_assessed")
            .when(flagged, "flagged")
            .otherwise("passed")
        )

    usable = F.col("input_evaluable")
    land_flag = F.col("inside_land") & (
        F.col("shoreline_distance_m") > COASTAL_TOLERANCE_M
    )
    d = d.withColumn(
        "rule_results",
        F.array(
            rule_struct(
                "coordinate",
                outcome(F.lit(True), ~F.col("coordinate_valid")),
                F.when(F.col("coordinate_unavailable"), F.lit("unavailable")).otherwise(
                    F.lit("range_and_finite")
                ),
            ),
            rule_struct(
                "timestamp",
                outcome(F.lit(True), ~F.col("time_valid")),
                F.lit("observation_time_vs_reception_time"),
            ),
            rule_struct(
                "inland",
                outcome(
                    usable & F.coalesce(F.col("mask_covered"), F.lit(False)), land_flag
                ),
                F.lit("hawaii_utm_mask_and_150m_tolerance"),
            ),
            rule_struct(
                "isolated_jump",
                outcome(
                    usable & F.col("isolated_jump").isNotNull(), F.col("isolated_jump")
                ),
                F.when(F.col("isolated_jump").isNull(), "missing_or_same_time_neighbour")
                .otherwise("two_sided_motion_with_plausible_bypass"),
            ),
            rule_struct(
                "circle_geometry",
                outcome(
                    usable & F.coalesce(F.col("circle_assessed"), F.lit(False)),
                    F.coalesce(F.col("circle_candidate"), F.lit(False)),
                ),
                F.lit("bounded_window_fit"),
            ),
            rule_struct(
                "fast_circle",
                outcome(
                    usable & F.coalesce(F.col("circle_assessed"), F.lit(False)),
                    F.coalesce(F.col("fast_circle"), F.lit(False)),
                ),
                F.lit("radius_and_implied_speed"),
            ),
            rule_struct(
                "position_unchanged_despite_reported_speed",
                outcome(
                    usable
                    & F.coalesce(F.col("unchanged_position_assessed"), F.lit(False))
                    & F.col("sog_known"),
                    F.coalesce(F.col("position_unchanged_despite_reported_speed"), F.lit(False)),
                ),
                F.lit("optional_consistency_exercise"),
            ),
            rule_struct(
                "motion_fields",
                outcome(
                    F.lit(True),
                    ~F.col("sog_known") | ~F.col("cog_known") | ~F.col("heading_known"),
                ),
                F.when(
                    F.col("motion_payload_conflict"),
                    F.lit("receiver_attribute_conflict"),
                ).otherwise(F.lit("optional_motion_availability_and_range")),
            ),
            rule_struct(
                "observation_payload",
                outcome(F.lit(True), F.col("identity_payload_conflict")),
                F.lit("one_observation_id_must_identify_one_position"),
            ),
        ),
    )
    return (
        d.withColumn(
            "flags",
            F.expr(
                "transform(filter(rule_results, r -> r.outcome='flagged'), r -> r.rule)"
            ),
        )
        .withColumn(
            "quality_status",
            F.when(~F.col("input_evaluable"), "invalid_input")
            .when(F.size("flags") > 0, "review")
            .otherwise("no_flags"),
        )
        .withColumn("assessed_at", F.current_timestamp())
    )


results = observation_decisions(inputs, metrics, members)
results.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    table("quality_observations")
)
results = spark.table(table("quality_observations"))
display(
    results.select(
        "obs_id",
        "MMSI",
        "BaseDateTime",
        "flags",
        "quality_status",
        "rule_results",
        "assessed_at",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Pass quality labels to downstream analyses
# MAGIC Keep the reported positions and attach the findings. A map can highlight an inland
# MAGIC position or fast circle; an analyst can inspect the reason codes and supporting windows.
# MAGIC A journey-distance calculation may choose to exclude particular flags, while an
# MAGIC investigation may need to show every report. Make that choice in the consuming analysis.
# MAGIC `invalid_input` means a report cannot enter these spatial/movement calculations;
# MAGIC `review` means at least one check flagged it. `no_flags` does not mean every check passed:
# MAGIC inspect `rule_results` for checks that were `not_assessed`.
# MAGIC
# MAGIC The display below lists one row per flagged check, so an observation can appear more
# MAGIC than once. All observations remain in `quality_observations`, including invalid inputs.
# MAGIC When building tracks and callings, carry the observation labels and report how many
# MAGIC supporting observations were flagged, with their reason codes. Keep links to those
# MAGIC observations so users can inspect them. The next two notebooks run independently;
# MAGIC this example does not add those summaries to their outputs.

# COMMAND ----------

display(
    results.withColumn("check", F.explode("rule_results"))
    .where("check.outcome = 'flagged'")
    .select(
        "obs_id",
        "MMSI",
        "BaseDateTime",
        "quality_status",
        F.col("check.rule").alias("reason_code"),
        F.col("check.reason").alias("reason"),
        "assessed_at",
    )
)

# Plot only the small fixture; calculations above remain distributed.
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon as PlotPolygon

plot_rows = results.where("input_evaluable").toPandas()
fig, axes = plt.subplots(1, 3, figsize=(17, 5))
for i, ring in enumerate(LAND_PLOT_RINGS):
    axes[0].add_patch(PlotPolygon(ring, color="#d7d5c6" if i == 0 else "white"))
for label in ("deep_inland", "nearshore", "offshore_water"):
    r = FIXTURE_BY_ID[LABEL_IDS[label][0]]
    axes[0].scatter(
        r["x_m"], r["y_m"], marker="x" if label == "deep_inland" else "o", s=45
    )
    axes[0].annotate(
        label.replace("_", " "),
        (r["x_m"], r["y_m"]),
        xytext=(4, 5),
        textcoords="offset points",
        fontsize=8,
    )
axes[0].set_xlim(160000, 320000)
axes[0].set_ylim(2080000, 2250000)
axes[0].set_title("Hawaiʻi coastline and synthetic reports")
for ax, label in zip(axes[1:], ("fast_circle", "slow_circle")):
    p = plot_rows[plot_rows.obs_id.isin(LABEL_IDS[label])].sort_values("event_time_s")
    ax.scatter(
        p.x_m, p.y_m, c=(p.event_time_s - FIXTURE_START_S) / 60, cmap="viridis", s=18
    )
    ax.set_title(label.replace("_", " ") + " — observation minute")
for ax in axes:
    ax.set_aspect("equal")
    ax.set_xlabel("UTM 5N easting (m)")
    ax.set_ylabel("UTM 5N northing (m)")
fig.suptitle("Illustrative synthetic observations — independent panel scales")
plt.tight_layout()
display(fig)
plt.close(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Using these checks with real AIS data
# MAGIC
# MAGIC Before applying these checks to real data, choose a suitable coastline dataset and tune
# MAGIC the thresholds for your vessels, geography and reporting frequency. Measure performance
# MAGIC with realistic MMSI group sizes. The tests here cover the small synthetic dataset;
# MAGIC accuracy and throughput on a real feed need separate measurement.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extending the checks to global coverage
# MAGIC Keep the reported positions in WGS 84 longitude/latitude. The ellipsoidal speed
# MAGIC calculations already work globally. Land checks need a global land dataset, indexed
# MAGIC or subdivided so each position is compared with nearby polygons. Project the relevant
# MAGIC land geometry and positions into the same local CRS for distances in metres.
# MAGIC
# MAGIC For circle fitting, pass each H3 cell's centre to
# MAGIC [ST_EstimateSRID](https://docs.databricks.com/aws/en/sql/language-manual/functions/st_estimatesrid)
# MAGIC to choose a local UTM or polar UPS projection. The query below selects one SRID per cell;
# MAGIC the Hawaiʻi checks above use the fixed EPSG:32605 projection.
# MAGIC
# MAGIC Join the selected SRID back to the cell's observations and use `ST_Transform` to project
# MAGIC them. Group positions into overlapping time windows and fit each MMSI's movement in
# MAGIC one shared CRS per fit. Include neighbouring cells so a circle crossing a cell boundary
# MAGIC is available in full, and deduplicate repeated window findings. Choose the H3 resolution
# MAGIC and neighbour coverage for the size of patterns being sought; resolution 3 below is
# MAGIC illustrative. Check that the projection is suitable for the full area being fitted.
# MAGIC Store the selected SRID with the measurements. A fuller analysis would also check
# MAGIC geographic-coordinate patterns, as a circle in one coordinate space can appear oval in another.

# COMMAND ----------

projection_regions = spark.sql("""
  WITH regions AS (
    SELECT DISTINCT h3_longlatash3(LON, LAT, 3) AS h3_region
    FROM quality_checked_inputs
    WHERE input_evaluable
  )
  SELECT h3_region,
         ST_EstimateSRID(ST_GeomFromText(h3_centeraswkt(h3_region), 4326)) AS projected_srid
  FROM regions
""")
display(projection_regions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying the checks to continually arriving reports
# MAGIC The runnable example analyses one complete batch. On a live feed, append every reception
# MAGIC to the raw table. Field and land checks can run as reports arrive. Movement checks need
# MAGIC neighbouring reports in observation-time order; the isolated-jump check needs a later
# MAGIC report as well as an earlier one. Circle checks need enough reports to assess a window.
# MAGIC
# MAGIC A practical extension is to use Structured Streaming `foreachBatch` to combine each
# MAGIC incoming batch with relevant recent history for the affected MMSIs, then reuse these
# MAGIC batch checks. Processing only the new batch would miss patterns spanning batch boundaries.
# MAGIC Retain enough event-time history to cover the 30-minute windows, the chosen allowance
# MAGIC for late reports and the neighbouring reports needed for motion checks. Validate reported
# MAGIC timestamps against reception time before using them to advance an event-time watermark.
# MAGIC Preserve older reports in the raw table for historical reprocessing.
# MAGIC
# MAGIC Write revised assessments by observation/window ID, recording the assessment time. A late report can change an earlier label, so
# MAGIC recompute the affected windows and update downstream quality summaries. Decide when
# MAGIC an assessment is considered complete and how to reprocess reports arriving after that
# MAGIC point. Retaining and labelling positions still requires handling these revisions.
# MAGIC
# MAGIC The following notebooks explore this Structured Streaming `foreachBatch` pattern in
# MAGIC more detail for track assignment and port/berth callings, including how to update
# MAGIC results when late reports arrive.
