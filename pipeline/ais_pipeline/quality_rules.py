"""Extracted from 00_position_quality_and_anomalies.py; regenerate with tools/sync_notebook_code.py."""

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
