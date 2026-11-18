"""Worldwide quality screening; geographic input is never tied to a regional CRS."""
import csv
import gzip
import hashlib
import json
from importlib.resources import files

from pyspark.sql import Window, functions as F

from . import global_windows, quality_rules as rules

COASTAL_TOLERANCE_M = 150.0
GLOBAL_MEMBER_SCHEMA = rules.WINDOW_MEMBER_SCHEMA + ", projection_definition string, projection_status string, window_evaluable boolean"


def settings(config):
    options = config.get("quality", {})
    if not isinstance(options, dict):
        raise ValueError("quality must be a mapping")
    if set(options) - {"land_reference"}:
        raise ValueError("Quality screening selects local projections automatically; configure only land_reference=global or none")
    reference = options.get("land_reference", "global")
    if reference not in ("global", "none"):
        raise ValueError("quality.land_reference must be global or none")
    return {"land_reference": reference}


def reference_rows(name):
    csv.field_size_limit(64 * 1024 * 1024)
    data = files("ais_pipeline").joinpath("data")
    manifest = json.loads(data.joinpath("global_land_source.json").read_text())
    payload = data.joinpath(name).read_bytes()
    if hashlib.sha256(payload).hexdigest() != manifest["files"][name]:
        raise ValueError("Global land reference checksum mismatch: " + name)
    return list(csv.DictReader(gzip.decompress(payload).decode("utf-8").splitlines()))


def check_reference(spark, options):
    """Validate packaged reference integrity before any output/checkpoint reset."""
    if options["land_reference"] == "global":
        for name in ("global_land.csv.gz", "global_land_cells.csv.gz", "global_coast_cells.csv.gz"):
            reference_rows(name)


def prepare_reference(spark, prefix, options):
    if options["land_reference"] == "none" or options.get("_reference_prefix") == prefix:
        return
    for file_name, table_name, key in (
        ("global_land.csv.gz", "quality_land_polygons", "polygon_id"),
        ("global_coast_cells.csv.gz", "quality_coast_cells", "cell"),
    ):
        rows = [(int(r[key]), r["wkt"]) for r in reference_rows(file_name)]
        frame = spark.createDataFrame(rows, f"{key} long,wkt string").selectExpr(
            key, "ST_AsBinary(ST_GeomFromText(wkt,4326)) AS geom_wkb")
        frame.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(prefix + "." + table_name)
    rows = [(int(r["cell"]), int(r["polygon_id"])) for r in reference_rows("global_land_cells.csv.gz")]
    (spark.createDataFrame(rows, "cell long,polygon_id long").write.mode("overwrite")
     .saveAsTable(prefix + ".quality_land_cells"))
    options["_reference_prefix"] = prefix


def land_checks(inputs, prefix, options):
    inputs.createOrReplaceTempView("quality_checked_inputs")
    spark = inputs.sparkSession
    if options["land_reference"] == "none":
        return spark.sql("""SELECT obs_id,MMSI,event_time_s,
          ST_SetSRID(ST_Point(LON,LAT),4326) AS geo_point,
          cast(NULL AS double) AS x_m,cast(NULL AS double) AS y_m,
          false AS mask_covered,cast(NULL AS boolean) AS inside_land,
          cast(NULL AS double) AS shoreline_distance_m,
          cast(NULL AS double) AS shoreline_distance_lower_bound_m,
          cast(NULL AS int) AS land_projected_srid,false AS beyond_coastal_tolerance
          FROM quality_checked_inputs WHERE input_evaluable""")
    prepare_reference(spark, prefix, options)
    # Candidate polygons are tested geographically. Only nearby coastlines are
    # projected, into the observation's own UTM/UPS CRS, then measured geodesically.
    return spark.sql(f"""
      WITH points AS (
        SELECT obs_id,MMSI,event_time_s,ST_SetSRID(ST_Point(LON,LAT),4326) AS geo_point,
               h3_longlatash3(LON,LAT,3) AS cell
        FROM quality_checked_inputs WHERE input_evaluable
      ), projected AS (
        SELECT *,ST_EstimateSRID(geo_point) AS projected_srid FROM points
      ), local AS (
        SELECT *,ST_Transform(geo_point,projected_srid) AS local_point FROM projected
      ), land_hits AS (
        SELECT DISTINCT p.obs_id
        FROM points p JOIN {prefix}.quality_land_cells c ON p.cell=c.cell
        JOIN {prefix}.quality_land_polygons l ON c.polygon_id=l.polygon_id
        WHERE ST_Intersects(ST_GeomFromWKB(l.geom_wkb,4326),p.geo_point)
      ), neighbourhoods AS (
        SELECT p.*,neighbour FROM local p
        LATERAL VIEW explode(h3_kring(cell,2)) cells AS neighbour
      ), coast_distances AS (
        SELECT p.obs_id,min(ST_DistanceSpheroid(p.geo_point,
          ST_Transform(ST_ClosestPoint(
            ST_Transform(ST_GeomFromWKB(c.geom_wkb,4326),p.projected_srid),p.local_point),4326))) AS distance_m
        FROM neighbourhoods p JOIN {prefix}.quality_coast_cells c ON p.neighbour=c.cell
        GROUP BY p.obs_id
      )
      SELECT p.obs_id,p.MMSI,p.event_time_s,p.geo_point,
        cast(NULL AS double) AS x_m,cast(NULL AS double) AS y_m,true AS mask_covered,
        l.obs_id IS NOT NULL AS inside_land,c.distance_m AS shoreline_distance_m,
        coalesce(c.distance_m,{COASTAL_TOLERANCE_M}) AS shoreline_distance_lower_bound_m,
        p.projected_srid AS land_projected_srid,
        c.distance_m IS NULL OR c.distance_m>{COASTAL_TOLERANCE_M} AS beyond_coastal_tolerance
      FROM projected p LEFT JOIN land_hits l USING (obs_id)
      LEFT JOIN coast_distances c USING (obs_id)
    """)


def window_inputs(enriched):
    # At most six memberships per report, even for history with years-long gaps.
    step, span = rules.WINDOW_STEP_S, rules.MAX_EPISODE_S
    last = (F.floor(F.max("event_time_s").over(Window.partitionBy("MMSI")) / step) + 1) * step
    first = ((F.floor(F.col("event_time_s") / step) + 1) * step).cast("long")
    return (enriched.withColumn("_last_end", last)
            .withColumn("window_end_s", F.explode(F.sequence(first, first + span - step, F.lit(step))))
            .where("window_end_s <= _last_end").drop("_last_end"))


def assess(spark, prefix, options):
    rules.spark = spark
    rules.COASTAL_TOLERANCE_M = COASTAL_TOLERANCE_M

    def materialize(frame, name):
        frame.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(prefix + "." + name)
        return spark.table(prefix + "." + name)

    receptions = spark.table(prefix + ".bronze_receptions").withColumn("received_time_s", F.unix_timestamp("received_at"))
    inputs = materialize(rules.field_checks(receptions), "quality_inputs_staging")
    land = materialize(land_checks(inputs, prefix, options), "quality_land_staging")
    extras = land.select("obs_id", "shoreline_distance_lower_bound_m", "land_projected_srid", "beyond_coastal_tolerance")
    metrics = materialize(rules.motion_checks(land).join(extras, "obs_id"), "quality_metrics_staging")
    enriched = inputs.join(metrics, "obs_id", "inner")
    members = materialize(window_inputs(enriched).groupBy("MMSI", "window_end_s").applyInPandas(
        global_windows.window_members_group, GLOBAL_MEMBER_SCHEMA), "quality_window_members")
    frozen_assessed = members.groupBy("obs_id").agg(F.max(
        (F.col("window_evaluable") & (F.col("sample_count") >= 3)
         & (F.col("observed_span_s") >= rules.MIN_WINDOW_S)).cast("int")).cast("boolean").alias("frozen_assessed"))
    decisions = rules.observation_decisions(inputs, metrics, members).join(frozen_assessed, "obs_id", "left")
    decisions = decisions.withColumn("rule_results", F.expr("""
      transform(rule_results,r -> named_struct('rule',r.rule,'outcome',
        CASE WHEN r.rule='inland' THEN
          CASE WHEN NOT input_evaluable OR NOT coalesce(mask_covered,false) THEN 'not_assessed'
               WHEN inside_land AND beyond_coastal_tolerance THEN 'flagged' ELSE 'passed' END
        WHEN r.rule='position_unchanged_despite_reported_speed' AND NOT coalesce(frozen_assessed,false)
          THEN 'not_assessed' ELSE r.outcome END,
        'reason',CASE WHEN r.rule='inland' THEN 'global_land_and_geodesic_coast_distance'
          WHEN r.rule='position_unchanged_despite_reported_speed' AND NOT coalesce(frozen_assessed,false)
            THEN 'no_evaluable_local_window' ELSE r.reason END))
    """))
    decisions = (decisions.withColumn("flags", F.expr("transform(filter(rule_results,r -> r.outcome='flagged'),r -> r.rule)"))
                 .withColumn("quality_status", F.when(~F.col("input_evaluable"), "invalid_input")
                             .when(F.size("flags") > 0, "review").otherwise("no_flags")))
    materialize(decisions, "quality_observations")


def track_inputs(receptions, observations):
    """Keep optional-motion and advisory findings; guard geometry and replay clocks."""
    return receptions.join(observations.where("input_evaluable").select("obs_id"), "obs_id", "left_semi")


def create_views(spark, prefix):
    # Views join current labels so revised assessments need no track/calling replay.
    for name in ("silver_track_positions", "silver_resolved_positions"):
        spark.sql(f"""CREATE OR REPLACE VIEW {prefix}.{name}_with_quality AS
          SELECT p.*, q.quality_status, q.flags, q.rule_results, q.assessed_at,
                 q.input_evaluable
          FROM {prefix}.{name} p LEFT JOIN {prefix}.quality_observations q USING (obs_id)
        """)
    spark.sql(f"""CREATE OR REPLACE VIEW {prefix}.gold_callings_with_quality AS
      WITH summaries AS (
        SELECT g.vessel_id,g.location_id,g.entry_ts,
          count(p.obs_id) AS quality_observation_count,
          count_if(p.quality_status='review') AS review_observation_count,
          array_sort(array_distinct(flatten(collect_list(p.flags)))) AS quality_flags,
          max(p.assessed_at) AS quality_assessed_at
        FROM {prefix}.gold_callings g
        LEFT JOIN {prefix}.silver_resolved_positions_with_quality p
          ON g.vessel_id=p.vessel_id AND p.BaseDateTime BETWEEN g.entry_ts AND g.last_seen_ts
        GROUP BY g.vessel_id,g.location_id,g.entry_ts
      )
      SELECT g.*,s.quality_observation_count,s.review_observation_count,
             s.quality_flags,s.quality_assessed_at
      FROM {prefix}.gold_callings g JOIN summaries s USING (vessel_id,location_id,entry_ts)
    """)
