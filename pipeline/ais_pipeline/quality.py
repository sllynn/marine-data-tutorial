"""Run notebook 00's batch rules against the demo's received history.

Recomputing the small demo history lets late neighbours revise earlier labels.
Review flags are advisory; only invalid inputs are withheld from track calculations.
"""
from pyspark.sql import functions as F

from . import quality_rules as rules

COASTAL_TOLERANCE_M = 150.0


def settings(config):
    options = config.get("quality", {})
    srid = options.get("projected_srid", 32631)  # Default Dover–Calais demo: UTM 31N.
    if type(srid) is not int or not (32601 <= srid <= 32660 or 32701 <= srid <= 32760):
        raise ValueError("quality.projected_srid must be a local WGS84 UTM SRID")
    land, coverage = options.get("land_wkt"), options.get("coverage_wkt")
    if bool(land) != bool(coverage):
        raise ValueError("Supply both quality.land_wkt and quality.coverage_wkt in WGS84")
    if land and (not isinstance(land, str) or not isinstance(coverage, str)):
        raise ValueError("Quality land and coverage geometries must be WKT strings")
    return dict(srid=srid, land=land or "MULTIPOLYGON EMPTY",
                coverage=coverage or "POLYGON EMPTY")


def check_reference(spark, options):
    """Check reference geometry before the runner resets any outputs."""
    row = spark.sql("""SELECT
        ST_IsValid(ST_GeomFromText(:land,4326)) AND
        ST_IsValid(ST_GeomFromText(:coverage,4326)) AS valid
    """, args={"land": options["land"], "coverage": options["coverage"]}).first()
    if not row.valid:
        raise ValueError("Invalid quality land/coverage geometry")


def land_checks(inputs, options):
    # The notebook's Hawaiʻi geometry is replaced with an explicit local reference.
    # An empty coverage geometry makes the inland rule not_assessed.
    inputs.createOrReplaceTempView("quality_checked_inputs")
    return inputs.sparkSession.sql(f"""
        WITH positions AS (
          SELECT *, ST_SetSRID(ST_Point(LON,LAT),4326) AS geo_point
          FROM quality_checked_inputs WHERE input_evaluable
        ), projected AS (
          SELECT *, ST_Transform(geo_point,{options['srid']}) AS local_point,
            ST_Transform(ST_GeomFromText(:land,4326),{options['srid']}) AS land,
            ST_Transform(ST_GeomFromText(:coverage,4326),{options['srid']}) AS coverage
          FROM positions
        )
        SELECT obs_id, MMSI, event_time_s, geo_point,
          ST_X(local_point) AS x_m, ST_Y(local_point) AS y_m,
          ST_Intersects(coverage,local_point) AS mask_covered,
          ST_Contains(land,local_point) AS inside_land,
          ST_Distance(ST_Boundary(land),local_point) AS shoreline_distance_m
        FROM projected
    """, args={"land": options["land"], "coverage": options["coverage"]})


def assess(spark, prefix, options):
    rules.spark = spark
    rules.COASTAL_TOLERANCE_M = COASTAL_TOLERANCE_M

    def materialize(frame, name):
        frame.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(prefix + "." + name)
        return spark.table(prefix + "." + name)

    receptions = spark.table(prefix + ".bronze_receptions").withColumn(
        "received_time_s", F.unix_timestamp("received_at"))
    inputs = materialize(rules.field_checks(receptions), "quality_inputs_staging")
    metrics = materialize(rules.motion_checks(land_checks(inputs, options)), "quality_metrics_staging")
    enriched = inputs.join(metrics, "obs_id", "inner")
    members = materialize(enriched.groupBy("MMSI").applyInPandas(
        rules.window_members_group, rules.WINDOW_MEMBER_SCHEMA), "quality_window_members")
    decisions = rules.observation_decisions(inputs, metrics, members)
    # The shared notebook rule has a Hawaiʻi-specific reason; describe this reference.
    decisions = decisions.withColumn("rule_results", F.expr("""
        transform(rule_results, r -> named_struct('rule',r.rule,'outcome',r.outcome,
          'reason',CASE WHEN r.rule='inland' THEN 'configured_mask_and_150m_tolerance'
                        ELSE r.reason END))
    """))
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
