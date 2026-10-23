"""Distributed form of notebook 1's registry-evidence example.

Every evidence item must match exactly one time-valid registry row, and every
item on the track must agree. Missing/conflicting evidence remains unresolved.
There is no additional movement-based segment-linking algorithm here.
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F


def norm(c):
    return F.upper(F.regexp_replace(F.trim(c), r"\s+", " "))


def associate(positions, evidence, registry):
    assigned = positions.where(F.col("track_id").isNotNull())
    ev = assigned.select("obs_id", "track_id", "event_time_s").join(
        evidence.select("obs_id", "VesselName", "CallSign", "IMO").distinct(), "obs_id"
    ).alias("e")
    reg = registry.alias("r")
    matches = ev.join(reg,
        (F.col("e.event_time_s") >= F.col("r.from_t")) &
        (F.col("e.event_time_s") < F.col("r.to_t")) &
        (norm(F.col("e.VesselName")) == norm(F.col("r.VesselName"))) &
        (norm(F.col("e.CallSign")) == norm(F.col("r.CallSign"))) &
        (F.col("e.IMO").isNull() | (norm(F.col("e.IMO")) == norm(F.col("r.IMO")))), "left")
    per_item = matches.groupBy("track_id", "obs_id", F.col("e.VesselName"), F.col("e.CallSign"), F.col("e.IMO")).agg(
        F.countDistinct("r.vessel_id").alias("candidate_count"), F.min("r.vessel_id").alias("candidate"))
    return per_item.groupBy("track_id").agg(
        F.min("candidate_count").alias("min_count"), F.max("candidate_count").alias("max_count"),
        F.countDistinct("candidate").alias("identities"), F.min("candidate").alias("candidate"),
    ).select("track_id", F.when(
        (F.col("min_count") == 1) & (F.col("max_count") == 1) & (F.col("identities") == 1),
        F.col("candidate")).alias("vessel_id"))


def process_batch(changes, batch_id, prefix):
    if changes.isEmpty():
        return
    s = changes.sparkSession
    positions = s.table(prefix + ".silver_track_positions")
    evidence = s.table(prefix + ".bronze_receptions")
    registry = s.table(prefix + ".vessel_registry")
    links = associate(positions, evidence, registry)
    links.write.mode("overwrite").saveAsTable(prefix + ".track_vessel_links")
    # Identical duplicate receptions carry identical attributes; the resolver has
    # already kept their receiver provenance separately in `sources`.
    attrs = evidence.dropDuplicates(["obs_id"]).drop(
        "MMSI", "BaseDateTime", "LAT", "LON", "event_time_s", "source", "reception_id")
    new = (positions.join(attrs, "obs_id").join(s.table(prefix + ".track_vessel_links"), "track_id", "left")
           .join(registry.select("vessel_id", F.col("IMO").alias("resolved_imo")), "vessel_id", "left")
           .withColumn("identity_status", F.when(F.col("vessel_id").isNotNull(), "supported_match").otherwise("unresolved")))
    columns = s.table(prefix + ".silver_resolved_positions").columns
    new = new.select(*columns)
    condition = " OR ".join(f"NOT (s.{c} <=> u.{c})" for c in columns if c != "obs_id")
    (DeltaTable.forName(s, prefix + ".silver_resolved_positions").alias("s")
     .merge(new.alias("u"), "s.obs_id = u.obs_id")
     .whenMatchedUpdateAll(condition=condition).whenNotMatchedInsertAll().execute())
