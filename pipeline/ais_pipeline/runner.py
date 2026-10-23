"""Release real micro-batches, drain each hop, and measure arrival-to-Gold time."""
import json
import re
import time
from functools import partial

import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from . import tracks, callings, identity
from .generator.fleet import build_routes, imo_for, load_config, vessel_reports

FEED_SCHEMA = tracks.AIS_SCHEMA + ", obs_id string, event_time_s long, source string, arrival_batch int, reception_id string"
RESOLVED_SCHEMA = tracks.AIS_SCHEMA + ", obs_id string, event_time_s long, vessel_id string, arrival_batch int, track_id string, track_status string, sources array<string>, resolved_imo string, identity_status string"
GOLD_SCHEMA = "vessel_id string, location_id string, entry_ts timestamp, last_seen_ts timestamp, exit_ts timestamp, n_positions long, as_of_ts timestamp"


def configure_callings(prefix):
    # Bind only table names; merge_micro_batch itself is extracted verbatim.
    callings.silver = prefix + ".silver_resolved_positions"
    callings.location_cells_table = prefix + ".location_cells"
    callings.callings_tbl = prefix + ".gold_callings"
    callings.staging_tbl = prefix + ".batch_positions_staging"
    callings.recomputed_tbl = prefix + ".recomputed_callings"
    callings.retraction_window_days = 7
    callings.ancestor_array = "array(named_struct('ancestor_cell', h3_longlatash3(LON, LAT, 8), 'res', 8))"


def track_batch(df, batch_id, prefix):
    tracks.SILVER = prefix + ".silver_track_positions"
    tracks.process_micro_batch(df, batch_id)


def calling_batch(df, batch_id, prefix):
    configure_callings(prefix)
    # Keep old and new IDs from CDF: an identity correction must reconsider both.
    # The notebook callback reads current Silver history, so preimages are only
    # triggers for candidate selection, never the history used to derive visits.
    callings.merge_micro_batch(df.where(F.col("vessel_id").isNotNull()), batch_id)


def drain(spark, source, callback, checkpoint, cdf=False):
    reader = spark.readStream
    if cdf:
        reader = reader.option("readChangeFeed", "true")
    q = (reader.table(source).writeStream.foreachBatch(callback)
         .option("checkpointLocation", checkpoint).trigger(availableNow=True).start())
    q.awaitTermination()


def generate(spark, config_path, prefix, c):
    routes = build_routes(config_path, c)
    # The routing graph runs once per configured pair; emission is partitioned
    # across workers. Bound each yielded Pandas frame independently of fleet size.
    def emit(iterator):
        pending = []
        for pdf in iterator:
            for index in pdf["id"]:
                for record in vessel_reports(int(index), c, routes):
                    pending.append(record)
                    if len(pending) >= 5000:
                        yield pd.DataFrame(pending)
                        pending = []
        if pending:
            yield pd.DataFrame(pending)
    spark.range(c["vessels"]).mapInPandas(emit, FEED_SCHEMA).write.mode("overwrite").saveAsTable(prefix + ".release_schedule")
    # Registry is external identity evidence; truth IDs never enter the AIS feed.
    rows = [(f"vessel-{i}", imo_for(i), f"TUTORIAL VESSEL {i}", f"TUT{i}",
             c["start_epoch"] - 86400, c["start_epoch"] + c["reports_per_vessel"] * c["cadence_seconds"] + 86400)
            for i in range(c["vessels"])]
    spark.createDataFrame(rows, "vessel_id string, IMO string, VesselName string, CallSign string, from_t long, to_t long").write.mode("overwrite").saveAsTable(prefix + ".vessel_registry")
    locations = []
    for p in c["ports"]:
        # Local teaching boundaries around the configured endpoint. Real port
        # polygons can replace this table without changing calling derivation.
        for kind, radius in (("port", 0.025), ("berth", 0.004)):
            lon, lat = p["lon"], p["lat"]
            xy = [(lon-radius, lat-radius), (lon+radius, lat-radius), (lon+radius, lat+radius), (lon-radius, lat+radius), (lon-radius, lat-radius)]
            wkt = "POLYGON ((" + ", ".join(f"{x} {y}" for x,y in xy) + "))"
            locations.append((p["name"] + "-" + kind, kind, wkt))
    spark.createDataFrame(locations, "location_id string, location_type string, wkt string").write.mode("overwrite").saveAsTable(prefix + ".locations")
    spark.sql(f"""CREATE OR REPLACE TABLE {prefix}.location_cells USING DELTA CLUSTER BY (cell) AS
    WITH prepared AS (
      SELECT location_id, 8 AS target_res, ST_AsBinary(ST_GeomFromWKT(wkt, 4326)) AS geom_wkb FROM {prefix}.locations
    )
    SELECT location_id, target_res, t.cellid AS cell, t.core, t.chip AS chip_wkb FROM prepared
    LATERAL VIEW inline(h3_tessellateaswkb(geom_wkb, target_res)) t""")


def setup(spark, dbutils, prefix, config_path, c):
    if not re.fullmatch(r"[A-Za-z_][\w]*\.[A-Za-z_][\w]*", prefix):
        raise ValueError("Use simple catalog.schema identifiers")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {prefix}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {prefix}.checkpoints")
    catalog, schema = prefix.split(".")
    checkpoint = f"/Volumes/{catalog}/{schema}/checkpoints/pipeline"
    dbutils.fs.rm(checkpoint, recurse=True)
    generate(spark, config_path, prefix, c)
    tables = [
        ("bronze_receptions", FEED_SCHEMA),
        ("silver_track_positions", tracks.ASSIGN_SCHEMA + ", BaseDateTime timestamp, sources array<string>"),
        ("silver_resolved_positions", RESOLVED_SCHEMA),
        ("batch_positions_staging", RESOLVED_SCHEMA),
        ("gold_callings", GOLD_SCHEMA),
        ("recomputed_callings", "vessel_id string, location_id string, entry_ts timestamp, last_seen_ts timestamp, last_pos_num int, n_positions long, exit_ts timestamp"),
    ]
    for name, schema in tables:
        props = "'delta.enableChangeDataFeed' = 'true'"
        if name in ("gold_callings", "batch_positions_staging", "recomputed_callings"):
            props += ", 'delta.feature.catalogManaged' = 'supported'"
        spark.sql(f"CREATE OR REPLACE TABLE {prefix}.{name} ({schema}) USING DELTA TBLPROPERTIES ({props})")
    return checkpoint


def release(spark, prefix, round_index):
    due = spark.table(prefix + ".release_schedule").where(F.col("arrival_batch") == round_index)
    (DeltaTable.forName(spark, prefix + ".bronze_receptions").alias("b")
     .merge(due.alias("u"), "b.reception_id = u.reception_id").whenNotMatchedInsertAll().execute())


def run(spark, dbutils, prefix, config_path, vessels=None, reports=None):
    c = load_config(config_path)
    if vessels is not None:
        c["vessels"] = int(vessels)
    if reports is not None:
        c["reports_per_vessel"] = int(reports)
    if c["vessels"] <= 0 or c["reports_per_vessel"] <= 0:
        raise ValueError("Vessels and reports must be positive")
    checkpoint = setup(spark, dbutils, prefix, config_path, c)
    rounds = spark.table(prefix + ".release_schedule").agg(F.max("arrival_batch")).first()[0] + 1
    metrics = []
    for i in range(rounds):
        start = time.monotonic()
        release(spark, prefix, i)
        released = time.monotonic()
        drain(spark, prefix + ".bronze_receptions", partial(track_batch, prefix=prefix), checkpoint + "/tracks")
        tracked = time.monotonic()
        drain(spark, prefix + ".silver_track_positions", partial(identity.process_batch, prefix=prefix), checkpoint + "/identity", True)
        resolved = time.monotonic()
        drain(spark, prefix + ".silver_resolved_positions", partial(calling_batch, prefix=prefix), checkpoint + "/callings", True)
        finished = time.monotonic()
        # Metrics exclude subsequent inspection queries. This is whole-release
        # latency, including stream start/drain overhead, never per-vessel latency.
        row = dict(arrival_batch=i, release_s=released-start, track_s=tracked-released,
                   identity_s=resolved-tracked, calling_s=finished-resolved,
                   arrival_to_gold_s=finished-released, round_s=finished-start)
        for name in ("bronze_receptions", "silver_track_positions", "silver_resolved_positions", "gold_callings"):
            row[name] = spark.table(prefix + "." + name).count()
        metrics.append(row)
        spark.createDataFrame(metrics).write.mode("overwrite").saveAsTable(prefix + ".round_metrics")
        print(json.dumps(row), flush=True)
    return {"prefix": prefix, "rounds": metrics, "vessels": c["vessels"], "reports_per_vessel": c["reports_per_vessel"]}
