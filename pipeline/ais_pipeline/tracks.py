"""Extracted from 01_identity_tracks_and_late_data.py; regenerate with tools/sync_notebook_code.py."""

import hashlib

import json

from collections import defaultdict

import pandas as pd

from pyspark.sql import functions as F, Window

import pyspark.databricks.sql.functions as DBF

from delta.tables import DeltaTable

from datetime import datetime, timezone

AIS_SCHEMA = """MMSI string, BaseDateTime timestamp, LAT double, LON double,
SOG double, COG double, Heading double, VesselName string, IMO string, CallSign string,
VesselType int, Status int, Length double, Width double, Draft double, Cargo int,
TransceiverClass string"""

AIS_COLUMNS = [field.strip().split()[0] for field in AIS_SCHEMA.replace("\n", " ").split(",")]

def prepare_reports(reports):
    """Parse AIS fields in UTC and derive the replay clock and position fingerprint.

    The spatial resolver uses MMSI, BaseDateTime, LAT and LON. Other reported fields
    remain in Bronze for identity evidence and analysis. Source identifies a receiver.
    """
    types = {field.strip().split()[0]: field.strip().split()[1]
             for field in AIS_SCHEMA.replace("\n", " ").split(",")}
    for column, dtype in types.items():
        value = F.col(column) if column in reports.columns else F.lit(None)
        reports = reports.withColumn(column, value.cast(dtype))
    reports = reports.withColumn("event_time_s", F.unix_timestamp("BaseDateTime"))
    if "obs_id" not in reports.columns:
        reports = reports.withColumn("obs_id", F.sha2(F.to_json(F.struct(
            "MMSI", "BaseDateTime", "LAT", "LON")), 256))
    if "source" not in reports.columns:
        reports = reports.withColumn("source", F.lit("unknown"))
    return reports

WINDOW_S = 45 * 60

MAX_GAP_S = 12 * 60

MAX_SPEED_MPS = 30 * 1852 / 3600

TIE_MARGIN_M = 100.0

H3_RES = 7

H3_K = 12  # Grid steps, unrelated to the 12-minute observation-time link gap.

ASSIGN_SCHEMA = "obs_id string, MMSI string, event_time_s long, LON double, LAT double, track_id string, track_status string"

ASSIGN_COLS = ["obs_id", "MMSI", "event_time_s", "LON", "LAT", "track_id", "track_status"]

POSITION_COLS = ["obs_id", "MMSI", "event_time_s", "LON", "LAT"]

def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:20]

def resolve_group(pdf):
    tracks, endpoints, answers = {}, {}, []
    # Local replay state, rebuilt for this one MMSI
    # tracks: track_id -> endpoint/support count; endpoints: observation_id -> track_id.
    # Only frozen seeds initialize tracks. Never trust a mutable row's old track_id.
    for p in pdf[pdf.kind == "seed"].itertuples():
        tracks[p.track_id] = {"endpoint": p.obs_id, "n": int(p.n)}
        endpoints[p.obs_id] = p.track_id
    # A destination can have several eligible predecessors, possibly on different tracks.
    edges = defaultdict(list)
    for e in pdf[pdf.kind == "edge"].itertuples():
        edges[e.obs_id].append((float(e.distance_m), e.prev_id))
    # Spark does not guarantee row order after a shuffle; the ID breaks equal-time ties.
    for p in pdf[pdf.kind == "position"].sort_values(["event_time_s", "obs_id"]).itertuples():
        # Discard edges from endpoints already consumed by a later position. This makes
        # each track a sequence, rather than letting it branch from an old observation.
        candidates = sorted(
            (d, endpoints[prev]) for d, prev in edges[p.obs_id] if prev in endpoints
        )
        tid = None
        if len(candidates) > 1 and candidates[1][0] - candidates[0][0] < TIE_MARGIN_M:
            # Retain this observation with no track. It does not become a new endpoint.
            track_status = "ambiguous"
        else:
            # New segment IDs use their first observation as a deterministic anchor.
            tid = candidates[0][1] if candidates else "trk_" + p.obs_id
            if tid in tracks:
                # Retire the old endpoint before exposing the new one to later positions.
                endpoints.pop(tracks[tid]["endpoint"])
                tracks[tid]["n"] += 1
                tracks[tid]["endpoint"] = p.obs_id
            else:
                tracks[tid] = {"endpoint": p.obs_id, "n": 1}
            endpoints[p.obs_id] = tid
            track_status = "pending"
        answers.append(
            [p.obs_id, p.MMSI, int(p.event_time_s), float(p.LON), float(p.LAT), tid, track_status]
        )
    # Wait until replay ends: a later observation can promote an earlier singleton.
    # Output slots 5/6 are track_id/track_status, matching ASSIGN_COLS above.
    for a in answers:
        if a[5]:
            a[6] = "supported" if tracks[a[5]]["n"] >= 2 else "tentative"
    return pd.DataFrame(answers, columns=ASSIGN_COLS)

def candidate_edges(nodes):
    # Seeds may precede new positions, but only mutable positions can be link targets.
    predecessors = (
        nodes.select(*POSITION_COLS)
        .withColumn("cell", DBF.h3_longlatash3("LON", "LAT", H3_RES))
    )
    # explode makes one target row per neighbour cell, allowing an equality join on cell.
    targets = (
        nodes.where("kind = 'position'")
        .select(*POSITION_COLS)
        .withColumn(
            "cell",
            F.explode(
                DBF.h3_kring(DBF.h3_longlatash3("LON", "LAT", H3_RES), H3_K)
            ),
        )
    )
    p, q = predecessors.alias("p"), targets.alias("q")
    # p is an earlier candidate; q is the destination. This is a pairwise gap check,
    # distinct from the wider correction-window filter in replay_inputs.
    condition = (
        (F.col("p.MMSI") == F.col("q.MMSI"))
        & (F.col("p.event_time_s") < F.col("q.event_time_s"))
        & (F.col("q.event_time_s") - F.col("p.event_time_s") <= MAX_GAP_S)
        & (F.col("p.cell") == F.col("q.cell"))
    )
    # DistanceSphere returns metres; dividing by elapsed seconds gives metres/second.
    edges = p.join(q, condition).select(
        F.col("q.MMSI"),
        F.col("q.obs_id"),
        F.col("p.obs_id").alias("prev_id"),
        (F.col("q.event_time_s") - F.col("p.event_time_s")).alias("elapsed_s"),
        DBF.st_distancesphere(
            DBF.st_point("p.LON", "p.LAT"), DBF.st_point("q.LON", "q.LAT")
        ).alias("distance_m"),
    )
    # Optimizers may evaluate projections before the positive-time join filter.
    # try_divide keeps same-time pairs safe under ANSI SQL evaluation.
    return edges.withColumn("speed_mps", F.try_divide("distance_m", "elapsed_s")).where(
        F.col("speed_mps") <= MAX_SPEED_MPS
    )

def resolve_distributed(nodes):
    # Tag links so each worker can distinguish them from positions and frozen seeds.
    edges = candidate_edges(nodes).withColumn("kind", F.lit("edge"))
    # Row types have different fields; union fills inapplicable fields with nulls.
    # elapsed_s has served its eligibility check and is not needed by resolve_group.
    tagged = nodes.unionByName(edges.drop("elapsed_s"), allowMissingColumns=True)
    # All dependent decisions for one MMSI meet on one worker; MMSIs run in parallel.
    return tagged.groupBy("MMSI").applyInPandas(resolve_group, schema=ASSIGN_SCHEMA)

def as_nodes(df, kind):
    # Normalize both inputs to one tagged shape; n is meaningful only for frozen seeds.
    return df.withColumn("kind",F.lit(kind)).selectExpr(
        "MMSI", "kind", "obs_id", "event_time_s", "LON", "LAT",
        "track_id" if "track_id" in df.columns else "cast(null as string) AS track_id",
        "n" if "n" in df.columns else "cast(0 as bigint) AS n")

def replay_inputs(silver, accepted, cutoff, horizon):
    affected = accepted.select("MMSI").distinct()
    # A semi-join selects matching Silver rows without collecting or duplicating keys.
    vessel_history = silver.join(affected,"MMSI","left_semi")
    # Silver contains accepted reports only; rejected Bronze reports cannot sneak back in.
    history = vessel_history.where(F.col("event_time_s").between(cutoff,horizon))
    incoming = accepted.select(*POSITION_COLS,F.array("source").alias("sources"))
    # Reconsider ALL mutable positions, including ones already assigned to a track.
    # Flatten receiver arrays, remove duplicates, and sort for stable values on retries.
    # collect_list is a distributed Spark aggregation, not a driver-side .collect().
    positions = (
        history.select(*POSITION_COLS,"sources")
        .unionByName(incoming)
        .groupBy(*POSITION_COLS)
        .agg(F.sort_array(F.array_distinct(F.flatten(F.collect_list("sources")))).alias("sources"))
        )
    # Frozen decisions are read as context, never re-resolved or updated.
    frozen = vessel_history.where((F.col("event_time_s") < cutoff) & F.col("track_id").isNotNull())
    partition = Window.partitionBy("MMSI","track_id")
    # Count the entire frozen prefix BEFORE selecting its last row; otherwise n=1.
    # Keep only endpoints close enough to the cutoff to reach any mutable observation.
    seeds = (
        frozen
        .withColumn("n", F.count("*").over(partition))
        .withColumn("rn", F.row_number().over(partition.orderBy(F.desc("event_time_s"), F.desc("obs_id"))))
        .where((F.col("rn")==1) & (F.col("event_time_s") >= cutoff-MAX_GAP_S))
        )
    nodes = as_nodes(positions, "position").unionByName(as_nodes(seeds, "seed"))
    return nodes, positions.select("obs_id","sources")

def process_micro_batch(batch_df, batch_id, fail_after_merge=False):
    batch_df = prepare_reports(batch_df)
    session = batch_df.sparkSession
    incoming_horizon = batch_df.agg(F.max("event_time_s").alias("event_time_s")).first().event_time_s
    if incoming_horizon is None:
        return
    silver = session.table(SILVER)
    # Read just a scalar. The newest accepted position preserves the frontier in Silver.
    previous_horizon = silver.agg(F.max("event_time_s").alias("event_time_s")).first().event_time_s
    horizon = max(previous_horizon if previous_horizon is not None else incoming_horizon, incoming_horizon)
    cutoff = horizon-WINDOW_S
    # between includes the cutoff. Older receptions remain in Bronze, never replayed.
    accepted = batch_df.where(F.col("event_time_s").between(cutoff,horizon))
    nodes, provenance = replay_inputs(silver,accepted,cutoff,horizon)
    updates = (resolve_distributed(nodes).join(provenance,"obs_id")
               .withColumn("BaseDateTime", F.timestamp_seconds("event_time_s")))
    # A replay may run again after a successful MERGE. Update only changed values;
    # NULL-safe comparison ensures an ambiguous result clears a previous assignment.
    # SQL <=> treats two NULLs as equal, but an old ID versus NULL as a real change.
    changed = "NOT (target.track_id <=> incoming.track_id) OR NOT (target.track_status <=> incoming.track_status) OR NOT (target.sources <=> incoming.sources)"
    # Match on position identity, not track ID: late evidence can change membership.
    # Update only mutable rows, including null assignments; insert genuinely new ones.
    (DeltaTable.forName(session,SILVER).alias("target")
     .merge(updates.alias("incoming"),"target.obs_id = incoming.obs_id")
     .whenMatchedUpdateAll(condition=f"target.event_time_s >= {cutoff} AND ({changed})")
     .whenNotMatchedInsertAll().execute())
    # Test hook models failure after publication but before streaming completion.
    if fail_after_merge:
        raise RuntimeError("Injected failure after MERGE, before checkpoint completion")

import matplotlib.pyplot as plt
