# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # AIS resolution: SQL motion features, distributed replay and late corrections
# MAGIC
# MAGIC AIS receivers deliver reports about where a vessel was observed, often through
# MAGIC several sources and sometimes out of order. Before we can ask which port it visited,
# MAGIC we need to decide which reports belong together. Joining every report with the same
# MAGIC MMSI into one line can invent a journey when two vessels share that identifier.
# MAGIC A missing report can also split a plausible journey until the evidence arrives later.
# MAGIC
# MAGIC ## What are we computing?
# MAGIC
# MAGIC A **track** is a sequence of positions whose movement is plausible under our chosen
# MAGIC time and speed limits. We allow several tracks for one reported MMSI and retain
# MAGIC uncertain observations. As late reports arrive, we can revise recent assignments;
# MAGIC older decisions become frozen so the amount of history we reconsider stays bounded.
# MAGIC
# MAGIC Track resolution and vessel identity are separate steps. A plausible track groups
# MAGIC observations; reported names and call signs can then support a match to a vessel
# MAGIC registry. A track ID alone does not establish which physical vessel sent the reports.
# MAGIC
# MAGIC ## What data do we use?
# MAGIC
# MAGIC - **AIS reports:** reported `MMSI`, UTC observation time (`BaseDateTime`), `LON`,
# MAGIC   `LAT`, motion attributes and vessel attributes such as `VesselName` and `CallSign`.
# MAGIC   Reported `IMO` is null in this example.
# MAGIC - **Reception metadata:** `source` identifies the receiver, `obs_id` fingerprints
# MAGIC   a position, and `arrival_batch` controls delivery order independently of event time.
# MAGIC - **Registry evidence:** a small time-valid registry supplies vessel IDs and IMOs
# MAGIC   when the track-associated identity evidence supports one consistent match.
# MAGIC
# MAGIC All data are synthetic. A and B share an MMSI but move along separate paths; C uses
# MAGIC another MMSI. We include a repeated reception, an isolated jump, a late report that
# MAGIC bridges A's missing position, and a report delivered after its correction window
# MAGIC has closed. Times are elapsed minutes from the Unix epoch, stored as UTC timestamps.
# MAGIC The A/B/C labels help explain and check the fixture; the resolver does not use them.
# MAGIC
# MAGIC ## How will we compute it?
# MAGIC
# MAGIC 1. Keep every reception in Bronze and deduplicate accepted positions by `obs_id`.
# MAGIC 2. For affected MMSIs, retrieve recent Silver positions and frozen track endpoints.
# MAGIC 3. Use H3 to find candidate predecessors, then native spatial SQL to check elapsed
# MAGIC    time, spherical distance and implied speed against a 12-minute/30-knot gate.
# MAGIC 4. Replay each MMSI in observation-time order, extending the nearest eligible
# MAGIC    current track endpoint. If the two closest candidates are within 100 metres
# MAGIC    of each other in distance, leave the assignment ambiguous.
# MAGIC 5. Publish revised positions and assignments to Silver with one Delta MERGE.
# MAGIC
# MAGIC Spark distributes candidate generation and runs different MMSIs in parallel. A
# MAGIC grouped Pandas function makes the dependent track decisions for each MMSI. This is
# MAGIC a deterministic greedy teaching resolver; crossings and sparse evidence can still
# MAGIC produce uncertain or incorrect associations.
# MAGIC
# MAGIC ## What does the output look like?
# MAGIC
# MAGIC Bronze retains receptions and reported attributes. Silver has one row per accepted
# MAGIC position, including its time, coordinates, receiver `sources`, `track_id` and
# MAGIC `track_status`. The status describes the evidence available after replay:
# MAGIC
# MAGIC | track_status | Meaning | track_id |
# MAGIC |---|---|---|
# MAGIC | `supported` | The track has at least two supporting positions. | Assigned |
# MAGIC | `tentative` | The track has only one supporting position. | Assigned |
# MAGIC | `ambiguous` | The closest eligible track endpoints are near-tied. | Null |
# MAGIC
# MAGIC Initially A0 and A20/A30 belong to separate segments because their gap exceeds
# MAGIC 12 minutes. When A10 arrives, it bridges the gap: A20 and A30 receive A0's track ID,
# MAGIC while B keeps its separate track despite sharing the MMSI. The isolated jump stays
# MAGIC tentative. Section 7 separately displays registry associations with `vessel_id`,
# MAGIC `resolved_imo` and `identity_status`; these are not columns written to Silver here.
# MAGIC
# MAGIC We will prepare the reports, explain candidate generation and replay, then deliver
# MAGIC four batches to see those corrections happen. The later checks cover frozen-history
# MAGIC continuity, ambiguity and retries. We finish with guidance on scaling the pattern
# MAGIC before using resolved vessel positions in the calling notebook.
# MAGIC
# MAGIC **Running the example:** Run all on serverless (environment version '6' was tested).
# MAGIC The setup cell keeps configuration together.
# MAGIC Run all starts fresh by replacing this tutorial's Bronze and Silver tables and
# MAGIC removing its checkpoint folder; the schema and volume are reused.

# COMMAND ----------

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
# Arbitrary teaching parameter: linked observations may be at most 12 minutes apart.
MAX_GAP_S = 12 * 60
MAX_SPEED_MPS = 30 * 1852 / 3600
TIE_MARGIN_M = 100.0
H3_RES = 7
H3_K = 12  # Grid steps, unrelated to the 12-minute observation-time link gap.

# Demo setup: reuse one schema; Run all resets its two tables and checkpoint folder.
ASSIGN_SCHEMA = "obs_id string, MMSI string, event_time_s long, LON double, LAT double, track_id string, track_status string"
ASSIGN_COLS = ["obs_id", "MMSI", "event_time_s", "LON", "LAT", "track_id", "track_status"]

spark.conf.set("spark.sql.session.timeZone", "UTC")
# Select a catalog where you can create this tutorial's dedicated schema.
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "ais_identity_tracks_tutorial", "Tutorial schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
PREFIX = f"{CATALOG}.{SCHEMA}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {PREFIX} COMMENT 'Synthetic AIS Silver replay tutorial'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {PREFIX}.checkpoints")
BRONZE, SILVER = [f"{PREFIX}.{table_name}" for table_name in ("bronze_receptions","silver_positions")]
CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/resolver"
RAW_SCHEMA = AIS_SCHEMA + ", obs_id string, event_time_s long, source string, arrival_batch int"
POSITION_COLS = ["obs_id", "MMSI", "event_time_s", "LON", "LAT"]
spark.sql(f"CREATE OR REPLACE TABLE {BRONZE} ({RAW_SCHEMA}) USING DELTA")
spark.sql(f"CREATE OR REPLACE TABLE {SILVER} ({ASSIGN_SCHEMA}, BaseDateTime TIMESTAMP, sources ARRAY<STRING>) USING DELTA")
# Start fresh alongside the replaced tables; retain the reusable checkpoint volume.
# This path belongs only to this tutorial. Do not reset while its stream is running.
dbutils.fs.rm(CHECKPOINT, recurse=True)
print(f"Tutorial outputs (reset on Run all): {PREFIX}")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:20]


def observation(MMSI, minute, LON, LAT=51.0):
    report = dict.fromkeys(AIS_COLUMNS)
    report.update(obs_id=digest([MMSI, minute*60, LON, LAT]), MMSI=str(MMSI),
                  BaseDateTime=datetime.fromtimestamp(minute*60, tz=timezone.utc),
                  event_time_s=minute*60, LON=float(LON), LAT=float(LAT), TransceiverClass="A")
    return report

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Contract and synthetic arrivals
# MAGIC The input uses the AIS fields declared in `AIS_SCHEMA`: `MMSI`, UTC `BaseDateTime`,
# MAGIC `LAT`, `LON`, motion attributes and vessel attributes. `prepare_reports` derives
# MAGIC `event_time_s` (whole Unix seconds) and an observation fingerprint when needed.
# MAGIC Bronze retains all reported fields. Silver stores accepted positions and track decisions.
# MAGIC `Status` is navigation status; `track_status` is our resolution decision.
# MAGIC Reported `IMO` can be null; section 7 derives `resolved_imo` using registry evidence.
# MAGIC
# MAGIC * Mutable time interval is **[global high-water mark − 45 minutes, high-water mark]**.
# MAGIC   In production this could be days. Older receptions stay in Bronze, never backfilled.
# MAGIC   Validate source timestamps before allowing them to advance a production frontier.
# MAGIC * Equivalent receptions share an observation fingerprint; retain receiver provenance.
# MAGIC   Conflicting positions at the same timestamp remain distinct observations.
# MAGIC * Native SQL calculates candidate distances, elapsed time and implied speeds.
# MAGIC   Use multiple predecessors per MMSI: a simple LAG would pre-merge interleaved tracks.
# MAGIC * Resolve by nearest eligible current endpoint, with a 12-minute gap/30-knot gate.
# MAGIC   Near ties abstain; singleton tracks stay tentative. No velocity prediction is used.
# MAGIC * Frozen output remains unchanged. Its last endpoint per track seeds replay continuity.
# MAGIC   Track IDs are hypothesis anchors, not permanent physical vessel IDs.
# MAGIC Scenario labels below are used only for checks and plots.

# COMMAND ----------

P = {
    "a0": observation(235000001, 0, 1.00),
    "a10": observation(235000001, 10, 1.015),
    "a20": observation(235000001, 20, 1.03),
    "a30": observation(235000001, 30, 1.045),
    "b0": observation(235000001, 0, 1.30),
    "b10": observation(235000001, 10, 1.285),
    "b20": observation(235000001, 20, 1.27),
    "b30": observation(235000001, 30, 1.255),
    "jump": observation(235000001, 15, 2.00),
    "c0": observation(235000002, 0, 1.5),
    "c10": observation(235000002, 10, 1.515),
    "c20": observation(235000002, 20, 1.53),
    "c30": observation(235000002, 30, 1.545),
    "clock": observation(235000003, 100, 1.8),
}


# Static attributes are carried on the position reports; reported IMO remains null.
for point_name, vessel_name, call_sign in [("a0", "West Star", "WST1"),
                                         ("a20", "WEST  STAR", "WST1"),
                                         ("b0", "East Star", "EST2")]:
    P[point_name].update(VesselName=vessel_name, CallSign=call_sign)


def reception(point_name, source="receiver-1"):
    return {**P[point_name], "source": source}


ARRIVALS = [
    [reception(n) for n in ("a0","b0","c0","b10","jump","a20","b20","c20","a30","b30","c30")]
    +[reception("a0", "receiver-2")],
    [reception("a10")],
    [reception("clock")],
    [reception("c10")],
]



# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. One bounded MMSI group per Python UDF task
# MAGIC
# MAGIC Each time new position data arrives, we recompute tracks for the affected MMSIs
# MAGIC using the new reports and their relevant, in-scope position history. Several
# MAGIC vessels can share an MMSI, so one group may contain several distinct tracks.
# MAGIC Reports inside the correction window can change earlier assignments; frozen
# MAGIC track endpoints provide continuity with the history outside that window.
# MAGIC
# MAGIC We express the track-building logic as a simple greedy algorithm in Python, then
# MAGIC apply it to the relevant row groups with `GroupedData.applyInPandas`:
# MAGIC `tagged.groupBy("MMSI").applyInPandas(resolve_group, schema=ASSIGN_SCHEMA)`.
# MAGIC Spark brings each MMSI's in-scope positions, frozen endpoints and precomputed
# MAGIC candidate links together, and runs the algorithm in a Spark task on a worker.
# MAGIC Different MMSI groups can execute in parallel across the cluster; the dependent
# MAGIC decisions within one MMSI group are made together in one function call.
# MAGIC
# MAGIC The function returns one row per mutable position, with its assigned `track_id`
# MAGIC and `track_status` (or a null ID when ambiguous). Together these rows describe the
# MAGIC tracks and their mapped positions, ready to publish to Silver. We keep the replay
# MAGIC distributed rather than collecting the position history onto the driver.
# MAGIC
# MAGIC + `position` rows are observations to resolve; `seed` rows are frozen endpoints.
# MAGIC + Python only chooses among precomputed edges. No trigonometry, Spark calls, I/O or
# MAGIC global state lives in this function.
# MAGIC + An edge is usable only while its predecessor remains the current endpoint of a track.
# MAGIC + Equal-time reports cannot link together.
# MAGIC + Replay event time in deterministic order; at a near tie leave the report unassigned.
# MAGIC
# MAGIC **Read the algorithm first** — an edge means “physically eligible”, not “assigned”.
# MAGIC Each call receives one MMSI's `position`, `seed` and `edge` rows together.
# MAGIC
# MAGIC ```text
# MAGIC Start tracks from frozen seeds; remember each track's current endpoint and count.
# MAGIC Index eligible edges by their destination observation.
# MAGIC For each mutable position, ordered by observation time then observation ID:
# MAGIC     Keep edges whose predecessor is still a current track endpoint.
# MAGIC     Rank those tracks by endpoint-to-position distance.
# MAGIC     If the closest two are near-tied: retain an ambiguous, unassigned position.
# MAGIC     Otherwise: extend the closest track, or start a segment if none is eligible.
# MAGIC     Move the chosen track's endpoint forward; increase its supporting count.
# MAGIC Mark assigned positions supported if their segment has at least two observations;
# MAGIC otherwise mark them tentative. Return mutable positions only, not frozen seeds.
# MAGIC ```
# MAGIC Existing assignments on `position` rows are deliberately ignored: replay can move
# MAGIC them to another segment or clear an assignment. Only frozen seeds fix a starting ID.
# MAGIC “Supported” means multiple positions in this hypothesis; it is not proof of identity.

# COMMAND ----------


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

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is H3, and why use it here?
# MAGIC
# MAGIC Before choosing tracks, we need to find which earlier positions could plausibly
# MAGIC precede each new position. Comparing every pair within an MMSI can become expensive
# MAGIC as its position history grows. Most of those pairs are too far apart to be useful.
# MAGIC
# MAGIC **H3** is a hierarchical spatial index that divides the Earth's surface into cells,
# MAGIC mostly hexagons, with a small number of pentagons. Each cell has an identifier.
# MAGIC A resolution controls the size of the cells: higher resolutions give smaller cells.
# MAGIC We can map a longitude/latitude point to its cell and look up neighbouring cells.
# MAGIC
# MAGIC Here we index each possible predecessor and search a neighbourhood around each
# MAGIC target position. Joining on cell ID and MMSI gives Spark a set of nearby candidate
# MAGIC pairs using an equality join, before calculating exact distances. This reduces
# MAGIC unnecessary spatial calculations and the number of candidate links passed to Python.
# MAGIC Searching neighbours also keeps points on opposite sides of a cell boundary in play.
# MAGIC
# MAGIC H3 is a coarse filter. Sharing a cell or neighbourhood does not establish a track
# MAGIC link: we still check observation times, spherical distance and implied speed, then
# MAGIC let the greedy resolver choose among eligible endpoints. The neighbourhood must be
# MAGIC large enough to cover the permitted travel distance; H3 grid steps are not a fixed
# MAGIC distance in metres, so the resolution and search radius need checking for the region.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Find earlier positions that a vessel could have travelled from
# MAGIC
# MAGIC For each position we want to assign, we first ask: **which earlier positions could
# MAGIC the vessel have reached this one from?** We call each possible connection a
# MAGIC candidate link, or **edge**. This section builds those links; the Python algorithm
# MAGIC from section 2 will then choose which ones extend a track.
# MAGIC
# MAGIC We narrow the search in three steps:
# MAGIC
# MAGIC 1. **Look nearby.** Use H3 cells and their neighbours to find positions in the same
# MAGIC    area that report the same MMSI.
# MAGIC 2. **Check the time gap.** Keep only earlier positions no more than 12 minutes before
# MAGIC    the position we are assigning. Reports at exactly the same time cannot link.
# MAGIC 3. **Check the journey is plausible.** Calculate the distance between each pair and
# MAGIC    divide by elapsed time. Keep the link only if that implied speed is at most 30 knots.
# MAGIC
# MAGIC For example, a position ten minutes earlier may be nearby enough to be reachable,
# MAGIC while another with the same MMSI may be too far away. Both reports remain in the
# MAGIC position history; only the plausible connection is offered to the track resolver.
# MAGIC Several links may pass these checks, leaving the resolver to choose or abstain.
# MAGIC
# MAGIC **H3 settings for this example.** We use resolution 7 and search all cells within
# MAGIC 12 grid steps, called a *k-ring*. At 30 knots, a vessel can travel at most 11.112 km
# MAGIC in 12 minutes. The neighbourhood is a conservative choice for this fixture's domain
# MAGIC (latitude 50–52, longitude −5–5). Grid steps are not metres, so check coverage again
# MAGIC before using these settings in another region or changing the motion limits.
# MAGIC
# MAGIC **Putting those steps together:**
# MAGIC
# MAGIC ```text
# MAGIC Index every potential predecessor (mutable position or frozen seed) in one H3 cell.
# MAGIC For each mutable target, expand its H3 cell to the k-ring neighbourhood.
# MAGIC Join predecessors to targets where:
# MAGIC     MMSIs match, and the predecessor cell belongs to the target neighbourhood;
# MAGIC     predecessor time < target time, and their time difference <= MAX_GAP_S.
# MAGIC Calculate exact spherical distance and implied speed for each surviving pair.
# MAGIC Keep pairs with speed <= MAX_SPEED_MPS; return target ID, predecessor ID and features.
# MAGIC ```
# MAGIC The correction window bounds which history we replay; the maximum link gap bounds
# MAGIC individual pairs. Two in-window positions can still be more than 12 minutes apart.
# MAGIC H3 proximity alone never establishes a track link, and a failed link drops neither
# MAGIC position. Equal-time observations remain separate and cannot link to each other.

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Update the tracks as new reports arrive
# MAGIC
# MAGIC We now have the pieces to resolve tracks. How do we run them whenever new reports
# MAGIC arrive, and use the results to correct positions we have already published?
# MAGIC
# MAGIC ### What does `foreachBatch` do?
# MAGIC
# MAGIC Spark Structured Streaming processes incoming data in small **micro-batches**.
# MAGIC `foreachBatch` lets us supply a Python function that Spark calls for each batch,
# MAGIC passing it a DataFrame of that batch's rows and a batch ID. Inside the function,
# MAGIC we can use ordinary batch DataFrame operations to read history, compute results
# MAGIC and write them to a table. We register it with
# MAGIC `.writeStream.foreachBatch(process_micro_batch)` in the next section.
# MAGIC
# MAGIC This is useful here because a new report can change an existing track assignment.
# MAGIC Simply appending the new position would leave the earlier assignments unchanged.
# MAGIC Our function instead combines the incoming positions with relevant Silver history,
# MAGIC replays the affected MMSIs, and uses a Delta **MERGE** to insert new positions and
# MAGIC update existing ones. Bronze keeps every reception; Silver keeps one accepted
# MAGIC position and its current track decision per observation ID.
# MAGIC
# MAGIC The callback coordinates the work on the driver, while Spark runs the joins,
# MAGIC spatial calculations and grouped Python replay on workers. Reading a batch through
# MAGIC `foreachBatch` does not collect its rows onto the driver; here only scalar maximum
# MAGIC timestamps are brought back to decide the correction boundary.
# MAGIC
# MAGIC ### Which history can change?
# MAGIC
# MAGIC First we find the latest observation time across Silver and the incoming batch.
# MAGIC This is our **horizon**. Positions from the horizon back through the previous
# MAGIC 45 minutes can be reconsidered; positions strictly before that cutoff are frozen.
# MAGIC Incoming reports older than the cutoff remain in Bronze and do not enter Silver.
# MAGIC
# MAGIC For each MMSI with accepted new positions, we retrieve its positions inside that
# MAGIC window and combine them with the new reports. The latest eligible endpoint of
# MAGIC each frozen track supplies starting context, including its supporting position
# MAGIC count, so a track can continue across the boundary without rewriting frozen rows.
# MAGIC
# MAGIC Silver retains the newest accepted observation even if its track is unresolved,
# MAGIC so we can recover the horizon from the table after a restart. This assumes trusted
# MAGIC timestamps, one sequential writer and no deletion of accepted history.
# MAGIC
# MAGIC ### What if Spark retries a batch?
# MAGIC
# MAGIC A callback can run again after a failure, including a failure after its table write
# MAGIC succeeded but before Spark recorded completion in the checkpoint. `foreachBatch`
# MAGIC therefore does not by itself make our custom writes exactly-once.
# MAGIC
# MAGIC We make publication **idempotent**: processing the same batch again at the same
# MAGIC horizon produces the same table contents. Replay is deterministic, and the MERGE
# MAGIC matches on `obs_id`, inserting a position only if it is new and updating existing
# MAGIC rows only when their mutable values change. An update can also clear an old track
# MAGIC ID if new evidence makes the assignment ambiguous. The later checks exercise this
# MAGIC retry behavior explicitly.
# MAGIC
# MAGIC Silver holds the current decisions. Delta table history supports audit and time
# MAGIC travel within its configured retention; this example keeps no separate revision ledger.
# MAGIC
# MAGIC **Replay and publication in one pass:**
# MAGIC ```text
# MAGIC Compute horizon and cutoff; admit only incoming positions inside the window.
# MAGIC Select affected MMSIs, then read their accepted Silver history.
# MAGIC Combine mutable history and incoming positions; deduplicate and union provenance.
# MAGIC Seed each track with its last eligible frozen endpoint and frozen support count.
# MAGIC Generate eligible edges, then replay each MMSI independently.
# MAGIC MERGE all mutable results by observation ID; leave the frozen prefix untouched.
# MAGIC ```

# COMMAND ----------


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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Run the four arrival batches
# MAGIC
# MAGIC In a real application, we would keep this stream running with a time-based trigger,
# MAGIC such as `.trigger(processingTime="30 seconds")`. Spark would continually process
# MAGIC newly available receptions in successive micro-batches, incrementally updating the
# MAGIC affected tracks as positions arrive. The trigger sets the desired interval between
# MAGIC batches; actual processing time also depends on the work in each batch.
# MAGIC
# MAGIC To demonstrate the concept here, we deliver four pre-prepared arrival batches in
# MAGIC a controlled order. After each delivery, we run the stream over the newly available
# MAGIC data and inspect how the published positions and track assignments have changed.
# MAGIC This lets us follow a late correction and the correction-window limit step by step.
# MAGIC
# MAGIC Each `availableNow` query processes newly appended Bronze receptions and stops.
# MAGIC All four queries share one checkpoint, so each resumes from the previous offsets.
# MAGIC
# MAGIC 1. The first batch omits A10, leaving a gap between A0 and A20 that exceeds our 12-minute link limit.
# MAGIC 2. A10 arrives late, inside the correction window, and bridges that gap.
# MAGIC 3. A report at minute 100 advances the correction-window cutoff to minute 55.
# MAGIC 4. C10 arrives outside that window: it stays in Bronze but cannot change Silver.
# MAGIC
# MAGIC We save a small Silver snapshot after each query so the next cell can compare
# MAGIC the answers before and after late arrivals. These driver-side collections are
# MAGIC only for this tiny teaching dataset; the processing callback runs without them.

# COMMAND ----------

SNAPSHOTS = []
for arrival_batch, rows in enumerate(ARRIVALS):
    incoming = spark.createDataFrame(
        [{**r, "arrival_batch": arrival_batch} for r in rows], RAW_SCHEMA
    )
    incoming.write.mode("append").saveAsTable(BRONZE)
    query = (
        spark.readStream.table(BRONZE)
        .writeStream.foreachBatch(process_micro_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    SNAPSHOTS.append({r.obs_id: r.asDict() for r in spark.table(SILVER).collect()})

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Check what changed, and what stayed the same
# MAGIC An assertion stops the notebook if an expected result is false. Here, `a0` through
# MAGIC `a3` are the four saved snapshots, rather than individual positions.
# MAGIC
# MAGIC - **Late correction:** A0 and A20 initially have different track IDs. After A10 arrives,
# MAGIC   they share a track; only A20 and A30 change their existing assignments.
# MAGIC - **Keep separate evidence:** B shares A's MMSI but follows a separate path, so it stays
# MAGIC   on another track. The isolated jump remains tentative, and the other MMSIs do not change.
# MAGIC - **Deduplication:** two receptions of A0 become one position, retaining both receiver names.
# MAGIC - **Correction limit:** C10 remains in Bronze and is absent from Silver; the last batch
# MAGIC   leaves the entire Silver snapshot unchanged.

# COMMAND ----------

labels = {v["obs_id"]: k for k, v in P.items()}
oid = lambda point_name: P[point_name]["obs_id"]
a0, a1, a2, a3 = SNAPSHOTS
assert a0[oid("a0")]["track_id"] != a0[oid("a20")]["track_id"]
assert a1[oid("a0")]["track_id"] == a1[oid("a20")]["track_id"]
assert a1[oid("b0")]["track_id"] != a1[oid("a0")]["track_id"]
assert a1[oid("jump")]["track_status"] == "tentative"
assert a2 == a3 and oid("c10") not in a3
changed_reports = [labels[k] for k in a0 if a0[k]["track_id"] != a1[k]["track_id"]]
assert set(changed_reports) == {"a20", "a30"}
assert all(a0[k] == a1[k] for k in a0 if a0[k]["MMSI"] != "235000001")
assert a1[oid("a0")]["sources"] == ["receiver-1", "receiver-2"]
assert len(a0) == len(ARRIVALS[0]) - 1
assert spark.table(BRONZE).where(F.col("obs_id") == oid("c10")).count() == 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Check that replaying a completed batch is harmless
# MAGIC This is a direct call to the processing function, separate from the four streaming
# MAGIC queries above. We replay the already accepted minute-100 batch and check that every
# MAGIC Silver value stays the same. This checks the MERGE's idempotence: repeating the input
# MAGIC must neither add duplicate positions nor change the answer.

# COMMAND ----------

# Retry an accepted batch after its MERGE has already succeeded, before moving the frontier.
before = spark.table(SILVER)
assert {r.obs_id: r.asDict() for r in before.collect()} == a3
process_micro_batch(spark.table(BRONZE).where("arrival_batch = 2"), 2)
assert {r.obs_id: r.asDict() for r in spark.table(SILVER).collect()} == a3

display(spark.table(SILVER).orderBy("MMSI", "event_time_s", "obs_id"))
print("Late bridge changes:", changed_reports)
print("Too-late report retained in Bronze, absent from Silver:", oid("c10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Boundary continuity, ambiguity and recovery
# MAGIC These additional synthetic scenarios call the processing function directly. They test
# MAGIC boundary and recovery behavior independently of the stream's offset handling.
# MAGIC
# MAGIC ### 6.1 Preserve continuity across the correction boundary and recover after publication
# MAGIC A vessel reports every ten minutes from minute 55 through minute 120. At first,
# MAGIC minute 55 is exactly on the inclusive cutoff and must be accepted. As the frontier
# MAGIC advances, minutes 55 and 65 become frozen; their endpoint must still seed the mutable
# MAGIC positions so the vessel keeps one track across the boundary.
# MAGIC
# MAGIC We deliberately fail after the continuation's MERGE, then retry it. The checks confirm
# MAGIC that the retry preserves the published answer, all eight positions share one track,
# MAGIC and frozen rows stay byte-for-byte equivalent in their saved values. A later duplicate
# MAGIC of minute 55 must not even update the frozen position's receiver provenance.

# COMMAND ----------

def frame(points, arrival=4):
    return spark.createDataFrame(
        [{**p, "source": "test", "arrival_batch": arrival} for p in points], RAW_SCHEMA
    )


# At horizon100 cutoff=55; at horizon120 cutoff=75. Frozen55/65 seed75+.
prefix = [
    observation(8, event_time_s, 1.0 + (event_time_s - 55) * 0.001)
    for event_time_s in (55, 65, 75, 85, 95, 105, 115, 120)
]
process_micro_batch(frame(prefix[:5]), 4)

frozen_before = {
    r.obs_id: r.asDict()
    for r in spark.table(SILVER).where("MMSI = '8' AND event_time_s < 4500").collect()
}
continuation = frame(prefix[5:], 5)

try:
    process_micro_batch(continuation, 5, fail_after_merge=True)
    raise AssertionError("Failure injection did not fire")
except RuntimeError as exc:
    assert "Injected failure" in str(exc)

published = {r.obs_id: r.asDict() for r in spark.table(SILVER).collect()}
process_micro_batch(continuation, 5)

assert {r.obs_id: r.asDict() for r in spark.table(SILVER).collect()} == published

boundary = spark.table(SILVER).where("MMSI = '8'")
assert boundary.select("track_id").distinct().count() == 1 and boundary.count() == len(
    prefix
)
assert {
    r.obs_id: r.asDict() for r in boundary.where("event_time_s < 4500").collect()
} == frozen_before
# Inclusive cutoff: minute55 was accepted with frontier100.
assert boundary.where(F.col("obs_id") == prefix[0]["obs_id"]).count() == 1
# New evidence at the old cutoff may not alter even provenance of frozen positions.
late_copy = frame([prefix[0]], 6).withColumn("source", F.lit("new-receiver"))
process_micro_batch(late_copy, 6)
assert {r.obs_id: r.asDict() for r in spark.table(SILVER).collect()} == published

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Withdraw an assignment when new evidence makes it ambiguous
# MAGIC Initially Z has one plausible predecessor, X, and receives a track ID. We then add Y
# MAGIC at the same time as X, equally close to Z. The resolver must mark Z ambiguous and
# MAGIC clear its old track ID, rather than silently retaining a now-unsupported assignment.
# MAGIC We also check that retries and corrections have left exactly one row per observation ID.

# COMMAND ----------

x, y, z = (
    observation(9, 120, 1.0),
    observation(9, 120, 1.002),
    observation(9, 121, 1.001),
)
process_micro_batch(frame([x, z], 7), 7)
assert (
    spark.table(SILVER).where(F.col("obs_id") == z["obs_id"]).first().track_id
    is not None
)
process_micro_batch(frame([y], 8), 8)
ambiguous = spark.table(SILVER).where(F.col("obs_id") == z["obs_id"]).first()
assert ambiguous.track_status == "ambiguous" and ambiguous.track_id is None
# Every observation ID remains unique after retries and reassignment.
assert (
    spark.table(SILVER).count()
    == spark.table(SILVER).select("obs_id").distinct().count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Check that input row order does not determine the answer
# MAGIC Resolve the same positions twice, with opposite input ordering. Comparing all result
# MAGIC rows in both directions checks that neither run produces extra or different assignments.
# MAGIC This is an ordering check on the resolver, separate from the streaming-window checks above.

# COMMAND ----------

all_nodes = as_nodes(spark.table(BRONZE).select(*POSITION_COLS).distinct(), "position")
normal = resolve_distributed(all_nodes)
reordered = resolve_distributed(all_nodes.orderBy(F.desc("obs_id")))
assert (
    normal.exceptAll(reordered).count() == 0
    and reordered.exceptAll(normal).count() == 0
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Identity evidence stays separate
# MAGIC `MMSI` is the reported transponder identity, `track_id` identifies a track segment,
# MAGIC and `vessel_id` identifies the vessel associated with its segments and observations.
# MAGIC The calling notebook groups those observations by `vessel_id` to compute visits.
# MAGIC
# MAGIC Each position report can carry `VesselName`, `CallSign` and `IMO`. Here `IMO` is
# MAGIC null: the names and call signs attached to observations within each track match a
# MAGIC time-valid registry, yielding `vessel_id` and `resolved_imo`. Every evidence item
# MAGIC must support one consistent registry vessel; conflicting evidence leaves it unresolved.
# MAGIC The example below associates track-level evidence with the registry.

# COMMAND ----------

REGISTRY = [
    {"vessel_id":"demo-west", "IMO":"9000003", "VesselName":"WEST STAR", "CallSign":"WST1", "from_t":-10000, "to_t":10000},
    {"vessel_id":"demo-east", "IMO":"9000015", "VesselName":"EAST STAR", "CallSign":"EST2", "from_t":-10000, "to_t":10000},
]
EVIDENCE = [
    {column: P[point_name][column] for column in ("obs_id", "VesselName", "CallSign", "IMO")}
    for point_name in ("a0", "a20", "b0")
]


def norm(s):
    return " ".join(s.upper().split()) if s else None


def identities(assignments, evidence=EVIDENCE):
    supporting = defaultdict(list)
    for e in evidence:
        a = assignments.get(e["obs_id"])
        if not a or not a["track_id"]:
            continue
        # Evidence is already associated to this observation; MMSI alone cannot do that.
        candidates = [v["vessel_id"] for v in REGISTRY
                      if v["from_t"] <= a["event_time_s"] < v["to_t"]
                      and norm(v["VesselName"]) == norm(e["VesselName"])
                      and norm(v["CallSign"]) == norm(e["CallSign"])
                      and (not e.get("IMO") or norm(v["IMO"]) == norm(e["IMO"]))]
        supporting[a["track_id"]].append(candidates)
    result = {}
    for tid in {a["track_id"] for a in assignments.values() if a["track_id"]}:
        ev = supporting[tid]
        candidates = sorted({c for cs in ev for c in cs})
        # Require evidence, a unique match for every item, and agreement across items.
        resolved = bool(ev) and all(len(cs)==1 for cs in ev) and len(candidates)==1
        matched_imos = {v["IMO"] for v in REGISTRY if resolved and v["vessel_id"] == candidates[0]}
        result[tid] = {"vessel_id":candidates[0] if resolved else None,
                       "resolved_imo":next(iter(matched_imos)) if len(matched_imos) == 1 else None,
                       "identity_status":"supported_match" if resolved else "unresolved",
                       "candidates":candidates, "evidence_count":len(ev)}
    return result


identity_result = identities(a1)
assert identity_result[a1[oid("a0")]["track_id"]]["vessel_id"] == "demo-west"
conflict = EVIDENCE+[{"obs_id":oid("a30"),"VesselName":"EAST STAR","CallSign":"EST2"}]
assert identities(a1,conflict)[a1[oid("a0")]["track_id"]]["identity_status"] == "unresolved"
assert all(e["IMO"] is None for e in EVIDENCE)
assert identity_result[a1[oid("a0")]["track_id"]]["resolved_imo"] == "9000003"
display(spark.createDataFrame([(k,v["vessel_id"],v["resolved_imo"],v["identity_status"]) for k,v in identity_result.items()],
                            "track_id string, vessel_id string, resolved_imo string, identity_status string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 See how track corrections affect identity evidence
# MAGIC The previous cell uses track-associated `VesselName`/`CallSign` evidence to resolve
# MAGIC a registry vessel and its IMO. The two panels below show the track
# MAGIC assignments that feed that association, before and after the late A10 report.
# MAGIC
# MAGIC Both panels show positions with the **same MMSI**. Longitude is on the horizontal
# MAGIC axis and observation time (minutes) is on the vertical axis. Each line connects
# MAGIC positions assigned to one track; legend labels are shortened track IDs, not vessel IDs.
# MAGIC
# MAGIC Before A10 arrives, A0 is separate from A20/A30. Afterwards, A10 bridges the gap
# MAGIC and those positions share a track, bringing their identity evidence together under
# MAGIC one track ID. B remains a separate track despite sharing the MMSI; the isolated
# MAGIC jump also remains visible. Motion continuity groups the evidence, while the registry
# MAGIC checks in the previous cell determine whether that evidence supports an identity.
# MAGIC This plot shows the track assignments used to group identity evidence.

# COMMAND ----------

import matplotlib.pyplot as plt

fig, axes = plt.subplots(1, 2, figsize=(14, 4), sharex=True, sharey=True)
for ax, title, state in zip(
    axes, ["Before late bridge", "After late bridge"], SNAPSHOTS[:2]
):
    groups = defaultdict(list)
    for a in state.values():
        if a["MMSI"] == "235000001":
            groups[a["track_id"]].append(a)
    for tid, points in sorted(groups.items()):
        points.sort(key=lambda p: p["event_time_s"])
        ax.plot(
            [p["LON"] for p in points],
            [p["event_time_s"] / 60 for p in points],
            "o-",
            label=tid[-6:],
        )
        for p in points:
            ax.annotate(labels[p["obs_id"]], (p["LON"], p["event_time_s"] / 60))
    ax.set(
        title=title, xlabel="Longitude", ylabel="Event-time minute", xlim=(0.99, 1.32)
    )
    ax.legend(title="Track anchor", fontsize=8)
plt.tight_layout()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Why replay rather than a separate vessel-state store?
# MAGIC A direct streaming implementation might maintain recent vessel positions and track
# MAGIC state in memory/a streaming state store, or coordinate a control table of commits and
# MAGIC touched rows. Here Bronze preserves receptions and Silver holds accepted positions and
# MAGIC current decisions. Replaying only affected MMSIs' bounded history and MERGEing once
# MAGIC avoids keeping a second resolver state store consistent with those outputs.
# MAGIC The checkpoint holds streaming progress. Deterministic replay makes a retried callback
# MAGIC safe after publication; it does not depend on a marker or on batch_id for deduplication.
# MAGIC Stateful streaming can reduce history reads/latency, but needs equivalent late-event
# MAGIC buffering, rewind, timeout and recovery logic. Use it when that tradeoff pays.
# MAGIC
# MAGIC
# MAGIC ### How would we scale this approach?
# MAGIC
# MAGIC Start with the same division of work: native SQL for candidate features, replay
# MAGIC only for affected MMSIs, and a bounded interval of mutable history. Then measure
# MAGIC the workload before widening the window or increasing the arrival rate:
# MAGIC
# MAGIC - **Bound replay using observed lateness.** Choose the correction interval from
# MAGIC   source delivery delays and the corrections the application needs. A longer window
# MAGIC   accepts more late evidence but increases history reads, candidate pairs and replay
# MAGIC   work. This is an application-managed cutoff, not a Spark watermark. Validate
# MAGIC   timestamps before they advance the global frontier: one future-dated report can
# MAGIC   freeze history for every MMSI. Keep too-late receptions in Bronze for a separately
# MAGIC   designed historical rebuild.
# MAGIC - **Reduce history I/O.** Evaluate liquid clustering on MMSI and event time against
# MAGIC   the affected-key and time-range reads. Profile the frozen-endpoint/support-count
# MAGIC   aggregation and global maximum timestamp query too: bounded replay does not mean
# MAGIC   every table scan is bounded. If summaries are needed, persist and recover them
# MAGIC   consistently with published assignments and preserve the same boundary semantics.
# MAGIC - **Watch the largest group.** Each grouped Pandas call materializes one MMSI's
# MAGIC   positions, seeds AND candidate edges in worker memory. Track rows and edges per
# MAGIC   MMSI, peak memory, task skew and batch duration. More workers help across MMSIs;
# MAGIC   they do not split one large group. Dense groups can still generate quadratic
# MAGIC   candidate counts. Decomposing a large key needs explicit boundary context and
# MAGIC   reconciliation so a track can cross partitions; arbitrary key salting is unsafe.
# MAGIC - **Tune candidate pruning with correctness checks.** Compare H3 resolution and
# MAGIC   neighbourhood size against the allowed travel distance in each operating region.
# MAGIC   Check candidate recall against an unpruned reference on representative samples.
# MAGIC   Smaller neighbourhoods reduce work only if they still include every eligible
# MAGIC   predecessor. The fixture's resolution 7/k=12 choice is not a global guarantee.
# MAGIC - **Operate a persistent stream.** Keep a durable checkpoint and one sequential
# MAGIC   writer for this Silver table. Size batches so processing keeps up with arrivals;
# MAGIC   remove the teaching snapshots and driver-side collections from the production path.
# MAGIC   Retain retry checks and monitor input lag, replay volume and ambiguous/tentative
# MAGIC   rates. Do not reset tables or checkpoints on a production restart.
# MAGIC
# MAGIC This notebook makes no measured throughput claim. If history reads dominate a
# MAGIC profiled workload, a stateful resolver may be worthwhile, with the equivalent
# MAGIC correction and recovery behavior described above. Retrying a batch at the current
# MAGIC frontier is different from historical backfill: replaying old batches after the
# MAGIC frontier advances will not reopen frozen decisions.
# MAGIC
# MAGIC ### From tracks to vessel activity
# MAGIC
# MAGIC The reusable output is a position history with explicit track decisions and receiver
# MAGIC provenance, plus the separate registry-association example. A production handoff
# MAGIC would attach supported vessel associations to positions and propagate changes when
# MAGIC track membership or identity evidence changes. Notebook 2 starts with resolved
# MAGIC `vessel_id` values in its own synthetic fixture and turns positions into port and
# MAGIC berth callings; it does not directly read this notebook's tables.
# MAGIC
# MAGIC Outputs remain for review until the next Run all resets this tutorial. The setup
# MAGIC cell retains its schema/volume and clears only the resolver checkpoint folder.

# COMMAND ----------

SUMMARY = {
    "schema": PREFIX,
    "raw_receptions": spark.table(BRONZE).count(),
    "streaming_batches": 4,
    "silver_positions": spark.table(SILVER).count(),
    "changed_reports": sorted(changed_reports),
    "too_late_ignored": int(oid("c10") not in a3),
    "publication": "one Delta MERGE per callback",
    "control_tables": 0,
    "distributed_by": "MMSI",
    "spatial_features": "native DBF functions",
    "checks": "passed",
}
print(json.dumps(SUMMARY, indent=2))
if "dbutils" in globals():
    dbutils.notebook.exit(json.dumps(SUMMARY))
