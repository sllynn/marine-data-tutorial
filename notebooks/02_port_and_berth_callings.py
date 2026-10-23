# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # From resolved positions to observed port and berth callings
# MAGIC
# MAGIC A vessel's position history tells us where it was observed. We now want to turn
# MAGIC those observations into a history of the ports and berths it visited: where did it
# MAGIC call, when was it first and last seen there, and when did we observe it leaving?
# MAGIC This gives us a calling timeline we can query by vessel, location and time.
# MAGIC
# MAGIC ## What are we computing?
# MAGIC
# MAGIC A **calling** is a consecutive run of observations inside a port or berth polygon.
# MAGIC The first inside observation starts the run. Further inside observations extend
# MAGIC it, and the next outside observation supplies departure evidence. A long gap
# MAGIC between reports keeps the run together; the supporting observations describe
# MAGIC what we have seen during that interval.
# MAGIC
# MAGIC A berth sits within a port, so a vessel can have a port calling containing one or
# MAGIC more berth visits. We compute each location's intervals separately. Vessel ID,
# MAGIC location hierarchy and overlapping times let us relate the visits afterwards.
# MAGIC
# MAGIC ## What data do we use?
# MAGIC
# MAGIC - **Vessel positions:** UTC observation time (`BaseDateTime`), `LON`, `LAT`, an
# MAGIC   observation identifier (`obs_id`) and the resolved vessel identifier (`vessel_id`).
# MAGIC   The feed also carries `MMSI`, `IMO` and the other reported AIS attributes.
# MAGIC - **Location polygons:** a `location_id`, `location_type` (`port` or `berth`) and geometry.
# MAGIC - **Delivery batches:** `arrival_batch` lets us deliver the reports in a chosen
# MAGIC   order, including repeated receptions and observations that arrive late.
# MAGIC
# MAGIC The identity stage introduced in the first notebook supplies `vessel_id`.
# MAGIC Here we begin with two resolved vessels, A and B, each with its own MMSI. A lets
# MAGIC us follow entry, departure and corrections. B lets us follow an open visit through
# MAGIC several batches. The locations are a rectangular port and a berth within it.
# MAGIC
# MAGIC ## How will we compute it?
# MAGIC
# MAGIC 1. Store receptions in Bronze and deduplicate positions by `obs_id` into Silver.
# MAGIC 2. Use an H3 index to find candidate position–location matches, then check the geometry.
# MAGIC 3. Order each vessel's complete position history and identify consecutive inside runs.
# MAGIC 4. Summarize each run and find the next outside observation.
# MAGIC 5. Publish one row per visit in Gold.
# MAGIC
# MAGIC Spark SQL joins, windows and aggregations perform the calculation. Silver supplies
# MAGIC the position history; Gold holds the calling intervals. As new micro-batches arrive,
# MAGIC we retrieve the affected histories and recompute the relevant visits.
# MAGIC
# MAGIC ## What does the output look like?
# MAGIC
# MAGIC Consider A's first five observations: outside at minute 0, inside the port and berth
# MAGIC at minutes 10, 20 and 30, then outside at minute 40. Gold contains:
# MAGIC
# MAGIC | vessel_id | location_id | entry_ts | last_seen_ts | exit_ts | n_positions |
# MAGIC |---|---|---:|---:|---:|---:|
# MAGIC | A | port | 10 | 30 | 40 | 3 |
# MAGIC | A | berth | 10 | 30 | 40 | 3 |
# MAGIC
# MAGIC The example table shows elapsed minutes; the stored time columns are UTC timestamps.
# MAGIC `entry_ts` is the first observed inside position, `last_seen_ts` the latest inside
# MAGIC position, and `exit_ts` the first outside position after the run. Here the departure
# MAGIC is bracketed by minutes 30–40. A visit awaiting departure evidence has a null `exit_ts`.
# MAGIC `as_of_ts` records when a row was published.
# MAGIC
# MAGIC We will first prepare the data and spatial index, then explain how position
# MAGIC numbering identifies visits. With that foundation in place, we will introduce
# MAGIC streaming batches and show how a late observation can change the answer.

# COMMAND ----------

import hashlib
import json
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime, timezone

AIS_SCHEMA = """MMSI string, BaseDateTime timestamp, LAT double, LON double,
SOG double, COG double, Heading double, VesselName string, IMO string, CallSign string,
VesselType int, Status int, Length double, Width double, Draft double, Cargo int,
TransceiverClass string"""
AIS_COLUMNS = [field.strip().split()[0] for field in AIS_SCHEMA.replace("\n", " ").split(",")]


def prepare_reports(reports):
    """Parse AIS fields in UTC and derive the replay clock and position fingerprint.

    Calling derivation uses vessel_id, BaseDateTime, LAT and LON. Reported AIS fields
    remain in Bronze for analysis.
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
    return reports


# Select a catalog where you can create this tutorial's dedicated schema.
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "ais_callings_tutorial", "Tutorial schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
PREFIX = f"{CATALOG}.{SCHEMA}"
BRONZE = f"{PREFIX}.bronze_receptions"
silver = f"{PREFIX}.silver_positions"
location_cells_table = f"{PREFIX}.location_cells"
locations_table = f"{PREFIX}.locations"
callings_tbl = f"{PREFIX}.gold_callings"
staging_tbl = f"{PREFIX}.batch_positions_staging"
recomputed_tbl = f"{PREFIX}.recomputed_callings"
checkpoint = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/callings"
retraction_window_days = 7
RAW_SCHEMA = AIS_SCHEMA + ", obs_id string, event_time_s long, vessel_id string, arrival_batch int"

# Run all resets this tutorial's data and its calling checkpoint.
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {PREFIX}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {PREFIX}.checkpoints")
spark.sql(f"CREATE OR REPLACE TABLE {BRONZE} ({RAW_SCHEMA}) USING DELTA")
spark.sql(f"CREATE OR REPLACE TABLE {silver} ({RAW_SCHEMA}) USING DELTA")
# Managed Delta staging tables; each micro-batch replaces their contents.
spark.sql(f"""CREATE OR REPLACE TABLE {staging_tbl} ({RAW_SCHEMA}) USING DELTA
TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')""")
spark.sql(f"""CREATE OR REPLACE TABLE {recomputed_tbl} (
 vessel_id STRING, location_id STRING, entry_ts TIMESTAMP, last_seen_ts TIMESTAMP,
 last_pos_num INT, n_positions BIGINT, exit_ts TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')""")
spark.sql(f"""CREATE OR REPLACE TABLE {callings_tbl} (
 vessel_id STRING, location_id STRING, entry_ts TIMESTAMP, last_seen_ts TIMESTAMP,
 exit_ts TIMESTAMP, n_positions BIGINT, as_of_ts TIMESTAMP
) USING DELTA CLUSTER BY (vessel_id, location_id)
TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')""")
dbutils.fs.rm(checkpoint, recurse=True)
FIXTURE_MMSIS = {
    "A": "235000001", "B": "235000002",
}


def observation(vessel_id, minute, LON, LAT=51.0):
    key = f"{vessel_id}|{minute}|{LON:.6f}|{LAT:.6f}"
    report = dict.fromkeys(AIS_COLUMNS)
    report.update(obs_id=hashlib.sha256(key.encode()).hexdigest()[:20],
                  vessel_id=vessel_id,
                  MMSI=FIXTURE_MMSIS[vessel_id],
                  event_time_s=int(minute * 60),
                  BaseDateTime=datetime.fromtimestamp(minute*60, tz=timezone.utc),
                  LON=float(LON), LAT=float(LAT), TransceiverClass="A")
    return report

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Prepare AIS positions and locations
# MAGIC
# MAGIC The feed uses the familiar AIS columns: `MMSI`, `BaseDateTime`, `LAT`, `LON`,
# MAGIC `SOG`, `COG` and the remaining reported vessel attributes. `IMO` can be null;
# MAGIC the upstream identity stage has already supplied the `vessel_id` used here.
# MAGIC `obs_id` identifies a position so repeated receptions contribute one Silver row.
# MAGIC
# MAGIC The fixture uses elapsed minutes from the Unix epoch. A starts outside, visits
# MAGIC the port and berth, then leaves. B visits the port with wider gaps between reports.
# MAGIC The port and berth have their own IDs and types, so one position can match both.
# MAGIC
# MAGIC `RECEPTION_BATCHES` groups reports by delivery time. Their `BaseDateTime` values
# MAGIC retain observation time. Keeping those two clocks separate lets us introduce late
# MAGIC evidence after an initial calling has been published.


# COMMAND ----------

LOCATIONS = [
    ("port", "port", "POLYGON ((1 50.99, 1.1 50.99, 1.1 51.03, 1 51.03, 1 50.99))"),
    ("berth", "berth", "POLYGON ((1.03 50.999, 1.05 50.999, 1.05 51.001, 1.03 51.001, 1.03 50.999))"),
]
locations = spark.createDataFrame(LOCATIONS, "location_id string, location_type string, wkt string")
P = {
    "a0": observation("A", 0, .98),
    "a8": observation("A", 8, 1.01),
    "a10": observation("A", 10, 1.04),
    "a20": observation("A", 20, 1.045),
    "a25": observation("A", 25, 1.12),  # Late OUT: splits both port and berth visits.
    "a30": observation("A", 30, 1.035),  # Inside the berth.
    "a40": observation("A", 40, 1.12),
    "b5": observation("B", 5, 1.08, 51.02),
    "b35": observation("B", 35, 1.09, 51.02),  # Inside the port.
    "b65": observation("B", 65, 1.08, 51.02),
    "b70": observation("B", 70, 1.12, 51.02),
}


# Reports grouped by delivery time, independently of their observation times.
# A10 is delivered twice to demonstrate deduplication by observation ID.
RECEPTION_BATCHES = [
    [P[n] for n in ("a0", "a10", "a20", "a30", "a40", "b5", "b35", "a10")],
    [P["a25"]],
    [P["a8"]],
    [P[n] for n in ("b65", "b70")],
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Find position–location matches with H3
# MAGIC
# MAGIC We first build a spatial index for the locations and persist it for the stream to
# MAGIC reuse. Tessellation divides each polygon into H3 **chips**: the part of the polygon
# MAGIC covered by each cell. Each row records the cell, its resolution, the location ID
# MAGIC and the chip geometry.
# MAGIC
# MAGIC A position is indexed at each resolution present in the location index. Joining
# MAGIC on cell and resolution gives us candidate locations. A **core** chip means the
# MAGIC whole cell lies inside its location, so the position matches directly. For a
# MAGIC **boundary** chip, `ST_Contains` checks the point against the chip geometry.
# MAGIC
# MAGIC These two locations use resolution 8. The candidate join limits the geometry work
# MAGIC to positions and locations sharing an H3 cell. We use the resulting matches for
# MAGIC both the new micro-batch and the retrieved vessel history.


# COMMAND ----------

# Persist the location chips once for reuse by every micro-batch.
locations.write.mode("overwrite").saveAsTable(locations_table)
spark.sql(f"""
CREATE OR REPLACE TABLE {location_cells_table}
USING DELTA CLUSTER BY (cell) AS
WITH prepared AS (
 SELECT location_id, 8 AS target_res,
        ST_AsBinary(ST_GeomFromWKT(wkt, 4326)) AS geom_wkb
 FROM {locations_table}
)
SELECT location_id, target_res, t.cellid AS cell, t.core, t.chip AS chip_wkb
FROM prepared
LATERAL VIEW inline(h3_tessellateaswkb(geom_wkb, target_res)) t
""")
target_res_values = [r.target_res for r in spark.sql(
    f"SELECT DISTINCT target_res FROM {location_cells_table} ORDER BY target_res"
).collect()]
ancestor_array = "array(" + ", ".join(
    f"named_struct('ancestor_cell', h3_longlatash3(LON, LAT, {r}), 'res', {r})"
    for r in target_res_values
) + ")"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Turn inside observations into visits
# MAGIC
# MAGIC The key is to number observations twice: once across the vessel's complete
# MAGIC position sequence, and once across just its inside matches for a location.
# MAGIC
# MAGIC | minute | inside? | position number | inside number | difference |
# MAGIC |---:|---|---:|---:|---:|
# MAGIC | 0 | outside | 1 | — | — |
# MAGIC | 10 | inside | 2 | 1 | 1 |
# MAGIC | 20 | inside | 3 | 2 | 1 |
# MAGIC | 25 | outside | 4 | — | — |
# MAGIC | 30 | inside | 5 | 3 | 2 |
# MAGIC | 40 | outside | 6 | — | — |
# MAGIC
# MAGIC While the inside observations are consecutive, both numbers advance together and
# MAGIC their difference stays constant. An outside observation advances the complete
# MAGIC sequence. The next inside match therefore has a different difference and starts
# MAGIC a new run. Grouping by vessel, location and difference gives us the visits.
# MAGIC
# MAGIC For each run we take the first and last inside times and count its positions.
# MAGIC The next position in the complete sequence supplies `exit_ts`; when there is no
# MAGIC next position, the visit stays open. This is why we retain outside positions in
# MAGIC Silver alongside inside matches.
# MAGIC
# MAGIC ### Apply that calculation to a micro-batch
# MAGIC
# MAGIC A new report can extend an open visit or revise an existing one. We select affected
# MAGIC vessel–location pairs from three sources: new inside matches, open visits for
# MAGIC vessels in the batch, and closed visits whose entry-to-exit interval contains a
# MAGIC new report. A late outside report can therefore trigger a correction even when
# MAGIC it produces no inside match.
# MAGIC
# MAGIC For each affected vessel we retrieve history from the earliest affected entry or
# MAGIC new inside match, with a seven-day lookback cap measured from the latest Silver
# MAGIC observation. We then number that history and recompute its visits.
# MAGIC
# MAGIC ```text
# MAGIC Persist the incoming micro-batch.
# MAGIC Match new positions to indexed locations.
# MAGIC Select affected vessel/location pairs.
# MAGIC Find each vessel's replay start, bounded by the lookback cap.
# MAGIC Read and number the vessel's full position sequence from that start.
# MAGIC Number its inside matches for each candidate location.
# MAGIC Group equal sequence-number differences into visits.
# MAGIC Summarize each visit and look up its next outside position.
# MAGIC Store the recomputed visits in the reusable staging table.
# MAGIC Upsert the visits and remove obsolete rows within the affected scope.
# MAGIC ```
# MAGIC
# MAGIC ### Publish a consistent calling history
# MAGIC
# MAGIC The key is `(vessel_id, location_id, entry_ts)`. A late inside report can move the
# MAGIC entry and change this key; a late outside report can split one visit into two.
# MAGIC The MERGE publishes the recomputed visits and a scoped DELETE removes old rows
# MAGIC that the replay no longer produces. Both execute in one transaction. Before that
# MAGIC transaction, we write the recomputed visits to a managed Delta staging table so
# MAGIC MERGE and DELETE share one fixed result. Setup creates both staging tables with
# MAGIC their table properties; each micro-batch overwrites their contents. One stream
# MAGIC owns these staging tables, and Silver remains the source of position history.


# COMMAND ----------

def merge_micro_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # Use the micro-batch session for its SQL views and transaction.
    s = batch_df.sparkSession

    # Overwrite the existing managed table, retaining its schema and properties.
    (batch_df.select(*s.table(staging_tbl).columns)
        .write.insertInto(staging_tbl, overwrite=True))
    s.sql(f"CREATE OR REPLACE TEMP VIEW batch_positions AS SELECT * FROM {staging_tbl}")

    # ---------------------------------------------------------------------
    # 1. batch_matches — the new inside matches in this batch.
    # ---------------------------------------------------------------------
    s.sql(f"""
    CREATE OR REPLACE TEMP VIEW batch_matches AS
    WITH position_ancestors AS (
      SELECT
        p.vessel_id, p.VesselName, p.VesselType, p.MMSI,
        p.BaseDateTime, p.LON, p.LAT, p.SOG, p.COG, p.Status,
        a.ancestor_cell, a.res
      FROM batch_positions p
      LATERAL VIEW inline({ancestor_array}) a
    )
    SELECT
      pa.vessel_id, pa.VesselName, pa.VesselType, pa.MMSI,
      sc.location_id, pa.BaseDateTime,
      pa.LON, pa.LAT, pa.SOG, pa.COG, pa.Status,
      sc.core
    FROM position_ancestors pa
    JOIN {location_cells_table} sc
      ON sc.cell       = pa.ancestor_cell
     AND sc.target_res = pa.res
    WHERE sc.core
       OR ST_Contains(ST_GeomFromWKB(sc.chip_wkb, 4326),
                      ST_Point(pa.LON, pa.LAT, 4326))
    """)

    # ---------------------------------------------------------------------
    # 2. candidate_pairs — union the three sources of affected visits.
    # ---------------------------------------------------------------------
    s.sql(f"""
    CREATE OR REPLACE TEMP VIEW candidate_pairs AS
    SELECT DISTINCT vessel_id, location_id FROM batch_matches
    UNION
    -- Open callings for vessels with new positions in this batch
    SELECT c.vessel_id, c.location_id
    FROM {callings_tbl} c
    WHERE c.exit_ts IS NULL
      AND c.vessel_id IN (SELECT DISTINCT vessel_id FROM batch_positions)
    UNION
    -- Closed callings whose envelope contains a new batch position (late-OUT splits)
    SELECT c.vessel_id, c.location_id
    FROM {callings_tbl} c
    JOIN batch_positions p
      ON p.vessel_id = c.vessel_id
     AND p.BaseDateTime BETWEEN c.entry_ts AND c.exit_ts
    WHERE c.exit_ts IS NOT NULL
    """)

    # ---------------------------------------------------------------------
    # 3. vessel_lookback — capped at retraction_window
    # ---------------------------------------------------------------------
    s.sql(f"""
    CREATE OR REPLACE TEMP VIEW vessel_lookback AS
    WITH cap AS (
      SELECT max(BaseDateTime) - INTERVAL {retraction_window_days} DAYS AS cap_ts
      FROM {silver}
    )
    SELECT cp.vessel_id,
      GREATEST(
        LEAST(
          coalesce(MIN(c.entry_ts),  TIMESTAMP'9999-01-01'),
          coalesce(MIN(bm.BaseDateTime), TIMESTAMP'9999-01-01')
        ),
        (SELECT cap_ts FROM cap)
      ) AS lookback_ts
    FROM candidate_pairs cp
    LEFT JOIN {callings_tbl} c
      ON c.vessel_id = cp.vessel_id AND c.location_id = cp.location_id
    LEFT JOIN batch_matches bm
      ON bm.vessel_id = cp.vessel_id AND bm.location_id = cp.location_id
    GROUP BY cp.vessel_id
    """)

    # ---------------------------------------------------------------------
    # 4. numbered_positions — global per-vessel position numbering
    # ---------------------------------------------------------------------
    s.sql(f"""
    CREATE OR REPLACE TEMP VIEW numbered_positions AS
    SELECT p.*,
      row_number() OVER (PARTITION BY p.vessel_id ORDER BY p.BaseDateTime) AS pos_num
    FROM {silver} p
    JOIN vessel_lookback l ON l.vessel_id = p.vessel_id
    WHERE p.BaseDateTime >= l.lookback_ts
    """)

    # ---------------------------------------------------------------------
    # 5. matched — H3 ancestor + chip refinement, scoped to candidate_pairs
    # ---------------------------------------------------------------------
    s.sql(f"""
    CREATE OR REPLACE TEMP VIEW matched AS
    WITH expanded AS (
      SELECT np.vessel_id, np.BaseDateTime, np.pos_num, np.LON, np.LAT,
             a.ancestor_cell, a.res
      FROM numbered_positions np
      LATERAL VIEW inline({ancestor_array}) a
    )
    SELECT e.vessel_id, sc.location_id, e.BaseDateTime, e.pos_num
    FROM expanded e
    JOIN {location_cells_table} sc
      ON sc.cell       = e.ancestor_cell
     AND sc.target_res = e.res
    JOIN candidate_pairs cp
      ON cp.vessel_id = e.vessel_id AND cp.location_id = sc.location_id
    WHERE sc.core
       OR ST_Contains(ST_GeomFromWKB(sc.chip_wkb, 4326),
                      ST_Point(e.LON, e.LAT, 4326))
    """)

    # ---------------------------------------------------------------------
    # 6. numbered_matched — per-(vessel, shape) match numbering
    # ---------------------------------------------------------------------
    s.sql(f"""
    CREATE OR REPLACE TEMP VIEW numbered_matched AS
    SELECT *,
      row_number() OVER (PARTITION BY vessel_id, location_id ORDER BY BaseDateTime) AS match_num
    FROM matched
    """)

    # ---------------------------------------------------------------------
    # 7+8. callings_from_diff — gaps-and-islands aggregation + exit_ts lookup
    # ---------------------------------------------------------------------
    s.sql(f"""
    CREATE OR REPLACE TEMP VIEW callings_from_diff AS
    WITH derived AS (
      SELECT vessel_id, location_id,
        MIN(BaseDateTime) AS entry_ts,
        MAX(BaseDateTime) AS last_seen_ts,
        MAX(pos_num)  AS last_pos_num,
        COUNT(*)      AS n_positions
      FROM numbered_matched
      GROUP BY vessel_id, location_id, (pos_num - match_num)
    )
    -- exit_ts: by construction, the position at (vessel_id, pos_num = last_pos_num+1)
    -- is necessarily OUT for this shape. LEFT JOIN avoids the correlated-scalar-
    -- subquery restriction Spark imposes on uncorrelated outer references.
    SELECT
      d.vessel_id, d.location_id, d.entry_ts, d.last_seen_ts,
      d.last_pos_num, d.n_positions,
      next_pos.BaseDateTime AS exit_ts
    FROM derived d
    LEFT JOIN numbered_positions next_pos
      ON  next_pos.vessel_id = d.vessel_id
     AND  next_pos.pos_num   = d.last_pos_num + 1
    """)

    # Evaluate the calling derivation once. Publication reads this fixed result.
    s.sql(f"""
    INSERT OVERWRITE TABLE {recomputed_tbl}
    SELECT vessel_id, location_id, entry_ts, last_seen_ts,
           last_pos_num, n_positions, exit_ts
    FROM callings_from_diff
    """)

    # ---------------------------------------------------------------------
    # 9. Atomic UPDATE + INSERT + DELETE
    # ---------------------------------------------------------------------
    # MERGE + DELETE wrapped in a single BEGIN ATOMIC transaction so the
    # downstream front end never sees an inconsistent intermediate state
    # between the upsert and the orphan cleanup. Requires the target table
    # to have the `delta.feature.catalogManaged` feature — set above.
    s.sql(f"""
    BEGIN ATOMIC

      -- (a+b) MERGE INTO handles UPDATE and INSERT in one statement.
      --       Spark SQL doesn't support `UPDATE ... FROM <joined-table>`;
      --       MERGE is the canonical update-with-join pattern.
      MERGE INTO {callings_tbl} AS c
      USING {recomputed_tbl} AS n
      ON  c.vessel_id = n.vessel_id
      AND c.location_id  = n.location_id
      AND c.entry_ts  = n.entry_ts
      WHEN MATCHED THEN UPDATE SET
          last_seen_ts = n.last_seen_ts,
          exit_ts      = n.exit_ts,
          n_positions  = n.n_positions,
          as_of_ts     = current_timestamp()
      WHEN NOT MATCHED THEN INSERT (
          vessel_id, location_id, entry_ts, last_seen_ts, exit_ts, n_positions, as_of_ts
      ) VALUES (
          n.vessel_id, n.location_id, n.entry_ts, n.last_seen_ts, n.exit_ts,
          n.n_positions, current_timestamp()
      );

      -- (c) DELETE orphans inside the affected window that the
      --     re-derivation no longer produces — **scoped to locations that
      --     are actually being re-derived in this batch** (candidate_pairs).
      --
      --     Without the candidate_pairs scope, this clause would delete
      --     every existing calling for the vessel whose entry_ts >=
      --     lookback_ts, even for locations the vessel left several batches
      --     ago and that aren't in candidate_pairs (so callings_from_diff
      --     legitimately doesn't include them — they're not orphans,
      --     they're simply out-of-scope for this batch).
      DELETE FROM {callings_tbl}
       WHERE EXISTS (
           SELECT 1 FROM vessel_lookback l
            WHERE l.vessel_id = {callings_tbl}.vessel_id
              AND {callings_tbl}.entry_ts >= l.lookback_ts)
         AND EXISTS (
           SELECT 1 FROM candidate_pairs cp
            WHERE cp.vessel_id = {callings_tbl}.vessel_id
              AND cp.location_id  = {callings_tbl}.location_id)
         AND NOT EXISTS (
           SELECT 1 FROM {recomputed_tbl} n
            WHERE n.vessel_id = {callings_tbl}.vessel_id
              AND n.location_id  = {callings_tbl}.location_id
              AND n.entry_ts  = {callings_tbl}.entry_ts);

    END
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Watch the answer change as reports arrive
# MAGIC
# MAGIC We write each reception batch to Bronze, deduplicate its positions into Silver,
# MAGIC and let the calling stream consume the new Silver rows. Each `availableNow` query
# MAGIC processes the available data and stops. Reusing one checkpoint carries the stream
# MAGIC forward between batches.
# MAGIC
# MAGIC | Batch | New evidence | Expected calling history |
# MAGIC |---|---|---|
# MAGIC | 1 | A inside at 10, 20, 30 and outside at 40; B inside at 5, 35 | One A visit per location; B has an open port visit |
# MAGIC | 2 | A's late outside report at 25 | A's visit splits into runs at 10–20 and 30 |
# MAGIC | 3 | A's late inside report at 8, inside the port | The first port visit starts at 8; its old entry key disappears |
# MAGIC | 4 | B inside at 65, outside at 70 | B's port visit extends and closes |
# MAGIC
# MAGIC The duplicate A10 reception in batch 1 produces one Silver position. A8 is outside
# MAGIC the berth, so the berth's first inside observation remains at minute 10. All these
# MAGIC observations fall within the configured replay window. We save a small snapshot
# MAGIC after each query so the checks below can compare the published answers.


# COMMAND ----------

SNAPSHOTS = []
for arrival_batch, rows in enumerate(RECEPTION_BATCHES):
    incoming = spark.createDataFrame(
        [{**r, "arrival_batch": arrival_batch} for r in rows], RAW_SCHEMA
    )
    incoming.write.mode("append").saveAsTable(BRONZE)
    incoming = prepare_reports(incoming).dropDuplicates(["obs_id"])
    (DeltaTable.forName(spark, silver).alias("target")
     .merge(incoming.alias("incoming"), "target.obs_id = incoming.obs_id")
     .whenNotMatchedInsertAll().execute())
    query = (spark.readStream.table(silver).writeStream
             .foreachBatch(merge_micro_batch)
             .option("checkpointLocation", checkpoint)
             .trigger(availableNow=True).start())
    query.awaitTermination()
    SNAPSHOTS.append([r.asDict() for r in spark.table(callings_tbl).collect()])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Check the calling history
# MAGIC
# MAGIC The checks follow the story above. First, A has one visit supported by three inside
# MAGIC positions. The late outside report splits it and supplies the first visit's exit
# MAGIC at minute 25. The next late report moves the first port entry to minute 8, while
# MAGIC the berth keeps its entry at 10. Comparing the complete visit lists also checks
# MAGIC that obsolete calling rows have been removed.
# MAGIC
# MAGIC B's calling starts open, retains all three inside observations across the reporting
# MAGIC gaps, and closes at minute 70. We also check that processing B leaves A's visits
# MAGIC unchanged, that duplicate A10 contributes one Silver position, and that Gold has
# MAGIC one row per natural calling key.


# COMMAND ----------

def visits(snapshot, vessel, location):
    return sorted([r for r in snapshot if r["vessel_id"] == vessel
                   and r["location_id"] == location], key=lambda r: r["entry_ts"])


def minute(t):
    return int(t.replace(tzinfo=timezone.utc).timestamp() // 60) if t is not None else None


s0, s1, s2, s3 = SNAPSHOTS
assert len(visits(s0, "A", "port")) == 1
assert visits(s0, "A", "port")[0]["n_positions"] == 3
assert [(minute(r["entry_ts"]), minute(r["last_seen_ts"]), minute(r["exit_ts"]))
        for r in visits(s1, "A", "port")] == [(10, 20, 25), (30, 30, 40)]
assert [(minute(r["entry_ts"]), minute(r["last_seen_ts"]), minute(r["exit_ts"]))
        for r in visits(s2, "A", "port")] == [(8, 20, 25), (30, 30, 40)]
assert len(visits(s2, "A", "berth")) == 2
assert minute(visits(s2, "A", "berth")[0]["entry_ts"]) == 10
assert visits(s0, "B", "port")[0]["exit_ts"] is None
b = visits(s3, "B", "port")[0]
assert (minute(b["entry_ts"]), minute(b["last_seen_ts"]), minute(b["exit_ts"])) == (5, 65, 70)
assert b["n_positions"] == 3
assert visits(s2, "A", "port") == visits(s3, "A", "port")
assert spark.table(silver).where(F.col("obs_id") == P["a10"]["obs_id"]).count() == 1
assert spark.table(callings_tbl).count() == spark.table(callings_tbl).select(
    "vessel_id", "location_id", "entry_ts"
).distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Query the vessel's calling timeline
# MAGIC
# MAGIC Gold now provides a compact activity history: which location a vessel visited,
# MAGIC its first and last inside observations, the next observed outside position and
# MAGIC the number of supporting reports. Join `location_id` to the location table to
# MAGIC include the port or berth type and relate nested visits through time overlap.
# MAGIC
# MAGIC For an observed visit duration, subtract `entry_ts` from `last_seen_ts`. Use
# MAGIC `last_seen_ts` and `exit_ts` together to understand the departure interval.
# MAGIC An open visit has a null `exit_ts` and can extend when the next batch arrives.
# MAGIC
# MAGIC The same calculation runs for every micro-batch: identify affected visits, retrieve
# MAGIC their position history within the replay cap, recompute the intervals, and publish
# MAGIC the updated timeline. This turns the resolved vessel positions from our first
# MAGIC example into activity that readers can query and build on.


# COMMAND ----------

display(spark.table(callings_tbl).orderBy("vessel_id", "location_id", "entry_ts"))
SUMMARY = {
    "schema": PREFIX,
    "example": "port_and_berth_callings",
    "streaming_batches": len(RECEPTION_BATCHES),
    "bronze_receptions": spark.table(BRONZE).count(),
    "silver_positions": spark.table(silver).count(),
    "callings": spark.table(callings_tbl).count(),
    "assertions": "passed",
}
print(json.dumps(SUMMARY, indent=2))
dbutils.notebook.exit(json.dumps(SUMMARY))
