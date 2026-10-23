"""Extracted from 02_port_and_berth_callings.py; regenerate with tools/sync_notebook_code.py."""

import hashlib

import json

from pyspark.sql import functions as F

from delta.tables import DeltaTable

from datetime import datetime, timezone

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
