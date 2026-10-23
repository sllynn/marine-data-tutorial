"""End-to-end checks, including an independent visit oracle for a small sample."""
import re
from datetime import datetime
from pyspark.sql import functions as F


def validate(spark, prefix):
    tracks = spark.table(prefix + ".silver_track_positions")
    resolved = spark.table(prefix + ".silver_resolved_positions")
    gold = spark.table(prefix + ".gold_callings")
    assert tracks.count() > 0, "No accepted positions"
    assert gold.count() > 0, "No port calls: extend simulation to reach port"
    assert tracks.select("obs_id").distinct().count() == tracks.count()
    assert resolved.count() == tracks.count(), "Identity association lost positions"
    assert gold.groupBy("vessel_id", "location_id", "entry_ts").count().where("count > 1").isEmpty()
    assert gold.where("n_positions <= 0 OR last_seen_ts < entry_ts OR exit_ts <= last_seen_ts").isEmpty()
    assert resolved.where("IMO IS NOT NULL").isEmpty(), "Fixture should retain missing reported IMO"
    assert resolved.where("vessel_id IS NOT NULL AND resolved_imo IS NULL").isEmpty()
    # Pure Python sequential inside/outside processing independently checks the
    # SQL gaps-and-islands implementation. Sampling bounds driver inspection.
    sample = [r.vessel_id for r in resolved.where("vessel_id IS NOT NULL").select("vessel_id").distinct().orderBy("vessel_id").limit(5).collect()]
    locations = spark.table(prefix + ".locations").collect()
    expected = {}
    for vessel in sample:
        positions = resolved.where(F.col("vessel_id") == vessel).select("BaseDateTime", "LON", "LAT").orderBy("BaseDateTime").collect()
        for location in locations:
            nums = [float(x) for x in re.findall(r"-?\d+(?:\.\d+)?", location.wkt)]
            xs, ys = nums[::2], nums[1::2]
            current = None
            for p in positions:
                inside = min(xs) < p.LON < max(xs) and min(ys) < p.LAT < max(ys)
                if inside:
                    if current is None:
                        current = [p.BaseDateTime, p.BaseDateTime, None, 1]
                    else:
                        current[1] = p.BaseDateTime
                        current[3] += 1
                elif current is not None:
                    current[2] = p.BaseDateTime
                    expected[(vessel, location.location_id, current[0])] = tuple(current[1:])
                    current = None
            if current is not None:
                expected[(vessel, location.location_id, current[0])] = tuple(current[1:])
    actual = {(r.vessel_id,r.location_id,r.entry_ts):(r.last_seen_ts,r.exit_ts,r.n_positions)
              for r in gold.where(F.col("vessel_id").isin(sample)).collect()}
    assert actual == expected, f"Calling oracle mismatch: expected {expected}, actual {actual}"
    return dict(sampled_vessels=len(sample), oracle_callings=len(expected),
                silver_positions=tracks.count(), gold_callings=gold.count(),
                unresolved_positions=resolved.where("vessel_id IS NULL").count())
