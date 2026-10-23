# Validation — 23 October 2026

The serverless environment-6 Lakeflow job completed successfully with the default
Dover–Calais route pair, 20 vessels and 40 one-minute observations per vessel.
Five delivery releases exercised repeated receptions and late reports.

- Bronze: 835 receptions.
- Track Silver: 798 distinct accepted positions.
- Resolved Silver: 798 positions, all with supported registry matches.
- Gold: 19 calling records.
- Position conservation, unique observation/visit keys, timestamp ordering and
  registry-derived IMO checks passed.
- Five sampled vessels matched an independent chronological inside/outside
  reference exactly (four visits across the sampled vessels).
- The track functions and calling callback matched the teaching notebook source.
- Local tests passed for deterministic generation, real searoute legs, endpoints,
  port dwell, repeated receptions and delayed release buckets.

| Release | Track seconds | Identity seconds | Calling seconds | Bronze release to Gold seconds |
|---|---:|---:|---:|---:|
| 0 | 38.89 | 16.58 | 31.77 | 87.24 |
| 1 | 26.27 | 16.18 | 31.23 | 73.68 |
| 2 | 23.76 | 14.74 | 28.32 | 66.82 |
| 3 | 24.58 | 14.27 | 27.82 | 66.67 |
| 4 | 23.40 | 14.48 | 30.75 | 68.62 |

Times cover each whole release and include starting/draining the three streams.
The final release contained five delayed positions. Setup, route generation and
post-publication inspection queries are outside the arrival-to-Gold measurement.
These results establish integration behaviour for this small scenario. The
separate calling-only scale observations in the root README cover larger inputs;
the full generated pipeline should be measured independently as fleet/history
size and location count change.

A separate local iterator check generated 188,004 receptions for 1,000 vessels
with 180 nominal observations each in 0.64 seconds. This measures Python emission
through the iterator, excluding Spark, Delta writes and downstream processing.
