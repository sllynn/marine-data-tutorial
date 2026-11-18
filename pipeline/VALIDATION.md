# Validation

## Quality integration — 18 November 2026

All 27 local tests passed, including numeric detector controls, generator output
compatibility, configured episode detection amid baseline movement, pre-reset
configuration checks and extracted notebook-code parity.
Strict bundle validation passed with the `sme` profile.

A separate serverless environment-6 validation run passed the following checks
against the packaged quality adapter and native spatial SQL:

- A late return report changed an earlier isolated-jump result from unassessed to flagged.
- A repeated assessment reproduced all 87 observation rows and measurements,
  excluding the assessment timestamp.
- All 25 fast-circle observations were flagged; the slow-circle control was not fast.
- All 13 frozen-with-motion observations were flagged; the moored control was not.
- Invalid coordinates and a future timestamp stayed in the quality output and
  were excluded from track inputs; an advisory jump remained admitted.
- Duplicate receiver provenance was retained.
- Without a land reference, the inland check remained explicitly unassessed.

Validation run: [1042312246276586](https://fevm-geo-sme-emea.cloud.databricks.com/?o=7474660412302141#job/819004750027073/run/432417088484445).
The development validation notebook and raw results are kept outside this
distribution under `ais-community-tutorial-development/anomaly-detection/validation/connected-pipeline/`.

The corrected connected demo passed on SME in an isolated deployment and schema:
[run 73794099595548](https://fevm-geo-sme-emea.cloud.databricks.com/jobs/516547227540084/runs/73794099595548?o=7474660412302141).
It used 12 vessels and 60 one-minute ticks, with 30-minute release buckets for
three releases (the distributed example defaults to 10-minute release buckets).

- All 747 receptions and 715 distinct observations were retained in Bronze/quality output.
- Four invalid observations were withheld; all 711 accepted track positions reached resolved Silver.
- Findings included 35 fast-circle, 30 frozen-with-motion, six inland and one isolated-jump observations.
- All 15 Gold callings passed key/time checks; five sampled vessels matched the
  independent calling oracle exactly, covering five visits.
- Quality-summary position counts matched `n_positions` for every calling.

The initial eight-release connected smoke run also passed conservation and calling
checks. Its final detector check exposed that the original circle episode was
shorter than the 30-minute fitting window. The example circle and frozen-moving
episodes were extended to 35 reports, and a local regression now checks detection
with baseline reports on either side. Detector thresholds and notebook algorithms
were preserved.

## Historical connected run — 23 October 2026

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
