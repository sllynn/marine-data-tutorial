# Generate a fleet and process its calling history

This companion job releases AIS reports into Bronze, assesses position quality, resolves tracks, associates
track evidence with a synthetic vessel registry, and computes port/berth callings.
The quality rules, track resolver and calling callback are extracted from the teaching
notebooks. `tools/sync_notebook_code.py --check` verifies they remain identical.
The quality adapter supplies the demo's reception clock and local spatial reference.
The identity bridge expresses notebook 1's evidence-matching rules as distributed
joins: every item must have a unique, consistent, time-valid registry match.

## Configure the generator

Edit `config/fleet.yaml`: named port coordinates, explicit `port_pairs`, fleet
size, observation cadence, speed range, dwell, jitter, lane offsets, reception
duplicates, dropouts and late delivery. Each pair is routed in both directions
with `searoute`, cached once, and interpolated by distance travelled. Vessels
shuttle between their configured pair, with staggered starts and port dwell.
Generation runs on Spark workers; the routing graph is evaluated per port pair.

The reusable Python entry points are `load_config`, `build_routes` and
`vessel_reports` under `ais_pipeline/generator`. They work independently of Spark
and the resolvers. `searoute==1.4.3` and `PyYAML==6.0.2` are required.

`release_schedule` is a delivery queue. Each release appends only its due reports
to Bronze, so late delivery is exercised by actual new table writes. The AIS
schema retains a null reported `IMO`. Vessel identity comes from reported name
and call sign matched to `vessel_registry`; a truth vessel ID is never supplied
as a processing input. The default fleet has distinct MMSIs so this run focuses
on connecting and measuring the notebook algorithms. Notebook 1 separately
exercises shared MMSIs and ambiguous assignments.

Location polygons are rectangles around the configured endpoints. The notebook's
H3 tessellation and chip-containment matching is reused. Replace `locations` and
rebuild `location_cells` to experiment with your own boundaries. Routes use
searoute's sea network with endpoint connectors: inspect the configured port
approaches before using the simulation to assess route realism. The default Channel
routes exercise the notebook's existing H3 resolution/neighbourhood settings.
When changing geography, check candidate recall for those settings as explained
in notebook 1.

## Deploy and run in your workspace

Install the Databricks CLI and `uv` (used to build the Python wheel), then authenticate a named profile. From this folder:

```sh
databricks bundle validate --strict --profile YOUR_PROFILE --var catalog=YOUR_CATALOG
databricks bundle deploy --profile YOUR_PROFILE --var catalog=YOUR_CATALOG
databricks bundle run ais_pipeline --profile YOUR_PROFILE --var catalog=YOUR_CATALOG
```

Use a dedicated schema (`ais_pipeline_tutorial` by default). **Each run resets
this job's tables and checkpoint folder.** Set `--var schema=YOUR_TEST_SCHEMA` on
all three commands to select a separate experiment. Run only one writer per
schema. The bundle sets `max_concurrent_runs: 1`; different deployments must also
use separate schemas. Serverless environment 6 supplies the spatial functions
and managed-table transactions used by the notebooks. Performance-optimized mode
keeps starts responsive during this interactive experiment.

Optional job parameters `vessels` and `reports` override the YAML fleet size and number of
reports per vessel. Increasing reports extends simulated observation time;
increasing vessels increases fleet size. Keep cadence and release intervals
small relative to notebook 1's 45-minute correction window. A delay outside that
window stays in Bronze and is excluded from accepted Silver positions.
The `config` parameter selects a file under `config/` (default `fleet.yaml`). To run
the opt-in anomaly examples after deployment:

```sh
databricks bundle run ais_pipeline --profile YOUR_PROFILE --var catalog=YOUR_CATALOG --params config=fleet-anomalies.yaml
```

Overrides must still include every configured anomaly vessel and tick; they are
validated before outputs are reset.

## Quality screening

Before each release reaches tracks, the job reassesses all received Bronze history
using notebook 00's field/time, neighbour-motion and overlapping circle-window rules.
`quality_observations` retains every observation, including invalid inputs, with
measurements, flags, rule outcomes, reasons and assessment time. Invalid coordinate,
timestamp or observation-payload inputs do not enter the track resolver. Optional
motion fields and advisory land/jump/circle flags do not exclude a valid position.
Reception timestamps are immutable synthetic delivery-bucket times, independent of
the reported observation clock; simulation truth never enters the detector feed.

The default spatial projection is UTM 31N for Dover–Calais. Change
`quality.projected_srid` when changing region. Optional `quality.land_wkt` and
`quality.coverage_wkt` are explicit WGS84 reference polygons; both are required
together. Without them, the inland rule is `not_assessed`. The anomaly configuration
supplies a **synthetic land rectangle**, separate from the feed, to exercise that
rule. It is not a real coastline. The notebook's Hawaiʻi mask is not used here.

Late arrivals can revise earlier quality labels. This demo recomputes its full
received history, including overlapping window membership, rather than bounding
quality corrections to the track resolver's separate 45-minute window. Grouped
Pandas holds each MMSI's history in memory: this is a small demo implementation.
`quality_inputs_staging` and `quality_metrics_staging` materialize intermediate
calculations; `quality_window_members` preserves the window evidence.

Inspect `silver_track_positions_with_quality`,
`silver_resolved_positions_with_quality` and `gold_callings_with_quality` for current
labels and calling summaries. Summaries cover resolved observations between entry
and last seen, and do not establish whether the calling is physically correct.
These views reflect revised labels without altering track or calling membership.
Advisory findings are never mutable suppression instructions. The generator's
immutable observations make input validity stable across releases; arbitrary feed
payload corrections would require downstream retraction support before admission
changes could be propagated safely.

## Inspect the result

The job drains three checkpointed `availableNow` streams in order for each
release. Silver Change Data Feed carries track/identity corrections downstream;
calling candidate selection receives both old and new vessel IDs, while deriving
visits from current Silver history. This finite job keeps the stages synchronized
between releases. It is suitable for reproducible experiments with streaming
micro-batches.

`round_metrics` records the release write, quality, track, identity and calling times.
`arrival_to_gold_s` is elapsed time from completion of the Bronze release write
to completion of Gold publication, including stream start/drain overhead. It is
**whole-release latency**, covering every vessel in that release. It excludes
post-publication counting queries. It does not measure vessel observation age or
per-record wall-clock latency. The identity bridge currently reads accepted
track history and evidence broadly; measure it separately when scaling.

Final checks cover key uniqueness, position conservation, missing reported IMO,
and visit histories for five vessels against an independent chronological
inside/outside oracle. Increase fleet size and history depth separately, compare
stage timings and query profiles, and inspect history reads and skew before
extrapolating throughput.

## Local checks

For opt-in inland/jump/circle and field-quality scenarios, see [ANOMALIES.md](ANOMALIES.md).
These generate deliberately incorrect reports alongside normal movement, with expected
results available separately for testing. The connected job applies the same quality
rules before tracking and exposes their current labels through the quality views.
The standalone [quality notebook](../notebooks/00_position_quality_and_anomalies.py)
demonstrates the checks and quality labels on its own batch of test data.

```sh
python -m pip install -e '.[test]'
python tools/sync_notebook_code.py --check
PYTHONPATH=. python -m unittest discover -s tests
```

The quality notebook's local numeric tests use PROJ through `pyproj` as an
independent coordinate-conversion reference. The Databricks notebook uses
native `ST_Transform` for projection and `pyproj.Geod.inv` for ellipsoidal distances.

After editing notebook algorithms, regenerate the extracted functions with
`python tools/sync_notebook_code.py`, then repeat the checks and job run.

The [validation record](VALIDATION.md) distinguishes historical timings from the quality-enabled validation.
