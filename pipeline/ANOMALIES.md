# Generate AIS errors and unusual movement for testing

The generator can add errors to reported positions while continuing the simulated
vessel journeys. It can also produce circular tracks, repeated positions and gaps.
Use these scenarios to test position checks like those in notebook **00 · Position
quality and anomalies**. The sizes, speeds and frequencies are configurable test
settings chosen for these examples.

The connected job accepts `config=fleet-anomalies.yaml`. It screens invalid inputs
before track calculations and publishes quality labels alongside resolved positions
and calling summaries. See [the pipeline guide](README.md#quality-screening) for the
reassessment and admission rules. You can also generate reports independently below.
The configuration includes an explicit synthetic land reference rectangle; replace
it and its coverage with suitable coastline data for a real land check.

## Generate reports and expected results

Run from `pipeline/` after installing its package in a local environment, for
example `python -m pip install -e .`. The pinned dependencies include `searoute`
and PyYAML. This example requires no Databricks connection and makes no UC writes.

```python
from ais_pipeline.generator.fleet import load_config, build_routes, vessel_events

path = "config/fleet-anomalies.yaml"
config = load_config(path)
routes = build_routes(path, config)

receptions, evaluation_truth = [], []
for vessel_index in range(config["vessels"]):
    for delivered, truth in vessel_events(vessel_index, config, routes):
        receptions.extend(delivered)
        evaluation_truth.append(truth)

# Feed only receptions into a detector. Keep evaluation_truth outside its inputs.
print(len(receptions), "receptions;", len(evaluation_truth), "nominal ticks")
```

`vessel_reports(index, config, routes)` retains its original feed-only iterator
API. `vessel_truth(index, config, routes)` replays the deterministic simulation to
yield only the ledger. `vessel_events` computes both in one pass and yields one
`(receptions_tuple, truth_row)` per nominal tick. The tuple is empty for a missing
report, so the ledger retains controlled gaps and ordinary baseline dropouts.

The ledger includes `vessel_index`, `tick`, `scenario_id`, `scenario_type`, baseline
and reported coordinates/times, original delivery bucket, separate baseline/scenario
dropout indicators, and emitted observation/reception IDs. A non-emitted tick has
no observation ID or reception IDs. Use these fields to compare a detector's results
with the simulated scenario. Keep them out of its input. The `slow-circle-control`
scenario provides normal circular movement for comparison with the fast circle.

## Configure the scenarios

The normal `config/fleet.yaml` is unchanged. Omit `anomalies` or set it to `[]` to
preserve its existing output exactly, including feed fields, fingerprints,
duplicate choices and release buckets. The example `config/fleet-anomalies.yaml`
contains 12 vessels and named positive/negative cases; vessels 9 and 11 retain
ordinary baseline movement.

Each scenario has a unique nonempty `id`, a `type`, a nonempty list of zero-based
`vessels`, and inclusive `start_tick` / `end_tick` values within
`reports_per_vessel`. Unknown keys, malformed values, unsupported types and any
overlapping intervals on the same vessel are rejected. Assign scenarios to separate
intervals or vessels. The configured reporting cadence sets observation times;
the delivery schedule determines when those reports arrive.

| Type | Parameters | Reported effect |
|---|---|---|
| `inland` | `land_bounds: [west, south, east, north]`, optional `jitter_metres` | Reports near the centre of an explicit synthetic land rectangle. Jitter is clamped to its interior half. These bounds are not a real coastline. |
| `jump` | `lon`, `lat`; one tick only | A displaced report; the following tick resumes baseline simulation. |
| `circle` | `center_lon`, `center_lat`, `radius_metres`, `period_seconds`; optional `phase_degrees`, `clockwise`, `motion_mode`, `reported_sog_knots` | Geodesic destinations around a centre, with a forward tangent course. `constant` motion reports configured SOG (default 16); `consistent` reports the circular arc speed. |
| `frozen` | Optional paired `lon`/`lat`; optional `reported_sog_knots` | Holds a fixed location, defaulting to the first nominal baseline position in the interval. Nonzero SOG supplies a contradiction; zero SOG supplies a stationary control. |
| `fields` | `values` selecting numeric/null `LAT`, `LON`, `SOG`, `COG`, `Heading` | Explicit unavailable, missing, nonfinite or out-of-range field fixtures. Other field names and structures are rejected. |
| `future_time` | Positive `offset_seconds` (at most one year) | Shifts reported `BaseDateTime` and `event_time_s`, while keeping delivery unchanged. |
| `gap` | No additional parameters | Suppresses receptions for the interval, while retaining the missing ticks in the evaluation ledger. |

Circle radius is limited to 1–100,000 metres; centre latitude to −89°…89°.
Latitude restrictions also apply to the simple synthetic geometry parameters.
The explicit `fields` type permits invalid coordinates intentionally. It is the
appropriate place to test decoded unavailable values such as longitude 181°,
latitude 91°, SOG 102.3 knots, COG 360°, heading 511, and provider-null variants.
Interpret these according to the actual feed contract before geometry or motion
calculations. Missing optional motion alone need not invalidate a position.

The fast example has radius 5 km and period 12 minutes, sampled every minute for
35 reports. The fast-circle and frozen-moving episodes each last long enough to
contain a full 30-minute detector window without surrounding baseline positions.
Its arc speed is about 85 knots and its inter-report implied speed
about 84 knots, while reported SOG is 16 knots. The slow-circle control has radius
100 metres and period 30 minutes, about 0.68 knots. Tune these settings for your
own tests. A position can jump when a replacement scenario starts or ends;
the generator keeps those transitions in the output.

## Reproducibility and delivery

Route advancement, jitter, dropout, duplicates and delays retain their original
per-vessel RNG. Scenario jitter uses a separate SHA-256-derived RNG keyed by seed,
scenario ID, vessel index and tick; adding a scenario does not shift baseline
draws elsewhere. Gap suppression still consumes the duplicate draw that ordinary
delivery would have consumed. Scenario order does not alter nonoverlapping results.

Perturbations happen before final observation fingerprints and before receiver
duplication. All receptions of an observation share its perturbed fields and
`obs_id`; receiver name and `reception_id` distinguish receptions. Reported time
and `event_time_s` remain consistent, even when deliberately wrong. Raw reported
values are not repaired.

When at least one scenario is enabled, every feed row also has `received_at`, an
immutable UTC timestamp at the end of its scheduled release bucket:
`start_epoch + (arrival_batch + 1) * release_seconds`. This provides a trusted
synthetic reception clock independent of a corrupted reported time; it is not a
measurement from a real receiver. Validate future-time reports against this value,
not the wall clock at retry. The existing `arrival_batch` still controls actual
release ordering. Disabled scenarios do not add this column.

No true vessel IDs, scenario labels or baseline positions enter the feed.
Simulation identity is used internally to produce fingerprints, as before. Do not
infer physical vessel identity from those deterministic observation IDs.

## Checks

From `pipeline/` run:

```sh
python -m unittest discover -s tests -p 'test_generator*.py' -v
```

Tests include a pre-change output SHA-256 oracle over fixed route legs; seeded
reproducibility; unaffected-vessel/tick and gap-delivery isolation; geodesic circle
radius, timing and motion modes; null/nonfinite fields; frozen/jump cases; duplicate
consistency; separate truth and immutable delivery; configuration validation; and
the existing real-searoute endpoint/dwell checks. These validate generator behaviour,
not detector accuracy or Databricks pipeline performance.
