# From AIS positions to vessel activity

Two Databricks notebooks accompany the blog: resolve position reports into plausible tracks, then turn resolved vessel positions into port and berth callings. Both examples include streaming input, late observations and executable checks.

## Start here

| Notebook | What you will build |
|---|---|
| [01 · Identity, tracks and late data](notebooks/01_identity_tracks_and_late_data.py) | H3 candidate generation, spatial motion checks, distributed track replay and an introduction to registry identity evidence |
| [02 · Port and berth callings](notebooks/02_port_and_berth_callings.py) | Indexed location matching, consecutive inside runs, departure evidence and atomic publication of corrected calling histories |

Read them in order. Each notebook generates its own small synthetic dataset and runs independently. Notebook 2 begins with resolved `vessel_id` values in its fixture. The first notebook illustrates registry association separately from its streamed track output.

## Run in Databricks

1. Import the two `.py` files under `notebooks/` into your workspace as Python source notebooks. Keep their names to preserve the reading order.
2. Attach serverless compute. Environment version **6** was used for validation; it supplies the native spatial/H3 functions, grouped Pandas support, Delta and matplotlib used here. Notebook 2 also uses managed-table transactions (`BEGIN ATOMIC`).
3. In each notebook's setup cell, set the **catalog** widget to a catalog where you can create schemas, managed Delta tables and volumes. The default is `main`.
4. Keep the separate default schemas, `ais_identity_tracks_tutorial` and `ais_callings_tutorial`, or select dedicated tutorial schemas of your own. Run all.

**Run all resets the selected tutorial tables and its checkpoint folder.** Use dedicated schemas and run one copy of each notebook at a time against a given schema. The notebooks retain their outputs for inspection and recreate their fixtures when you run them again.

No external AIS files, registry downloads, Python H3 package or companion wheel are required. The notebooks create their own checkpoint volumes under the selected schemas. To remove the outputs afterwards, stop the tutorial streams and drop the dedicated schemas and their contents.

## Data and outputs

The reported feed fields are:

```text
MMSI,BaseDateTime,LAT,LON,SOG,COG,Heading,VesselName,IMO,CallSign,
VesselType,Status,Length,Width,Draft,Cargo,TransceiverClass
```

`BaseDateTime` is a UTC observation timestamp; `LON`/`LAT` use WGS84 coordinates. Observation IDs and delivery-batch metadata let the examples handle duplicates and distinguish observation time from delivery time. Reported `IMO` is null in the fixtures, and identity evidence is handled separately.

The first notebook keeps receptions in Bronze and publishes accepted positions with track decisions to Silver. Its 45-minute correction window and 12-minute link gap control different parts of replay. The second reads resolved Silver positions, uses a seven-day replay cap, and writes one Gold row per visit keyed by `(vessel_id, location_id, entry_ts)`. `exit_ts` is the next observed outside position; it stays null for an open visit.

## Illustrations

Open the [offline illustration gallery](illustrations/gallery.html) for all eight figures. Each has an editable SVG and a 2400-pixel PNG export. The [figure guide](illustrations/README.md) maps them to the notebook sections and explains the future segment-linking illustration.

The calling illustrations cover nested locations, position-numbering islands and late corrections. The identity illustrations cover shared MMSIs, late bridges, time boundaries, H3 motion gates and segment-to-vessel association.

## Scale observations

The calling implementation was exercised with 10,000, 100,000 and 1 million positions, varying both vessel count and history depth. Initial stream runs took approximately 20–25 seconds after warm-up; batches containing a late correction for every vessel took approximately 22–24 seconds. Calling counts, supporting-position counts and key uniqueness passed at every stage, with no reported spill.

These were single serverless runs over two locations. Work increased with data volume even when elapsed time stayed similar. Small correction batches still caused broad history reads; clustering and data skipping are useful next measurements. The track resolver has different scaling characteristics: each grouped Pandas task holds one MMSI's positions and candidate edges in worker memory.
