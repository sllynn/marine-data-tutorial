# From AIS positions to vessel activity

Three Databricks notebooks accompany the blog: screen reported positions, resolve plausible tracks, then derive observed port and berth callings. All three include synthetic inputs and display their results. Notebook 00 analyses a complete batch; notebooks 01 and 02 also demonstrate late observations.

## Start here

| Notebook | What you will build |
|---|---|
| [00 · Position quality and anomalies](notebooks/00_position_quality_and_anomalies.py) | Field/time checks, land evidence, jump diagnostics and circular episodes, with explicit review flags and normal controls |
| [01 · Identity, tracks and late data](notebooks/01_identity_tracks_and_late_data.py) | H3 candidate generation, spatial motion checks, distributed track replay and an introduction to registry identity evidence |
| [02 · Port and berth callings](notebooks/02_port_and_berth_callings.py) | Indexed location matching, consecutive inside runs, departure evidence and atomic publication of corrected calling histories |

The [generator-backed end-to-end companion](pipeline/README.md) connects generated receptions, quality screening, track resolution, registry association and calling publication. Its [opt-in anomaly scenarios](pipeline/ANOMALIES.md) exercise the quality checks in the connected job or in separate generator experiments.

Read them in order. Each notebook generates its own small synthetic dataset and runs independently. Notebook 00 publishes observations with quality labels; notebook 01 illustrates registry association separately from its streamed track output; notebook 02 begins with resolved `vessel_id` values. Running one notebook does not supply another notebook's input.

## Run in Databricks

1. Import the three `.py` files under `notebooks/` into your workspace as Python source notebooks. Keep their names to preserve the reading order. For notebook 00, import [`notebooks/resources/ais_anomaly_fixtures.py`](notebooks/resources/ais_anomaly_fixtures.py) as a Python notebook in a `resources/` folder beside it, and upload [`notebooks/data/hawaii_land_utm.csv`](notebooks/data/hawaii_land_utm.csv) into a `data/` folder beside the notebook. You can instead place it in a Volume and set the `coastline_csv` widget to that path.
2. Attach serverless compute. Environment version **6** was used for validation; it supplies the native spatial/H3 functions, grouped Pandas support, Delta and matplotlib used here. Notebook 00 installs `circle-fit==0.2.1` for circle fitting and `pyproj==3.7.2` for WGS 84 ellipsoidal distances in its first code cell. Notebook 2 also uses managed-table transactions (`BEGIN ATOMIC`).
3. In each notebook's setup cell, set the **catalog** widget to a catalog where you can create schemas, managed Delta tables and volumes. Notebook 00 requires an explicit catalog; notebooks 01/02 retain their existing `main` default.
4. Use separate dedicated schemas: `ais_anomalies_tutorial`, `ais_identity_tracks_tutorial` and `ais_callings_tutorial`, or select tutorial schemas of your own. Run all.

**Run all overwrites the selected tutorial tables.** Notebooks 01 and 02 also reset their checkpoint folders. Use dedicated schemas and run one copy of each notebook at a time against a given schema. The notebooks retain their outputs for inspection and recreate their fixtures when you run them again.

No external AIS files, registry downloads, Python H3 package or companion wheel are required. Notebooks 01 and 02 create their own checkpoint volumes under the selected schemas. To remove the outputs afterwards, stop the tutorial streams and drop the dedicated schemas and their contents.

## Data and outputs

The reported feed fields are:

```text
MMSI,BaseDateTime,LAT,LON,SOG,COG,Heading,VesselName,IMO,CallSign,
VesselType,Status,Length,Width,Draft,Cargo,TransceiverClass
```

`BaseDateTime` is a UTC observation timestamp; `LON`/`LAT` use WGS84 coordinates. Observation IDs and delivery-batch metadata let the examples handle duplicates and distinguish observation time from delivery time. Reported `IMO` is null in the fixtures, and identity evidence is handled separately.

Notebook 00 calls its fixture helper with `%run` and displays the prepared `fixture_receptions` table before the analysis. The helper generates all reports for a single batch analysis. The example assumes all reports for the period have arrived. Notebook 00 keeps every reception and saves the measurements and results of its position checks. It saves only `fixture_receptions` and `quality_observations`; intermediate calculations and window results remain DataFrames or temporary views. Every observation remains in the results, with quality status, rule outcomes, reason codes and assessment time. Invalid coordinates and timestamps are excluded only from the spatial and movement calculations. Downstream analyses choose how to use the labels. A projection-selection example uses `ST_EstimateSRID` on H3 cell centres; accompanying guidance explains global coverage and a streaming extension with recent history and revised assessments. The coastline is the island of Hawaiʻi from Natural Earth’s public-domain 1:10m land dataset, bundled in UTM zone 5N (EPSG:32605). The notebook uses `ST_Transform` for coordinate conversion, native `ST_DistanceSpheroid` for SQL motion checks and `pyproj.Geod.inv` for distances inside the grouped Pandas calculation. Vessel positions and circular tracks are synthetic test data; investigating errors in real AIS reports requires other sources, such as receiver coverage and independent positions. See the [coastline guide](notebooks/data/README.md) for source, licence and accuracy limits.

Notebook 01 keeps receptions in Bronze and publishes accepted positions with track decisions to Silver. Its 45-minute correction window and 12-minute link gap control different parts of replay. Notebook 02 reads resolved Silver positions, uses a seven-day replay cap, and writes one Gold row per visit keyed by `(vessel_id, location_id, entry_ts)`. `exit_ts` is the next observed outside position; it stays null for an open visit.

## Illustrations

Open the [offline illustration gallery](illustrations/gallery.html). Each static figure has an editable SVG and a 2400-pixel PNG export. The [figure guide](illustrations/README.md) maps them to notebook sections.

The anomaly illustrations cover the observation chain, land/coastal evidence and fast versus slow circular patterns. The calling illustrations cover nested locations, position-numbering islands and late corrections. The identity illustrations cover shared MMSIs, late bridges, time boundaries, H3 motion gates and segment-to-vessel association.

## Scale observations

The calling implementation was exercised with 10,000, 100,000 and 1 million positions, varying both vessel count and history depth. Initial stream runs took approximately 20–25 seconds after warm-up; batches containing a late correction for every vessel took approximately 22–24 seconds. Calling counts, supporting-position counts and key uniqueness passed at every stage, with no reported spill.

These were single serverless runs over two locations. Work increased with data volume even when elapsed time stayed similar. Small correction batches still caused broad history reads; clustering and data skipping are useful next measurements. The track resolver has different scaling characteristics: each grouped Pandas task holds one MMSI's positions and candidate edges in worker memory.

## Generator and end-to-end job

The [streaming companion](pipeline/README.md) provides a YAML-configured `searoute` fleet generator and a Lakeflow job that connects the notebook algorithms through Bronze, track Silver, resolved Silver and Gold callings. It includes release-to-Gold timings and end-to-end checks.

The connected job runs notebook 00's quality rules over received history before tracking, with a bundled worldwide land reference and automatic local projections for windows anywhere on the globe. It retains all observations in its quality output, withholds invalid inputs from track calculations, and exposes advisory labels with resolved positions and calling summaries. Select `config=fleet-anomalies.yaml` for the anomaly examples. The calling timings above predate this integration and cover only the calling stage.
