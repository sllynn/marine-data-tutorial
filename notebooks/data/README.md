# Hawaiʻi coastline for notebook 00

`hawaii_land_utm.csv` contains one land polygon for the island of Hawaiʻi (the Big Island), extracted from Natural Earth's **1:10m land** dataset. It does not include the other Hawaiian islands. Natural Earth data is [public domain](https://www.naturalearthdata.com/about/terms-of-use/).

The CSV has `feature_id`, `name`, `srid` and `wkt` columns. Its geometry uses **WGS 84 / UTM zone 5N, EPSG:32605**, with easting and northing in metres. The island's western edge extends slightly west of the zone's nominal boundary; this local projection remains suitable for the example. The notebook uses Spatial SQL `ST_Transform` to convert its synthetic UTM positions to WGS 84 longitude/latitude and to project reported positions back to UTM for analysis.

Copy the CSV into a `data/` folder beside notebook 00 in your Databricks workspace, and import `resources/ais_anomaly_fixtures.py` as a notebook in the sibling `resources/` folder. Importing only the Python notebook will not copy this file. Alternatively, upload it to a Unity Catalog Volume and set the notebook's `coastline_csv` widget to its `/Volumes/.../hawaii_land_utm.csv` path. The coastline needs no runtime download. The notebook installs `circle-fit==0.2.1` for circle fitting and `pyproj==3.7.2` for ellipsoidal distance calculations; coordinate conversions use native Spatial SQL.

This is generalised cartography, unsuitable for navigation or detailed harbour and inland-water classification. The notebook's 150 m coastal band is a configurable teaching threshold, not a claim about the outline's accuracy. Use a more detailed, appropriately licensed land dataset for operational checks.

`hawaii_land_source.json` records the pinned source URL, input/output hashes, CRS and processing. To regenerate the CSV, download that source GeoJSON and run:

```sh
uv run --no-project --with shapely --with pyproj \
  python build_hawaii_coastline.py /path/to/ne_10m_land.geojson
```

The build tool uses PROJ to prepare the bundled coastline. The customer notebook performs its coordinate conversions with native Spatial SQL.
