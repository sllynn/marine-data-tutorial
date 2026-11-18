# Worldwide land reference

The job bundles Natural Earth's public-domain **1:10m land** dataset. The manifest
records the download URL, source SHA-256, processing parameters and hashes of all
three compressed CSVs. Runtime loading verifies the hashes before outputs reset.
There are no runtime downloads or Python GIS build dependencies on workers.

Polygon components retain geographic coordinates, rounded consistently to seven
decimal places before indexing and serialization to keep source rounding noise
from turning dataset cuts into coastline. An H3 resolution-3 candidate
index covers polygon interiors and a two-ring halo around densified boundary
segments. Exact geographic predicates determine land membership. Coastline segments
are geodesically densified to at most 5 km and indexed separately for local
closest-point queries using a two-ring neighbourhood. Artificial date-line and
South Pole polygon cuts are excluded from the coastline. The source's artificial
Null Island rectangle at (0,0) is excluded from both references.

This is generalised cartography. Small islands, reclaimed land and detailed shores
may be absent or displaced. The demo's 150 m coastal tolerance does not establish
map accuracy. An inland flag is advisory evidence, unsuitable for navigation.

To rebuild, download the exact source URL in `global_land_source.json`, verify its
SHA-256, then run from `pipeline/`:

```sh
uv run --no-project --with pyshp --with shapely==2.1.2 --with h3==4.3.1 \
  --with pyproj==3.7.2 python tools/build_global_land.py /path/to/ne_10m_land.zip
```

Review the regenerated manifest and run the global quality tests. The builder
fails if the expected source artifact or geographic bounds change. The wheel
includes the manifest and compressed data files.
