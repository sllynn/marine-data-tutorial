"""Extract Hawaiʻi from a downloaded Natural Earth 1:10m land GeoJSON.

Development only: uv run --no-project --with shapely --with pyproj \
    python build_hawaii_coastline.py /path/to/ne_10m_land.geojson
The tutorial reads the generated CSV and requires neither package.
"""
import csv
import hashlib
import json
from pathlib import Path
import sys

from pyproj import Transformer
from shapely.geometry import Point, shape
from shapely.ops import transform
from shapely import to_wkt


HERE = Path(__file__).resolve().parent
COMMIT = "ace5fed0eaf3c6c03c951e75b439ba8fffbc218e"
SOURCE = f"https://raw.githubusercontent.com/nvkelso/natural-earth-vector/{COMMIT}/geojson/ne_10m_land.geojson"


def build(source):
    payload = Path(source).read_bytes()
    polygons = []
    for feature in json.loads(payload)["features"]:
        geometry = shape(feature["geometry"])
        polygons.extend(getattr(geometry, "geoms", [geometry]))
    island, = [p for p in polygons if p.contains(Point(-155.5, 19.6))]
    projected = transform(Transformer.from_crs(4326, 32605, always_xy=True).transform, island)
    assert projected.is_valid and projected.area > 1e10
    target = HERE / "hawaii_land_utm.csv"
    with target.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["feature_id", "name", "srid", "wkt"])
        writer.writeheader()
        writer.writerow(dict(feature_id="hawaii-island", name="Hawaiʻi", srid=32605,
                             wkt=to_wkt(projected, rounding_precision=3)))
    (HERE / "hawaii_land_source.json").write_text(json.dumps(dict(
        dataset="Natural Earth 1:10m land", source_url=SOURCE, source_commit=COMMIT,
        source_sha256=hashlib.sha256(payload).hexdigest(),
        licence="Public domain", licence_url="https://www.naturalearthdata.com/about/terms-of-use/",
        geographic_crs="EPSG:4326", bundled_crs="EPSG:32605 — WGS 84 / UTM zone 5N",
        selection="Polygon containing longitude -155.5, latitude 19.6; island of Hawaiʻi only",
        processing="Reprojected with PROJ through pyproj; rounded to millimetres; no additional simplification",
        csv_sha256=hashlib.sha256(target.read_bytes()).hexdigest(),
        limitations="Generalised 1:10m cartography, unsuitable for navigation or precise shoreline decisions. The tutorial's 150-metre band is a configurable teaching threshold, not a source-accuracy estimate."
    ), ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    build(sys.argv[1])
