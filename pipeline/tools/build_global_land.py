"""Build the bundled worldwide Natural Earth land/coast index.

Usage: uv run --no-project --with pyshp --with shapely==2.1.2 --with h3==4.3.1
       --with pyproj==3.7.2 python tools/build_global_land.py ne_10m_land.zip
Build dependencies are not needed by the Databricks job. No runtime downloads.
"""
import csv
import gzip
import hashlib
import io
import json
import math
import sys
import zipfile
from collections import defaultdict
from pathlib import Path

import h3
import shapefile
from pyproj import Geod
from shapely import STRtree, MultiLineString, Point, from_wkt, to_wkt
from shapely.geometry import shape

DEST = Path(__file__).resolve().parents[1] / "ais_pipeline/data"
GEOD = Geod(ellps="WGS84")


def write_csv(name, columns, rows):
    with (DEST / name).open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as zipped:
            with io.TextIOWrapper(zipped, encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(columns)
                writer.writerows(rows)


def build(source):
    DEST.mkdir(exist_ok=True)
    with zipfile.ZipFile(source) as archive:
        reader = shapefile.Reader(shp=io.BytesIO(archive.read("ne_10m_land.shp")),
                                  shx=io.BytesIO(archive.read("ne_10m_land.shx")),
                                  dbf=io.BytesIO(archive.read("ne_10m_land.dbf")))
        polygons = []
        for record in reader.shapes():
            geometry = shape(record.__geo_interface__)
            polygons.extend(geometry.geoms if geometry.geom_type == "MultiPolygon" else [geometry])
    # Use the same precision for indexing and saved geometries. Source vertices
    # a few ulps off ±180/-90 otherwise turn artificial cuts into false coasts.
    polygons = [from_wkt(to_wkt(p, rounding_precision=7)) for p in polygons]
    # Natural Earth includes an artificial rectangular "Null Island" at (0,0).
    # Exclude only that source artifact; it is open ocean.
    artifacts = [p for p in polygons if p.intersects(Point(0, 0))
                 and all(abs(v) < 0.01 for v in p.bounds)]
    assert len(artifacts) == 1, "Recheck source Null Island artifact"
    polygons = [p for p in polygons if p not in artifacts]
    assert all(p.is_valid for p in polygons), "Invalid source land geometry"
    assert all(-180 <= p.bounds[0] <= p.bounds[2] <= 180
               and -90 <= p.bounds[1] <= p.bounds[3] <= 90 for p in polygons)
    # Cell centres cover interiors; the dense boundary halo also covers small islands
    # and land within cells whose centres are water. This avoids planar H3 polygons
    # at the antimeridian and polar caps entirely.
    cells = sorted(h3.uncompact_cells(h3.get_res0_cells(), 3))
    centres = [Point(lon, lat) for lat, lon in map(h3.cell_to_latlng, cells)]
    matches = STRtree(polygons).query(centres, predicate="intersects")
    index = {(int(cells[i], 16), int(j)) for i, j in zip(*matches)}
    coast = defaultdict(list)
    for polygon_id, polygon in enumerate(polygons):
        boundary_cells = set()
        for ring in (polygon.exterior, *polygon.interiors):
            xy = list(ring.coords)
            for a, b in zip(xy, xy[1:]):
                _, _, distance = GEOD.inv(*a, *b)
                if distance == 0:
                    continue
                count = max(1, math.ceil(distance / 5000))
                points = [a, *GEOD.npts(*a, *b, count - 1), b] if count > 1 else [a, b]
                seam = ((abs(a[0]) == 180 and a[0] == b[0]) or (a[1] == b[1] == -90))
                for left, right in zip(points, points[1:]):
                    azimuth, _, length = GEOD.inv(*left, *right)
                    lon, lat, _ = GEOD.fwd(*left, azimuth, length / 2)
                    cell = h3.latlng_to_cell(lat, lon, 3)
                    boundary_cells.add(cell)
                    if seam:
                        continue  # Dataset cuts at ±180° and the South Pole are not coasts.
                    if abs(left[0] - right[0]) > 180:
                        # Natural Earth splits at the date line. PROJ may encode an
                        # endpoint as -180 instead of +180; use its adjacent spelling.
                        if abs(left[0]) == 180:
                            left = (-left[0], left[1])
                        elif abs(right[0]) == 180:
                            right = (-right[0], right[1])
                        else:
                            raise ValueError("Unsplit coastline crossing the antimeridian")
                    lines = coast[cell]
                    if lines and lines[-1][-1] == left:
                        lines[-1].append(right)
                    else:
                        lines.append([left, right])
        for cell in boundary_cells:
            index.update((int(neighbour, 16), polygon_id) for neighbour in h3.grid_disk(cell, 2))
    write_csv("global_land.csv.gz", ["polygon_id", "wkt"],
              ((i, to_wkt(p, rounding_precision=7)) for i, p in enumerate(polygons)))
    write_csv("global_land_cells.csv.gz", ["cell", "polygon_id"], sorted(index))
    write_csv("global_coast_cells.csv.gz", ["cell", "wkt"],
              ((int(cell, 16), to_wkt(MultiLineString(lines), rounding_precision=7))
               for cell, lines in sorted(coast.items())))
    manifest = dict(dataset="Natural Earth 1:10m land", srid=4326, h3_resolution=3,
                    source_url="https://naciscdn.org/naturalearth/10m/physical/ne_10m_land.zip",
                    source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                    licence="Public domain", licence_url="https://www.naturalearthdata.com/about/terms-of-use/",
                    polygons=len(polygons), land_cell_pairs=len(index), coast_cells=len(coast),
                    max_coast_segment_metres=5000, candidate_halo=2,
                    processing="Worldwide polygon components excluding the artificial Null Island rectangle at (0,0); geodesically densified coastline, excluding artificial date-line/pole cuts. H3 candidate index only; exact spatial predicates follow.",
                    limitations="Generalised cartography, unsuitable for navigation or precise shoreline decisions; 150 m is a teaching threshold, not an accuracy claim.",
                    files={p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(DEST.glob("*.csv.gz"))})
    (DEST / "global_land_source.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({k: manifest[k] for k in ("polygons", "land_cell_pairs", "coast_cells")}))


if __name__ == "__main__":
    build(Path(sys.argv[1]))
