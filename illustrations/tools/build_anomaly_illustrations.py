"""Build editable AIS screening SVGs and 2400-pixel PNGs.

Run from any directory with Python 3, pyproj and rsvg-convert installed. Fixture-based
figures extract only the notebook's pure fixture functions/constants with AST;
they never execute Spark setup, use expected flags as detector inputs, or claim
to reproduce a historical event. The expectation ledger selects display labels.
"""

from pathlib import Path
import ast
import csv
import html
import hashlib
import json
import shutil
import subprocess

from pyproj import Geod

WGS84_GEOD = Geod(ellps="WGS84")


ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "illustrations" / "anomalies"
NOTEBOOK = ROOT / "notebooks" / "00_position_quality_and_anomalies.py"
FIXTURE_NOTEBOOK = ROOT / "notebooks/resources/ais_anomaly_fixtures.py"
INK, MUTED, TEAL, ORANGE, RED = "#172321", "#61716e", "#087f8c", "#cb6329", "#b84048"
WATER, LAND, BORDER = "#eef7f9", "#e8e4d8", "#dce5e2"


def text(x, y, value, size=18, fill=INK, weight=400, anchor="start"):
    return (f'<text x="{x:.2f}" y="{y:.2f}" font-size="{size}" fill="{fill}" '
            f'font-weight="{weight}" text-anchor="{anchor}">{html.escape(str(value))}</text>')


def lines(x, y, values, size=17, fill=MUTED, step=27):
    return "".join(text(x, y + i * step, value, size, fill) for i, value in enumerate(values))


def rect(x, y, w, h, fill="white", stroke="none", radius=14):
    return (f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" '
            f'fill="{fill}" stroke="{stroke}"/>')


def path(points, stroke=TEAL, width=3, dash=None, arrow=False):
    d = "M " + " L ".join(f"{x:.3f} {y:.3f}" for x, y in points)
    return (f'<path d="{d}" fill="none" stroke="{stroke}" stroke-width="{width}" '
            + (f'stroke-dasharray="{dash}" ' if dash else "")
            + ('marker-end="url(#arrow)" ' if arrow else "") + "/>")


def point(x, y, color=TEAL, shape="circle", r=6):
    if shape == "square":
        return f'<rect x="{x-r}" y="{y-r}" width="{r*2}" height="{r*2}" fill="{color}" stroke="white" stroke-width="2"/>'
    if shape == "cross":
        return path([(x-r, y-r), (x+r, y+r)], color, 3) + path([(x-r, y+r), (x+r, y-r)], color, 3)
    return f'<circle cx="{x}" cy="{y}" r="{r}" fill="{color}" stroke="white" stroke-width="2"/>'


def save(name, title, description, body, height):
    OUT.mkdir(parents=True, exist_ok=True)
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 {height}" '
           'role="img" aria-labelledby="title desc">'
           f'<title id="title">{html.escape(title)}</title><desc id="desc">{html.escape(description)}</desc>'
           f'<defs><marker id="arrow" markerWidth="9" markerHeight="9" refX="7" refY="4" orient="auto">'
           f'<path d="M1 1 L7 4 L1 7" fill="none" stroke="{TEAL}" stroke-width="1.6"/></marker></defs>'
           f'<rect width="1200" height="{height}" fill="white"/>'
           f'<g font-family="Arial,Helvetica,sans-serif">{body}</g></svg>')
    target = OUT / f"{name}.svg"
    target.write_text(svg)
    if not shutil.which("rsvg-convert"):
        raise SystemExit("SVGs written; install rsvg-convert to produce PNG exports.")
    subprocess.run(["rsvg-convert", "-w", "2400", "-o", str(target.with_suffix(".png")), str(target)], check=True)
    print(target.relative_to(ROOT))


def chain():
    body = text(40, 48, "How position errors reach an AIS dataset", 29, weight=700)
    body += text(40, 82, "Errors can occur during positioning, broadcast, reception or delivery.", 18, MUTED)
    cards = [
        (40, "1 · Position source", "GNSS / navigation", ["The vessel calculates", "its position and includes", "it in an AIS report."], "Positioning errors"),
        (330, "2 · AIS report", "Vessel broadcast", ["Position, time, speed", "and identifiers are", "reported over radio."], "Reported-field faults"),
        (620, "3 · Reception", "Terrestrial / satellite", ["Receivers can miss reports", "or receive copies of", "the same transmission."], "Gaps and duplicates"),
        (910, "4 · Delivery", "Provider / dataset", ["Records are assembled", "and delivered, sometimes", "late or out of order."], "Processing / feed errors"),
    ]
    for i, (x, title, subtitle, rows, issue) in enumerate(cards):
        body += rect(x, 125, 250, 235, "#f5f8f7")
        body += text(x+18, 165, title, 21, weight=700)
        body += text(x+18, 198, subtitle, 17, TEAL, 600)
        body += lines(x+18, 245, rows, 16, step=28)
        body += path([(x+125, 371), (x+125, 399)], MUTED, 2)
        body += text(x+125, 429, issue, 16, ORANGE, 600, "middle")
        if i < 3:
            body += path([(x+258, 245), (x+281, 245)], TEAL, 2.5, arrow=True)
    body += rect(40, 470, 1120, 92, "#e2f4f3")
    body += text(60, 505, "Keep the original reports alongside the results of each check.", 21, weight=700)
    body += text(60, 537, "Receiver data, vessel records and independent positions can help explain a flagged report.", 17, MUTED)
    body += text(40, 603, "GNSS positioning, AIS radio transmission and feed processing can each fail separately.", 14, MUTED)
    save("01-position-to-feed", "How position errors reach an AIS dataset", "Position calculation, AIS broadcast, radio reception and feed delivery can each introduce errors into a dataset.", body, 630)


def fixture_api():
    """Load the notebook's explicit pure-fixture block without Spark side effects."""
    namespace = {}
    start, end = "# FIGURE_FIXTURE_START", "# FIGURE_FIXTURE_END"
    source = FIXTURE_NOTEBOOK.read_text()
    if start not in source or end not in source:
        raise RuntimeError("Notebook must expose its pure fixture block between FIGURE_FIXTURE_START/END markers")
    block = source.split(start, 1)[1].split(end, 1)[0]
    # AST parsing ensures accidental notebook magic/code separators cannot be
    # interpreted as an invocation of the whole notebook.
    exec(compile(ast.parse(block), str(FIXTURE_NOTEBOOK), "exec"), namespace)
    with (ROOT / "notebooks/data/hawaii_land_utm.csv").open(newline="") as stream:
        row, = list(csv.DictReader(stream))
    namespace["LAND_WKT"] = row["wkt"]
    return namespace


def wkt_rings(wkt):
    """Parse polygon rings in the bundled UTM coastline."""
    import re
    rings = re.findall(r"\(([-0-9.,\s]+)\)", wkt)
    return [[tuple(map(float, p.strip().split())) for p in ring.split(",")] for ring in rings]


def labelled_records(api):
    records, expected = api["fixture_observations"]()
    # Offline artwork uses PROJ. The notebook itself uses native Spatial SQL.
    from pyproj import Transformer
    geographic = Transformer.from_crs(api["UTM_SRID"], 4326, always_xy=True)
    for record in records:
        lon, lat = geographic.transform(record["x_m"], record["y_m"])
        if record["LON"] is None:
            record["LON"] = lon
        if record["LAT"] is None:
            record["LAT"] = lat
    items = []
    seen = set()
    for record in records:
        if record["obs_id"] in seen:
            continue
        seen.add(record["obs_id"])
        spec = expected.get(record["obs_id"], {})
        label = spec.get("label", "") if isinstance(spec, dict) else str(spec)
        items.append((label, record))
    return items


def scale_bar(x, y, metres, pixels_per_m, label):
    end = x + metres * pixels_per_m
    return (path([(x, y-5), (x, y+5), (x, y), (end, y), (end, y-5), (end, y+5)], INK, 2)
            + text((x+end)/2, y+25, label, 14, MUTED, anchor="middle"))


def land(api):
    rings = wkt_rings(api["LAND_WKT"])
    records = {label: row for label, row in labelled_records(api)}
    body = text(40, 48, "Checking whether a reported position is on land", 28, weight=700)
    body += text(40, 82, "Hawaiʻi land outline in WGS 84 / UTM zone 5N (EPSG:32605).", 18, MUTED)
    body += rect(40, 115, 650, 590, WATER, BORDER)

    def draw_land(project, band_scale):
        d = " ".join("M " + " L ".join(f"{a:.2f} {b:.2f}" for a, b in [project(*p) for p in ring]) + " Z" for ring in rings)
        return (f'<path d="{d}" fill="{LAND}" fill-rule="evenodd" stroke="#aaa791" stroke-width="1.5"/>'
                f'<path d="{d}" fill="none" stroke="{ORANGE}" stroke-opacity=".25" '
                f'stroke-width="{2 * api["COASTAL_TOLERANCE_M"] * band_scale}"/>')

    scale = 0.0031
    def overview(x, y):
        return 90 + (x-150000)*scale, 140 + (2250000-y)*scale
    body += draw_land(overview, scale)
    body += text(*overview(242000, 2170000), "HAWAIʻI", 19, "#77715f", 700)
    body += text(65, 150, "Synthetic AIS positions", 16, MUTED)
    for label, title, color, marker, offset in [
        ("deep_inland", "Inland position", RED, "cross", (12,-12)),
        ("offshore_water", "Offshore position", TEAL, "circle", (-18,25)),
        ("nearshore", "Coastal example →", ORANGE, "square", (-135,-18)),
    ]:
        r = records[label]
        px, py = overview(r["x_m"], r["y_m"])
        body += point(px, py, color, marker, 7)
        body += text(px+offset[0], py+offset[1], title, 15, color, 600)
    body += scale_bar(90, 660, 20000, scale, "20 km")

    # Clip the real coastline to the zoom panel, retaining the same UTM geometry.
    bx, by, zoom = api["COAST_EASTING_M"], api["COAST_NORTHING_M"], 0.12
    def closeup(x,y):
        return 935+(x-bx)*zoom, 300-(y-by)*zoom
    body += '<defs><clipPath id="coast-zoom"><rect x="730" y="155" width="430" height="290" rx="12"/></clipPath></defs>'
    body += text(730, 140, "Coastal detail near Kailua-Kona", 20, weight=700)
    body += rect(730, 155, 430, 290, WATER, BORDER)
    body += '<g clip-path="url(#coast-zoom)">' + draw_land(closeup, zoom) + '</g>'
    r = records["nearshore"]
    px, py = closeup(r["x_m"], r["y_m"])
    body += point(px,py,ORANGE,"square",7)
    body += text(750,220,"WATER",14,TEAL,600) + text(1075,390,"LAND",14,"#77715f",600)
    body += path([(px-8,py-5),(850,260),(750,260)],MUTED,1.5)
    body += text(750,248,"Report near the mapped coast",14,ORANGE,600)
    body += scale_bar(755, 413, 500, zoom, "500 m")
    body += text(730, 488, "Inside land and over 150 m from its edge:", 17, weight=600)
    body += text(730, 515, "flag the position for review.", 17, RED)
    body += text(730, 561, "Within 150 m on either side of the edge:", 17, weight=600)
    body += text(730, 588, "leave the coastal result uncertain.", 17, ORANGE)
    body += lines(730, 637, ["The band is a teaching threshold.", "This map is too generalised for", "detailed harbour or navigation checks."], 15, step=25)
    body += text(40, 752, "Land: Natural Earth 1:10m, public domain. Positions: synthetic reports from notebook 00.", 15, MUTED)
    body += text(40, 782, "The notebook uses ST_Transform for UTM ↔ longitude/latitude conversions. Panels use different scales.", 15, MUTED)
    save("02-land-and-coast", "Checking whether a reported position is on land",
         "The island of Hawaiʻi from Natural Earth in UTM zone 5N, with synthetic inland, offshore and coastal positions. A coastal zoom shows the 150-metre uncertainty band.", body, 810)


def geodesic_distance_m(a, b):
    return WGS84_GEOD.inv(a["LON"], a["LAT"], b["LON"], b["LAT"])[2]


def circles(api):
    body = text(40, 48, "Comparing fast and slow circular tracks", 29, weight=700)
    body += text(40, 82, "Check speed from the position intervals as well as reported SOG.", 18, MUTED)
    labelled = labelled_records(api)
    for left, prefix, color, shape, radius, period, center, bar in [
        (40, "fast", TEAL, "circle", 5000, 12, (-5000, 0), 2000),
        (620, "slow", ORANGE, "square", 100, 30, (9000, 0), 50),
    ]:
        records = [r for label, r in labelled if prefix in label.lower() and "circle" in label.lower()]
        records.sort(key=lambda r: r["BaseDateTime"])
        if len(records) < 12:
            raise RuntimeError(f"Missing {prefix}-circle fixture")
        body += rect(left, 115, 540, 620, "#f5f8f7", BORDER)
        body += text(left+24, 155, "Large fast circle" if prefix == "fast" else "Small slow circle", 21, weight=700)
        body += text(left+24, 185, "5 km radius · 12 min per rotation" if prefix == "fast" else "100 m radius · 30 min per rotation", 17, color, 600)
        cx, cy, scale = left+270, 375, 150/radius
        body += path([(left+70, cy), (left+470, cy)], BORDER, 1)
        body += path([(cx, cy-180), (cx, cy+180)], BORDER, 1)
        coords = []
        for r in records:
            x, y = r["x_m"] - api["FIXTURE_EASTING_M"], r["y_m"] - api["FIXTURE_NORTHING_M"]
            coords.append((cx+(x-center[0])*scale, cy-(y-center[1])*scale))
        body += path(coords, color, 2)
        for i, (x, y) in enumerate(coords):
            body += point(x, y, color, shape, 4.5)
        # The first three samples show observation-time direction on each scale.
        body += path(coords[:4], TEAL, 2.5, arrow=True)
        body += text(coords[0][0]+10, coords[0][1]-12, "t=0", 14, color, 600)
        body += text(cx, cy+190, "Arrows show direction of movement", 13, MUTED, anchor="middle")
        body += scale_bar(left+55, 585, bar, scale, "2 km" if prefix == "fast" else "50 m")
        intervals = []
        for a, b in zip(records, records[1:]):
            seconds = (b["BaseDateTime"]-a["BaseDateTime"]).total_seconds()
            if seconds > 0:
                intervals.append(geodesic_distance_m(a,b)/seconds*3600/1852)
        import statistics
        speed = statistics.median(intervals)
        body += text(left+24, 646, f'Reported SOG: {records[0]["SOG"]:.2f} knots', 18, weight=600)
        body += text(left+24, 679, f"Median implied speed: {speed:.2f} knots", 18, color, 600)
        body += text(left+24, 710, "Positions sampled every minute", 14, MUTED)
    body += text(40, 780, "Synthetic positions from notebook 00, plotted at different scales.", 14, MUTED)
    body += text(40, 812, "The fast circle implies about 84 knots while reporting 16. The slow circle's reported and calculated speeds agree.", 16, MUTED)
    save("03-fast-circle", "Comparing fast and slow circular tracks", "Synthetic circles plotted at different scales. The large circle has a five-kilometre radius and twelve-minute period; the small circle has a hundred-metre radius and thirty-minute period. Each panel compares reported and calculated speed.", body, 845)


def manifest(api):
    """Record the fixture source so a reviewer can reproduce the evidence artwork."""
    import statistics
    labelled = labelled_records(api)
    result = {
        "source_notebook": str(NOTEBOOK.relative_to(ROOT)),
        "source_sha256": hashlib.sha256(NOTEBOOK.read_bytes()).hexdigest(),
        "fixture_notebook": str(FIXTURE_NOTEBOOK.relative_to(ROOT)),
        "fixture_sha256": hashlib.sha256(FIXTURE_NOTEBOOK.read_bytes()).hexdigest(),
        "fixture_function": "fixture_observations",
        "purpose": "Synthetic input illustration; no detector results or execution claims",
        "land_csv": "notebooks/data/hawaii_land_utm.csv",
        "land_csv_sha256": hashlib.sha256((ROOT / "notebooks/data/hawaii_land_utm.csv").read_bytes()).hexdigest(),
        "land_srid": api["UTM_SRID"],
        "coastal_tolerance_metres": api["COASTAL_TOLERANCE_M"],
        "distance_model": "WGS84 ellipsoid, pyproj.Geod.inv",
        "circles": {},
    }
    for prefix in ("fast", "slow"):
        records = [r for label, r in labelled if prefix in label.lower() and "circle" in label.lower()]
        records.sort(key=lambda r: r["BaseDateTime"])
        speeds = [geodesic_distance_m(a,b)/(b["BaseDateTime"]-a["BaseDateTime"]).total_seconds()*3600/1852
                  for a,b in zip(records, records[1:]) if b["BaseDateTime"] > a["BaseDateTime"]]
        result["circles"][prefix] = {
            "n_observations": len(records),
            "span_minutes": (records[-1]["BaseDateTime"]-records[0]["BaseDateTime"]).total_seconds()/60,
            "reported_sog_knots": records[0]["SOG"],
            "median_implied_knots": statistics.median(speeds),
        }
    (OUT / "fixture-manifest.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    chain()
    api = fixture_api()
    land(api)
    circles(api)
    manifest(api)
