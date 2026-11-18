"""Fit complete time windows in an observation-centred metric CRS worldwide."""
import hashlib

import numpy as np
import pandas as pd
from pyproj import Geod, Transformer
from pyproj.exceptions import ProjError

from .quality_numeric import MIN_WINDOW_S, fit_circle

MAX_WINDOW_RADIUS_M = 250_000.0
PROJECTION_COLUMNS = ["projection_definition", "projection_status", "window_evaluable"]
MEMBER_COLUMNS = [
    "window_id", "MMSI", "window_start_s", "window_end_s", "obs_id", "sample_count",
    "observed_span_s", "radius_m", "radial_residual_ratio", "angular_coverage_deg",
    "angular_consistency", "median_implied_knots", "circle_candidate", "fast_circle",
    "position_unchanged_despite_reported_speed", "fit_status", *PROJECTION_COLUMNS,
]


def project_window(lon, lat):
    """AEQD accepts either hemisphere, poles and crossings of the antimeridian.

    Widely separated reports are retained as evidence but cannot support one local
    geometric fit. Geodesic neighbour checks remain independent of this decision.
    """
    lon, lat = np.asarray(lon, float), np.asarray(lat, float)
    _, _, distances = Geod(ellps="WGS84").inv(
        np.full(len(lon), lon[0]), np.full(len(lat), lat[0]), lon, lat)
    if not np.isfinite(distances).all() or np.max(distances) > MAX_WINDOW_RADIUS_M:
        return None, None, None, "nonlocal_extent"
    definition = f"+proj=aeqd +lat_0={lat[0]:.12f} +lon_0={lon[0]:.12f} +datum=WGS84 +units=m +no_defs"
    try:
        x, y = Transformer.from_crs(4326, definition, always_xy=True).transform(lon, lat, errcheck=True)
    except ProjError:
        return None, None, definition, "projection_failed"
    if not np.isfinite([x, y]).all():
        return None, None, definition, "projection_failed"
    return np.asarray(x), np.asarray(y), definition, "projected"


def window_members_group(pdf):
    """One already bounded (MMSI, window_end_s) group, independent of spatial cells."""
    w = pdf.sort_values(["event_time_s", "obs_id"], kind="mergesort")
    end = int(w.window_end_s.iloc[0])
    start = end - 1800
    span = int(w.event_time_s.max() - w.event_time_s.min())
    x, y, definition, status = project_window(w.LON, w.LAT)
    # The kernel supplies the same thresholds and geodesic speed as notebook 00.
    fit = fit_circle([], [], [], [], [])
    frozen = False
    if status == "projected":
        fit = fit_circle(x, y, w.event_time_s, w.LON, w.LAT)
        frozen = bool(len(w) >= 3 and span >= MIN_WINDOW_S
                      and np.hypot(np.ptp(x), np.ptp(y)) < 5
                      and w.sog_known.all() and (w.SOG > 5).all())
    window_id = hashlib.sha256(f"{w.MMSI.iloc[0]}|{start}|{end}".encode()).hexdigest()[:24]
    return pd.DataFrame([
        dict(window_id=window_id, MMSI=str(w.MMSI.iloc[0]), window_start_s=start,
             window_end_s=end, obs_id=obs_id, sample_count=int(len(w)), observed_span_s=span,
             position_unchanged_despite_reported_speed=frozen, projection_definition=definition,
             projection_status=status, window_evaluable=status == "projected", **fit)
        for obs_id in w.obs_id
    ], columns=MEMBER_COLUMNS)
