"""Extracted from 00_position_quality_and_anomalies.py; regenerate with tools/sync_notebook_code.py."""

import numpy as np

from pyproj import Geod

from circle_fit import taubinSVD

MAX_EPISODE_S = 30 * 60  # Length of each circle-fitting window, in seconds.

WINDOW_STEP_S = 5 * 60  # Start a new, overlapping window every five minutes.

MAX_MOTION_KNOTS = 50.0  # Implied-speed threshold for isolated jumps and fast circles.

MIN_WINDOW_S = 12 * 60  # Minimum observation span for circle and unchanged-position checks.

def fit_circle(x, y, times, lon, lat):
    """Pure numeric evidence; no fixture labels, expected outcomes or vessel truth."""
    x, y, times = np.asarray(x, float), np.asarray(y, float), np.asarray(times, float)
    result = dict(radius_m=None, radial_residual_ratio=None, angular_coverage_deg=None,
                  angular_consistency=None, median_implied_knots=None, circle_candidate=False,
                  fast_circle=False, fit_status="not_assessed")
    if len(x) < 12 or times[-1] - times[0] < MIN_WINDOW_S or np.any(np.diff(times) <= 0):
        return result
    coordinates = np.column_stack((x, y))
    centred = coordinates - coordinates.mean(axis=0)
    if np.linalg.matrix_rank(centred) < 2 or np.linalg.cond(centred) > 1e8:
        return result
    try:
        center_x, center_y, radius, error = taubinSVD(coordinates)
    except np.linalg.LinAlgError:
        return result
    if radius <= 0 or not np.isfinite([center_x, center_y, radius, error]).all():
        return result
    residual = float(error / radius)
    angles = np.arctan2(y - center_y, x - center_x)
    ordered_angles = np.sort(np.mod(angles, 2 * np.pi))
    gaps = np.diff(np.r_[ordered_angles, ordered_angles[0] + 2 * np.pi])
    coverage = float(np.degrees(2 * np.pi - gaps.max()))
    changes = np.diff(np.unwrap(angles))
    moving_changes = changes[np.abs(changes) > 1e-6]
    consistency = float(max(np.mean(moving_changes > 0), np.mean(moving_changes < 0))) if len(moving_changes) else 0.0
    _, _, distances = Geod(ellps="WGS84").inv(
        np.asarray(lon)[:-1], np.asarray(lat)[:-1],
        np.asarray(lon)[1:], np.asarray(lat)[1:])
    speed = float(np.median(distances / np.diff(times) * 3600 / 1852))
    candidate = bool(residual < 0.1 and coverage >= 270 and consistency >= 0.8)
    result.update(radius_m=radius, radial_residual_ratio=residual, angular_coverage_deg=coverage,
                  angular_consistency=consistency, median_implied_knots=speed,
                  circle_candidate=candidate, fast_circle=bool(candidate and radius > 2000 and speed > MAX_MOTION_KNOTS),
                  fit_status="assessed")
    return result
