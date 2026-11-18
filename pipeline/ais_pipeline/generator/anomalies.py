"""Opt-in perturbations of *reported* AIS, independent of physical route simulation.

The rectangles used by ``inland`` scenarios are explicit synthetic land fixtures,
not a coastline dataset. None of these patterns establishes spoofing or intent.
"""
import hashlib
import math
import random
from datetime import timedelta

from .world import EARTH_R_KM, bearing_deg


COMMON = {"id", "type", "vessels", "start_tick", "end_tick"}
PARAMETERS = {
    "inland": {"land_bounds", "jitter_metres"},
    "jump": {"lon", "lat"},
    "circle": {"center_lon", "center_lat", "radius_metres", "period_seconds",
               "phase_degrees", "clockwise", "motion_mode", "reported_sog_knots"},
    "frozen": {"lon", "lat", "reported_sog_knots"},
    "fields": {"values"},
    "future_time": {"offset_seconds"},
    "gap": set(),
}
FIELD_NAMES = {"LAT", "LON", "SOG", "COG", "Heading"}


def _number(value, name, minimum=None, maximum=None):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number")
    if minimum is not None and value < minimum or maximum is not None and value > maximum:
        raise ValueError(f"{name} is outside its allowed range")
    return value


def _coordinate(lon, lat, name):
    _number(lon, name + " longitude", -180, 180)
    _number(lat, name + " latitude", -89, 89)


def validate_scenarios(config):
    """Validate explicit scenario selection; intervals are inclusive, zero based.

    Unknown keys and overlapping scenarios on a vessel are rejected. Composition
    is deliberately not supported, so the evaluation label remains unambiguous.
    """
    scenarios = config.get("anomalies", [])
    if not isinstance(scenarios, list):
        raise ValueError("anomalies must be a list (use [] to disable)")
    identifiers, intervals = set(), {}
    for scenario in scenarios:
        if not isinstance(scenario, dict):
            raise ValueError("Each anomaly must be a mapping")
        kind = scenario.get("type")
        if not isinstance(kind, str) or kind not in PARAMETERS:
            raise ValueError(f"Unknown anomaly type: {kind!r}")
        unknown = set(scenario) - COMMON - PARAMETERS[kind]
        if unknown:
            raise ValueError(f"Unknown {kind} scenario keys: {sorted(unknown, key=str)}")
        identifier = scenario.get("id")
        if not isinstance(identifier, str) or not identifier.strip() or identifier in identifiers:
            raise ValueError("Scenario id must be a nonempty unique string")
        identifiers.add(identifier)
        vessels = scenario.get("vessels")
        if not isinstance(vessels, list) or not vessels:
            raise ValueError(f"{identifier}: vessels must be a nonempty unique list")
        if any(type(v) is not int or not 0 <= v < config["vessels"] for v in vessels):
            raise ValueError(f"{identifier}: vessel index outside configured fleet")
        if len(set(vessels)) != len(vessels):
            raise ValueError(f"{identifier}: duplicate vessel selection")
        start, end = scenario.get("start_tick"), scenario.get("end_tick")
        if type(start) is not int or type(end) is not int or not 0 <= start <= end < config["reports_per_vessel"]:
            raise ValueError(f"{identifier}: invalid inclusive tick interval")
        for vessel in vessels:
            for a, b in intervals.get(vessel, []):
                if start <= b and a <= end:
                    raise ValueError(f"{identifier}: overlapping scenarios for vessel {vessel}")
            intervals.setdefault(vessel, []).append((start, end))
        if kind == "inland":
            bounds = scenario.get("land_bounds")
            if not isinstance(bounds, list) or len(bounds) != 4:
                raise ValueError(f"{identifier}: land_bounds must be [west, south, east, north]")
            w, s, e, n = bounds
            _coordinate(w, s, identifier)
            _coordinate(e, n, identifier)
            if not w < e or not s < n:
                raise ValueError(f"{identifier}: land_bounds must have positive width/height")
            _number(scenario.get("jitter_metres", 0), "jitter_metres", 0)
        elif kind == "jump":
            _coordinate(scenario.get("lon"), scenario.get("lat"), identifier)
            if start != end:
                raise ValueError("A jump changes one tick; use an inland/fields episode for longer offsets")
        elif kind == "circle":
            _coordinate(scenario.get("center_lon"), scenario.get("center_lat"), identifier)
            _number(scenario.get("radius_metres"), "radius_metres", 1, 100000)
            _number(scenario.get("period_seconds"), "period_seconds", 1)
            _number(scenario.get("phase_degrees", 0), "phase_degrees")
            if type(scenario.get("clockwise", True)) is not bool:
                raise ValueError("clockwise must be a boolean")
            if scenario.get("motion_mode", "constant") not in ("constant", "consistent"):
                raise ValueError("motion_mode must be constant or consistent")
            _number(scenario.get("reported_sog_knots", 16), "reported_sog_knots", 0)
        elif kind == "frozen":
            if ("lon" in scenario) != ("lat" in scenario):
                raise ValueError("Frozen position requires both lon and lat, or neither")
            if "lon" in scenario:
                _coordinate(scenario["lon"], scenario["lat"], identifier)
            _number(scenario.get("reported_sog_knots", 16), "reported_sog_knots", 0)
        elif kind == "fields":
            values = scenario.get("values")
            if not isinstance(values, dict) or not values or set(values) - FIELD_NAMES:
                raise ValueError(f"values must select from {sorted(FIELD_NAMES)}")
            # Nonfinite/out-of-range values are intentional here, but strings,
            # booleans and structures are not part of this numeric feed fixture.
            if any(v is not None and (isinstance(v, bool) or not isinstance(v, (int, float))) for v in values.values()):
                raise ValueError("Field overrides must be numeric or null")
        elif kind == "future_time":
            _number(scenario.get("offset_seconds"), "offset_seconds", 1, 31536000)
    return scenarios


def _rng(seed, scenario_id, index, tick):
    payload = f"{seed}|{scenario_id}|{index}|{tick}".encode()
    return random.Random(int.from_bytes(hashlib.sha256(payload).digest()[:16], "big"))


def destination(lon, lat, distance_metres, bearing):
    """Geodesic destination on the same mean-radius sphere as the generator."""
    angular = distance_metres / (EARTH_R_KM * 1000)
    lat1, lon1, theta = map(math.radians, (lat, lon, bearing))
    lat2 = math.asin(math.sin(lat1) * math.cos(angular) + math.cos(lat1) * math.sin(angular) * math.cos(theta))
    lon2 = lon1 + math.atan2(math.sin(theta) * math.sin(angular) * math.cos(lat1),
                            math.cos(angular) - math.sin(lat1) * math.sin(lat2))
    return (math.degrees(lon2) + 180) % 360 - 180, math.degrees(lat2)


def perturb(record, scenario, index, tick, config, anchors):
    """Return modified reported fields and scenario-gap flag; never add truth."""
    if scenario is None:
        return record, False
    result, kind = dict(record), scenario["type"]
    rng = _rng(config["seed"], scenario["id"], index, tick)
    if kind == "inland":
        w, s, e, n = scenario["land_bounds"]
        # Clamp jitter to the interior half of the synthetic rectangle.
        lon, lat = (w + e) / 2, (s + n) / 2
        jitter = scenario.get("jitter_metres", 0)
        dlon = rng.uniform(-jitter, jitter) / (111320 * math.cos(math.radians(lat)))
        dlat = rng.uniform(-jitter, jitter) / 111320
        result.update(LON=lon + max(-(e - w) / 4, min((e - w) / 4, dlon)),
                      LAT=lat + max(-(n - s) / 4, min((n - s) / 4, dlat)))
    elif kind == "jump":
        result.update(LON=scenario["lon"], LAT=scenario["lat"])
    elif kind == "circle":
        elapsed = (tick - scenario["start_tick"]) * config["cadence_seconds"]
        direction = 1 if scenario.get("clockwise", True) else -1
        angle = scenario.get("phase_degrees", 0) + direction * elapsed * 360 / scenario["period_seconds"]
        lon, lat = destination(scenario["center_lon"], scenario["center_lat"], scenario["radius_metres"], angle)
        # A short geodesic step gives the forward course of the circular curve.
        qlon, qlat = destination(scenario["center_lon"], scenario["center_lat"], scenario["radius_metres"], angle + direction * 0.001)
        cog = bearing_deg((lon, lat), (qlon, qlat))
        sog = (2 * math.pi * scenario["radius_metres"] / scenario["period_seconds"] * 3600 / 1852
               if scenario.get("motion_mode", "constant") == "consistent" else scenario.get("reported_sog_knots", 16))
        result.update(LON=lon, LAT=lat, SOG=sog, COG=cog, Heading=cog, Status=0)
    elif kind == "frozen":
        anchor = anchors.setdefault(scenario["id"], (record["LON"], record["LAT"]))
        result.update(LON=scenario.get("lon", anchor[0]), LAT=scenario.get("lat", anchor[1]),
                      SOG=scenario.get("reported_sog_knots", 16))
    elif kind == "fields":
        result.update(scenario["values"])
    elif kind == "future_time":
        result["BaseDateTime"] += timedelta(seconds=scenario["offset_seconds"])
        result["event_time_s"] = int(result["BaseDateTime"].timestamp())
    return result, kind == "gap"
