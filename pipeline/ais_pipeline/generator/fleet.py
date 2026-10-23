"""Reusable seeded emission iterator using the original cached searoute primitive.

Routing runs once per configured directed port pair. Each vessel then advances by
speed × observation time, dwells at a port and returns along the reverse route.
The iterator returns reported AIS values and a separate delivery schedule.
"""
import hashlib
import math
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import yaml

from .routing import Leg, RouteCache
from .world import World, haversine_km


def load_config(path):
    c = yaml.safe_load(Path(path).read_text())
    for key in ("vessels", "reports_per_vessel", "cadence_seconds", "release_seconds"):
        if not isinstance(c[key], int) or c[key] <= 0:
            raise ValueError(f"{key} must be a positive integer")
    if c["cadence_seconds"] > 720:
        raise ValueError("Use cadence <= 720 seconds for the notebook's 12-minute link gap")
    if not 0 < c["speed_knots"][0] <= c["speed_knots"][1] < 30:
        raise ValueError("Use speeds below the notebook's 30-knot motion limit")
    for key in ("dropout_percent", "duplicate_percent", "lateness_percent"):
        if not 0 <= c[key] <= 100:
            raise ValueError(f"{key} must be between 0 and 100")
    for key in ("jitter_metres", "lane_sigma_metres", "lateness_max_seconds", "port_dwell_minutes"):
        if c[key] < 0:
            raise ValueError(f"{key} cannot be negative")
    names = {p["name"] for p in c["ports"]}
    if not c["port_pairs"] or any(len(p) != 2 or p[0] == p[1] or not set(p) <= names for p in c["port_pairs"]):
        raise ValueError("Each port pair must name two distinct configured ports")
    c["start_epoch"] = int(datetime.fromisoformat(c["start_time"].replace("Z", "+00:00")).timestamp())
    return c


def build_routes(path, config):
    world = World.load(path)
    cache = RouteCache()
    result = []
    for a, b in config["port_pairs"]:
        pair = []
        for origin, dest in ((a, b), (b, a)):
            p, q = world.ports[origin], world.ports[dest]
            leg = cache.get((p.lon, p.lat), (q.lon, q.lat))
            # searoute snaps to its sea network. Connect that network to our
            # configured endpoints so port dwell and travel meet continuously.
            points = [(p.lon, p.lat)] + leg.waypoints + [(q.lon, q.lat)]
            points = [x for i, x in enumerate(points) if i == 0 or x != points[i - 1]]
            if len(points) < 2:
                raise ValueError(f"Degenerate sea route: {origin} → {dest}")
            cumulative = [0.0]
            for x, y in zip(points, points[1:]):
                cumulative.append(cumulative[-1] + haversine_km(x, y))
            pair.append(Leg(points, cumulative))
        result.append(pair)
    return result


def imo_for(index):
    body = str(900000 + index)
    return body + str(sum(int(d) * (7 - i) for i, d in enumerate(body)) % 10)


def vessel_reports(index, config, routes):
    """Yield dictionaries suitable for an AIS feed; no vessel truth ID is emitted.

    RNG state is private to one vessel, so repartitioning preserves the output.
    `arrival_batch` is a release bucket, not the observation timestamp.
    """
    rng = random.Random(config["seed"] + index * 1000003)
    legs = routes[index % len(routes)]
    direction = index % 2
    speed = rng.uniform(*config["speed_knots"])
    lane = rng.gauss(0, config["lane_sigma_metres"])
    # Stagger the fleet so a short run includes port entries and departures.
    arc = rng.uniform(0, legs[direction].length_km)
    dwell = 0.0
    dt = config["cadence_seconds"]
    for tick in range(config["reports_per_vessel"]):
        if tick:
            if dwell > 0:
                dwell = max(0, dwell - dt)
                if dwell == 0:
                    direction = 1 - direction
                    arc = 0.0
            else:
                arc = min(legs[direction].length_km, arc + speed * 1.852 * dt / 3600)
                if arc >= legs[direction].length_km:
                    dwell = max(dt, config["port_dwell_minutes"] * 60 * rng.uniform(0.75, 1.25))
        lon, lat, cog = legs[direction].position_at(arc)
        moored = dwell > 0
        # Fade the lane offset at endpoints, keeping berth dwell inside its location.
        taper = min(1.0, arc / 0.5, (legs[direction].length_km - arc) / 0.5)
        offset = 0 if moored else lane * taper
        beta = math.radians(cog + 90)
        scale = 111320 * max(0.01, abs(math.cos(math.radians(lat))))
        lon += (offset * math.sin(beta) + rng.gauss(0, config["jitter_metres"])) / scale
        lat += (offset * math.cos(beta) + rng.gauss(0, config["jitter_metres"])) / 111320
        t = config["start_epoch"] + tick * dt
        delay = min(rng.lognormvariate(0, 1) * 300, config["lateness_max_seconds"]) if rng.random() * 100 < config["lateness_percent"] else 0
        if rng.random() * 100 < config["dropout_percent"]:
            continue
        batch = int((tick * dt + delay) // config["release_seconds"])
        obs_id = hashlib.sha256(f"{index}|{t}|{lon:.7f}|{lat:.7f}".encode()).hexdigest()[:20]
        record = dict(
            MMSI=str(235000000 + index), BaseDateTime=datetime.fromtimestamp(t, timezone.utc),
            LAT=round(lat, 7), LON=round(lon, 7), SOG=0.0 if moored else speed,
            COG=cog, Heading=cog, VesselName=f"TUTORIAL VESSEL {index}", IMO=None,
            CallSign=f"TUT{index}", VesselType=70, Status=5 if moored else 0,
            Length=150.0, Width=24.0, Draft=7.5, Cargo=70, TransceiverClass="A",
            obs_id=obs_id, event_time_s=t, source="receiver-1", arrival_batch=batch,
        )
        record["reception_id"] = obs_id + "-1"
        yield record
        if rng.random() * 100 < config["duplicate_percent"]:
            yield {**record, "source": "receiver-2", "reception_id": obs_id + "-2"}
