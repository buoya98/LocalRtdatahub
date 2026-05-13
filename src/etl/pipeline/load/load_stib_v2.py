"""Reconstruct distinct STIB vehicle trips from rt_v2.stib_vehicle_position.

Mirrors source/src/etl/pipeline/load/load_stib_v2.py at the schema level
(produces rt_v2.stib_trip / rt_v2.stib_trip_open with `tgeompoint`
trajectories), but operates offline on a fully-loaded position table.

Bench dumps record every observation but lack a vehicle id — without one
the upstream `valueAtTimestamp(trip)` endpoints can't tell concurrent
vehicles on the same (lineid, direction) apart. We rebuild trip identity
with the same greedy spatial+temporal assignment used in real time:

    For each (lineid, direction):
        sort positions by time
        for each position:
            find an open trip whose last point is within MATCH_RADIUS_M
              and whose end_ts is within MATCH_WINDOW_S
            if found  → extend that trip
            else      → open a new trip
            close any trip idle for > TRIP_TIMEOUT_S

Run with:
    python -m src.etl.pipeline.load.load_stib_v2          # all lines
    python -m src.etl.pipeline.load.load_stib_v2 53 56    # specific lineids
"""
from __future__ import annotations

import hashlib
import logging
import math
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Iterator

import psycopg2

# Make `from src.env_manager import …` work when invoked directly.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from src.env_manager import DATABASE_URL  # noqa: E402

# Same constants as upstream load_stib_v2.py — keep matching decisions
# comparable.
MATCH_RADIUS_M = 500.0
MATCH_WINDOW_S = 300.0      # 5 min
TRIP_TIMEOUT_S = 1800       # 30 min

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("load_stib_v2")


def _haversine_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    R = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _new_trip_id(seed: str) -> str:
    return hashlib.sha256(("local|" + seed).encode()).hexdigest()[:32]


def _format_tgeompoint(samples: list[tuple[datetime, float, float]]) -> str:
    parts = [f"Point({lon} {lat})@{ts.isoformat()}" for (ts, lon, lat) in samples]
    if len(parts) == 1:
        # MobilityDB rejects a length-1 sequence with brackets; the instant
        # form is valid.
        return parts[0]
    return "[" + ", ".join(parts) + "]"


class _OpenTrip:
    __slots__ = ("trip_id", "lineid", "direction", "start_ts", "end_ts",
                 "samples", "vehicle_uuid")

    def __init__(self, lineid: str, direction: int, ts: datetime,
                 lon: float, lat: float):
        self.vehicle_uuid = uuid.uuid4().hex
        self.trip_id = _new_trip_id(f"{self.vehicle_uuid}|{ts.isoformat()}")
        self.lineid = lineid
        self.direction = direction
        self.start_ts = ts
        self.end_ts = ts
        self.samples: list[tuple[datetime, float, float]] = [(ts, lon, lat)]

    def can_extend(self, ts: datetime, lon: float, lat: float) -> float | None:
        if (ts - self.end_ts).total_seconds() > MATCH_WINDOW_S:
            return None
        if ts <= self.end_ts:
            return None  # tgeompoint requires strictly increasing ts
        last_ts, last_lon, last_lat = self.samples[-1]
        d = _haversine_m(last_lon, last_lat, lon, lat)
        if d > MATCH_RADIUS_M:
            return None
        return d

    def extend(self, ts: datetime, lon: float, lat: float) -> None:
        self.samples.append((ts, lon, lat))
        self.end_ts = ts


def _flush_trips(conn, trips: list[_OpenTrip], target: str = "rt_v2.stib_trip") -> int:
    if not trips:
        return 0
    inserted = 0
    chunk = 50
    sql = f"""
        INSERT INTO {target}
            (trip_id, vehicle_uuid, lineid, direction, start_ts, end_ts,
             point_count, line_trip_id, trip)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::tgeompoint)
        ON CONFLICT (trip_id) DO NOTHING;
    """
    with conn.cursor() as cur:
        for i in range(0, len(trips), chunk):
            for t in trips[i : i + chunk]:
                cur.execute(
                    sql,
                    (
                        t.trip_id, t.vehicle_uuid, t.lineid, t.direction,
                        t.start_ts, t.end_ts, len(t.samples),
                        # line_trip_id is recomputed at read time by the Flask
                        # endpoints via ROW_NUMBER() (per-line, per-day) — same
                        # pattern as upstream stib_service.py — so we leave it
                        # NULL here instead of writing a misleading lineid_dir
                        # string.
                        None,
                        _format_tgeompoint(t.samples),
                    ),
                )
                inserted += 1
            conn.commit()
    return inserted


def _build_for_pair(
    conn, lineid: str, direction: int,
    positions: list[tuple[datetime, float, float]],
) -> tuple[int, int]:
    open_trips: list[_OpenTrip] = []
    closed: list[_OpenTrip] = []

    for ts, lon, lat in positions:
        kept = []
        for t in open_trips:
            if (ts - t.end_ts).total_seconds() > TRIP_TIMEOUT_S:
                closed.append(t)
            else:
                kept.append(t)
        open_trips = kept

        best_idx = -1
        best_d = float("inf")
        for i, t in enumerate(open_trips):
            d = t.can_extend(ts, lon, lat)
            if d is not None and d < best_d:
                best_d = d
                best_idx = i

        if best_idx >= 0:
            open_trips[best_idx].extend(ts, lon, lat)
        else:
            open_trips.append(_OpenTrip(lineid, direction, ts, lon, lat))

    closed.extend(open_trips)
    keep = [t for t in closed if len(t.samples) >= 2]
    n_dropped = len(closed) - len(keep)
    n_inserted = _flush_trips(conn, keep, target="rt_v2.stib_trip")
    return n_inserted, n_dropped


def _list_pairs(conn, line_filter: list[str] | None) -> list[tuple[str, int]]:
    where = ["lineid IS NOT NULL", "direction IS NOT NULL"]
    params: list = []
    if line_filter:
        where.append("lineid = ANY(%s)")
        params.append(line_filter)
    where_sql = " AND ".join(where)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT lineid, direction, COUNT(*) AS n
              FROM rt_v2.stib_vehicle_position
             WHERE {where_sql}
             GROUP BY lineid, direction
             ORDER BY lineid, direction;
            """,
            tuple(params),
        )
        return [(r[0], int(r[1])) for r in cur.fetchall()]


def _stream_positions(conn, lineid: str, direction: int) -> Iterator[tuple[datetime, float, float]]:
    with conn.cursor(name=f"cur_{lineid}_{direction}") as cur:
        cur.itersize = 5000
        cur.execute(
            """
            SELECT fetched_at, ST_X(geom), ST_Y(geom)
              FROM rt_v2.stib_vehicle_position
             WHERE lineid = %s AND direction = %s AND geom IS NOT NULL
             ORDER BY fetched_at ASC;
            """,
            (lineid, direction),
        )
        for ts, lon, lat in cur:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            yield ts, float(lon), float(lat)


def main() -> int:
    line_filter = sys.argv[1:] or None
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE rt_v2.stib_trip, rt_v2.stib_trip_open;")
            conn.commit()
        log.info("truncated rt_v2.stib_trip and rt_v2.stib_trip_open")

        pairs = _list_pairs(conn, line_filter)
        log.info("processing %d (lineid, direction) pairs", len(pairs))

        total_trips = 0
        total_dropped = 0
        for lineid, direction in pairs:
            positions = list(_stream_positions(conn, lineid, direction))
            if not positions:
                continue
            trips, dropped = _build_for_pair(conn, lineid, direction, positions)
            total_trips += trips
            total_dropped += dropped
            log.info("[%s/%d] positions=%d trips=%d dropped_singletons=%d",
                     lineid, direction, len(positions), trips, dropped)

        log.info("done. total_trips=%d dropped_singletons=%d",
                 total_trips, total_dropped)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
