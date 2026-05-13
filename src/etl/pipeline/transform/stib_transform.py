"""STIB position transform — resolve (lineid, direction, pointid, distance)
into a GPS point and insert into rt_v2.stib_vehicle_position.

Mirrors the role of source/src/etl/pipeline/transform/stib_transform.py
(the "v2 shape-based interp"), but does the geometry math in PostGIS
rather than walking the polyline in Python:

    fraction_at_stop = ST_LineLocatePoint(line_geom, stop_geom)
    target_fraction  = clamp(fraction_at_stop + distance / line_length_m, 0, 1)
    geom             = ST_LineInterpolatePoint(line_geom, target_fraction)

Anchor fallback (no shape, no terminus, etc.): we use the stop geom
directly. Records with no resolvable stop AND no line_geom are dropped.

Used by both ingestion paths:
  * src/etl/ingestion/stib/vehicle_positions_ingestor.py  (live API)
  * src/etl/ingestion/bench/ingestor.py                   (raw .jsonl.gz)
"""
from __future__ import annotations

import hashlib
from typing import Any, Iterable

from psycopg2.extras import execute_values


def position_id(lineid: str, direction: Any, pointid: str,
                fetched_at: str, distance: Any) -> str:
    """Deterministic 128-bit hex hash of one observation's identity.

    The five normalized fields uniquely identify a single STIB vehicle
    sighting. Using the same hash on both ingestion paths (live ODP API
    and raw .jsonl.gz dumps) means re-running ingestion over overlapping
    data is idempotent: duplicates collide on the `position_id` primary
    key and are silently dropped via `ON CONFLICT DO NOTHING`.

    Crucially, `distance` is float-coerced by `_coerce()` before we get
    here, so `int(34)` and `float(34.0)` from the two paths produce the
    same hash. See `_coerce` for the full normalization rules.
    """
    key = f"{lineid}|{direction}|{pointid}|{fetched_at}|{distance}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]


_INSERT_SQL = """
WITH input(position_id, lineid, direction, pointid,
           distance_from_point, fetched_at) AS (
    VALUES %s
),
typed AS (
    SELECT position_id,
           lineid,
           direction::int                   AS direction,
           pointid,
           distance_from_point::double precision AS distance_from_point,
           fetched_at::timestamptz          AS fetched_at
      FROM input
),
resolved AS (
    SELECT t.*,
           lt.direction AS dir_label
      FROM typed t
      LEFT JOIN static.stib_line_terminus lt
             ON lt.lineid = t.lineid
            AND lt.terminus_pointid = t.direction::text
),
geom_resolved AS (
    SELECT r.*,
           ls.geom                         AS line_geom,
           sp.geom                         AS stop_geom,
           ST_Length(ls.geom::geography)   AS line_length_m
      FROM resolved r
      LEFT JOIN static.stib_line_shape ls
             ON ls.lineid = r.lineid AND ls.direction = r.dir_label
      LEFT JOIN static.stib_stop sp
             ON sp.pointid = r.pointid
)
INSERT INTO rt_v2.stib_vehicle_position
    (position_id, vehicle_uuid, lineid, direction, pointid,
     distance_from_point, fetched_at, geom)
SELECT g.position_id,
       NULL,
       g.lineid,
       g.direction,
       g.pointid,
       g.distance_from_point,
       g.fetched_at,
       CASE
           WHEN g.line_geom IS NOT NULL
                AND g.stop_geom IS NOT NULL
                AND g.line_length_m > 0 THEN
               ST_LineInterpolatePoint(
                   g.line_geom,
                   LEAST(1.0, GREATEST(0.0,
                       ST_LineLocatePoint(g.line_geom, g.stop_geom)
                       + COALESCE(g.distance_from_point, 0) / g.line_length_m
                   ))
               )
           ELSE g.stop_geom    -- anchor fallback
       END AS geom
  FROM geom_resolved g
 WHERE g.stop_geom IS NOT NULL OR g.line_geom IS NOT NULL
ON CONFLICT (position_id) DO NOTHING;
"""


def _coerce(rec: dict[str, Any]) -> tuple | None:
    """Convert one position dict into the SQL VALUES tuple. Accepts both the
    raw STIB-feed shape (`directionId`, `pointId`, `distanceFromPoint`,
    `poll_ts`) and the normalized shape (`direction`, `pointid`,
    `distance_from_point`, `fetched_at`)."""
    # Use explicit None-check rather than `or` so that legitimate falsy
    # values (e.g. `direction=0`, `pointid="0"`) don't fall through to
    # the camelCase alias.
    def _first_set(*candidates: Any) -> Any:
        for c in candidates:
            if c is not None and c != "":
                return c
        return None

    lineid = _first_set(rec.get("lineid"), rec.get("lineId"))
    direction = _first_set(rec.get("direction"), rec.get("directionId"))
    pointid = _first_set(rec.get("pointid"), rec.get("pointId"))
    if lineid is None or direction is None or pointid is None:
        return None

    raw_distance = rec.get("distance_from_point", rec.get("distanceFromPoint"))
    # Coerce to float so int(34)/float(34.0) hash to the same position_id —
    # otherwise the API path and the raw bench path produce duplicate rows.
    try:
        distance = float(raw_distance) if raw_distance is not None else None
    except (TypeError, ValueError):
        distance = None

    fetched_at = (
        rec.get("fetched_at")
        or rec.get("poll_ts")
        or rec.get("fetched_at_utc")
    )
    if not fetched_at:
        return None

    pid = rec.get("position_id") or position_id(
        str(lineid), direction, str(pointid), str(fetched_at), distance
    )
    return (
        pid,
        str(lineid),
        str(direction),
        str(pointid),
        distance,
        str(fetched_at),
    )


def insert_positions(conn, records: Iterable[dict[str, Any]],
                     batch_size: int = 5000) -> int:
    """Insert position dicts into rt_v2.stib_vehicle_position with geom
    computed in SQL. Returns the number of *new* rows inserted (excludes
    ON CONFLICT skips). Caller is responsible for `conn.commit()`."""
    batch: list[tuple] = []
    inserted = 0
    for rec in records:
        row = _coerce(rec)
        if row is None:
            continue
        batch.append(row)
        if len(batch) >= batch_size:
            inserted += _flush(conn, batch)
            batch.clear()
    if batch:
        inserted += _flush(conn, batch)
    return inserted


def _flush(conn, batch: list[tuple]) -> int:
    with conn.cursor() as cur:
        execute_values(cur, _INSERT_SQL, batch, page_size=len(batch))
        # rowcount on INSERT … SELECT reports actually-inserted rows after
        # ON CONFLICT filtering on PostgreSQL ≥ 9.5.
        return max(cur.rowcount, 0)
