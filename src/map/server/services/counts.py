"""Health + counts — minimal subset of source/src/map/server/services/counts.py.

LocalRtdatahub doesn't ingest the upstream's full multi-source dataset, so
counts only cover the tables we actually populate.
"""
from __future__ import annotations

from typing import Any

from ..context import MapAppContext


def fetch_health(ctx: MapAppContext) -> dict[str, Any]:
    with ctx.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM pg_extension WHERE extname='postgis')    AS postgis,
                    (SELECT COUNT(*) FROM pg_extension WHERE extname='mobilitydb') AS mobilitydb,
                    (SELECT COUNT(*) FROM rt.stib_vehicle_position)             AS positions,
                    (SELECT COUNT(*) FROM rt.stib_trip)                         AS trips;
                """
            )
            row = cur.fetchone()
    return {
        "ok": True,
        "postgis":    bool(row[0]),
        "mobilitydb": bool(row[1]),
        "positions":  int(row[2] or 0),
        "trips":      int(row[3] or 0),
    }


def fetch_counts(ctx: MapAppContext) -> dict[str, int]:
    with ctx.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM rt.stib_vehicle_position)        AS positions,
                    (SELECT COUNT(*) FROM rt.stib_trip)                    AS trips,
                    (SELECT COUNT(*) FROM static.stib_stop)                   AS stops,
                    (SELECT COUNT(*) FROM static.stib_line_shape)             AS line_shapes,
                    (SELECT COUNT(*) FROM transport_local.stib_waiting_time)  AS waiting_times;
                """
            )
            row = cur.fetchone()
    return {
        "positions": int(row[0] or 0),
        "trips": int(row[1] or 0),
        "stops": int(row[2] or 0),
        "line_shapes": int(row[3] or 0),
        "waiting_times": int(row[4] or 0),
    }
