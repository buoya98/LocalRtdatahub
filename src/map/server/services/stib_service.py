"""STIB service — mirrors source/src/map/server/services/stib_service.py.

Functions match the upstream signatures and return shapes so the upstream
templates/index.html works against this server unchanged.

Reads from:
  * static.stib_line_shape, static.stib_line_stops, static.stib_line_terminus
    (populated by src/etl/ingestion/stib/)
  * static.stib_stop  (populated by both ingestion paths)
  * rt_v2.stib_trip_all  (populated by src/etl/pipeline/load/load_stib_v2.py)
  * rt_v2.stib_vehicle_position  (populated by src/etl/ingestion/bench/)
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from ..context import MapAppContext


# ---------------------------------------------------------------------------
# Lines catalog — /api/stib/lines
# ---------------------------------------------------------------------------

# Refactored from N+1 correlated subqueries (one per line, ~73 loops) to a
# single grouped scan + ROW_NUMBER. Backport of upstream commit c43aaa2
# ("Optimise massivement la fluidité de la map") — measured 29s cold / 3.5s
# warm → 1.1s / 2ms in upstream's bench. We keep the offline-replay-friendly
# `> (MAX(end_ts) - 7 days)` time predicate instead of upstream's `> NOW()`.
_LINES_CATALOG_SQL = """
    WITH shape_agg AS (
        SELECT
            lineid,
            COALESCE(MAX(mode), 'other') AS mode,
            MAX(route_color)             AS route_color,
            MAX(route_text_color)        AS route_text_color
          FROM static.stib_line_shape
         GROUP BY lineid
    ),
    dests AS (
        SELECT
            ls.lineid,
            array_agg(DISTINCT ls.destination ORDER BY ls.destination) AS destinations
          FROM static.stib_line_stops ls
         WHERE ls.destination IS NOT NULL
         GROUP BY ls.lineid
    ),
    -- Single global scan of rt_v2.stib_trip_all over the last 7 days,
    -- joined once to the static lookup tables. Replaces N+1 correlated
    -- subqueries (~73 line loops in upstream's data, ~257k heap pages).
    cutoff AS (
        SELECT GREATEST(MAX(end_ts) - INTERVAL '7 days', MIN(start_ts)) AS t
          FROM rt_v2.stib_trip_all
    ),
    trip_term AS (
        SELECT
            t.lineid,
            t.direction,
            COALESCE(lt.destination, sp.stop_name_fr) AS destination,
            COUNT(*) AS trips
          FROM rt_v2.stib_trip_all t
          CROSS JOIN cutoff c
          LEFT JOIN static.stib_line_terminus lt
                 ON lt.lineid = t.lineid
                AND lt.terminus_pointid = t.direction::text
          LEFT JOIN static.stib_stop sp
                 ON sp.pointid = t.direction::text
         WHERE t.direction IS NOT NULL
           AND t.start_ts > c.t
         GROUP BY t.lineid, t.direction, lt.destination, sp.stop_name_fr
    ),
    per_dest AS (
        -- Dedup per (lineid, destination name). Anonymous (NULL destination)
        -- rows stay separate via the 'dir_<direction>' synthetic key.
        SELECT
            lineid,
            COALESCE(destination, 'dir_' || direction::text) AS dest_key,
            destination,
            SUM(trips)                                       AS trips,
            (ARRAY_AGG(direction ORDER BY trips DESC))[1]    AS direction
          FROM trip_term
         GROUP BY lineid, dest_key, destination
    ),
    ranked AS (
        SELECT
            lineid, direction, destination, trips,
            ROW_NUMBER() OVER (
                PARTITION BY lineid
                ORDER BY trips DESC, destination NULLS LAST, direction
            ) AS rn
          FROM per_dest
    ),
    top4 AS (
        SELECT
            lineid,
            json_agg(jsonb_build_object(
                'direction',   direction,
                'destination', destination,
                'trips',       trips)
                ORDER BY trips DESC, destination NULLS LAST, direction
            ) AS directions_detail
          FROM ranked
         WHERE rn <= 4
         GROUP BY lineid
    )
    SELECT
        s.lineid,
        s.mode,
        s.route_color,
        s.route_text_color,
        d.destinations,
        t4.directions_detail
      FROM shape_agg s
      LEFT JOIN dests d  ON d.lineid  = s.lineid
      LEFT JOIN top4  t4 ON t4.lineid = s.lineid;
"""


def _line_sort_key(entry: dict[str, Any]) -> tuple:
    rank = {"metro": 0, "tram": 1, "bus": 2, "rail": 3, "funicular": 4, "other": 5}
    lid = (entry.get("lineid") or "").strip()
    mode = entry.get("mode") or "other"
    mrank = rank.get(mode, 5)
    try:
        return (mrank, 0, int(lid))
    except (TypeError, ValueError):
        return (mrank, 1, lid)


def fetch_stib_lines_catalog(ctx: MapAppContext) -> dict[str, Any]:
    with ctx.pool.connection() as conn:
        with conn.cursor() as cur:
            # JIT compilation costs ~600 ms on this query (estimated cost
            # exceeds default jit_above_cost=100k) but the query itself only
            # runs ~1-2 s — net negative. Disable for this transaction.
            cur.execute("SET LOCAL jit = off")
            cur.execute(_LINES_CATALOG_SQL)
            rows = cur.fetchall()
    lines = [
        {
            "lineid": r[0],
            "mode": r[1] or "other",
            "route_color": r[2],
            "route_text_color": r[3],
            "destinations": list(r[4] or []),
            "directions_detail": list(r[5] or []),
        }
        for r in rows
    ]
    lines.sort(key=_line_sort_key)
    return {"ok": True, "count": len(lines), "lines": lines}


# ---------------------------------------------------------------------------
# Stop details — /api/stib/stop/<pointid>
# ---------------------------------------------------------------------------

_STOP_DETAILS_SQL = """
    WITH line_colors AS (
        SELECT lineid,
               MAX(route_color)      AS route_color,
               MAX(route_text_color) AS route_text_color,
               MAX(mode)             AS mode
          FROM static.stib_line_shape
         GROUP BY lineid
    ),
    line_rows AS (
        SELECT ls.lineid, ls.direction, ls.destination,
               lc.route_color, lc.route_text_color, lc.mode
          FROM static.stib_line_stops ls
          LEFT JOIN line_colors lc ON lc.lineid = ls.lineid
         WHERE ls.pointid = %s
    ),
    lines_agg AS (
        SELECT lineid,
               MAX(route_color)      AS route_color,
               MAX(route_text_color) AS route_text_color,
               MAX(mode)             AS mode,
               array_agg(DISTINCT destination ORDER BY destination)
                   FILTER (WHERE destination IS NOT NULL) AS destinations,
               array_agg(DISTINCT direction ORDER BY direction)
                   FILTER (WHERE direction IS NOT NULL)   AS directions
          FROM line_rows
         GROUP BY lineid
    )
    SELECT s.pointid, s.stop_name_fr, s.stop_name_nl,
           ST_X(s.geom) AS lon, ST_Y(s.geom) AS lat,
           COALESCE(
               (SELECT json_agg(jsonb_build_object(
                           'lineid',       la.lineid,
                           'mode',         la.mode,
                           'color',        la.route_color,
                           'text_color',   la.route_text_color,
                           'destinations', la.destinations,
                           'directions',   la.directions)
                       ORDER BY la.lineid)
                  FROM lines_agg la),
               '[]'::json
           ) AS lines
      FROM static.stib_stop s
     WHERE s.pointid = %s;
"""


def fetch_stib_stop_details(ctx: MapAppContext, pointid: str) -> dict[str, Any]:
    with ctx.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_STOP_DETAILS_SQL, (pointid, pointid))
            row = cur.fetchone()
    if row is None:
        return {"ok": False, "error": "stop not found", "pointid": pointid}

    def _sort_key(entry: dict[str, Any]) -> tuple:
        lid = entry.get("lineid") or ""
        try:
            return (0, int(lid))
        except (TypeError, ValueError):
            return (1, lid)

    lines = sorted(list(row[5] or []), key=_sort_key)
    return {
        "ok": True,
        "pointid": row[0],
        "stop_name_fr": row[1],
        "stop_name_nl": row[2],
        "lon": row[3],
        "lat": row[4],
        "lines": lines,
    }


# ---------------------------------------------------------------------------
# Shape — /api/stib/shape/<lineid>/<direction>
# ---------------------------------------------------------------------------

_SHAPE_SQL = """
    SELECT lineid, direction, mode, route_color, route_text_color,
           ST_AsGeoJSON(geom)::json AS geometry
      FROM static.stib_line_shape
     WHERE lineid = %s AND direction = %s
     LIMIT 1;
"""


def fetch_stib_shape(ctx: MapAppContext, lineid: str, direction: str) -> dict[str, Any]:
    with ctx.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_SHAPE_SQL, (lineid, direction))
            row = cur.fetchone()
    if not row:
        return {"ok": True, "lineid": lineid, "direction": direction,
                "shape_id": None, "geometry": None}
    return {
        "ok": True,
        "lineid": row[0],
        "direction": row[1],
        "mode": row[2],
        "route_color": row[3],
        "route_text_color": row[4],
        "shape_id": f"{row[0]}_{row[1]}",
        "geometry": row[5],
    }


# ---------------------------------------------------------------------------
# Live positions — /api/stib/live-positions
# ---------------------------------------------------------------------------

_LIVE_POSITIONS_SQL = """
    WITH win AS (SELECT %s::timestamptz AS at_ts),
    active AS (
        SELECT t.trip_id,
               t.lineid,
               t.direction,
               t.lineid || '_' || LPAD(ROW_NUMBER() OVER (
                   PARTITION BY t.lineid,
                                date_trunc('day',
                                    CASE WHEN t.lineid ~ '^N'
                                         THEN t.start_ts AT TIME ZONE 'Europe/Brussels'
                                         ELSE (t.start_ts AT TIME ZONE 'Europe/Brussels')
                                              - INTERVAL '3 hours 30 minutes'
                                    END)
                   ORDER BY t.start_ts, t.trip_id
               )::text, 3, '0') AS line_trip_id,
               valueAtTimestamp(t.trip, w.at_ts) AS pt
          FROM rt_v2.stib_trip_all t
          CROSS JOIN win w
         WHERE (%s::text[] IS NULL OR t.lineid = ANY(%s::text[]))
           AND (%s::int    IS NULL OR t.direction = %s::int)
           -- Backport of upstream c43aaa2: express the time bounds on
           -- start_ts / end_ts so the planner can use a composite
           -- (lineid, start_ts) index. The tstzspan check stays as a
           -- tighter post-filter. Measured: 3.4s → 22ms in upstream.
           AND t.start_ts <= w.at_ts + interval '30 minutes'
           AND t.end_ts   >= w.at_ts - interval '30 minutes'
           AND t.trip::tstzspan && span(
                   w.at_ts - interval '30 minutes',
                   w.at_ts + interval '30 minutes')
    ),
    line_meta AS (
        SELECT lineid,
               MAX(route_color)      AS route_color,
               MAX(route_text_color) AS route_text_color,
               MAX(mode)             AS mode
          FROM static.stib_line_shape
         GROUP BY lineid
    )
    SELECT a.trip_id, a.lineid, a.direction, a.line_trip_id,
           ST_X(a.pt::geometry) AS lon,
           ST_Y(a.pt::geometry) AS lat,
           lm.route_color, lm.route_text_color, lm.mode
      FROM active a
      LEFT JOIN line_meta lm ON lm.lineid = a.lineid
     WHERE a.pt IS NOT NULL
     LIMIT 5000;
"""


def _resolve_at_ts(ctx: MapAppContext, time_filter: dict[str, Any] | None) -> datetime:
    if time_filter and time_filter.get("mode") == "range" and time_filter.get("end"):
        return time_filter["end"]
    # relative mode → use MAX(fetched_at) so an offline-loaded DB still resolves
    # to a meaningful instant (instead of "now()" which leaves no active trips).
    with ctx.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(end_ts) FROM rt_v2.stib_trip_all;")
            row = cur.fetchone()
    return (row and row[0]) or datetime.now(timezone.utc)


def fetch_stib_live_positions(
    ctx: MapAppContext,
    lines: list[str] | None,
    time_filter: dict[str, Any] | None,
    direction: int | None,
) -> dict[str, Any]:
    at_ts = _resolve_at_ts(ctx, time_filter)
    with ctx.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL jit = off")
            cur.execute(
                _LIVE_POSITIONS_SQL,
                (at_ts, lines, lines, direction, direction),
            )
            rows = cur.fetchall()
    features: list[dict[str, Any]] = []
    for trip_id, lineid, dir_, line_trip_id, lon, lat, color, tcolor, mode in rows:
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "trip_id": trip_id,
                "lineid": lineid,
                "direction": dir_,
                "line_trip_id": line_trip_id,
                "route_color": color,
                "route_text_color": tcolor,
                "mode": mode,
            },
        })
    return {"ok": True, "count": len(features),
            "type": "FeatureCollection", "features": features}


# ---------------------------------------------------------------------------
# Trajectories — /api/stib/trajectories
# ---------------------------------------------------------------------------

# Backport of upstream c43aaa2 ("Optimise massivement la fluidité de la map"):
# emit one row per *native* GPS fix stored in the MobilityDB tgeompoint (via
# `atTime` + `instants()`), instead of resampling every N seconds with
# `generate_series + valueAtTimestamp`. `valueAtTimestamp` only ever
# linearly interpolates between native points anyway, so resampling produced
# ~18× more samples with zero extra precision. The frontend does the linear
# interpolation locally. Measured upstream: 12s/32s → 100ms/230ms.
#
# `geom_lambert` STORED GENERATED column from upstream is skipped here (it
# would force a schema change) — we keep `frac=NULL` in the output. The
# frontend just renders the point; it doesn't depend on `frac`.
_TRAJECTORIES_SQL = """
    WITH win AS (SELECT %s::timestamptz AS w_start, %s::timestamptz AS w_end),
    trips_in_window AS (
        SELECT t.trip_id, t.lineid, t.direction, t.start_ts, t.trip,
               -- per-line, per-day sequential trip number: noctis lines
               -- (N*) count from local midnight, others from 03:30 to keep
               -- a whole operational night together.
               t.lineid || '_' || LPAD(ROW_NUMBER() OVER (
                   PARTITION BY t.lineid,
                                date_trunc('day',
                                    CASE WHEN t.lineid ~ '^N'
                                         THEN t.start_ts AT TIME ZONE 'Europe/Brussels'
                                         ELSE (t.start_ts AT TIME ZONE 'Europe/Brussels')
                                              - INTERVAL '3 hours 30 minutes'
                                    END)
                   ORDER BY t.start_ts, t.trip_id
               )::text, 3, '0') AS computed_line_trip_id,
               (SELECT ls.direction FROM static.stib_line_stops ls
                 WHERE ls.lineid = t.lineid AND ls.pointid = t.direction::text
                 ORDER BY ls.sequence_idx DESC LIMIT 1) AS dir_label
          FROM rt_v2.stib_trip_all t
          CROSS JOIN win w
         WHERE (%s::text[] IS NULL OR t.lineid = ANY(%s::text[]))
           AND (%s::int    IS NULL OR t.direction = %s::int)
           -- Indexable predicate on start_ts/end_ts (composite (lineid,
           -- start_ts) index). The tstzspan check stays as a tighter
           -- post-filter.
           AND t.start_ts < w.w_end
           AND t.end_ts   > w.w_start
           AND t.trip::tstzspan && span(w.w_start, w.w_end)
    ),
    -- One row per native GPS instant in the window (MobilityDB tgeompoint).
    native_samples AS (
        SELECT
            t.trip_id, t.lineid, t.dir_label,
            t.computed_line_trip_id AS line_trip_id,
            getTimestamp(inst) AS ts,
            ST_SetSRID(getValue(inst)::geometry, 4326) AS pt
          FROM trips_in_window t
          CROSS JOIN win w
          CROSS JOIN LATERAL unnest(
              instants(atTime(t.trip, span(w.w_start, w.w_end)))
          ) AS inst
    ),
    line_meta AS (
        SELECT lineid,
               MAX(route_color)      AS route_color,
               MAX(route_text_color) AS route_text_color,
               MAX(mode)             AS mode
          FROM static.stib_line_shape
         GROUP BY lineid
    )
    SELECT s.trip_id, s.lineid, s.dir_label, s.line_trip_id,
           lm.mode, lm.route_color, lm.route_text_color,
           array_agg(jsonb_build_array(
               (extract(epoch FROM s.ts) * 1000)::bigint,
               ST_X(s.pt), ST_Y(s.pt), NULL
           ) ORDER BY s.ts) AS samples
      FROM native_samples s
      LEFT JOIN line_meta lm ON lm.lineid = s.lineid
     WHERE s.pt IS NOT NULL
     GROUP BY s.trip_id, s.lineid, s.dir_label, s.line_trip_id,
              lm.mode, lm.route_color, lm.route_text_color
     LIMIT 5000;
"""


def _resolve_window(ctx: MapAppContext, time_filter: dict[str, Any]) -> tuple[datetime, datetime]:
    if time_filter.get("mode") == "range" and time_filter.get("start") and time_filter.get("end"):
        return time_filter["start"], time_filter["end"]
    # relative window ending at MAX(fetched_at)
    end = _resolve_at_ts(ctx, time_filter)
    start = end - timedelta(hours=float(time_filter.get("hours") or 1.0))
    return start, end


def fetch_stib_trajectories(
    ctx: MapAppContext,
    lines: list[str] | None,
    time_filter: dict[str, Any],
    sample: str,
    direction: int | None,
) -> dict[str, Any]:
    """`sample` is accepted for URL backwards-compat but ignored: the SQL now
    returns native MobilityDB instants (one row per real GPS fix), not a
    fixed-interval resample. The frontend interpolates locally."""
    _ = sample  # explicitly unused
    start, end = _resolve_window(ctx, time_filter)
    with ctx.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL jit = off")
            cur.execute(
                _TRAJECTORIES_SQL,
                (start, end, lines, lines, direction, direction),
            )
            rows = cur.fetchall()

    trips: list[dict[str, Any]] = []
    for trip_id, lineid, dir_label, line_trip_id, mode, color, tcolor, samples in rows:
        if not samples:
            continue
        trips.append({
            "trip_id": trip_id,
            "lineid": lineid,
            "mode": mode,
            "route_color": color,
            "route_text_color": tcolor,
            # Defensive fallback to bare lineid if computed_line_trip_id is
            # somehow NULL (shouldn't happen with our ROW_NUMBER+LPAD but
            # matches upstream contract — frontend uses this as a stable key).
            "line_trip_id": line_trip_id or lineid,
            "dir_label": dir_label,
            "shape_id": f"stib:{lineid}:{dir_label}" if dir_label else None,
            "samples": samples,
        })
    return {
        "ok": True,
        "count": len(trips),
        "trips": trips,
        "window_start_ms": int(start.timestamp() * 1000),
        "window_end_ms":   int(end.timestamp()   * 1000),
    }


# ---------------------------------------------------------------------------
# Alerts — rt.stib_alert is not populated locally
# ---------------------------------------------------------------------------

def fetch_stib_alerts(
    ctx: MapAppContext,
    lines: list[str] | None,
    time_filter: dict[str, Any],
    alert_type: str | None,
    active_only: bool,
) -> dict[str, Any]:
    return {"ok": True, "count": 0, "alerts": []}
