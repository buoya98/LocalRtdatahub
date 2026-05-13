"""Unified search — STIB-only subset of source/src/map/server/services/search.py.

De Lijn / FLIR cameras / traffic lights aren't ingested locally, so the
upstream UNIONs over those tables are removed.
"""
from __future__ import annotations

from typing import Any

from ..context import MapAppContext


_MAX_PER_CATEGORY = 5

_SEARCH_SQL = """
WITH stib_stops AS (
    SELECT
        'stib_stop'  AS type,
        'Arrêt STIB' AS category,
        pointid      AS id,
        COALESCE(stop_name_fr, stop_name_nl, pointid) AS label,
        pointid      AS sub,
        ST_Y(geom)   AS lat,
        ST_X(geom)   AS lon
    FROM static.stib_stop
    WHERE (stop_name_fr ILIKE %(pat)s
        OR stop_name_nl ILIKE %(pat)s
        OR pointid      ILIKE %(pat)s)
      AND geom IS NOT NULL
    LIMIT %(n)s
),
stib_lines AS (
    SELECT DISTINCT ON (lineid)
        'stib_line'              AS type,
        'Ligne STIB'             AS category,
        lineid                   AS id,
        'Ligne STIB ' || lineid  AS label,
        COALESCE(mode, '')       AS sub,
        ST_Y(ST_Centroid(geom))  AS lat,
        ST_X(ST_Centroid(geom))  AS lon
    FROM static.stib_line_shape
    WHERE lineid ILIKE %(pat)s
    ORDER BY lineid
    LIMIT %(n)s
)
SELECT * FROM stib_stops
UNION ALL SELECT * FROM stib_lines;
"""

_CATEGORY_ORDER = ["Arrêt STIB", "Ligne STIB"]


def fetch_search_results(ctx: MapAppContext, query: str) -> list[dict[str, Any]]:
    pattern = f"%{query}%"
    rows: list[dict[str, Any]] = []

    with ctx.pool.connection(timeout=ctx.cfg.db_acquire_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT set_config('statement_timeout', %s, true)",
                (f"{ctx.cfg.db_statement_timeout_ms}ms",),
            )
            cur.execute(_SEARCH_SQL, {"pat": pattern, "n": _MAX_PER_CATEGORY})
            for type_, category, id_, label, sub, lat, lon in cur.fetchall():
                if lat is None or lon is None:
                    continue
                rows.append({
                    "type": type_,
                    "category": category,
                    "id": id_,
                    "label": label,
                    "sub": sub or "",
                    "lat": float(lat),
                    "lon": float(lon),
                })

    rows.sort(key=lambda r: (
        _CATEGORY_ORDER.index(r["category"]) if r["category"] in _CATEGORY_ORDER else 99,
        r["label"],
    ))
    return rows
