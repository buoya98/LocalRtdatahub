"""MVT layer SQL — mirrors source/src/map/server/queries/layers/stib*.py.

LocalRtdatahub only ingests STIB lines + stops, so only those two layers
return real tiles. All other upstream layers (transport_segments,
delijn_*, tomtom_*, waze_*, magneto_*, anpr_*, traffic_lights, etc.) are
absent from LAYER_SQL and the `/tiles/<layer>/...` route returns 204.
"""

_STIB_LINES_MVT_SQL = """
    WITH bounds AS (SELECT ST_TileEnvelope(%s, %s, %s) AS geom_3857),
    bounds_4326 AS (SELECT ST_Transform(geom_3857, 4326) AS geom_4326 FROM bounds),
    mvtgeom AS (
        SELECT s.lineid,
               s.direction,
               COALESCE(s.mode, 'other') AS mode,
               s.route_color,
               s.route_text_color,
               s.variant,
               ST_AsMVTGeom(
                   ST_SimplifyVW(ST_Transform(s.geom, 3857), 4.0),
                   b.geom_3857, 4096, 64, true
               ) AS geom
          FROM static.stib_line_shape s
          CROSS JOIN bounds b
          CROSS JOIN bounds_4326 b4326
         WHERE s.geom IS NOT NULL AND NOT ST_IsEmpty(s.geom)
           AND s.geom && b4326.geom_4326
           AND (%s::text[] IS NULL OR s.lineid = ANY(%s::text[]))
    )
    SELECT ST_AsMVT(mvtgeom, 'stib_lines', 4096, 'geom') AS tile FROM mvtgeom;
"""

_STIB_STOPS_MVT_SQL = """
    WITH bounds AS (SELECT ST_TileEnvelope(%s, %s, %s) AS geom_3857),
    bounds_4326 AS (SELECT ST_Transform(geom_3857, 4326) AS geom_4326 FROM bounds),
    mvtgeom AS (
        SELECT s.pointid,
               s.stop_name_fr,
               s.stop_name_nl,
               ST_AsMVTGeom(
                   ST_Transform(s.geom, 3857),
                   b.geom_3857, 4096, 64, true
               ) AS geom
          FROM static.stib_stop s
          CROSS JOIN bounds b
          CROSS JOIN bounds_4326 b4326
         WHERE s.geom IS NOT NULL AND NOT ST_IsEmpty(s.geom)
           AND s.geom && b4326.geom_4326
           AND (
                %s::text[] IS NULL
                OR EXISTS (
                    SELECT 1 FROM static.stib_line_stops ls
                     WHERE ls.pointid = s.pointid
                       AND ls.lineid = ANY(%s::text[])
                )
           )
    )
    SELECT ST_AsMVT(mvtgeom, 'stib_stops', 4096, 'geom') AS tile FROM mvtgeom;
"""


# Layers that accept a `lines=…` filter from the URL.
LINE_FILTERED_LAYERS = {"stib_lines", "stib_stops"}

LAYER_SQL: dict[str, str] = {
    "stib_lines": _STIB_LINES_MVT_SQL,
    "stib_stops": _STIB_STOPS_MVT_SQL,
}
