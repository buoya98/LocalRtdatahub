-- LocalRtdatahub — Transport schema (PostGIS + MobilityDB)
--
-- Extracted from upstream RTDataHub
--   source/src/etl/pipeline/load/create_tables_v2.py  (rt_v2.stib_*)
--   source/src/etl/pipeline/load/create_tables.py     (static.stib_*)
--
-- Scope: Transport-only standalone — bench data + STIB GTFS static data.

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS mobilitydb CASCADE;

CREATE SCHEMA IF NOT EXISTS static;
CREATE SCHEMA IF NOT EXISTS rt_v2;
CREATE SCHEMA IF NOT EXISTS transport_local;


-- ============================================================
-- STATIC reference tables — populated by the upstream STIB ODP ingestors
-- (scripts/ingest_gtfs.sh) and as a side-effect of ingest_bench.py.
-- ============================================================

CREATE TABLE IF NOT EXISTS static.stib_stop (
    pointid        TEXT PRIMARY KEY,
    stop_name_fr   TEXT,
    stop_name_nl   TEXT,
    geom           GEOMETRY(Point, 4326),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_stib_stop_geom ON static.stib_stop USING GIST(geom);
ALTER TABLE static.stib_stop ADD COLUMN IF NOT EXISTS stop_name_fr TEXT;
ALTER TABLE static.stib_stop ADD COLUMN IF NOT EXISTS stop_name_nl TEXT;

CREATE TABLE IF NOT EXISTS static.stib_line_stops (
    lineid        TEXT NOT NULL,
    direction     TEXT NOT NULL,
    sequence_idx  INTEGER NOT NULL,
    pointid       TEXT NOT NULL,
    destination   TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (lineid, direction, sequence_idx)
);
CREATE INDEX IF NOT EXISTS idx_stib_line_stops_point ON static.stib_line_stops(lineid, direction, pointid);
CREATE INDEX IF NOT EXISTS idx_stib_line_stops_pid   ON static.stib_line_stops(pointid);

CREATE TABLE IF NOT EXISTS static.stib_line_terminus (
    lineid             TEXT NOT NULL,
    terminus_pointid   TEXT NOT NULL,
    direction          TEXT NOT NULL,
    destination        TEXT,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (lineid, terminus_pointid)
);

CREATE TABLE IF NOT EXISTS static.stib_line_shape (
    lineid            TEXT NOT NULL,
    direction         TEXT NOT NULL,
    variant           INTEGER,
    mode              TEXT,
    route_color       TEXT,
    route_text_color  TEXT,
    geom              GEOMETRY(LineString, 4326),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (lineid, direction)
);
CREATE INDEX IF NOT EXISTS idx_stib_line_shape_geom ON static.stib_line_shape USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_stib_line_shape_lid  ON static.stib_line_shape(lineid);


-- ============================================================
-- RT v2 schema — bench/shadow schema (target of ingest_bench.py)
-- ============================================================

CREATE TABLE IF NOT EXISTS rt_v2.stib_vehicle_position (
    position_id          TEXT PRIMARY KEY,
    vehicle_uuid         TEXT,
    lineid               TEXT,
    direction            INTEGER,
    pointid              TEXT,
    distance_from_point  DOUBLE PRECISION,
    fetched_at           TIMESTAMPTZ NOT NULL,
    geom                 GEOMETRY(Point, 4326),
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_v2_stib_pos_lineid  ON rt_v2.stib_vehicle_position(lineid);
CREATE INDEX IF NOT EXISTS idx_v2_stib_pos_fetched ON rt_v2.stib_vehicle_position(fetched_at);
CREATE INDEX IF NOT EXISTS idx_v2_stib_pos_geom    ON rt_v2.stib_vehicle_position USING GIST(geom);


-- ============================================================
-- TRIP tables — populated by scripts/build_trips.py via greedy
-- spatial+temporal assignment of rt_v2.stib_vehicle_position.
-- Schema is the upstream rt_v2 (MobilityDB tgeompoint).
-- ============================================================

CREATE TABLE IF NOT EXISTS rt_v2.stib_trip_open (
    trip_id              TEXT PRIMARY KEY,
    vehicle_uuid         TEXT,
    lineid               TEXT NOT NULL,
    direction            INTEGER NOT NULL,
    start_ts             TIMESTAMPTZ NOT NULL,
    end_ts               TIMESTAMPTZ NOT NULL,
    point_count          INTEGER NOT NULL DEFAULT 0,
    line_trip_id         TEXT,
    trip                 tgeompoint,
    last_point           GEOMETRY(Point, 4326),
    last_dir_label       TEXT,
    last_pos_m_on_shape  DOUBLE PRECISION,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_v2_stib_trip_open_lineid ON rt_v2.stib_trip_open(lineid, direction, end_ts);
CREATE INDEX IF NOT EXISTS idx_v2_stib_trip_open_end    ON rt_v2.stib_trip_open(end_ts);
CREATE INDEX IF NOT EXISTS idx_v2_stib_trip_open_lastpt ON rt_v2.stib_trip_open USING GIST(last_point);
-- Mirror the GIST on the trip column so the `t.trip::tstzspan && span(…)`
-- post-filter is index-eligible on both branches of stib_trip_all.
CREATE INDEX IF NOT EXISTS idx_v2_stib_trip_open_gist   ON rt_v2.stib_trip_open USING GIST(trip);

CREATE TABLE IF NOT EXISTS rt_v2.stib_trip (
    trip_id         TEXT PRIMARY KEY,
    vehicle_uuid    TEXT,
    lineid          TEXT NOT NULL,
    direction       INTEGER NOT NULL,
    start_ts        TIMESTAMPTZ NOT NULL,
    end_ts          TIMESTAMPTZ NOT NULL,
    point_count     INTEGER NOT NULL DEFAULT 0,
    line_trip_id    TEXT,
    daily_trip_seq  INTEGER,
    trip            tgeompoint,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_v2_stib_trip_lineid ON rt_v2.stib_trip(lineid);
CREATE INDEX IF NOT EXISTS idx_v2_stib_trip_start  ON rt_v2.stib_trip(start_ts);
CREATE INDEX IF NOT EXISTS idx_v2_stib_trip_end    ON rt_v2.stib_trip(end_ts);
CREATE INDEX IF NOT EXISTS idx_v2_stib_trip_gist   ON rt_v2.stib_trip USING GIST(trip);
-- Composite index used by /api/stib/{live-positions,trajectories} after the
-- c43aaa2 query refactor: predicate `start_ts <= … AND end_ts >= …` becomes
-- index-eligible, eliminating a planner BitmapAnd over the two single-col
-- indexes. Upstream-measured impact: 3.4s → 22ms (live-positions, no filter).
CREATE INDEX IF NOT EXISTS idx_v2_stib_trip_lineid_start
    ON rt_v2.stib_trip(lineid, start_ts DESC);
CREATE INDEX IF NOT EXISTS idx_v2_stib_trip_open_lineid_start
    ON rt_v2.stib_trip_open(lineid, start_ts DESC);

DROP VIEW IF EXISTS rt_v2.stib_trip_all;
CREATE VIEW rt_v2.stib_trip_all AS
SELECT trip_id, vehicle_uuid, lineid, direction, start_ts, end_ts,
       point_count, line_trip_id, NULL::INTEGER AS daily_trip_seq, trip, ingested_at
  FROM rt_v2.stib_trip_open
UNION ALL
SELECT trip_id, vehicle_uuid, lineid, direction, start_ts, end_ts,
       point_count, line_trip_id, daily_trip_seq, trip, ingested_at
  FROM rt_v2.stib_trip;


-- ============================================================
-- LOCAL extension — waiting times
-- ============================================================
CREATE TABLE IF NOT EXISTS transport_local.stib_waiting_time (
    id           BIGSERIAL PRIMARY KEY,
    poll_ts      TIMESTAMPTZ NOT NULL,
    pointid      TEXT,
    lineid       TEXT,
    expected     TIMESTAMPTZ,
    destination  TEXT,
    message      TEXT
);
CREATE INDEX IF NOT EXISTS idx_stib_wt_poll    ON transport_local.stib_waiting_time(poll_ts);
CREATE INDEX IF NOT EXISTS idx_stib_wt_line_pt ON transport_local.stib_waiting_time(lineid, pointid);
