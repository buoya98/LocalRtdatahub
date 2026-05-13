# LocalRtdatahub — how it works

This document explains the *inside* of LocalRtdatahub: how ingestion,
storage, transformation and the Flask serving layer fit together.

For **install / setup / run** instructions see
[../README.md](../README.md). This file is about
**why the code is the way it is** and **how to trace a row from the STIB
feed to a pixel on the MapLibre map**.

Every claim below points to a file and a line number so you can verify
without trusting the prose.

---

## 1. Big picture

LocalRtdatahub is a single-machine port of the **Transport** module of
upstream [RTDataHub](https://github.com/BM-CM-MC/RTDataHub). It keeps the
upstream layout under `src/` (so paths in upstream commits and
discussions map 1:1) but strips everything that isn't STIB
(Brussels public transport): no De Lijn ingestion, no Kafka, no Docker,
no Azure managed identities.

The whole stack is:

- **PostgreSQL 18 + PostGIS 3.6 + MobilityDB** holding three schemas
  (`static`, `rt`, `transport_local`) defined by
  [sql/schema.sql](../sql/schema.sql).
- **Two interchangeable ingestion paths** that both end up writing the
  same rows into `rt.stib_vehicle_position`: a live STIB Open Data
  Portal poller and a bench replayer for gzipped JSONL / CSV dumps.
- **One SQL transform** that turns a (lineid, direction, pointid,
  distanceFromPoint) tuple into a real GPS point using PostGIS, since
  the STIB live feed carries no coordinates.
- **One offline trip reconstructor** that groups individual
  observations into MobilityDB `tgeompoint` trajectories.
- **One Flask app** (factory pattern, psycopg2 pool wrapped to look
  like psycopg3) that serves JSON endpoints + Mapbox Vector Tiles to a
  minimal MapLibre frontend.

```
                          ┌──────────────────────────────────────┐
                          │              SOURCES                 │
                          │                                      │
   STIB ODP (Azure APIM)  │   ┌────────────────────────────┐     │
   /rt/VehiclePositions   │──▶│ stib/vehicle_positions_     │     │
   /static/stopDetails    │   │   ingestor.py  (live)       │     │
   /static/stopsByLine    │   └─────────────┬───────────────┘     │
   /gtfs/feed/stibmivb…   │                 │                     │
                          │   ┌─────────────▼───────────────┐     │
   data/bench_algo_data/  │──▶│ bench/ingestor.py  (replay) │     │
   ├ static/*.jsonl.gz    │   │   • static/stops.jsonl.gz   │     │
   │  (catalogue, shipped)│   │   • static/line_stops…       │     │
   ├ positions/*.jsonl.gz │   │   • static/line_terminus…   │     │
   │  (7-day sample,      │   │   • static/line_shapes…     │     │
   │   shipped)           │   │   • positions/*.jsonl.gz    │     │
   └ runs/*/{assignments, │   │   • assignments.csv.gz      │     │
     wt}.jsonl.gz         │   │   • wt.jsonl.gz             │     │
     (optional, BYO)      │   │     (both optional)         │     │
                          │   └─────────────┬───────────────┘     │
                          └─────────────────┼─────────────────────┘
                                            │
                            (live + bench feed the same transform)
                                            │
                          ┌─────────────────▼─────────────────────┐
                          │ src/etl/pipeline/transform/           │
                          │   stib_transform.py                   │
                          │ deterministic position_id +           │
                          │ geom = ST_LineInterpolatePoint(       │
                          │   ST_LineLocatePoint(stop)            │
                          │   + distance/length, clamped 0..1)    │
                          └─────────────────┬─────────────────────┘
                                            │
                          ┌─────────────────▼─────────────────────┐
                          │ rt.stib_vehicle_position           │
                          │ (one row per observed vehicle ping)   │
                          └─────────────────┬─────────────────────┘
                                            │
                          ┌─────────────────▼─────────────────────┐
                          │ src/etl/pipeline/load/                │
                          │   load_stib.py                     │
                          │ greedy spatial+temporal grouping →    │
                          │ MobilityDB tgeompoint trajectories    │
                          └─────────────────┬─────────────────────┘
                                            │
                          ┌─────────────────▼─────────────────────┐
                          │ rt.stib_trip / rt.stib_trip_open│
                          │ unioned by view rt.stib_trip_all   │
                          └─────────────────┬─────────────────────┘
                                            │
                          ┌─────────────────▼─────────────────────┐
                          │ Flask (src/map/server/app.py)         │
                          │   /api/stib/lines                     │
                          │   /api/stib/live-positions  ←─┐       │
                          │   /api/stib/trajectories    ──┤ value │
                          │   /api/stib/shape/...        AtTimestamp│
                          │   /tiles/<layer>/z/x/y.pbf            │
                          └─────────────────┬─────────────────────┘
                                            │  JSON / MVT
                          ┌─────────────────▼─────────────────────┐
                          │ src/map/templates/index.html          │
                          │ (MapLibre SPA, ~5k lines reused)      │
                          └───────────────────────────────────────┘
```

The next sections walk this diagram top-to-bottom.

---

## 2. Data model

Tables live in three schemas, all created by
[sql/schema.sql](../sql/schema.sql).

### Schema `static` — STIB catalogue (slowly changing)

| Table                          | Purpose                                                   | Written by                                                                                            | Read by                                                            | Schema lines |
| ------------------------------ | --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ | ------------ |
| `static.stib_stop`             | Stop catalogue: pointid → FR/NL name + Point(4326) geom   | [stop_details_ingestor.py:60-86](../src/etl/ingestion/stib/stop_details_ingestor.py) + best-effort upsert in [bench/ingestor.py:175-190](../src/etl/ingestion/bench/ingestor.py) | [stib_transform.py:62-64](../src/etl/pipeline/transform/stib_transform.py), services search/stop/lines | [22-31](../sql/schema.sql) |
| `static.stib_line_stops`       | Ordered list of pointids per (lineid, direction)          | [stops_by_line_ingestor.py:65-113](../src/etl/ingestion/stib/stops_by_line_ingestor.py)                                                                                       | `fetch_stib_stop_details`, `fetch_stib_trajectories` (dir_label lookup) | [33-43](../sql/schema.sql) |
| `static.stib_line_terminus`    | (lineid, terminus_pointid) → direction label              | [stops_by_line_ingestor.py:65-113](../src/etl/ingestion/stib/stops_by_line_ingestor.py)                                                                                       | [gtfs_shapes_ingestor.py:53-67](../src/etl/ingestion/stib/gtfs_shapes_ingestor.py), [stib_transform.py:51-53](../src/etl/pipeline/transform/stib_transform.py), `/api/stib/lines` | [45-52](../sql/schema.sql) |
| `static.stib_line_shape`       | One canonical LineString per (lineid, direction), + mode + GTFS colours | [gtfs_shapes_ingestor.py:152-200](../src/etl/ingestion/stib/gtfs_shapes_ingestor.py)                                                                                          | `stib_transform`, `/api/stib/shape`, `/tiles/stib_lines/...`        | [54-66](../sql/schema.sql) |

Note: `stib_line_terminus` is the bridge between the STIB ODP world
(directions identified by their *terminus pointid*) and the GTFS world
(direction identified by `direction_id = 0/1`).
[stops_by_line_ingestor.py:65-113](../src/etl/ingestion/stib/stops_by_line_ingestor.py)
writes the last pointid of each direction's sequence as the terminus;
[gtfs_shapes_ingestor.py:166-181](../src/etl/ingestion/stib/gtfs_shapes_ingestor.py)
then matches each GTFS shape's endpoint to the closest terminus
(haversine, tolerance 500 m — see `TERMINUS_TOLERANCE_M` at
[gtfs_shapes_ingestor.py:32](../src/etl/ingestion/stib/gtfs_shapes_ingestor.py)).
This is why running `shapes` before `lines` is a no-op (the LEFT JOIN
finds nothing — [gtfs_shapes_ingestor.py:91-94](../src/etl/ingestion/stib/gtfs_shapes_ingestor.py)
bails out explicitly).

### Schema `rt` — observations + reconstructed trips

`rt` is the upstream "shadow" schema, kept under that exact name to
stay drop-in compatible with upstream's queries.

```
                              rt.stib_vehicle_position
                                  (sql/schema.sql:73-86)
                                          │
                                          │ load_stib.py
                                          │ greedy (lineid, direction)
                                          ▼
        ┌───────────────────────────┐         ┌────────────────────────────┐
        │ rt.stib_trip_open      │         │ rt.stib_trip            │
        │ (sql/schema.sql:95-112)   │         │ (sql/schema.sql:114-130)   │
        │ trips still being         │         │ closed/finalised trips     │
        │ extended in real time     │         │ (target of load_stib)   │
        │ — UNUSED locally          │         │                            │
        └────────────┬──────────────┘         └─────────────┬──────────────┘
                     │                                      │
                     └────────────── UNION ALL ─────────────┘
                                          │
                                          ▼
                              rt.stib_trip_all (VIEW)
                                  (sql/schema.sql:132-140)
```

- `rt.stib_vehicle_position`
  ([sql/schema.sql:73-86](../sql/schema.sql)): one row per (lineid,
  direction, pointid, distance, fetched_at) observation. Primary key is
  the deterministic `position_id` (SHA-256 truncated to 32 hex chars,
  see §3). Geometry is the *reconstructed* Point — when ingested
  through the SQL transform, no Python ever sees the GPS coords.
- `rt.stib_trip` ([sql/schema.sql:114-130](../sql/schema.sql)):
  closed trips produced by
  [load_stib.py](../src/etl/pipeline/load/load_stib.py). The
  `trip` column is a MobilityDB `tgeompoint` — a
  *temporal geometry* you can query with `valueAtTimestamp(trip, ts)`.
- `rt.stib_trip_open` ([sql/schema.sql:95-112](../sql/schema.sql)):
  schema-compatible with upstream's online trip builder but
  unpopulated locally — `load_stib.py` flushes everything as a
  closed trip
  ([load_stib.py:171-175](../src/etl/pipeline/load/load_stib.py)).
  It's kept so upstream-style frontend queries don't have to switch
  tables.
- `rt.stib_trip_all` ([sql/schema.sql:132-140](../sql/schema.sql)):
  a `UNION ALL` view that hides the open/closed split from the Flask
  service. Every `/api/stib/...` query reads from this view, never
  from the two underlying tables — see
  [stib_service.py:44, 245, 340](../src/map/server/services/stib_service.py).
  Locally that means the view degenerates to `stib_trip`, but the
  *query stays valid* if you swap in upstream's bicephalous builder.

### Schema `transport_local` — local-only additions

| Table                                | Purpose                                            | Written by                                                                            | Read by                                                  | Schema lines |
| ------------------------------------ | -------------------------------------------------- | ------------------------------------------------------------------------------------- | -------------------------------------------------------- | ------------ |
| `transport_local.stib_waiting_time`  | ETAs replayed from `wt.jsonl.gz` (optional — not shipped: daily files exceed GitHub's per-file limit, BYO)   | [bench/ingestor.py](../src/etl/ingestion/bench/ingestor.py) `ingest_waiting_times` | [waiting_times.py:13-36](../src/map/server/services/waiting_times.py) | [146-156](../sql/schema.sql) |

This schema is the only one with no upstream equivalent — upstream
serves waiting times from a separate Azure Service Bus pipeline.

---

## 3. Ingestion paths

The whole point of the design is that **two completely different input
formats end up writing byte-identical rows** to
`rt.stib_vehicle_position`. Idempotency is provided by the
deterministic `position_id`.

### 3.a — Live API path (STIB Open Data Portal)

Entry point: [stib/vehicle_positions_ingestor.py](../src/etl/ingestion/stib/vehicle_positions_ingestor.py).

```
HTTP GET  STIB_ODP_BASE_URL + /rt/VehiclePositions
          ?limit=1000&offset=...                       (vehicle_positions_ingestor.py:117-123)
          headers: bmc-partner-key: $STIB_ODP_KEY
                                          │
                                          ▼
          paginate until empty/<PAGE_LIMIT             (vehicle_positions_ingestor.py:135-144)
                                          │
                                          ▼
          For each result row:                         (vehicle_positions_ingestor.py:82-108)
            lineid = row.lineid | row.lineId
            raw    = row.vehiclepositions             ← nested JSON-encoded STRING
            inner  = json.loads(raw)                   (vehicle_positions_ingestor.py:91-95)
            for pos in inner:
                normalise → {lineid, direction:int, pointid:str,
                             distance_from_point:float|None,
                             fetched_at: iso8601}
                                          │
                                          ▼
          insert_positions(conn, records, batch_size)  (vehicle_positions_ingestor.py:260)
            └─ src/etl/pipeline/transform/stib_transform.py
```

Two key details:

1. **`vehiclepositions` is a JSON-encoded string** inside the JSON
   response. The ingestor parses it with `json.loads` at
   [vehicle_positions_ingestor.py:91-95](../src/etl/ingestion/stib/vehicle_positions_ingestor.py).
   If the field is already a list (rare alternate format) it's used as-is
   at [:96-99](../src/etl/ingestion/stib/vehicle_positions_ingestor.py).
2. **Auth + paging** uses a `bmc-partner-key` header
   ([vehicle_positions_ingestor.py:113](../src/etl/ingestion/stib/vehicle_positions_ingestor.py))
   instead of the more common `Authorization: Bearer …` — that's how
   STIB's Azure APIM gateway is wired. A `--interval N` flag loops the
   poll forever, sleeping `N` seconds between iterations
   ([:266-268](../src/etl/ingestion/stib/vehicle_positions_ingestor.py)).

Optional `--output PATH` mirrors each poll into a fresh
timestamped `.jsonl.gz` so the bench replay path can re-consume it
([vehicle_positions_ingestor.py:155-194](../src/etl/ingestion/stib/vehicle_positions_ingestor.py)).
The mirror writer refuses to write under `data/bench_algo_data/` and
refuses to overwrite an existing file
([:178-193](../src/etl/ingestion/stib/vehicle_positions_ingestor.py)) — these
two checks are the no-overwrite guarantee surfaced in the README.

### 3.b — Bench (raw `.jsonl.gz` / `.csv.gz`) path

Entry point: [bench/ingestor.py](../src/etl/ingestion/bench/ingestor.py).

```
$ python -m src.etl.ingestion.bench.ingestor <path>...
            │
            ▼
   _walk(paths)             (bench/ingestor.py:77-87)
            │                ├ recursively yields *.jsonl.gz / *.csv.gz
            ▼
   _classify(path)          (bench/ingestor.py)
            │
   ┌────────┼──────────────┬──────────────────┐
   │        │              │                  │
   ▼        ▼              ▼                  ▼
static_*  assignments   waiting_times      positions
   │        │              │                  │
   ▼        │              ▼                  ▼
ingest_static_{stops, |  ingest_waiting_times  ingest_raw_positions
  line_stops, line_   │   • parses JSONL       • parses JSONL
  terminus,           │                       • feeds insert_positions()
  line_shapes}        │                          (shared with live path)
   • parses JSONL     │
   • execute_values   │
     batch upsert     │
   │   • bulk INSERT into                  • passes records to
   │     transport_local.stib_waiting_time   insert_positions(...)
   │                                         (same fn as live API)
   ▼
ingest_assignments  (:106-172)
   • CSV reader: position_id, fetched_at_utc,
     lineId, direction, pointId, distance,
     latitude, longitude
   • INSERT directly with
     ST_MakePoint(lon,lat) — no transform needed
   • side-effect: best-effort upsert of
     (pointid → last observed lon/lat) into
     static.stib_stop  (_upsert_stops, :175-190)
```

Five file types are routed differently because **they carry different
information**:

- **`static/{stops,line_stops,line_terminus,line_shapes}.jsonl.gz`** —
  pre-exported STIB catalogue (shipped with the repo, ~420 KB total).
  Each maps 1:1 to a `static.stib_*` table; the bench ingestor upserts
  them in dependency order via `_KIND_ORDER` (stops →
  line_stops → line_terminus → line_shapes). Lets a fresh clone
  bootstrap without a `STIB_ODP_KEY`.
- **`positions/*.jsonl.gz`** — raw STIB feed (same shape as the live
  API payload, minus the outer wrapper). No GPS coordinates. Pushed
  through the same `insert_positions` transform as the live path
  ([bench/ingestor.py](../src/etl/ingestion/bench/ingestor.py)). This
  is the parity guarantee the README mentions: *same transform, same
  position_id, same rows*.
- **`waiting_times/*.jsonl.gz`** / `wt.jsonl.gz` — ETAs only,
  populates `transport_local.stib_waiting_time`. **Not shipped**
  (single-day files exceed GitHub's 100 MB per-file limit); the
  ingestor still handles them if you bring your own.
- **`assignments.csv.gz`** — output of an offline matching algorithm
  (bench runs). Already has resolved `latitude` / `longitude` and its
  *own* `position_id` column, so it bypasses the transform entirely.
  [bench/ingestor.py](../src/etl/ingestion/bench/ingestor.py) keeps
  the CSV's `position_id` verbatim instead of recomputing one — that's
  on purpose so two ingests of the same run produce stable rows even
  when the CSV's distance was rounded differently from the raw feed.

### 3.c — The deterministic `position_id`

Defined at
[stib_transform.py:27-30](../src/etl/pipeline/transform/stib_transform.py):

```python
def position_id(lineid, direction, pointid, fetched_at, distance):
    key = f"{lineid}|{direction}|{pointid}|{fetched_at}|{distance}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]
```

Why this exact concatenation:

- Two observations of the *same* vehicle at the *same* fetched_at
  share all five fields → identical hash → ON CONFLICT skips the
  duplicate. Re-running the live poller every 20 seconds therefore
  never inserts the same observation twice.
- The live API path and the bench replay of a saved mirror file
  produce the same SHA-256 because
  [stib_transform.py:117-121](../src/etl/pipeline/transform/stib_transform.py)
  coerces `distance` to `float` first, so a raw int `34` and a mirrored
  float `34.0` hash to the same value. This is the bug fix called out
  in the inline comment ("otherwise the API path and the raw bench
  path produce duplicate rows").
- The hash is truncated to 32 hex chars (128 bits) — wide enough that
  collisions among a few million daily observations are effectively
  impossible, narrow enough to keep `rt.stib_vehicle_position.position_id`
  a comfortable `TEXT PRIMARY KEY`.

The `ON CONFLICT (position_id) DO NOTHING` clause at
[stib_transform.py:91](../src/etl/pipeline/transform/stib_transform.py)
makes both ingestion paths fully idempotent: replay the same dump 10
times, you still have one row per observation.

---

## 4. The geom-reconstruction transform

This is the most non-obvious bit of the project. The STIB ODP real-time
feed reports each vehicle as **(lineid, directionId, pointId,
distanceFromPoint)** — i.e. "I'm on line 71 heading toward terminus
1234, last passed stop 5678, and I'm 42.7 m past it". It does **not**
give GPS coordinates. Yet the map needs Points.

The transform lives at
[src/etl/pipeline/transform/stib_transform.py](../src/etl/pipeline/transform/stib_transform.py),
and the whole geometry logic is the
[_INSERT_SQL](../src/etl/pipeline/transform/stib_transform.py) statement
at lines 33-92. It's a single `INSERT … SELECT` with three CTEs:

### Step 1 — locate the stop on the line

```sql
ST_LineLocatePoint(ls.geom, sp.geom)
```

[stib_transform.py:83](../src/etl/pipeline/transform/stib_transform.py).
Returns a fraction in [0, 1] giving where the stop's projection lies on
the line. `ls.geom` is `static.stib_line_shape.geom` (the canonical
LineString for that (lineid, direction)), `sp.geom` is
`static.stib_stop.geom` for the observed pointid.

### Step 2 — advance by `distance / length`

```sql
… + COALESCE(g.distance_from_point, 0) / g.line_length_m
```

[stib_transform.py:84](../src/etl/pipeline/transform/stib_transform.py).
`line_length_m` is computed once per row via
`ST_Length(ls.geom::geography)`
([stib_transform.py:59](../src/etl/pipeline/transform/stib_transform.py)),
which gives **meters on the WGS84 ellipsoid** (not the planar 4326
"degrees"). Dividing meters past the stop by total meters yields the
fractional advance.

### Step 3 — clamp and interpolate

```sql
ST_LineInterpolatePoint(
    g.line_geom,
    LEAST(1.0, GREATEST(0.0,  <fraction_at_stop> + <advance>))
)
```

[stib_transform.py:80-86](../src/etl/pipeline/transform/stib_transform.py).
`ST_LineInterpolatePoint` does the actual interpolation. The
`LEAST/GREATEST` clamp guards two real-world edge cases:

- **Vehicle past the last stop**: `fraction_at_stop` is e.g. 0.97 and
  `distance/length` adds another 0.06 → without the clamp we'd ask
  PostGIS to interpolate at 1.03, which raises an error. The clamp
  pins it to 1.0 (the line endpoint).
- **Negative `distanceFromPoint`**: STIB sometimes reports a small
  negative offset when a vehicle is approaching but hasn't yet passed
  the next stop. Without the clamp this could produce a fraction below
  0. We pin to 0.0 (the line start).

Both clamping cases mean the worst-case error is **bounded by the
distance to the line endpoint**, which is good enough to render.

### Anchor fallback

```sql
CASE WHEN line_geom IS NOT NULL AND stop_geom IS NOT NULL AND line_length_m > 0
     THEN ST_LineInterpolatePoint(...)
     ELSE stop_geom   -- anchor fallback
END
```

[stib_transform.py:76-88](../src/etl/pipeline/transform/stib_transform.py).
If the stop has a geom but no canonical line shape is available (line
ingested before shapes, or a new line not yet in GTFS), we drop the
vehicle at the stop itself. Geometrically wrong by a few dozen meters,
but visually plausible.

If **neither** the line shape nor the stop exists, the row is dropped
entirely by the `WHERE g.stop_geom IS NOT NULL OR g.line_geom IS NOT
NULL` clause at
[stib_transform.py:90](../src/etl/pipeline/transform/stib_transform.py).
This is the silent-drop behavior the README warns about: if you run the
`rt` step before `stops` + `lines` + `shapes`, you'll see polls succeed
but `/debug/counts` reports `positions: 0`.

### Why do this in SQL, not Python?

- Reading `static.stib_line_shape.geom` (potentially a 1000-vertex
  LineString) back to Python for each record would be horrendously
  expensive. Keeping the math in PostGIS means one round-trip per
  batch of 5000 rows.
- PostGIS already gives us geodesic length (`::geography`) and exact
  interpolation. Reimplementing in Python would be reinventing badly.
- The transform is therefore *just* a `WITH input(...) AS (VALUES %s)`
  ([stib_transform.py:34-37](../src/etl/pipeline/transform/stib_transform.py))
  parameterized with `psycopg2.extras.execute_values` — one prepared
  statement, many parameter tuples.

---

## 5. Trip reconstruction

`rt.stib_vehicle_position` holds raw observations. To play a vehicle
back through time the frontend needs *trip identity*: "this stream of
points is one bus". The STIB feed gives us no `vehicle_uuid`, so we
have to **rebuild identity** with a greedy
spatio-temporal heuristic.

That's the job of
[src/etl/pipeline/load/load_stib.py](../src/etl/pipeline/load/load_stib.py).

### Algorithm

Three tunables, kept identical to upstream
([load_stib.py:48-50](../src/etl/pipeline/load/load_stib.py)):

```
MATCH_RADIUS_M = 500.0      # two consecutive pings within 500 m = same vehicle
MATCH_WINDOW_S = 300.0      # …and within 5 min of the previous ping
TRIP_TIMEOUT_S = 1800       # 30 min idle = trip closed
```

Per (lineid, direction) pair
([load_stib.py:178-196](../src/etl/pipeline/load/load_stib.py)):

1. Stream all positions ordered by `fetched_at`
   ([load_stib.py:199-214](../src/etl/pipeline/load/load_stib.py)
   — uses a server-side cursor so even multi-million-row tables stay
   memory-light).
2. Keep a list of *open trips*. For each new (ts, lon, lat):
   a. Expire trips idle for more than `TRIP_TIMEOUT_S`
      ([load_stib.py:150-156](../src/etl/pipeline/load/load_stib.py)).
   b. Among the remaining open trips, score each one by haversine
      distance to its last point
      ([load_stib.py:92-101](../src/etl/pipeline/load/load_stib.py)).
      Reject any trip whose last ping is more than `MATCH_WINDOW_S`
      ago, whose timestamp would not be strictly increasing (MobilityDB
      requirement), or whose last ping is more than `MATCH_RADIUS_M`
      away.
   c. Attach to the *closest* surviving trip, or open a new one
      ([load_stib.py:166-169](../src/etl/pipeline/load/load_stib.py)).
3. After the stream ends, flush all open trips and drop any that ended
   up with fewer than 2 samples
   ([load_stib.py:171-175](../src/etl/pipeline/load/load_stib.py)).
   Singletons can't form a `tgeompoint` sequence anyway.

The choice of *closest* (not *first matching*) means a temporary
500 m matching radius doesn't smear two parallel vehicles into one as
soon as they cross paths — they each pick their own nearer trail.

### Why `tgeompoint`?

MobilityDB's `tgeompoint` is a **temporal geometry**: a function of
time → Point. The trip column ([sql/schema.sql:124](../sql/schema.sql))
stores the whole trajectory as one value
serialized as

```
[Point(lon₁ lat₁)@ts₁, Point(lon₂ lat₂)@ts₂, …]
```

(see [load_stib.py:69-75](../src/etl/pipeline/load/load_stib.py)).
This unlocks three killer operators that the Flask service uses
extensively:

- `valueAtTimestamp(trip, ts)` → "where was this vehicle at exactly
  `ts`?" — linearly interpolated between the two surrounding samples.
  Used by `/api/stib/live-positions`
  ([stib_service.py:244](../src/map/server/services/stib_service.py))
  to draw markers at any cursor instant the frontend asks for.
- `trip::tstzspan` → "what time interval does this trip cover?" —
  becomes a `tstzrange`-shaped value used with `&&` for fast
  intersection. The trip GIST index at
  [sql/schema.sql:130](../sql/schema.sql) makes "trips active during
  this window" cheap. Used at
  [stib_service.py:248-250, 344](../src/map/server/services/stib_service.py).
- `atTime(trip, span(w_start, w_end))` → restricts the temporal
  geometry to the user-selected window. `instants(...)` then unpacks
  it into one row per native GPS fix in that window. Used by the
  trajectory player at
  [stib_service.py](../src/map/server/services/stib_service.py)
  (`_TRAJECTORIES_SQL`). The older
  `generate_series + valueAtTimestamp` resample was dropped — see
  §6 "Query optimizations" below.

In short: the trip table is what makes the Play button cheap. Without
MobilityDB we'd be doing per-vehicle binary search in Python for every
frame.

---

## 6. Flask serving layer

Source root: [src/map/server/](../src/map/server). Five top-level
files, then `services/`, `queries/`, `utils/`.

### Factory pattern

`create_app(cfg, pool)` at
[app.py:53](../src/map/server/app.py) returns a fully-wired Flask app
given a `ServerConfig` and a `Pool`. The factory pattern is used here
for three reasons:

1. The DB pool can be created and destroyed by the caller — important
   for `main.py:91` (clean shutdown on Ctrl+C) and for any test that
   wants to inject a mock pool.
2. The `MapAppContext`
   ([context.py:42-49](../src/map/server/context.py)) bundles
   `cfg + pool + tile_cache + counts_cache` so route handlers don't
   reach into module globals.
3. Werkzeug's `make_server(..., threaded=True)`
   ([main.py:84](../src/map/server/main.py)) needs an app whose
   resources are explicitly threadsafe — the pool and the two caches
   each have their own lock
   ([context.py:46, 48](../src/map/server/context.py)).

### The `Pool` shim — psycopg2 wearing a psycopg3 mask

`src/map/server/context.py` defines a tiny class:

```python
class Pool:
    def __init__(self, dsn, min_size=1, max_size=8):
        self._pool = ThreadedConnectionPool(min_size, max_size, dsn=dsn)

    @contextmanager
    def connection(self, timeout=None):
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)
```

[context.py:19-39](../src/map/server/context.py).

Why bother? Upstream RTDataHub uses `psycopg_pool.ConnectionPool` from
psycopg3. Every service handler in upstream writes:

```python
with ctx.pool.connection() as conn:
    with conn.cursor() as cur:
        ...
```

Locally we kept `psycopg2` (one of the two pinned dependencies in
[requirements.txt](../requirements.txt) — adding psycopg3 would have
meant juggling two DB drivers in parallel). The shim mimics exactly
the `connection() / close()` subset of the psycopg3 API used by
upstream, so the service code at e.g.
[stib_service.py:95-98](../src/map/server/services/stib_service.py)
is **textually identical to upstream**. That's the parity goal: a diff
of `src/map/server/services/*.py` against upstream should only show
removals, not rewrites.

The shim also commits on context exit (and rolls back on exception) —
psycopg3's pool does the same.

### Caching strategy

Two caches, both LRU + TTL:

- **Tile cache**
  ([context.py:46-47](../src/map/server/context.py),
  consumed at [tiles.py:20-40](../src/map/server/services/tiles.py)):
  key = `layer|z|x|y|extra_params`, payload = raw MVT bytes. TTL is
  `cfg.tile_cache_ttl_s` (default 160 s), capacity is
  `cfg.tile_cache_max_entries` (default 4000) — both configurable from
  `main.py` flags
  ([main.py:50-51](../src/map/server/main.py)). When the cache is full
  the **least-recently-used** item is evicted
  ([tiles.py:38-40](../src/map/server/services/tiles.py)). Both reads
  and writes hold `ctx.tile_cache_lock`.
- **Counts cache** is allocated on `MapAppContext`
  ([context.py:48-49](../src/map/server/context.py)) but isn't read
  from in the current `fetch_counts` implementation — it's a slot kept
  for the upstream behavior. Default TTL is 4 s
  ([main.py:52](../src/map/server/main.py)).

Tile responses also send `Cache-Control: public, max-age=300` to the
client
([app.py:241](../src/map/server/app.py)). Shape responses (which are
heavy LineStrings that rarely change) send `max-age=86400`
([app.py:69-74](../src/map/server/app.py)). The HTML index is
explicitly `no-store` so live reloads work
([app.py:61-67](../src/map/server/app.py)).

### Query optimizations (backport of upstream `c43aaa2`)

The three hot-path handlers (`fetch_stib_lines_catalog`,
`fetch_stib_live_positions`, `fetch_stib_trajectories`) were rewritten
along the lines of upstream commit
[`c43aaa2`](https://github.com/BM-CM-MC/RTDataHub) ("Optimise
massivement la fluidité de la map"). What changed:

| Endpoint                       | Old approach                                                   | New approach                                                                                                                            | Upstream-measured speedup            |
| ------------------------------ | -------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------ |
| `/api/stib/lines`              | ~73 correlated subqueries (one per line)                       | 1 grouped scan + ROW_NUMBER over `rt.stib_trip_all` ([stib_service.py:30-115](../src/map/server/services/stib_service.py))           | 29 s cold / 3.5 s warm → 1.1 s / 2 ms |
| `/api/stib/live-positions`     | `tstzspan && span(...)` only — required BitmapAnd of two idx   | Adds `start_ts <= … AND end_ts >= …` *before* the tstzspan check → composite (lineid, start_ts) index hit                              | 3.4 s → 22 ms (no filter)            |
| `/api/stib/trajectories`       | `generate_series + valueAtTimestamp` resample every 5 s         | `unnest(instants(atTime(trip, span(...))))` emits one row per **native** GPS fix; client linearly interpolates between                  | 12 s/32 s → 100 ms/230 ms             |

Three supporting tactics:

1. **`SET LOCAL jit = off`** at the top of each transaction. JIT
   compilation costs ~600 ms on these queries (estimated cost above the
   default `jit_above_cost = 100k`) but the queries themselves run in
   1-2 s — net negative.
2. **Composite index** `idx_stib_trip_lineid_start ON
   rt.stib_trip(lineid, start_ts DESC)` (plus the same on
   `stib_trip_open`) — see
   [sql/schema.sql:130-138](../sql/schema.sql). Without it the new
   `start_ts <=`/`end_ts >=` predicate can still be planned, but a
   BitmapAnd over the single-column indexes is slower.
3. **Native instants instead of resampling.** `valueAtTimestamp` only
   linearly interpolates between native GPS fixes anyway, so resampling
   at e.g. 5 s produces ~18× more samples without adding precision.
   We keep the `sample` query parameter for URL backwards-compat but
   ignore it
   ([stib_service.py:463-466](../src/map/server/services/stib_service.py)).

Two upstream features were **not** backported because they would push
us outside the "minimalist local port" scope:

- The `static.stib_line_shape.geom_lambert` STORED GENERATED column
  (pre-projected to EPSG:31370). It saves one `ST_Transform` per
  trajectory sample but requires a schema change. Local trajectories
  return `frac = NULL` instead of a normalized line-fraction. The
  frontend doesn't use `frac` for rendering, only for
  shape-attached effects we don't have locally.
- The 10-minute in-memory TTL cache on `/api/stib/lines`. For a
  single-user local instance the 1-2 s cold-cache hit is fine; in
  upstream production the cache absorbs many concurrent users.

### Time-filter parsing

[utils/time_utils.py](../src/map/server/utils/time_utils.py) parses
query strings into a single `time_filter` dict consumed by the STIB
service. Two modes:

| Mode      | Trigger                                     | Resolved by                                                                            |
| --------- | ------------------------------------------- | -------------------------------------------------------------------------------------- |
| `range`   | both `start=` and `end=` query params, with `end > start` | `{"mode": "range", "hours": h, "start": dt, "end": dt}` ([time_utils.py:42-43](../src/map/server/utils/time_utils.py)) |
| `relative` | otherwise                                   | `{"mode": "relative", "hours": h, "start": None, "end": None}` ([time_utils.py:44](../src/map/server/utils/time_utils.py)) |

In **relative** mode the reference instant isn't `now()` — it's
`MAX(end_ts)` of `rt.stib_trip_all`
([stib_service.py:271-280](../src/map/server/services/stib_service.py)).
That's deliberate: when you replay a bench dump from last week, "now"
in DB-time is last week's last observation, not the wall clock. Using
`now()` would result in zero active trips and an empty map even
though the data is there.

In **range** mode the window is `[start, end]` taken verbatim. The
`/api/stib/trajectories` endpoint then emits one row per **native**
MobilityDB instant in the window via
`unnest(instants(atTime(trip, span(w_start, w_end))))`
([stib_service.py](../src/map/server/services/stib_service.py)). The
older `generate_series + valueAtTimestamp` resample was dropped: since
`valueAtTimestamp` only linearly interpolates between native points,
resampling at e.g. 5 s produced ~18× more samples for zero added
precision. The frontend now does the linear interpolation locally.

The `sample` query parameter is still accepted by the endpoint for
URL backwards-compat, but ignored.

`parse_lines_filter` ([time_utils.py:47-52](../src/map/server/utils/time_utils.py))
splits comma-separated lists; an empty list becomes `None`, which is
how SQL `WHERE %s::text[] IS NULL OR lineid = ANY(%s::text[])` becomes
"no filter".

---

## 7. API surface

The full route table is wired in
[src/map/server/app.py](../src/map/server/app.py). Each route is a
thin shell that parses HTTP-level inputs and delegates to a service
function. The frontend at
[src/map/templates/index.html](../src/map/templates/index.html) is the
only consumer.

| Route                                              | Service fn                                                                          | Frontend caller                                                       |
| -------------------------------------------------- | ----------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| `GET /`                                            | renders index.html with `hours` baked in ([app.py:77-81](../src/map/server/app.py)) | (the SPA itself)                                                      |
| `GET /api/health`                                  | [counts.py:13](../src/map/server/services/counts.py)                                | startup probe (no explicit caller; useful for ops)                    |
| `GET /debug/counts`                                | [counts.py:35](../src/map/server/services/counts.py)                                | [index.html:2886](../src/map/templates/index.html)                    |
| `GET /api/search?q=…`                              | [search.py:53](../src/map/server/services/search.py)                                | [index.html:5234](../src/map/templates/index.html)                    |
| `GET /api/stib/lines`                              | [stib_service.py:130](../src/map/server/services/stib_service.py)                   | [index.html:4015](../src/map/templates/index.html)                    |
| `GET /api/stib/live-positions`                     | [stib_service.py:331](../src/map/server/services/stib_service.py)                   | implicit via the trajectory player                                    |
| `GET /api/stib/trajectories`                       | [stib_service.py:456](../src/map/server/services/stib_service.py)                   | [index.html:3592](../src/map/templates/index.html)                    |
| `GET /api/stib/shape/<lineid>/<direction>`         | [stib_service.py:245](../src/map/server/services/stib_service.py)                   | [index.html:3660-3661](../src/map/templates/index.html)               |
| `GET /api/stib/stop/<pointid>`                     | [stib_service.py:205](../src/map/server/services/stib_service.py)                   | [index.html:3049, 3138](../src/map/templates/index.html)              |
| `GET /api/stib/alerts`                             | [stib_service.py:505](../src/map/server/services/stib_service.py) (always empty)    | [index.html:3553](../src/map/templates/index.html)                    |
| `GET /api/waiting-times?pointid=…`                 | [waiting_times.py:13](../src/map/server/services/waiting_times.py)                  | stop popup                                                            |
| `GET /tiles/<layer>/<z>/<x>/<y>.pbf`               | [tiles.py:43](../src/map/server/services/tiles.py)                                  | [index.html:1519, 3207, 4393](../src/map/templates/index.html)        |

Any other endpoint the upstream frontend asks for (e.g.
`/api/delijn/*`, `/api/traffic-lights`, `/api/fid/...`,
`/api/xtream-camera/...`) now returns a Flask 404. The frontend's
`fetch().catch()` handlers degrade silently; the corresponding UI
panels render empty.

Two contract details worth noting:

- `/api/stib/trajectories` returns
  `{trips: [...], window_start_ms, window_end_ms}` — the `_ms` fields
  are what the player uses to position the scrubber, even when `trips`
  is empty.
- `/api/stib/lines` returns `directions_detail` (top-4 destinations
  ranked by trip count over the last 7 days)
  ([stib_service.py:30-115](../src/map/server/services/stib_service.py)).
  The "7 days" are anchored on `MAX(end_ts)`, again for offline-replay
  parity with the relative-time-mode logic.
- Tile layers other than `stib_lines` / `stib_stops` return HTTP 204
  ([app.py](../src/map/server/app.py)) — keeps the frontend's MapLibre
  source requests for layers that never had local data quiet.

---

## 8. Operational notes

### What to watch

```bash
curl -s http://127.0.0.1:8090/debug/counts | jq
# {
#   "ok": true,
#   "counts": {
#     "positions": 142_318,
#     "trips": 1_843,
#     "stops": 2_741,
#     "line_shapes": 88,
#     "waiting_times": 0
#   }
# }
```

[counts.py:35-55](../src/map/server/services/counts.py). The five row
counts are the canonical pulse:

- `stops == 0` → never ran `stops_by_line_ingestor` or
  `stop_details_ingestor`; map will show no stops and the geom
  transform will fall through to the anchor branch with no result.
- `line_shapes == 0` → never ran `gtfs_shapes_ingestor`; transform
  drops everything (no line + no stop = WHERE clause filters it out at
  [stib_transform.py:90](../src/etl/pipeline/transform/stib_transform.py)).
- `positions > 0` but `trips == 0` → forgot `load_stib`. The
  position table is full but the map is empty because the Flask
  service reads from `rt.stib_trip_all`, not from
  `stib_vehicle_position`.
- `waiting_times == 0` → expected on a fresh clone — none ship with
  the repo. Bring your own `wt.jsonl.gz` or `waiting_times/*.jsonl.gz`
  and re-run the bench ingestor to populate.

### Silent failure modes

The four LEFT JOINs that *can* drop data without raising:

| Where                                                                                          | Failure mode                                                                                                         |
| ---------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| [stib_transform.py:51-53](../src/etl/pipeline/transform/stib_transform.py): `LEFT JOIN static.stib_line_terminus` | A new line not yet in the terminus catalogue ⇒ `dir_label = NULL` ⇒ the next join can't find a shape ⇒ falls back to the stop geom. |
| [stib_transform.py:61-62](../src/etl/pipeline/transform/stib_transform.py): `LEFT JOIN static.stib_line_shape`   | No shape for this (lineid, direction) ⇒ anchor fallback or row dropped.                                              |
| [stib_transform.py:63-64](../src/etl/pipeline/transform/stib_transform.py): `LEFT JOIN static.stib_stop`         | No stop in catalogue (most common: ran `rt` before `stops`) ⇒ row dropped.                                           |
| [load_stib.py:171-175](../src/etl/pipeline/load/load_stib.py): singleton trips                            | Trips with `<2` samples are dropped (MobilityDB doesn't accept a bracketed sequence with one instant).               |

None of these raise — they just silently lower the count. That's why
`/debug/counts` is the first thing to check.

### Debugging a blank map

In order:

1. `curl /api/health` — `mobilitydb: true` and `postgis: true`?
2. `curl /debug/counts` — are `positions` / `trips` / `stops` /
   `line_shapes` all > 0?
3. `curl '/api/stib/lines' | jq '.count'` — should be the number of
   distinct STIB lines (~85 in 2024).
4. Inspect the JS console: tile 204s for `stib_lines` mean you have no
   `static.stib_line_shape` rows.
5. If `positions > 0` but `trips == 0`, run
   `python -m src.etl.pipeline.load.load_stib`.

---

## 9. Design decisions FAQ

### Q. Why psycopg2 instead of psycopg3?

`requirements.txt` already pins `psycopg2-binary` for the ingestion
scripts (see imports at
[bench/ingestor.py:44-45](../src/etl/ingestion/bench/ingestor.py),
[load_stib.py:36](../src/etl/pipeline/load/load_stib.py),
[stib_transform.py:24](../src/etl/pipeline/transform/stib_transform.py)).
Adding psycopg3 just so the web server can use `psycopg_pool` would
mean **two database drivers in the same venv** — different escape
rules, different default codecs, two pools to size, two more wheels in
each pip install. So the web server reuses psycopg2 and we wrap
`ThreadedConnectionPool` in a 20-line shim
([context.py:19-39](../src/map/server/context.py)) that exposes only
the subset of psycopg3's API the upstream handlers call. The handlers
themselves stay textually identical to upstream.

### Q. Why mirror the upstream layout 1:1?

Two reasons:

1. **Cherry-picking from upstream stays trivial.** If upstream fixes
   a bug in `src/map/server/services/stib_service.py:123`, the patch
   applies to the same path here. Renaming files would break that.
2. **Discussion and tickets transfer.** When you read a commit message
   from RTDataHub mentioning "trip_open dual-table dance in
   load_stib.py", you can `Ctrl-Click` the same filename and read
   the local version. The shadow layout is documentation.

The cost is some empty-ish artefacts (e.g. `rt.stib_trip_open` exists
but stays empty locally because we don't run a real-time trip
stitcher). Out-of-scope domains (De Lijn, TomTom, Waze, ANPR,
traffic-light, FLIR) used to ship as empty backend stubs but were
removed in a later pass — the upstream `index.html` still references
them, but its `fetch().catch()` paths swallow the 404s and the
affected UI panels render empty.

### Q. Why no Docker?

See [README §1-3](../README.md). Short version: MobilityDB has no
prebuilt apt package for PostgreSQL 18 yet, and we want native
PostgreSQL/PostGIS performance on the host (the GIST indexes on
`tgeompoint` are sensitive to filesystem latency). The setup is a
one-shot `make install`; Docker would add an image-build step *and* a
container that still has to bind-mount the database volume.

### Q. Why is `rt.stib_trip_open` empty locally?

Upstream uses it as a hot-write target: real-time trips are extended
in `stib_trip_open` and only moved to `stib_trip` once finalised. The
local `load_stib.py` runs in batch mode over a fully-loaded
position table, so by the time we write anything, every trip is
already "closed". We flush everything to `stib_trip` directly
([load_stib.py:171-175](../src/etl/pipeline/load/load_stib.py))
and rely on the `rt.stib_trip_all` view
([sql/schema.sql:132-140](../sql/schema.sql)) to keep the query
surface identical to upstream's bicephalous layout.

### Q. Why does `/api/stib/lines` rank directions by *trips over the last 7 days* instead of the GTFS direction order?

Two reasons, both at
[stib_service.py:40-77](../src/map/server/services/stib_service.py):

- A line can have **more than two** GTFS directions (loops with
  branches, school-day variants). The upstream UI only displays the
  top destinations, so we pick the busiest.
- The "last 7 days" window is anchored on `MAX(end_ts)` (line 53), not
  on `NOW()`. Replaying a static bench dump from last year still
  surfaces the busy directions of that dataset, not nothing.

### Q. Where does the `line_trip_id` (`<lineid>_001`, `<lineid>_002`, …) come from?

Computed at read time, not at write time, inside `/api/stib/live-positions`
and `/api/stib/trajectories`:

```sql
ROW_NUMBER() OVER (
    PARTITION BY t.lineid,
                 date_trunc('day',
                     CASE WHEN t.lineid ~ '^N'
                          THEN t.start_ts AT TIME ZONE 'Europe/Brussels'
                          ELSE (t.start_ts AT TIME ZONE 'Europe/Brussels')
                               - INTERVAL '3 hours 30 minutes'
                     END)
    ORDER BY t.start_ts
)
```

[stib_service.py:234-243](../src/map/server/services/stib_service.py)
(live positions) and
[stib_service.py:327-336](../src/map/server/services/stib_service.py)
(trajectories). Noctis lines (those whose `lineid` starts with `N`)
count from local midnight; all other lines shift the day boundary by
03:30 so an early-morning trip belongs to the *previous* operational
day. This matches upstream's convention and means a single bus
finishing its night at 02:45 keeps the same per-day sequence number
as the trip that started it.

`load_stib.py` stores `line_trip_id = NULL`
([load_stib.py:128-134](../src/etl/pipeline/load/load_stib.py))
because computing it at write-time would be lying — the value depends
on the day-boundary rule, which is a presentation choice.

---

## 10. Appendix — file index

The tour above touched these files; here they are in execution order
so you can re-walk the data flow with a single side-by-side viewer.

### Configuration
- [src/env_manager.py](../src/env_manager.py) — `.env` loader; dotted and
  underscore keys accepted ([:18-23](../src/env_manager.py)).
- [src/map/server/config.py](../src/map/server/config.py) — `ServerConfig`
  dataclass with `dsn` / `conninfo` helpers.

### Ingestion
- [src/etl/ingestion/stib/_common.py](../src/etl/ingestion/stib/_common.py)
  — auth header, paginator, `haversine_m`, `normalise_color`.
- [src/etl/ingestion/stib/stop_details_ingestor.py](../src/etl/ingestion/stib/stop_details_ingestor.py)
  — `/static/stopDetails` → `static.stib_stop`.
- [src/etl/ingestion/stib/stops_by_line_ingestor.py](../src/etl/ingestion/stib/stops_by_line_ingestor.py)
  — `/static/stopsByLine` → `static.stib_line_stops` +
  `static.stib_line_terminus`.
- [src/etl/ingestion/stib/gtfs_shapes_ingestor.py](../src/etl/ingestion/stib/gtfs_shapes_ingestor.py)
  — GTFS zip → `static.stib_line_shape`. Direction-resolution heuristic
  at `_resolve_dir` / `TERMINUS_TOLERANCE_M`.
- [src/etl/ingestion/stib/ingestor.py](../src/etl/ingestion/stib/ingestor.py)
  — CLI dispatcher for the four steps above + `rt`.
- [src/etl/ingestion/stib/vehicle_positions_ingestor.py](../src/etl/ingestion/stib/vehicle_positions_ingestor.py)
  — live ODP poll → `insert_positions`. Mirror-to-file safety at
  `_safe_output_path`.
- [src/etl/ingestion/bench/ingestor.py](../src/etl/ingestion/bench/ingestor.py)
  — `.jsonl.gz` / `.csv.gz` replay, with three file-type branches.

### Transform & load
- [src/etl/pipeline/transform/stib_transform.py](../src/etl/pipeline/transform/stib_transform.py)
  — deterministic `position_id`, `_INSERT_SQL` with
  `ST_LineLocatePoint` + `ST_LineInterpolatePoint` + anchor fallback.
- [src/etl/pipeline/load/load_stib.py](../src/etl/pipeline/load/load_stib.py)
  — greedy trip reconstruction, `_OpenTrip`, `_format_tgeompoint`.

### Web serving
- [src/map/server/main.py](../src/map/server/main.py) — CLI entry,
  Werkzeug `make_server`, pool lifecycle.
- [src/map/server/context.py](../src/map/server/context.py) — psycopg2
  pool shim mimicking psycopg3's `connection()` context manager.
- [src/map/server/app.py](../src/map/server/app.py) — `create_app()`,
  all routes, response-cache wiring.
- [src/map/server/queries/layer_sql.py](../src/map/server/queries/layer_sql.py)
  — MVT SQL for `stib_lines` and `stib_stops` (lines-filter aware).
- [src/map/server/utils/time_utils.py](../src/map/server/utils/time_utils.py)
  — `parse_hours`, `parse_time_filter`, `parse_lines_filter`.
- [src/map/server/services/stib_service.py](../src/map/server/services/stib_service.py)
  — line catalogue, stop details, shapes, live positions,
  trajectories, alerts (empty).
- [src/map/server/services/tiles.py](../src/map/server/services/tiles.py)
  — TTL'd LRU tile cache + execution.
- [src/map/server/services/counts.py](../src/map/server/services/counts.py)
  — `/api/health`, `/debug/counts`.
- [src/map/server/services/search.py](../src/map/server/services/search.py)
  — unified search over `static.stib_stop` + `static.stib_line_shape`.
- [src/map/server/services/waiting_times.py](../src/map/server/services/waiting_times.py)
  — popup ETAs from `transport_local.stib_waiting_time`.

### Schema
- [sql/schema.sql](../sql/schema.sql) — three schemas, six tables,
  one view, extensions.
