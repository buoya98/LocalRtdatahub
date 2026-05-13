# LocalRtdatahub

Standalone, minimalist, 100% local port of the **Transport** module of
[RTDataHub](https://github.com/BM-CM-MC/RTDataHub) — Flask + MapLibre +
PostgreSQL/PostGIS/MobilityDB running natively on Linux. No Docker, no
runtime dependency on external APIs once the static catalogue is loaded.
Two ingestion paths produce the same rows: **live STIB Open Data Portal**
or **raw `.jsonl.gz` / `.csv.gz` dumps**.

> 📖 **How does it work?** See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
> for the end-to-end story: data model, ingestion flow, geom-reconstruction
> SQL, trip rebuilding algorithm, Flask serving layer.
> This README is install + run only.

```
LocalRtdatahub/
├── src/                            # Python code (mirrors upstream layout)
│   ├── env_manager.py              # .env reader (dotted or underscore keys)
│   ├── etl/
│   │   ├── ingestion/
│   │   │   ├── bench/              # raw .jsonl.gz / .csv.gz → rt.*
│   │   │   │   └── ingestor.py
│   │   │   └── stib/               # STIB ODP: GTFS static + live positions
│   │   │       ├── ingestor.py     # CLI dispatcher
│   │   │       ├── stop_details_ingestor.py
│   │   │       ├── stops_by_line_ingestor.py
│   │   │       ├── gtfs_shapes_ingestor.py
│   │   │       └── vehicle_positions_ingestor.py
│   │   └── pipeline/
│   │       ├── transform/
│   │       │   └── stib_transform.py   # geom from line_shape + stop in SQL
│   │       └── load/
│   │           └── load_stib.py     # rt.stib_vehicle_position → trips
│   └── map/
│       ├── server/                 # Flask app split by domain
│       │   ├── main.py             # entry point (Werkzeug)
│       │   ├── app.py              # create_app() + routes
│       │   ├── config.py           # ServerConfig
│       │   ├── context.py          # MapAppContext + connection pool
│       │   ├── services/           # one file per domain (stib, delijn, …)
│       │   ├── queries/            # MVT layer SQL
│       │   └── utils/              # query parsing, time filters, …
│       └── templates/index.html    # MapLibre viewer
├── data/                           # raw bench dumps (.jsonl.gz / .csv.gz)
│   └── bench_algo_data/
├── sql/
│   └── schema.sql                  # PostGIS + MobilityDB + rt.stib_*
├── requirements.txt
└── .env.example
```

---

## 1. Linux prerequisites

Tested on **Ubuntu 25.04 (Plucky)** with:

- PostgreSQL **18** (installed via `pgdg`)
- PostGIS **3.6**
- Python **3.11+**

Check:

```bash
psql --version                    # >= 18
pg_isready -h 127.0.0.1 -p 5432   # accepting connections
python3 --version                 # >= 3.11
```

---

## 2. MobilityDB install (build from source)

MobilityDB has no `apt` package for PostgreSQL 18 / Plucky yet. Compile
it against the system `pg_config`:

```bash
# Build dependencies (Ubuntu/Debian)
sudo apt install -y build-essential cmake git \
  postgresql-server-dev-18 \
  libgeos-dev libproj-dev libjson-c-dev libgsl-dev \
  libprotobuf-c-dev protobuf-c-compiler

# Fedora/RHEL equivalent:
#   sudo dnf install -y gcc gcc-c++ make cmake git \
#     postgresql-server-devel postgresql-devel \
#     geos-devel proj-devel json-c-devel gsl-devel \
#     protobuf-c-devel protobuf-c-compiler

# Arch equivalent:
#   sudo pacman -S --needed base-devel cmake git postgresql \
#     geos proj json-c gsl protobuf-c

# Fetch + build
cd /tmp
git clone --depth 1 https://github.com/MobilityDB/MobilityDB.git
cd MobilityDB
mkdir build && cd build

# IMPORTANT: force cmake to use the system pg_config (not the conda one).
# Path varies by distro:
#   Ubuntu/Debian:  /usr/lib/postgresql/18/bin/pg_config
#   Fedora/RHEL:    /usr/pgsql-18/bin/pg_config
#   Arch/generic:   $(which pg_config)
PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config cmake ..

make -j"$(nproc)"
sudo make install

# Reload PostgreSQL so the extension shows up
sudo systemctl restart postgresql        # systemd
# sudo rc-service postgresql restart     # OpenRC (Alpine, Devuan)
```

Verify:

```bash
psql -h 127.0.0.1 -U postgres -c "SELECT name FROM pg_available_extensions WHERE name='mobilitydb';"
```

You should see `mobilitydb` listed.

> ℹ️ If `pg_config` returns the conda version (e.g. 18.3) instead of
> `/usr/lib/postgresql/18/bin/pg_config`, deactivate conda for the build:
> `conda deactivate` (until `(base)` disappears from your prompt).

---

## 3. Database + extensions

Create a dedicated user + database:

```bash
sudo -u postgres psql <<'EOF'
CREATE USER rtdatahub WITH PASSWORD 'rtdatahub';
CREATE DATABASE rtdatahub_local OWNER rtdatahub;
EOF
```

Enable extensions and create tables:

```bash
sudo -u postgres psql -d rtdatahub_local -c "CREATE EXTENSION postgis;"
sudo -u postgres psql -d rtdatahub_local -c "CREATE EXTENSION mobilitydb CASCADE;"

# Schema + tables (PostGIS + MobilityDB)
psql "postgresql://rtdatahub:rtdatahub@localhost:5432/rtdatahub_local" \
  -f sql/schema.sql
```

Verify:

```bash
psql "postgresql://rtdatahub:rtdatahub@localhost:5432/rtdatahub_local" \
  -c "\dt rt.*"
```

You should see three tables (`stib_vehicle_position`, `stib_trip`,
`stib_trip_open`) and one view (`stib_trip_all`), all created by
[sql/schema.sql](sql/schema.sql).

---

## 4. Python environment (venv)

From the repo root:

```bash
python3 -m venv env
source env/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Configure your `.env`:

```bash
cp .env.example .env
# Edit if needed (e.g. change the DB password or add STIB_ODP_KEY)
```

---

## 5. Data ingestion

The repo ships with everything needed to populate the DB **without an
API key**: a pre-extracted STIB catalogue (~420 KB) + 7 days of raw
vehicle positions (~85 MB). One command loads the lot.

### 5.a — Quick start: bench replay from the shipped sample

```bash
python -m src.etl.ingestion.bench.ingestor data/bench_algo_data/
```

This auto-detects every `.jsonl.gz` / `.csv.gz` under the tree, loads
the static catalogue first (stops → line_stops → line_terminus →
line_shapes), then the vehicle positions. Idempotent: re-running on the
same files only re-tries inserts (`ON CONFLICT … DO NOTHING / DO UPDATE`).

Repository data layout:

```
data/bench_algo_data/
├── static/                              # STIB catalogue (~420 KB total)
│   ├── stops.jsonl.gz                   # static.stib_stop
│   ├── line_stops.jsonl.gz              # static.stib_line_stops
│   ├── line_terminus.jsonl.gz           # static.stib_line_terminus
│   └── line_shapes.jsonl.gz             # static.stib_line_shape (geom_wkt)
└── positions/YYYY-MM-DD.jsonl.gz        # raw STIB feed, 7-day sample
```

You can also feed individual files or sub-trees:

```bash
# Just the static catalogue (refresh the static.* tables)
python -m src.etl.ingestion.bench.ingestor data/bench_algo_data/static/

# Just one day of positions
python -m src.etl.ingestion.bench.ingestor data/bench_algo_data/positions/2026-05-07.jsonl.gz
```

> Raw inputs are **read-only**: the ingestor opens every file with
> `gzip.open(path, "rb")`. Nothing is renamed, moved or written back.

### 5.b — Live API (optional, refreshes catalogue + collects new positions)

Requires `STIB_ODP_KEY` in `.env`. Get one at
<https://opendata.stib-mivb.be/>. Skip this section if you're happy with
the shipped sample.

> ⚠️ **Step ordering matters (ODP path only).** `stops` populates
> `static.stib_stop` → `lines` populates `static.stib_line_terminus` →
> `shapes` reads both and populates `static.stib_line_shape`. The
> default invocation runs them in order; running `shapes` alone before
> `lines` silently inserts nothing. The bench replay path (§5.a) is
> immune to this — `_KIND_ORDER` in the ingestor enforces the same
> dependency order automatically.

```bash
# Refresh the static catalogue from the ODP (re-runs once GTFS changes)
python -m src.etl.ingestion.stib.ingestor             # stops + lines + shapes

# Poll live vehicle positions
python -m src.etl.ingestion.stib.ingestor rt                    # one poll → DB
python -m src.etl.ingestion.stib.ingestor rt --interval 20      # loop forever

# Optional: mirror raw polls to a fresh .jsonl.gz so they can be replayed
# later via the bench path. The output directory is created automatically.
# Each invocation writes to a UNIQUE timestamped filename — existing files
# are never touched.
python -m src.etl.ingestion.stib.ingestor rt --interval 20 \
       --output data/raw/stib/
```

The ODP live feed has no GPS coordinates — geom is reconstructed in
SQL from `static.stib_line_shape` + `static.stib_stop` via
[src/etl/pipeline/transform/stib_transform.py](src/etl/pipeline/transform/stib_transform.py)
(`ST_LineLocatePoint` + `ST_LineInterpolatePoint`). If the static
tables are empty when `rt` runs, observations are silently dropped —
check `/debug/counts` after a poll to spot this.

The raw `positions/*.jsonl.gz` dumps share that exact transform, so a
fresh clone using only §5.a writes the same rows in
`rt.stib_vehicle_position` as a live ODP poll would have.

`assignments.csv.gz` (under `runs/<run-name>/tables/`) is an optional
third format: it carries lat/lon already resolved and is inserted
directly with `ST_MakePoint(lon, lat)` — no transform.

Records are deduplicated by a deterministic `position_id` (SHA-256 of
`lineid|direction|pointid|fetched_at|distance`), so any mix of paths is
idempotent.

```bash
# Recap — common invocations
python -m src.etl.ingestion.bench.ingestor data/bench_algo_data/
python -m src.etl.ingestion.bench.ingestor data/bench_algo_data/positions/

# A single file
python -m src.etl.ingestion.bench.ingestor data/bench_algo_data/positions/2026-05-04.jsonl.gz
```

File-type auto-detection:

| File                              | Target                                                                  |
| --------------------------------- | ----------------------------------------------------------------------- |
| `static/stops.jsonl.gz`           | `static.stib_stop`                                                      |
| `static/line_stops.jsonl.gz`      | `static.stib_line_stops`                                                |
| `static/line_terminus.jsonl.gz`   | `static.stib_line_terminus`                                             |
| `static/line_shapes.jsonl.gz`     | `static.stib_line_shape` (geom from WKT)                                |
| `positions/*.jsonl.gz`            | `rt.stib_vehicle_position` (geom computed like the live API path)       |
| `assignments.csv.gz`              | `rt.stib_vehicle_position` (lat/lon already present)                    |

> ⚠️ **No-overwrite guarantee.** Raw `.jsonl.gz` files are never touched.
> The bench ingestor opens them read-only (`gzip.open(path, "rb")`). The
> live mirror (`--output`) refuses to write under `data/bench_algo_data/`
> and always generates a unique timestamped filename. If the target path
> already exists the program aborts instead of touching the existing file.

### 5.c — Trip reconstruction (MobilityDB `tgeompoint`)

**Prerequisite:** §5.a or §5.b must have populated
`rt.stib_vehicle_position`. Without rows there, this step produces
zero trips silently.

Whereas §5.a/§5.b insert individual vehicle observations,
`load_stib` groups them into trip-shaped `tgeompoint`
trajectories. The STIB feed has no vehicle identifier, so trip identity
is rebuilt offline by greedy spatial+temporal assignment (see
[docs/ARCHITECTURE.md §5](docs/ARCHITECTURE.md) for the algorithm).

```bash
python -m src.etl.pipeline.load.load_stib          # all lines
python -m src.etl.pipeline.load.load_stib 53 56    # specific lines
```

> ⚠️ This step **truncates** `rt.stib_trip` and
> `rt.stib_trip_open` before rebuilding — it's destructive by
> design (re-runs are reproducible). Closed trips land in
> `rt.stib_trip`; the `rt.stib_trip_all` view (used by the API)
> unions both tables.

Sanity check:

```bash
psql "postgresql://rtdatahub:rtdatahub@localhost:5432/rtdatahub_local" -c "
  SELECT lineid, COUNT(*)
    FROM rt.stib_vehicle_position
   GROUP BY lineid ORDER BY lineid LIMIT 10;
"
```

---

## 6. Running the Flask map server

```bash
source env/bin/activate
python -m src.map.server.main --listen-host 127.0.0.1 --listen-port 8090
```

Open **http://127.0.0.1:8090** in your browser.

All CLI flags (see `--help` for the full list):

| Flag                          | Default      | Purpose                                        |
| ----------------------------- | ------------ | ---------------------------------------------- |
| `--listen-host`               | `127.0.0.1`  | Bind address. Set `0.0.0.0` to expose on LAN.   |
| `--listen-port`               | `8090`       | TCP port.                                       |
| `--hours`                     | `1.0`        | Default lookback window (capped at 24 h).       |
| `--open-browser`              | off          | Open the URL in the default browser at startup. |
| `--max-db-connections`        | `10`         | psycopg2 pool max size.                         |
| `--db-acquire-timeout-s`      | `300`        | Wait time before pool returns "busy".           |
| `--db-statement-timeout-ms`   | `120000`     | PostgreSQL `statement_timeout` per request.     |
| `--tile-cache-ttl-s`          | `160`        | LRU tile cache freshness.                       |
| `--tile-cache-max-entries`    | `4000`       | Max tiles kept in memory (~64 KB each).         |
| `--counts-cache-ttl-s`        | `4`          | `/debug/counts` cache freshness.                |

CLI flags override the `LISTEN_HOST`/`LISTEN_PORT`/`DEFAULT_HOURS`
env vars from `.env`. You get:

- a MapLibre map centred on Brussels,
- a panel with a line selector grouped by mode (Metro / Tram / Bus),
- a temporal scrubber with a **Play** button to replay the
  `[cursor − window ; cursor]` window.

Available endpoints:

| Route                                              | Description                                          |
| -------------------------------------------------- | ---------------------------------------------------- |
| `GET /`                                            | MapLibre SPA                                         |
| `GET /api/health`                                  | DB status + PostGIS/MobilityDB extensions            |
| `GET /api/stib/lines`                              | STIB line catalogue with mode, colours, destinations |
| `GET /api/stib/live-positions?lines=53&hours=1`    | Vehicle positions at the reference instant (GeoJSON) |
| `GET /api/stib/trajectories?lines=53&hours=1`      | Sampled trajectories per trip for the player        |
| `GET /api/stib/shape/<lineid>/<direction>`         | LineString geometry for one line + direction        |
| `GET /api/stib/stop/<pointid>`                     | Stop info + lines serving it                        |
| `GET /api/stib/alerts`                             | Empty (no upstream alert pipeline locally)          |
| `GET /api/search?q=…`                              | Unified search across stops + lines                 |
| `GET /api/waiting-times?pointid=1775`              | ETA per stop. **Returns `[]` until you ingest your own `waiting_times/*.jsonl.gz` or `wt.jsonl.gz` dumps** — none ship with the repo (each daily file is 150+ MB, above GitHub's per-file limit). |
| `GET /tiles/<layer>/<z>/<x>/<y>.pbf`               | MVT vector tiles (`stib_lines`, `stib_stops`)        |
| `GET /debug/counts`                                | Row counts per table (debug)                        |

De Lijn endpoints (`/api/delijn/...`) and other domains (TomTom, Waze,
ANPR, traffic-light, FLIR) return empty stubs — see §9.

---

## 7. Daily reset workflow

Loading new `.jsonl.gz` every day? Start from a clean DB:

```bash
psql "postgresql://rtdatahub:rtdatahub@localhost:5432/rtdatahub_local" -c "
  TRUNCATE rt.stib_vehicle_position,
           rt.stib_trip,
           rt.stib_trip_open;
"
```

Then re-run the relevant ingestor.

---

## 8. Troubleshooting

| Symptom                                            | Cause / Fix                                                                 |
| -------------------------------------------------- | --------------------------------------------------------------------------- |
| `extension "mobilitydb" is not available`          | Build didn't install the extension into `/usr/lib/postgresql/18/`. Recompile forcing `PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config`. |
| `psql: error: connection refused`                  | `sudo systemctl start postgresql`; check `pg_isready`.                      |
| `permission denied for schema rt`                  | The schemas got created by a different role (e.g. `postgres`). Make sure the `psql` invocation that loaded `sql/schema.sql` used the `rtdatahub` user (`psql "postgresql://rtdatahub:rtdatahub@…"`), so `rt`, `static` and `transport_local` are owned by it. |
| Empty map                                          | `/api/health` should report `mobilitydb: true`. If yes, check `/api/stib/lines` (empty DB → re-run an ingestor).                          |
| Conda shadows the system `psql`                    | `conda deactivate` or use the absolute path `/usr/lib/postgresql/18/bin/psql`. |
| `STIB_ODP_KEY environment variable is not set`     | Add `STIB_ODP_KEY=…` to `.env` (request a key on the STIB Open Data Portal). |

---

## 9. Scope / differences vs upstream RTDataHub

**Kept** (mirrors the upstream — same paths under `src/`):

- Tables `rt.stib_vehicle_position`, `rt.stib_trip*`, `static.stib_*`
  ([sql/schema.sql](sql/schema.sql))
- Full map-server API: `/api/stib/lines`, `/live-positions`,
  `/trajectories`, `/shape/...`, `/stop/...`, `/api/search`,
  `/tiles/<layer>/...` ([src/map/server/app.py](src/map/server/app.py))
- STIB ODP ingestion: stops + lines + shapes + RT positions
  ([src/etl/ingestion/stib/](src/etl/ingestion/stib/))
- MobilityDB trip reconstruction
  ([src/etl/pipeline/load/load_stib.py](src/etl/pipeline/load/load_stib.py))

**Removed** (out of scope or dependent on credentialed external APIs):

- De Lijn, TomTom, Waze, ANPR, traffic-light, FLIR — endpoints kept as
  empty stubs so the upstream frontend can still talk to the server
- Azure / Kafka ingestors (ANPR, Magneto, …)
- SSH/QGIS tunnel and corporate network configuration
- The upstream `index.html` (≈5300 lines) is kept almost verbatim — the
  local copy is ~5265 lines, with Mapbox GL JS swapped for MapLibre GL JS
  (drop-in compatible) and the De Lijn / cameras / traffic-light layers
  silently hidden when their stub endpoints return empty

---

## License

The upstream RTDataHub code under `src/` is reproduced from
[BM-CM-MC/RTDataHub](https://github.com/BM-CM-MC/RTDataHub); credit goes
to its authors. This local port is provided as-is for educational and
research purposes.
