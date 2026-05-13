"""Bench ingestor — raw `.jsonl.gz` / `.csv.gz` dumps → local DB.

Auto-detects file type from filename:

  * static/stops.jsonl.gz          → static.stib_stop
  * static/line_stops.jsonl.gz     → static.stib_line_stops
  * static/line_terminus.jsonl.gz  → static.stib_line_terminus
  * static/line_shapes.jsonl.gz    → static.stib_line_shape
        Pre-extracted STIB catalogue. Lets a fresh clone bootstrap the
        whole pipeline without needing a STIB ODP API key. Ordering is
        enforced (stops → line_stops → line_terminus → line_shapes).

  * positions/*.jsonl.gz           → rt.stib_vehicle_position
        Raw STIB feed: {poll_ts, lineid, directionId, pointId,
                        distanceFromPoint} — no GPS. Geom is reconstructed
        in SQL from static.stib_line_shape + static.stib_stop via
        src/etl/pipeline/transform/stib_transform.py.

  * assignments.csv.gz             → rt.stib_vehicle_position
        Bench-algo output that already carries resolved lat/lon.
        Inserted directly with ST_MakePoint(lon, lat).

Raw inputs are read-only — every dump is opened with gzip.open(..., "rb")
and never modified, renamed, or moved. Re-running the ingestor on the
same files only re-tries inserts (ON CONFLICT … DO NOTHING / DO UPDATE).

Run with:
    python -m src.etl.ingestion.bench.ingestor PATH [PATH ...]
"""
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator

import psycopg2
from psycopg2.extras import execute_values

# Make `from src.env_manager import …` work whether this file is run via
# `python -m src.etl.ingestion.bench.ingestor` or directly.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from src.env_manager import DATABASE_URL, INGEST_BATCH_SIZE  # noqa: E402
from src.etl.pipeline.transform.stib_transform import insert_positions  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest_bench")


# ----------------------------- Helpers --------------------------------------

def _classify(path: Path) -> str:
    name = path.name.lower()
    parts = {p.lower() for p in path.parts}
    if name.startswith("assignments") and name.endswith(".csv.gz"):
        return "assignments"
    if "static" in parts and name.endswith(".jsonl.gz"):
        # Static catalogue files — each one maps to one static.stib_* table.
        stem = name[: -len(".jsonl.gz")]
        if stem in ("stops", "line_stops", "line_terminus", "line_shapes"):
            return f"static_{stem}"
    if name.startswith("wt") and name.endswith(".jsonl.gz"):
        return "waiting_times"
    if "waiting_times" in parts and name.endswith(".jsonl.gz"):
        return "waiting_times"
    if "positions" in parts and name.endswith(".jsonl.gz"):
        return "positions"
    return "unknown"


# Enforce FK-respecting load order: stops first (referenced by everything),
# then line_stops + line_terminus (the latter is read by line_shapes via the
# direction label), then line_shapes. positions/assignments last because they
# read line_shape/stop to resolve geom.
_KIND_ORDER = {
    "static_stops":         0,
    "static_line_stops":    1,
    "static_line_terminus": 2,
    "static_line_shapes":   3,
    "assignments":          4,
    "positions":            4,
    "waiting_times":        5,
    "unknown":              99,
}


def _walk(paths: Iterable[Path]) -> Iterator[Path]:
    for p in paths:
        if p.is_dir():
            for child in p.rglob("*"):
                if child.is_file() and (
                    child.suffix == ".gz"
                    and child.suffixes[-2:] in ([".csv", ".gz"], [".jsonl", ".gz"])
                ):
                    yield child
        elif p.is_file():
            yield p


def _open_text_gz(path: Path) -> io.TextIOWrapper:
    return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", newline="")


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        log.warning("unparseable timestamp: %r", value)
        return None


# ------------------------ Ingestion routines --------------------------------

def ingest_assignments(conn, path: Path) -> int:
    insert_sql = """
        INSERT INTO rt.stib_vehicle_position (
            position_id, vehicle_uuid, lineid, direction, pointid,
            distance_from_point, fetched_at, geom
        ) VALUES %s
        ON CONFLICT (position_id) DO NOTHING;
    """
    template = (
        "(%s, NULL, %s, %s, %s, %s, %s, "
        "ST_SetSRID(ST_MakePoint(%s, %s), 4326))"
    )

    inserted = 0
    seen_stops: dict[str, tuple[float, float]] = {}

    with _open_text_gz(path) as fh:
        reader = csv.DictReader(fh)
        batch: list[tuple] = []
        for row in reader:
            try:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
            except (TypeError, ValueError, KeyError):
                continue
            fetched_at = _parse_ts(row.get("fetched_at_utc"))
            if not fetched_at:
                continue
            try:
                direction = int(row["direction"]) if row.get("direction") not in ("", None) else None
            except ValueError:
                direction = None
            try:
                distance = float(row["distance"]) if row.get("distance") not in ("", None) else None
            except ValueError:
                distance = None

            pointid = row.get("pointId") or None
            if pointid:
                seen_stops[pointid] = (lat, lon)

            batch.append((
                row.get("position_id"),
                row.get("lineId"),
                direction,
                pointid,
                distance,
                fetched_at,
                lon,
                lat,
            ))
            if len(batch) >= INGEST_BATCH_SIZE:
                with conn.cursor() as cur:
                    execute_values(cur, insert_sql, batch, template=template,
                                   page_size=INGEST_BATCH_SIZE)
                inserted += len(batch)
                batch.clear()

        if batch:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, batch, template=template,
                               page_size=INGEST_BATCH_SIZE)
            inserted += len(batch)

    if seen_stops:
        _upsert_stops(conn, seen_stops)
    return inserted


def _upsert_stops(conn, stops: dict[str, tuple[float, float]]) -> None:
    """Best-effort: keep one (pointid -> last observed lat/lon) for popups.
    Real STIB stop coordinates differ from vehicle coordinates, but in the
    absence of GTFS we use the closest observed vehicle position as a stand-in.
    Overwritten when src/etl/ingestion/stib/stop_details_ingestor.py is run."""
    rows = [(pid, lon, lat) for pid, (lat, lon) in stops.items()]
    sql = """
        INSERT INTO static.stib_stop (pointid, geom, updated_at)
        VALUES %s
        ON CONFLICT (pointid) DO UPDATE SET
            geom = EXCLUDED.geom,
            updated_at = NOW();
    """
    template = "(%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), NOW())"
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, template=template, page_size=INGEST_BATCH_SIZE)


def ingest_waiting_times(conn, path: Path) -> int:
    insert_sql = """
        INSERT INTO transport_local.stib_waiting_time
            (poll_ts, pointid, lineid, expected, destination, message)
        VALUES %s;
    """
    inserted = 0
    batch: list[tuple] = []

    with _open_text_gz(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            poll_ts = _parse_ts(rec.get("poll_ts"))
            if not poll_ts:
                continue
            batch.append((
                poll_ts,
                rec.get("pointid"),
                rec.get("lineid"),
                _parse_ts(rec.get("expected")),
                rec.get("destination"),
                rec.get("message"),
            ))
            if len(batch) >= INGEST_BATCH_SIZE:
                with conn.cursor() as cur:
                    execute_values(cur, insert_sql, batch, page_size=INGEST_BATCH_SIZE)
                inserted += len(batch)
                batch.clear()

    if batch:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, batch, page_size=INGEST_BATCH_SIZE)
        inserted += len(batch)
    return inserted


def ingest_raw_positions(conn, path: Path) -> int:
    """positions/*.jsonl.gz → rt.stib_vehicle_position via transform.

    The raw feed has no GPS — geom is reconstructed in SQL from
    static.stib_line_shape + static.stib_stop. Records whose stop or line
    isn't in the static tables are silently dropped (anchor fallback in
    stib_transform handles the rest).
    """
    def _records() -> Iterator[dict]:
        with _open_text_gz(path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue

    return insert_positions(conn, _records(), batch_size=INGEST_BATCH_SIZE)


# ------------------------ Static catalogue replay ---------------------------
# Replays a pre-extracted STIB catalogue from the .jsonl.gz files shipped in
# data/bench_algo_data/static/. Same end-state as running the STIB ODP
# ingestors (stops + lines + shapes), without needing a STIB_ODP_KEY.

def _iter_jsonl(path: Path) -> Iterator[dict]:
    with _open_text_gz(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _batch_or_skip(name: str, batch: list, path: Path) -> int:
    """Log a warning when a static dump yielded no usable rows (likely
    malformed input) and return 0 so the caller short-circuits."""
    if not batch:
        log.warning("  -> 0 rows after filtering — %s in %s", name, path)
    return len(batch)


def ingest_static_stops(conn, path: Path) -> int:
    sql = """
        INSERT INTO static.stib_stop (pointid, stop_name_fr, stop_name_nl, geom, updated_at)
        VALUES %s
        ON CONFLICT (pointid) DO UPDATE SET
            stop_name_fr = EXCLUDED.stop_name_fr,
            stop_name_nl = EXCLUDED.stop_name_nl,
            geom         = EXCLUDED.geom,
            updated_at   = NOW();
    """
    template = "(%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), NOW())"
    batch = []
    for r in _iter_jsonl(path):
        pid = r.get("pointid")
        lon, lat = r.get("lon"), r.get("lat")
        if pid is None or lon is None or lat is None:
            continue
        batch.append((pid, r.get("stop_name_fr"), r.get("stop_name_nl"), lon, lat))
    n = _batch_or_skip("stops", batch, path)
    if n:
        with conn.cursor() as cur:
            execute_values(cur, sql, batch, template=template, page_size=INGEST_BATCH_SIZE)
    return n


def ingest_static_line_stops(conn, path: Path) -> int:
    sql = """
        INSERT INTO static.stib_line_stops
            (lineid, direction, sequence_idx, pointid, destination)
        VALUES %s
        ON CONFLICT (lineid, direction, sequence_idx) DO UPDATE SET
            pointid = EXCLUDED.pointid,
            destination = EXCLUDED.destination,
            updated_at = NOW();
    """
    batch = []
    for r in _iter_jsonl(path):
        lineid, direction, seq, pid = (r.get("lineid"), r.get("direction"),
                                        r.get("sequence_idx"), r.get("pointid"))
        if lineid is None or direction is None or seq is None or pid is None:
            continue
        batch.append((lineid, direction, seq, pid, r.get("destination")))
    n = _batch_or_skip("line_stops", batch, path)
    if n:
        with conn.cursor() as cur:
            execute_values(cur, sql, batch, page_size=INGEST_BATCH_SIZE)
    return n


def ingest_static_line_terminus(conn, path: Path) -> int:
    sql = """
        INSERT INTO static.stib_line_terminus
            (lineid, terminus_pointid, direction, destination)
        VALUES %s
        ON CONFLICT (lineid, terminus_pointid) DO UPDATE SET
            direction = EXCLUDED.direction,
            destination = EXCLUDED.destination,
            updated_at = NOW();
    """
    batch = []
    for r in _iter_jsonl(path):
        lineid, term, direction = (r.get("lineid"), r.get("terminus_pointid"),
                                    r.get("direction"))
        if lineid is None or term is None or direction is None:
            continue
        batch.append((lineid, term, direction, r.get("destination")))
    n = _batch_or_skip("line_terminus", batch, path)
    if n:
        with conn.cursor() as cur:
            execute_values(cur, sql, batch, page_size=INGEST_BATCH_SIZE)
    return n


def ingest_static_line_shapes(conn, path: Path) -> int:
    sql = """
        INSERT INTO static.stib_line_shape
            (lineid, direction, variant, mode, route_color, route_text_color, geom)
        VALUES %s
        ON CONFLICT (lineid, direction) DO UPDATE SET
            variant = EXCLUDED.variant,
            mode = EXCLUDED.mode,
            route_color = EXCLUDED.route_color,
            route_text_color = EXCLUDED.route_text_color,
            geom = EXCLUDED.geom;
    """
    template = (
        "(%s, %s, %s, %s, %s, %s, "
        "ST_SetSRID(ST_GeomFromText(%s), 4326))"
    )
    batch = []
    for r in _iter_jsonl(path):
        lineid, direction, wkt = (r.get("lineid"), r.get("direction"),
                                   r.get("geom_wkt"))
        if lineid is None or direction is None or not wkt:
            continue
        batch.append((lineid, direction, r.get("variant"), r.get("mode"),
                      r.get("route_color"), r.get("route_text_color"), wkt))
    n = _batch_or_skip("line_shapes", batch, path)
    if n:
        with conn.cursor() as cur:
            execute_values(cur, sql, batch, template=template, page_size=INGEST_BATCH_SIZE)
    return n


_STATIC_DISPATCH = {
    "static_stops":         ingest_static_stops,
    "static_line_stops":    ingest_static_line_stops,
    "static_line_terminus": ingest_static_line_terminus,
    "static_line_shapes":   ingest_static_line_shapes,
}


# -------------------------------- Main --------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  # Whole tree (auto-detected by name)\n"
            "  python -m src.etl.ingestion.bench.ingestor data/bench_algo_data/\n\n"
            "  # Just the raw positions\n"
            "  python -m src.etl.ingestion.bench.ingestor data/bench_algo_data/positions/\n\n"
            "  # Single file\n"
            "  python -m src.etl.ingestion.bench.ingestor data/bench_algo_data/positions/2026-05-04.jsonl.gz\n"
        ),
    )
    parser.add_argument(
        "paths",
        nargs="+",
        type=Path,
        metavar="PATH",
        help="One or more .jsonl.gz / .csv.gz files or directories "
             "(recursively scanned). Required.",
    )
    args = parser.parse_args()

    total = {"static": 0, "assignments": 0, "raw_positions": 0,
             "waiting_times": 0, "skipped": 0}

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    try:
        files = list(_walk(args.paths))
        if not files:
            log.error("no .jsonl.gz / .csv.gz files found in %s", args.paths)
            return 1

        # Static catalogue first (stops → line_stops → line_terminus →
        # line_shapes), then positions/assignments, then waiting_times.
        # See _KIND_ORDER for the priority table.
        ordered = sorted(files, key=lambda p: (_KIND_ORDER.get(_classify(p), 99), p.name))

        for path in ordered:
            kind = _classify(path)
            log.info("%s -> %s", path, kind)
            if kind in _STATIC_DISPATCH:
                n = _STATIC_DISPATCH[kind](conn, path)
                conn.commit()
                total["static"] += n
                log.info("  -> %d %s rows upserted", n, kind.replace("static_", ""))
            elif kind == "assignments":
                n = ingest_assignments(conn, path)
                conn.commit()
                total["assignments"] += n
                log.info("  -> %d positions inserted", n)
            elif kind == "waiting_times":
                n = ingest_waiting_times(conn, path)
                conn.commit()
                total["waiting_times"] += n
                log.info("  -> %d waiting times inserted", n)
            elif kind == "positions":
                n = ingest_raw_positions(conn, path)
                conn.commit()
                total["raw_positions"] += n
                log.info("  -> %d positions inserted (geom resolved from "
                         "static.stib_line_shape + static.stib_stop)", n)
            else:
                total["skipped"] += 1
                log.warning("  -> unrecognized, skipping")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    log.info("Done. static=%d, assignments=%d, raw_positions=%d, "
             "waiting_times=%d, skipped=%d",
             total["static"], total["assignments"], total["raw_positions"],
             total["waiting_times"], total["skipped"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
