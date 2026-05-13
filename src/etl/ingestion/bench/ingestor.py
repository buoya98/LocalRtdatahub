"""Bench ingestor — raw `.jsonl.gz` / `.csv.gz` dumps → local DB.

Auto-detects file type from filename:

  * assignments.csv.gz             → rt.stib_vehicle_position
        Columns: position_id, fetched_at_utc, timestamp_s, lineId,
                 direction, pointId, distance, latitude, longitude, obs_id
        These rows already carry resolved lat/lon, so we insert them with
        ST_MakePoint(lon, lat). Observed pointids are upserted into
        static.stib_stop as a side-effect (best-effort centroid).

  * positions/*.jsonl.gz           → rt.stib_vehicle_position
        Raw STIB feed: {poll_ts, lineid, directionId, pointId,
                        distanceFromPoint} — no GPS. Geom is reconstructed
        in SQL from static.stib_line_shape + static.stib_stop via
        src/etl/pipeline/transform/stib_transform.py (same path as the
        live API ingestor — feed-format parity is what makes both modes
        produce identical rows).

  * waiting_times/*.jsonl.gz       → transport_local.stib_waiting_time
        Columns: poll_ts, pointid, lineid, expected, destination, message

Raw inputs are read-only — every dump is opened with gzip.open(..., "rb")
and never modified, renamed, or moved. Re-running the ingestor on the
same files only re-tries inserts (ON CONFLICT (position_id) DO NOTHING).

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
    if name.startswith("wt") and name.endswith(".jsonl.gz"):
        return "waiting_times"
    if "waiting_times" in parts and name.endswith(".jsonl.gz"):
        return "waiting_times"
    if "positions" in parts and name.endswith(".jsonl.gz"):
        return "positions"
    return "unknown"


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
            "  python -m src.etl.ingestion.bench.ingestor data/bench_algo_data/positions/2026-05-01.jsonl.gz\n"
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

    total = {"assignments": 0, "raw_positions": 0, "waiting_times": 0, "skipped": 0}

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    try:
        files = list(_walk(args.paths))
        if not files:
            log.error("no .jsonl.gz / .csv.gz files found in %s", args.paths)
            return 1

        for path in sorted(files):
            kind = _classify(path)
            log.info("%s -> %s", path, kind)
            if kind == "assignments":
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

    log.info("Done. assignments=%d, raw_positions=%d, waiting_times=%d, skipped=%d",
             total["assignments"], total["raw_positions"],
             total["waiting_times"], total["skipped"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
