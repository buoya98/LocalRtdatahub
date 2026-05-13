"""STIB live vehicle-position ingestor — Open Data Portal.

Mirrors source/src/etl/ingestion/stib/ingestor.py: pulls
`/rt/VehiclePositions` from the STIB Azure APIM gateway and feeds the
records into rt_v2.stib_vehicle_position via stib_transform.

The ODP response carries no GPS coords — geom is reconstructed in SQL
from static.stib_line_shape + static.stib_stop (see
src/etl/pipeline/transform/stib_transform.py).

Two modes:
    --once         poll once, insert, exit (good for cron).
    --interval N   loop forever, polling every N seconds.

Optional `--output PATH` mirrors raw responses to `.jsonl.gz` so the bench
ingestor can replay them later (parity with raw .jsonl.gz dumps).
"""
from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import httpx

# Make `from src.…` imports work whether invoked via `python -m …` or directly.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import psycopg2  # noqa: E402

from src.env_manager import (  # noqa: E402
    DATABASE_URL,
    INGEST_BATCH_SIZE,
    STIB_ODP_BASE_URL,
    STIB_ODP_KEY,
)
from src.etl.pipeline.transform.stib_transform import insert_positions  # noqa: E402

log = logging.getLogger("ingest_stib.rt")

DEFAULT_URL = f"{STIB_ODP_BASE_URL}/rt/VehiclePositions"
PAGE_LIMIT = 1000


# ---------------------------------------------------------------------------
# ODP response parsing
# ---------------------------------------------------------------------------

def _normalise(lineid: str, pos: dict[str, Any], fetched_at: str) -> dict[str, Any] | None:
    direction = pos.get("directionId") or pos.get("direction")
    pointid = pos.get("pointId") or pos.get("pointid") or pos.get("point_id")
    distance = pos.get("distanceFromPoint", pos.get("distance_from_point"))
    if direction is None or pointid is None:
        return None
    try:
        direction = int(direction)
    except (TypeError, ValueError):
        return None
    try:
        distance = float(distance) if distance is not None else None
    except (TypeError, ValueError):
        distance = None
    return {
        "lineid": str(lineid),
        "direction": direction,
        "pointid": str(pointid),
        "distance_from_point": distance,
        "fetched_at": fetched_at,
    }


def _extract(results: list[Any], fetched_at: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in results:
        if not isinstance(row, dict):
            continue
        lineid = row.get("lineid") or row.get("lineId")
        if lineid is None:
            continue
        raw = row.get("vehiclepositions") or row.get("vehicle_positions")
        if isinstance(raw, str):
            try:
                inner = json.loads(raw)
            except (ValueError, TypeError):
                continue
        elif isinstance(raw, list):
            inner = raw
        else:
            continue
        if not isinstance(inner, list):
            continue
        for pos in inner:
            if not isinstance(pos, dict):
                continue
            n = _normalise(str(lineid), pos, fetched_at)
            if n is not None:
                out.append(n)
    return out


def fetch_one(client: httpx.Client, url: str, key: str) -> list[dict[str, Any]]:
    """Page through the ODP endpoint until exhausted; return all vehicles."""
    headers = {"bmc-partner-key": key, "Accept": "application/json"}
    fetched_at = datetime.now(timezone.utc).isoformat()
    all_vehicles: list[dict[str, Any]] = []
    offset = 0
    while True:
        resp = client.get(url, params={"limit": PAGE_LIMIT, "offset": offset},
                          headers=headers)
        if resp.status_code in (401, 403):
            log.error("Authentication failed (HTTP %d). Check STIB_ODP_KEY.",
                      resp.status_code)
            raise SystemExit(1)
        resp.raise_for_status()
        try:
            data = resp.json()
        except ValueError:
            log.error("non-JSON response at offset=%d: %s", offset, resp.text[:200])
            break
        results = []
        if isinstance(data, dict):
            results = data.get("results") or data.get("data") or data.get("items") or []
        elif isinstance(data, list):
            results = data
        if not results:
            break
        all_vehicles.extend(_extract(results, fetched_at))
        if len(results) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT
        if offset >= PAGE_LIMIT * 100:
            log.warning("page limit reached at offset=%d", offset)
            break
    return all_vehicles


# ---------------------------------------------------------------------------
# Optional raw mirror (so live polls can be replayed via the bench path)
# ---------------------------------------------------------------------------
# Raw `.jsonl.gz` dumps are precious — we never overwrite or append to an
# existing file. Each process invocation picks a fresh, timestamped
# filename, and we refuse to write anywhere under `source/` (which holds
# the user's reference dumps).

def _safe_output_path(base: Path, started_at: datetime) -> Path:
    """Build a unique output path that cannot collide with an existing raw dump.

    `base` may be a directory (we create one timestamped file inside) or a
    file path (we add a `.<timestamp>` suffix to guarantee uniqueness).
    Either way, if the resolved path already exists we bail out — the user
    keeps their raw data intact.

    Also forbids writing under `data/bench_algo_data/`, which holds the
    raw input dumps. The mirror should land in a fresh dir like
    `data/raw/stib/`.
    """
    repo_root = Path(_REPO_ROOT).resolve()
    target = base
    stamp = started_at.strftime("%Y-%m-%dT%H-%M-%SZ")
    if target.is_dir() or (not target.suffix and not target.exists()):
        target.mkdir(parents=True, exist_ok=True)
        target = target / f"vehicle_positions_{stamp}.jsonl.gz"
    else:
        if target.exists():
            target = target.with_name(f"{target.stem}.{stamp}{''.join(target.suffixes)}")
    target = target.resolve()

    forbidden = (repo_root / "data" / "bench_algo_data").resolve()
    try:
        target.relative_to(forbidden)
    except ValueError:
        pass
    else:
        raise SystemExit(
            f"refusing to write under data/bench_algo_data/ "
            f"(holds raw .jsonl.gz dumps): {target}"
        )

    if target.exists():
        raise SystemExit(
            f"refusing to overwrite existing dump: {target} "
            f"(pick a different --output or remove the file first)"
        )
    return target


def _append_jsonl_gz(path: Path, records: Iterator[dict[str, Any]]) -> int:
    """Append-only writer for our newly-created mirror file. The path is
    guaranteed unique by `_safe_output_path` — appending here is for the
    second-and-later polls within the SAME process, never an existing
    on-disk file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with gzip.open(path, "ab") as fh:
        for rec in records:
            fh.write((json.dumps(rec, ensure_ascii=False) + "\n").encode("utf-8"))
            n += 1
    return n


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(*, url: str, key: str, interval: float | None,
        output: Path | None, batch_size: int = INGEST_BATCH_SIZE) -> int:
    """Poll the ODP and insert into rt_v2.stib_vehicle_position. If
    `interval` is None or 0, polls once and returns. Returns inserted count
    (sum across iterations)."""
    if not key:
        log.error("STIB_ODP_KEY environment variable is not set. "
                  "Add it to .env (STIB_ODP_KEY=...).")
        return 0

    total = 0
    started_at = datetime.now(timezone.utc)
    mirror_path = _safe_output_path(output, started_at) if output else None
    if mirror_path:
        log.info("mirroring raw vehicles → %s (append-only, no overwrite)",
                 mirror_path)

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            log.info("polling %s every %ss", url, interval or "once")
            while True:
                try:
                    vehicles = fetch_one(client, url, key)
                except httpx.HTTPError as exc:
                    log.error("HTTP error: %s", exc)
                    if interval:
                        time.sleep(interval)
                        continue
                    return total

                # Optional jsonl.gz mirror — same shape as raw bench dumps,
                # written to a freshly-created file (never overwrites).
                if mirror_path and vehicles:
                    n = _append_jsonl_gz(mirror_path, iter([
                        {"poll_ts": v["fetched_at"],
                         "lineid": v["lineid"],
                         "directionId": str(v["direction"]),
                         "pointId": v["pointid"],
                         "distanceFromPoint": v["distance_from_point"]}
                        for v in vehicles
                    ]))
                    log.info("mirrored %d records → %s", n, mirror_path)

                inserted = insert_positions(conn, vehicles, batch_size=batch_size)
                conn.commit()
                total += inserted
                log.info("polled vehicles=%d inserted=%d (total=%d)",
                         len(vehicles), inserted, total)

                if not interval:
                    return total
                time.sleep(interval)
    except KeyboardInterrupt:
        log.info("interrupted")
    finally:
        conn.close()
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--key", default=STIB_ODP_KEY)
    parser.add_argument("--interval", type=float, default=None,
                        help="Poll interval in seconds (omit for one-shot).")
    parser.add_argument("--once", action="store_true",
                        help="Equivalent to --interval omitted.")
    parser.add_argument("--output", type=Path, default=None,
                        help="Mirror raw vehicles to this .jsonl.gz file (optional).")
    parser.add_argument("--batch-size", type=int, default=INGEST_BATCH_SIZE)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    interval = None if args.once else args.interval
    n = run(url=args.url, key=args.key, interval=interval,
            output=args.output, batch_size=args.batch_size)
    log.info("done. inserted=%d", n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
