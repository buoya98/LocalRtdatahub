"""Stops-by-line ingestor — mirrors
source/src/etl/ingestion/stib/stops_by_line_ingestor.py.

Fetches /static/stopsByLine and populates:

  * static.stib_line_stops      (lineid, direction, sequence_idx, pointid, destination)
  * static.stib_line_terminus   (lineid, terminus_pointid, direction, destination)

The terminus table is what /api/stib/lines uses to label directions.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from ._common import (
    STOPS_BY_LINE_URL,
    fetch_paginated,
    get_conn,
    require_key,
)

log = logging.getLogger("ingest_stib.lines")


def _parse_dest(raw: Any) -> str | None:
    if isinstance(raw, str):
        try:
            obj = json.loads(raw)
            return obj.get("fr") or obj.get("nl") if isinstance(obj, dict) else raw
        except (ValueError, TypeError):
            return raw
    if isinstance(raw, dict):
        return raw.get("fr") or raw.get("nl")
    return None


def _parse_points(raw: Any) -> list[tuple[int, str]]:
    if isinstance(raw, str):
        try:
            arr = json.loads(raw)
        except (ValueError, TypeError):
            return []
    elif isinstance(raw, list):
        arr = raw
    else:
        return []
    out: list[tuple[int, str]] = []
    for i, p in enumerate(arr, start=1):
        if not isinstance(p, dict):
            continue
        pid = p.get("id") or p.get("pointId") or p.get("pointid")
        if pid is None:
            continue
        try:
            order = int(p.get("order", i))
        except (TypeError, ValueError):
            order = i
        out.append((order, str(pid)))
    out.sort(key=lambda t: t[0])
    return out


def run() -> dict[str, int]:
    log.info("[lines] fetching %s", STOPS_BY_LINE_URL)
    items = fetch_paginated(STOPS_BY_LINE_URL, require_key())
    log.info("[lines] got %d (line, direction) entries", len(items))

    rows_stops: list[tuple[str, str, int, str, str | None]] = []
    rows_term: list[tuple[str, str, str, str | None]] = []
    for it in items:
        lineid = it.get("lineid") or it.get("lineId")
        direction = it.get("direction")
        if lineid is None or direction is None:
            continue
        lineid, direction = str(lineid), str(direction)
        dest = _parse_dest(it.get("destination"))
        points = _parse_points(it.get("points"))
        if not points:
            continue
        for seq, pid in points:
            rows_stops.append((lineid, direction, seq, pid, dest))
        rows_term.append((lineid, points[-1][1], direction, dest))

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE static.stib_line_stops;")
            cur.execute("TRUNCATE static.stib_line_terminus;")
            cur.executemany(
                "INSERT INTO static.stib_line_stops "
                "(lineid, direction, sequence_idx, pointid, destination) "
                "VALUES (%s,%s,%s,%s,%s);",
                rows_stops,
            )
            cur.executemany(
                """
                INSERT INTO static.stib_line_terminus
                    (lineid, terminus_pointid, direction, destination)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (lineid, terminus_pointid) DO UPDATE SET
                    direction = EXCLUDED.direction,
                    destination = EXCLUDED.destination,
                    updated_at = NOW();
                """,
                rows_term,
            )
        conn.commit()
    finally:
        conn.close()
    log.info("[lines] inserted stops=%d terminus=%d", len(rows_stops), len(rows_term))
    return {"stops": len(rows_stops), "terminus": len(rows_term)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
