"""Stops catalog ingestor — mirrors
source/src/etl/ingestion/stib/stop_details_ingestor.py.

Fetches /static/stopDetails from the STIB ODP and upserts each stop into
static.stib_stop with bilingual name + GPS coords.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Iterator

from ._common import (
    STOPS_URL,
    fetch_paginated,
    get_conn,
    log as _common_log,  # noqa: F401
    require_key,
)

log = logging.getLogger("ingest_stib.stops")


def _iter_rows(items: list[dict[str, Any]]) -> Iterator[tuple]:
    for it in items:
        pid = it.get("id") or it.get("pointId") or it.get("pointid")
        if pid is None:
            continue
        gps_raw = it.get("gpscoordinates") or it.get("gpsCoordinates")
        if isinstance(gps_raw, str):
            try:
                gps = json.loads(gps_raw)
            except (ValueError, TypeError):
                continue
        elif isinstance(gps_raw, dict):
            gps = gps_raw
        else:
            continue
        try:
            lat = float(gps.get("latitude") or gps.get("lat"))
            lon = float(gps.get("longitude") or gps.get("lon") or gps.get("lng"))
        except (TypeError, ValueError):
            continue

        name_raw = it.get("name")
        fr = nl = None
        if isinstance(name_raw, str):
            try:
                obj = json.loads(name_raw)
                if isinstance(obj, dict):
                    fr, nl = obj.get("fr"), obj.get("nl")
            except (ValueError, TypeError):
                fr = nl = name_raw
        elif isinstance(name_raw, dict):
            fr, nl = name_raw.get("fr"), name_raw.get("nl")

        yield (str(pid), fr, nl, lat, lon)


def run() -> int:
    log.info("[stops] fetching %s", STOPS_URL)
    items = fetch_paginated(STOPS_URL, require_key())
    log.info("[stops] got %d documents", len(items))

    rows = list(_iter_rows(items))
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for pid, fr, nl, lat, lon in rows:
                cur.execute(
                    """
                    INSERT INTO static.stib_stop (pointid, stop_name_fr, stop_name_nl, geom, updated_at)
                    VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), NOW())
                    ON CONFLICT (pointid) DO UPDATE SET
                        stop_name_fr = EXCLUDED.stop_name_fr,
                        stop_name_nl = EXCLUDED.stop_name_nl,
                        geom         = EXCLUDED.geom,
                        updated_at   = NOW();
                    """,
                    (pid, fr, nl, lon, lat),
                )
        conn.commit()
    finally:
        conn.close()
    log.info("[stops] upserted %d stops", len(rows))
    return len(rows)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
