"""Waiting-time lookup — local-only addition (no upstream equivalent).

Reads transport_local.stib_waiting_time which is filled by
src/etl/ingestion/bench/ingestor.py from `wt.jsonl.gz` dumps.
"""
from __future__ import annotations

from typing import Any

from ..context import MapAppContext


def fetch_waiting_times(ctx: MapAppContext, pointid: str) -> list[dict[str, Any]]:
    with ctx.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT poll_ts, lineid, expected, destination, message
                  FROM transport_local.stib_waiting_time
                 WHERE pointid = %s
                 ORDER BY poll_ts DESC, expected ASC
                 LIMIT 200;
                """,
                (pointid,),
            )
            rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for poll_ts, lineid, expected, destination, message in rows:
        out.append({
            "poll_ts": poll_ts.isoformat() if poll_ts else None,
            "lineid": lineid,
            "expected": expected.isoformat() if expected else None,
            "destination": destination,
            "message": message,
        })
    return out
