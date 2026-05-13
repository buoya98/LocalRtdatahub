"""De Lijn service — empty stubs.

LocalRtdatahub doesn't ingest De Lijn data, so every endpoint matches the
upstream signature but returns an empty payload. Stubs include any field
the upstream frontend reads unconditionally (e.g. trajectories'
`window_*_ms`), so the player can initialize even without data.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from ..context import MapAppContext


def fetch_delijn_lines_catalog(ctx: MapAppContext) -> dict[str, Any]:
    return {"ok": True, "count": 0, "lines": []}


def fetch_delijn_live_positions(
    ctx: MapAppContext,
    lines: list[str] | None,
    time_filter: dict[str, Any],
    direction: int | None,
) -> dict[str, Any]:
    return {"ok": True, "count": 0, "type": "FeatureCollection", "features": []}


def fetch_delijn_trajectories(
    ctx: MapAppContext,
    lines: list[str] | None,
    time_filter: dict[str, Any],
    sample: str,
    direction: int | None,
) -> dict[str, Any]:
    # Frontend reads `window_start_ms` / `window_end_ms` unconditionally
    # to initialize the player. Provide a sensible empty window so the
    # UI doesn't crash with NaN/undefined arithmetic.
    end = (time_filter or {}).get("end") or datetime.now(timezone.utc)
    start = (time_filter or {}).get("start") or (
        end - timedelta(hours=float((time_filter or {}).get("hours") or 1.0))
    )
    return {
        "ok": True,
        "count": 0,
        "trips": [],
        "window_start_ms": int(start.timestamp() * 1000),
        "window_end_ms":   int(end.timestamp()   * 1000),
    }


def fetch_delijn_shape(ctx: MapAppContext, route_id: str, direction_id: str) -> dict[str, Any]:
    return {"ok": True, "route_id": route_id, "direction_id": direction_id,
            "shape_id": None, "geometry": None}


def fetch_delijn_stop_details(ctx: MapAppContext, stop_id: str) -> dict[str, Any]:
    return {"ok": False, "error": "not found", "stop_id": stop_id}


def fetch_delijn_alerts(
    ctx: MapAppContext,
    lines: list[str] | None,
    time_filter: dict[str, Any],
    alert_type: str | None,
    active_only: bool,
) -> dict[str, Any]:
    return {"ok": True, "count": 0, "alerts": []}
