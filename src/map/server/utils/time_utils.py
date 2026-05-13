"""Time-filter parsing — mirrors source/src/map/server/utils/time_utils.py."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs


def parse_hours(query: str, default_hours: float) -> float:
    hours = default_hours
    try:
        parsed = parse_qs(query)
        if "hours" in parsed and parsed["hours"]:
            hours = float(parsed["hours"][0])
    except (TypeError, ValueError):
        pass
    return max(0.05, min(hours, 24.0))


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_time_filter(query: str, default_hours: float) -> dict[str, Any]:
    hours = parse_hours(query, default_hours)
    parsed = parse_qs(query)
    start = parse_dt((parsed.get("start") or [None])[0])
    end = parse_dt((parsed.get("end") or [None])[0])
    if start and end and end > start:
        return {"mode": "range", "hours": hours, "start": start, "end": end}
    return {"mode": "relative", "hours": hours, "start": None, "end": None}


def parse_lines_filter(raw: str | None) -> list[str] | None:
    """Split `71,92` into ['71','92']. Empty / missing → None (no filter)."""
    if not raw:
        return None
    items = [s.strip() for s in raw.split(",") if s.strip()]
    return items or None
