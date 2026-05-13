"""Shared helpers used by the STIB ingestors.

Kept private (`_common.py`) because upstream's ingestors import from
`src.env_manager` and a couple of local utilities — locally we keep the
helpers next to the ingestors instead of building a separate http_client
module.
"""
from __future__ import annotations

import logging
import math
import os
import sys
from typing import Any

import httpx
import psycopg2

# Make `from src.env_manager import …` work whether run via `python -m …`
# or directly.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from src.env_manager import (  # noqa: E402
    DATABASE_URL,
    STIB_ODP_BASE_URL,
    STIB_ODP_GTFS_URL,
    STIB_ODP_KEY,
)

log = logging.getLogger("ingest_stib")

PAGE_LIMIT = 1000
ROUTE_TYPE_MODE = {"0": "tram", "1": "metro", "2": "rail", "3": "bus", "7": "funicular"}

STOPS_URL         = f"{STIB_ODP_BASE_URL}/static/stopDetails"
STOPS_BY_LINE_URL = f"{STIB_ODP_BASE_URL}/static/stopsByLine"
GTFS_URL          = STIB_ODP_GTFS_URL


def require_key() -> str:
    if not STIB_ODP_KEY:
        log.error("STIB_ODP_KEY environment variable is not set. "
                  "Add it to .env (STIB_ODP_KEY=...).")
        sys.exit(1)
    return STIB_ODP_KEY


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def haversine_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    r = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def normalise_color(raw: str | None) -> str | None:
    s = (raw or "").strip().lstrip("#")
    if len(s) != 6:
        return None
    try:
        int(s, 16)
        return "#" + s.upper()
    except ValueError:
        return None


def fetch_paginated(url: str, key: str) -> list[dict[str, Any]]:
    headers = {"bmc-partner-key": key, "Accept": "application/json"}
    out: list[dict[str, Any]] = []
    offset = 0
    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        while True:
            resp = client.get(url, params={"limit": PAGE_LIMIT, "offset": offset}, headers=headers)
            if resp.status_code in (401, 403):
                log.error("Authentication failed (HTTP %d). Check STIB_ODP_KEY.", resp.status_code)
                sys.exit(1)
            resp.raise_for_status()
            data = resp.json()
            items = []
            if isinstance(data, dict):
                items = data.get("results") or data.get("data") or data.get("items") or []
            elif isinstance(data, list):
                items = data
            if not items:
                break
            out.extend(i for i in items if isinstance(i, dict))
            if len(items) < PAGE_LIMIT:
                break
            offset += PAGE_LIMIT
            if offset > PAGE_LIMIT * 100:
                break
    return out
