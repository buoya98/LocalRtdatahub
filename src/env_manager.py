"""Local-only environment manager.

Mirrors the role of source/src/env_manager.py but stripped down to the
variables LocalRtdatahub actually needs (DB connection + STIB ODP key).

Both upstream-style dotted keys (`MOBILITYDB.DB`) and underscore-style
(`MOBILITYDB_DB`) are accepted, so an upstream `.env` can be reused as-is.
"""
from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


def _get(key: str, default: str | None = None) -> str | None:
    for candidate in (key, key.replace(".", "_")):
        value = os.getenv(candidate)
        if value not in (None, ""):
            return value
    return default


# ── DB connection ──────────────────────────────────────────────────
MOBILITYDB_DB = _get("MOBILITYDB.DB", "rtdatahub_local")
MOBILITYDB_USER = _get("MOBILITYDB.USER", "rtdatahub")
MOBILITYDB_PASSWORD = _get("MOBILITYDB.PASSWORD", "rtdatahub")
MOBILITYDB_HOST = _get("MOBILITYDB.HOST", "127.0.0.1")
MOBILITYDB_PORT = int(_get("MOBILITYDB.PORT", "5432") or "5432")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql://{MOBILITYDB_USER}:{MOBILITYDB_PASSWORD}@"
    f"{MOBILITYDB_HOST}:{MOBILITYDB_PORT}/{MOBILITYDB_DB}",
)

# ── STIB Open Data Portal (used by src/etl/ingestion/stib/) ────────
STIB_ODP_KEY = _get("STIB_ODP.KEY", "") or ""
STIB_ODP_BASE_URL = _get(
    "STIB_ODP.BASE_URL",
    "https://api-management-opendata-production.azure-api.net/api/datasets/stibmivb",
)
STIB_ODP_GTFS_URL = _get(
    "STIB_ODP.GTFS_URL",
    "https://api-management-opendata-production.azure-api.net"
    "/api/gtfs/feed/stibmivb/static",
)

# ── Ingestion tuning ───────────────────────────────────────────────
INGEST_BATCH_SIZE = int(_get("INGEST_BATCH_SIZE", "5000") or "5000")
