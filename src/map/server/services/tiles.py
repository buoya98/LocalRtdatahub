"""MVT tile generator — minimal subset of source/src/map/server/services/tiles.py.

LocalRtdatahub only serves stib_lines and stib_stops; line filtering
is the only optional parameter. The upstream tile cache (TTL'd LRU) is
preserved verbatim.
"""
from __future__ import annotations

from time import monotonic
from typing import Any

from ..context import MapAppContext
from ..queries.layer_sql import LAYER_SQL


def _cache_key(layer: str, z: int, x: int, y: int, extra: tuple) -> str:
    return f"{layer}|{z}|{x}|{y}|{extra!r}"


def _get_cached(ctx: MapAppContext, key: str) -> bytes | None:
    now = monotonic()
    with ctx.tile_cache_lock:
        item = ctx.tile_cache.get(key)
        if item is None:
            return None
        ts, payload = item
        if now - ts > ctx.cfg.tile_cache_ttl_s:
            ctx.tile_cache.pop(key, None)
            return None
        ctx.tile_cache.move_to_end(key)
        return payload


def _set_cached(ctx: MapAppContext, key: str, payload: bytes) -> None:
    now = monotonic()
    with ctx.tile_cache_lock:
        ctx.tile_cache[key] = (now, payload)
        ctx.tile_cache.move_to_end(key)
        while len(ctx.tile_cache) > ctx.cfg.tile_cache_max_entries:
            ctx.tile_cache.popitem(last=False)


def fetch_tile(
    ctx: MapAppContext,
    layer: str,
    z: int,
    x: int,
    y: int,
    extra_params: tuple = (),
) -> bytes:
    if layer not in LAYER_SQL:
        return b""

    key = _cache_key(layer, z, x, y, extra_params)
    cached = _get_cached(ctx, key)
    if cached is not None:
        return cached

    sql = LAYER_SQL[layer]
    params = (z, x, y, *extra_params)
    with ctx.pool.connection(timeout=ctx.cfg.db_acquire_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT set_config('statement_timeout', %s, true)",
                (f"{ctx.cfg.db_statement_timeout_ms}ms",),
            )
            cur.execute(sql, params)
            row = cur.fetchone()
            payload = bytes(row[0]) if (row and row[0] is not None) else b""
    _set_cached(ctx, key, payload)
    return payload
