"""Application context — mirrors source/src/map/server/context.py.

Wraps a psycopg2 ThreadedConnectionPool with a `connection()` context
manager so service code can use the upstream `with ctx.pool.connection()
as conn:` pattern unchanged.
"""
from __future__ import annotations

import threading
from collections import OrderedDict
from contextlib import contextmanager
from typing import Iterator

from psycopg2.pool import ThreadedConnectionPool

from .config import ServerConfig


class Pool:
    """Thin shim that mimics the subset of psycopg_pool.ConnectionPool used
    by the upstream server (just `connection()` and `close()`)."""

    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 8):
        self._pool = ThreadedConnectionPool(min_size, max_size, dsn=dsn)

    @contextmanager
    def connection(self, timeout: float | None = None) -> Iterator:
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    def close(self) -> None:
        self._pool.closeall()


class MapAppContext:
    def __init__(self, cfg: ServerConfig, pool: Pool):
        self.cfg = cfg
        self.pool = pool
        self.tile_cache_lock = threading.Lock()
        self.tile_cache: OrderedDict[str, tuple[float, bytes]] = OrderedDict()
        self.counts_cache_lock = threading.Lock()
        self.counts_cache: OrderedDict[str, tuple[float, dict[str, int]]] = OrderedDict()
