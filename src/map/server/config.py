"""Server configuration dataclass — mirrors source/src/map/server/config.py."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ServerConfig:
    dbname: str
    user: str
    password: str
    host: str
    port: int
    default_hours: float
    max_db_connections: int
    db_acquire_timeout_s: float
    db_statement_timeout_ms: int
    tile_cache_ttl_s: float
    tile_cache_max_entries: int
    counts_cache_ttl_s: float

    @property
    def conninfo(self) -> str:
        return (
            f"dbname={self.dbname} user={self.user} password={self.password} "
            f"host={self.host} port={self.port}"
        )

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.dbname}"
        )
