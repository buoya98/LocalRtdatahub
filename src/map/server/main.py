"""Entry point — mirrors source/src/map/server/main.py.

Parses CLI flags, builds a ServerConfig from src/env_manager.py, opens a
psycopg2 connection pool, builds the Flask app and runs Werkzeug.

Run with:
    python -m src.map.server.main --listen-port 8090
"""
from __future__ import annotations

import argparse
import os
import sys
import webbrowser

from werkzeug.serving import make_server

# Make `src.…` imports work when run as a script.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from src.env_manager import (  # noqa: E402
    MOBILITYDB_DB,
    MOBILITYDB_HOST,
    MOBILITYDB_PASSWORD,
    MOBILITYDB_PORT,
    MOBILITYDB_USER,
)
from src.map.server.app import create_app  # noqa: E402
from src.map.server.config import ServerConfig  # noqa: E402
from src.map.server.context import Pool  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="LocalRtdatahub — local-only Brussels Transport map server."
    )
    parser.add_argument("--listen-host", default=os.environ.get("LISTEN_HOST", "127.0.0.1"))
    parser.add_argument("--listen-port", type=int,
                        default=int(os.environ.get("LISTEN_PORT", "8090")))
    parser.add_argument("--hours", type=float,
                        default=float(os.environ.get("DEFAULT_HOURS", "1.0")),
                        help="Default lookback window in hours.")
    parser.add_argument("--open-browser", action="store_true")
    parser.add_argument("--max-db-connections", type=int, default=10)
    parser.add_argument("--db-acquire-timeout-s", type=float, default=300.0)
    parser.add_argument("--db-statement-timeout-ms", type=int, default=120_000)
    parser.add_argument("--tile-cache-ttl-s", type=float, default=160.0)
    parser.add_argument("--tile-cache-max-entries", type=int, default=4_000)
    parser.add_argument("--counts-cache-ttl-s", type=float, default=4.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = ServerConfig(
        dbname=MOBILITYDB_DB,
        user=MOBILITYDB_USER,
        password=MOBILITYDB_PASSWORD,
        host=MOBILITYDB_HOST,
        port=MOBILITYDB_PORT,
        default_hours=max(0.05, min(args.hours, 24.0)),
        max_db_connections=max(1, args.max_db_connections),
        db_acquire_timeout_s=max(0.05, args.db_acquire_timeout_s),
        db_statement_timeout_ms=max(1000, args.db_statement_timeout_ms),
        tile_cache_ttl_s=max(1.0, args.tile_cache_ttl_s),
        tile_cache_max_entries=max(100, args.tile_cache_max_entries),
        counts_cache_ttl_s=max(0.5, args.counts_cache_ttl_s),
    )

    pool = Pool(dsn=cfg.dsn, min_size=1, max_size=cfg.max_db_connections)
    app = create_app(cfg, pool)

    url = f"http://{args.listen_host}:{args.listen_port}/?hours={cfg.default_hours}"
    print(f"[LocalRtdatahub] serving on {url}")

    if args.open_browser:
        webbrowser.open(url)

    server = None
    try:
        server = make_server(args.listen_host, args.listen_port, app, threaded=True)
        server.serve_forever()
    except KeyboardInterrupt:
        print("[LocalRtdatahub] stopped")
    finally:
        if server:
            server.server_close()
        pool.close()


if __name__ == "__main__":
    main()
