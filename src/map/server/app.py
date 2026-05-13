"""Flask app factory — mirrors source/src/map/server/app.py.

Routes delegate to per-domain services in `services/`; this module only
parses the HTTP layer (query string, path params, error → status code).
"""
from __future__ import annotations

import os

from flask import Flask, jsonify, make_response, render_template, request

from .config import ServerConfig
from .context import MapAppContext, Pool
from .queries.layer_sql import LAYER_SQL, LINE_FILTERED_LAYERS
from .services.counts import fetch_counts, fetch_health
from .services.search import fetch_search_results
from .services.stib_service import (
    fetch_stib_alerts,
    fetch_stib_lines_catalog,
    fetch_stib_live_positions,
    fetch_stib_shape,
    fetch_stib_stop_details,
    fetch_stib_trajectories,
)
from .services.tiles import fetch_tile
from .services.waiting_times import fetch_waiting_times
from .utils.time_utils import parse_hours, parse_lines_filter, parse_time_filter


def _tile_extra_params(layer: str, raw_lines: str | None) -> tuple:
    if layer in LINE_FILTERED_LAYERS:
        lines = parse_lines_filter(raw_lines)
        return (lines, lines)
    return ()


def _err_status(exc: Exception) -> int:
    return 503 if ("PoolTimeout" in type(exc).__name__ or "Server busy" in str(exc)) else 500


def _err(exc: Exception):
    return make_response(jsonify({"ok": False, "error": str(exc)}), _err_status(exc))


def create_app(cfg: ServerConfig, pool: Pool) -> Flask:
    template_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "templates")
    )
    app = Flask(__name__, template_folder=template_dir)
    ctx = MapAppContext(cfg, pool)

    # ── HTTP-level wiring (gzip, cache, etc.) ─────────────────────────
    @app.after_request
    def _no_cache_html(resp):
        if request.path in ("/", "/index.html") and resp.status_code == 200:
            resp.headers["Cache-Control"] = "no-store, must-revalidate"
            resp.headers["Pragma"] = "no-cache"
            resp.headers["Expires"] = "0"
        return resp

    @app.after_request
    def _cache_shapes(resp):
        if request.path.startswith("/api/stib/shape/"):
            if resp.status_code == 200:
                resp.headers["Cache-Control"] = "public, max-age=86400"
        return resp

    # ── Index ─────────────────────────────────────────────────────────
    @app.route("/")
    @app.route("/index.html")
    def index():
        hours = parse_hours(request.query_string.decode("utf-8"), ctx.cfg.default_hours)
        return render_template("index.html", hours=hours)

    # ── Health & debug ────────────────────────────────────────────────
    @app.route("/health")
    @app.route("/api/health")
    def health():
        try:
            return jsonify(fetch_health(ctx))
        except Exception as exc:
            return _err(exc)

    @app.route("/debug/counts")
    def debug_counts():
        try:
            return jsonify({"ok": True, "counts": fetch_counts(ctx)})
        except Exception as exc:
            return _err(exc)

    # ── Search ────────────────────────────────────────────────────────
    @app.route("/api/search")
    def search():
        q = request.args.get("q", "").strip()
        if len(q) < 2:
            return jsonify({"ok": True, "results": []})
        try:
            return jsonify({"ok": True, "results": fetch_search_results(ctx, q)})
        except Exception as exc:
            return _err(exc)

    # ── STIB API ──────────────────────────────────────────────────────
    @app.route("/api/stib/lines")
    def stib_lines_catalog():
        try:
            return jsonify(fetch_stib_lines_catalog(ctx))
        except Exception as exc:
            return _err(exc)

    @app.route("/api/stib/live-positions")
    def stib_live_positions():
        try:
            query = request.query_string.decode("utf-8")
            tf = parse_time_filter(query, ctx.cfg.default_hours)
            lines = parse_lines_filter(request.args.get("lines"))
            dir_raw = (request.args.get("direction") or "").strip()
            direction = int(dir_raw) if dir_raw else None
            return jsonify(fetch_stib_live_positions(ctx, lines, tf, direction))
        except Exception as exc:
            return _err(exc)

    @app.route("/api/stib/trajectories")
    def stib_trajectories():
        try:
            query = request.query_string.decode("utf-8")
            tf = parse_time_filter(query, ctx.cfg.default_hours)
            lines = parse_lines_filter(request.args.get("lines"))
            sample = request.args.get("sample", "5 seconds")
            dir_raw = (request.args.get("direction") or "").strip()
            direction = int(dir_raw) if dir_raw else None
            return jsonify(fetch_stib_trajectories(ctx, lines, tf, sample, direction))
        except Exception as exc:
            return _err(exc)

    @app.route("/api/stib/shape/<lineid>/<direction>")
    def stib_shape(lineid: str, direction: str):
        try:
            return jsonify(fetch_stib_shape(ctx, lineid, direction))
        except Exception as exc:
            return _err(exc)

    @app.route("/api/stib/stop/<pointid>")
    def stib_stop_details(pointid: str):
        try:
            return jsonify(fetch_stib_stop_details(ctx, pointid))
        except Exception as exc:
            return _err(exc)

    @app.route("/api/stib/alerts")
    def stib_alerts():
        try:
            query = request.query_string.decode("utf-8")
            tf = parse_time_filter(query, ctx.cfg.default_hours)
            lines = parse_lines_filter(request.args.get("lines"))
            alert_type = request.args.get("type") or None
            active_only = request.args.get("active", "").lower() in ("1", "true")
            return jsonify(fetch_stib_alerts(ctx, lines, tf, alert_type, active_only))
        except Exception as exc:
            return _err(exc)

    # ── Local-only: waiting times ─────────────────────────────────────
    @app.route("/api/waiting-times")
    def waiting_times():
        pointid = request.args.get("pointid")
        if not pointid:
            return jsonify({"ok": False, "error": "pointid is required"}), 400
        try:
            return jsonify(fetch_waiting_times(ctx, pointid))
        except Exception as exc:
            return _err(exc)

    # ── Tiles ─────────────────────────────────────────────────────────
    @app.route("/tiles/<layer>/<int:z>/<int:x>/<int:y>.pbf")
    def tiles(layer: str, z: int, x: int, y: int):
        if layer not in LAYER_SQL:
            # Layers we haven't ingested locally: silent 204 so the upstream
            # frontend can keep requesting them without error spam.
            resp = make_response("", 204)
            resp.headers["Cache-Control"] = "no-store"
            return resp
        try:
            extra = _tile_extra_params(layer, request.args.get("lines"))
            tile = fetch_tile(ctx, layer, z, x, y, extra_params=extra)
        except Exception as exc:
            return make_response(str(exc), _err_status(exc))
        if not tile:
            resp = make_response("", 204)
            resp.headers["Cache-Control"] = "no-store"
            return resp
        resp = make_response(bytes(tile))
        resp.headers["Content-Type"] = "application/vnd.mapbox-vector-tile"
        resp.headers["Cache-Control"] = "public, max-age=300"
        return resp

    return app
