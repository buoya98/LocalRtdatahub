"""GTFS shapes ingestor — mirrors
source/src/etl/ingestion/stib/gtfs_shapes_ingestor.py.

Downloads the STIB GTFS static feed (zip), picks one canonical shape per
(route_id, direction_id), and populates static.stib_line_shape with the
matching LineString geometry plus mode / official colours.

Requires that stops_by_line_ingestor has run first (uses
static.stib_line_terminus to assign each shape to a direction label).
"""
from __future__ import annotations

import csv
import io
import logging
import zipfile
from collections import Counter

import httpx

from ._common import (
    GTFS_URL,
    ROUTE_TYPE_MODE,
    get_conn,
    haversine_m,
    normalise_color,
    require_key,
)

log = logging.getLogger("ingest_stib.shapes")

TERMINUS_TOLERANCE_M = 500.0


def _download_gtfs_zip(key: str) -> bytes:
    log.info("[gtfs] downloading %s", GTFS_URL)
    headers = {"bmc-partner-key": key, "Accept": "application/zip"}
    with httpx.Client(timeout=120.0, follow_redirects=True) as c:
        r = c.get(GTFS_URL, headers=headers)
        if r.status_code in (401, 403):
            log.error("Authentication failed (HTTP %d).", r.status_code)
            raise SystemExit(1)
        r.raise_for_status()
        return r.content


def _iter_csv(zf: zipfile.ZipFile, name: str):
    with zf.open(name) as f:
        text = io.TextIOWrapper(f, encoding="utf-8-sig", newline="")
        yield from csv.DictReader(text)


def _load_termini(cur) -> dict[str, list[tuple[str, str, float, float]]]:
    cur.execute(
        """
        SELECT t.lineid, t.direction, t.terminus_pointid,
               ST_X(s.geom) AS lon, ST_Y(s.geom) AS lat
          FROM static.stib_line_terminus t
          JOIN static.stib_stop s ON s.pointid = t.terminus_pointid
        """
    )
    out: dict[str, list[tuple[str, str, float, float]]] = {}
    for row in cur.fetchall():
        out.setdefault(str(row[0]), []).append(
            (str(row[1]), str(row[2]), float(row[3]), float(row[4]))
        )
    return out


def _resolve_dir(lon: float, lat: float, candidates) -> tuple[str, float] | None:
    best = None
    for dir_label, _pid, tlon, tlat in candidates:
        d = haversine_m(lon, lat, tlon, tlat)
        if best is None or d < best[1]:
            best = (dir_label, d)
    return best


def _wkt_linestring(pts) -> str:
    return "LINESTRING(" + ", ".join(f"{lon} {lat}" for lon, lat in pts) + ")"


def run() -> dict[str, int]:
    zip_bytes = _download_gtfs_zip(require_key())
    log.info("[gtfs] got %d bytes", len(zip_bytes))

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            termini = _load_termini(cur)
            if not termini:
                log.error("[gtfs] static.stib_line_terminus is empty — "
                          "run stops_by_line_ingestor first")
                return {"inserted": 0}
            log.info("[gtfs] loaded termini for %d lines", len(termini))

            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                # routes.txt
                route_to_line, route_to_mode, route_to_color, route_to_text = {}, {}, {}, {}
                for row in _iter_csv(zf, "routes.txt"):
                    rid = (row.get("route_id") or "").strip()
                    name = (row.get("route_short_name") or "").strip()
                    rtype = (row.get("route_type") or "").strip()
                    color = normalise_color(row.get("route_color"))
                    tcolor = normalise_color(row.get("route_text_color"))
                    if rid and name:
                        route_to_line[rid] = name
                        route_to_mode[rid] = ROUTE_TYPE_MODE.get(rtype, "other")
                        if color:
                            route_to_color[rid] = color
                        if tcolor:
                            route_to_text[rid] = tcolor
                log.info("[gtfs] routes.txt: %d routes (%d coloured)",
                         len(route_to_line), len(route_to_color))

                # trips.txt → canonical (route, dir) → shape
                counts: dict[tuple[str, int], Counter] = {}
                for row in _iter_csv(zf, "trips.txt"):
                    rid = (row.get("route_id") or "").strip()
                    did_raw = (row.get("direction_id") or "").strip()
                    sid = (row.get("shape_id") or "").strip()
                    if not (rid and did_raw and sid):
                        continue
                    try:
                        did = int(did_raw)
                    except ValueError:
                        continue
                    counts.setdefault((rid, did), Counter())[sid] += 1
                canonical = {k: c.most_common(1)[0][0] for k, c in counts.items()}
                log.info("[gtfs] trips.txt: %d canonical mappings", len(canonical))

                # shapes.txt → polylines for the canonical shape ids
                wanted = set(canonical.values())
                by_shape: dict[str, list[tuple[int, float, float]]] = {}
                for row in _iter_csv(zf, "shapes.txt"):
                    sid = (row.get("shape_id") or "").strip()
                    if sid not in wanted:
                        continue
                    try:
                        seq = int(row.get("shape_pt_sequence") or 0)
                        lat = float(row["shape_pt_lat"])
                        lon = float(row["shape_pt_lon"])
                    except (ValueError, TypeError, KeyError):
                        continue
                    by_shape.setdefault(sid, []).append((seq, lon, lat))
                polylines = {}
                for sid, entries in by_shape.items():
                    entries.sort(key=lambda t: t[0])
                    polylines[sid] = [(lon, lat) for _, lon, lat in entries]
                log.info("[gtfs] shapes.txt: %d polylines loaded", len(polylines))

            cur.execute("TRUNCATE static.stib_line_shape;")
            inserted = skipped_unknown = skipped_no_term = skipped_far = 0
            for (rid, did), sid in canonical.items():
                lineid = route_to_line.get(rid)
                if not lineid:
                    skipped_unknown += 1
                    continue
                pts = polylines.get(sid)
                if not pts or len(pts) < 2:
                    continue
                cands = termini.get(lineid)
                if not cands:
                    skipped_no_term += 1
                    continue

                end_lon, end_lat = pts[-1]
                match = _resolve_dir(end_lon, end_lat, cands)
                if match is None:
                    skipped_no_term += 1
                    continue
                dir_label, dist_m = match
                if dist_m > TERMINUS_TOLERANCE_M:
                    start_lon, start_lat = pts[0]
                    m2 = _resolve_dir(start_lon, start_lat, cands)
                    if m2 and m2[1] <= TERMINUS_TOLERANCE_M:
                        pts = list(reversed(pts))
                        dir_label, dist_m = m2
                    else:
                        skipped_far += 1
                        continue

                cur.execute(
                    """
                    INSERT INTO static.stib_line_shape
                        (lineid, direction, variant, mode, route_color, route_text_color, geom)
                    VALUES (%s, %s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4326))
                    ON CONFLICT (lineid, direction) DO UPDATE SET
                        variant=EXCLUDED.variant, mode=EXCLUDED.mode,
                        route_color=EXCLUDED.route_color,
                        route_text_color=EXCLUDED.route_text_color,
                        geom=EXCLUDED.geom;
                    """,
                    (lineid, dir_label, did,
                     route_to_mode.get(rid, "other"),
                     route_to_color.get(rid),
                     route_to_text.get(rid),
                     _wkt_linestring(pts)),
                )
                inserted += 1
        conn.commit()
    finally:
        conn.close()

    log.info("[gtfs] done inserted=%d skipped_unknown=%d skipped_no_term=%d skipped_far=%d",
             inserted, skipped_unknown, skipped_no_term, skipped_far)
    return {"inserted": inserted}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
