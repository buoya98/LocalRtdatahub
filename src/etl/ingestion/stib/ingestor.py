"""STIB ingestion runner — dispatches to the per-step ingestors.

Static steps (one-shot, populate `static.stib_*`):
    stops    → stop_details_ingestor.run
    lines    → stops_by_line_ingestor.run
    shapes   → gtfs_shapes_ingestor.run

Real-time step (poll the ODP, populate `rt.stib_vehicle_position`):
    rt       → vehicle_positions_ingestor.run (one-shot or looped)

Run with:
    python -m src.etl.ingestion.stib.ingestor                 # all static steps
    python -m src.etl.ingestion.stib.ingestor stops           # one step
    python -m src.etl.ingestion.stib.ingestor rt              # one live poll
    python -m src.etl.ingestion.stib.ingestor rt --interval 20  # loop forever
"""
from __future__ import annotations

import logging
import sys

from . import gtfs_shapes_ingestor, stop_details_ingestor, stops_by_line_ingestor
from . import vehicle_positions_ingestor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest_stib")

_STATIC_STEPS = {
    "stops":  stop_details_ingestor.run,
    "lines":  stops_by_line_ingestor.run,
    "shapes": gtfs_shapes_ingestor.run,
}


def main() -> int:
    args = sys.argv[1:]
    # The "rt" step delegates its own argument parsing to the vehicle
    # positions ingestor (it accepts --interval, --once, --output, …).
    if args and args[0] == "rt":
        sys.argv = [sys.argv[0], *args[1:]]
        return vehicle_positions_ingestor.main()

    steps = args or list(_STATIC_STEPS.keys())
    for s in steps:
        if s not in _STATIC_STEPS:
            log.error("unknown step '%s' — pick from %s or 'rt'",
                      s, sorted(_STATIC_STEPS))
            return 1
    for s in steps:
        log.info("=== step: %s ===", s)
        result = _STATIC_STEPS[s]()
        # Each step's run() returns either an int (rows upserted) or a
        # dict of counters (rows per sub-table). Surface it so a user
        # running `python -m … stib.ingestor` sees confirmation
        # rather than just module-level log spam.
        log.info("    step '%s' done: %s", s, result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
