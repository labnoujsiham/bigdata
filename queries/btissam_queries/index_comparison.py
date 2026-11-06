"""
With / without index comparison (Person B), using explain("executionStats").

Run from the root of the repo (replica set running, data generated):
    python queries/btissam_queries/index_comparison.py

Three comparisons:
  T5  positions of a vehicle     index { vehicle_id: 1, ts: -1 }
  T10 missions of a driver       index { driver_id: 1, planned_start: 1 }
  T6  vehicle by plate           NO index in the base schema: the script creates
                                 { plate: 1 }, measures, and drops it at the end.

"Without index" for T5 and T10 means a forced full scan: hint({"$natural": 1}).
We do not drop the real indexes, because positions also has the TTL index on ts and
MongoDB could use it instead of scanning, which would hide the effect we want to show.

Results are printed and saved in results/index_comparison.csv.
"""
import csv
import os
import statistics
import sys
import time

# this file is in queries/btissam_queries/, so the repo root is 3 levels up
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from transactional import (db, explain_stats, _t5_filter, _t10_filter,
                           PV, PLATE, DRIVER)

REPEATS = 20
NO_INDEX = [("$natural", 1)]                      # forces a collection scan
T5_INDEX = [("vehicle_id", 1), ("ts", -1)]
T10_INDEX = [("driver_id", 1), ("planned_start", 1)]


def make_cursor(coll, filt, sort=None, limit=0, hint=None):
    cursor = db[coll].find(filt)
    if sort:
        cursor = cursor.sort(*sort)
    if limit:
        cursor = cursor.limit(limit)
    if hint:
        cursor = cursor.hint(hint)
    return cursor


def measure(label, variant, builder):
    """Explain once, then time the query REPEATS times (1 warm-up run first)."""
    stats = explain_stats(builder())
    list(builder())
    times = []
    for _ in range(REPEATS):
        start = time.perf_counter()
        list(builder())
        times.append((time.perf_counter() - start) * 1000)
    row = {
        "query": label,
        "variant": variant,
        "stage": stats["stage"],
        "returned": stats["returned"],
        "keys_examined": stats["keys_examined"],
        "docs_examined": stats["docs_examined"],
        "median_ms": round(statistics.median(times), 2),
        "mean_ms": round(statistics.mean(times), 2),
    }
    print("%-4s %-14s %-8s returned=%-6d keys=%-7d docs=%-7d median=%.2f ms"
          % (label, variant, row["stage"], row["returned"], row["keys_examined"],
             row["docs_examined"], row["median_ms"]))
    return row


def compare_t5(rows):
    f = _t5_filter(PV, 60)
    for variant, hint in (("with index", T5_INDEX), ("without index", NO_INDEX)):
        rows.append(measure("T5", variant,
                            lambda h=hint: make_cursor("positions", f, ("ts", -1), 0, h)))


def compare_t10(rows):
    f = _t10_filter(DRIVER)
    for variant, hint in (("with index", T10_INDEX), ("without index", NO_INDEX)):
        rows.append(measure("T10", variant,
                            lambda h=hint: make_cursor("missions", f, ("planned_start", -1), 20, h)))


def compare_t6(rows):
    f = {"plate": PLATE}
    # no index on plate in the base schema: this is already a full scan
    rows.append(measure("T6", "without index", lambda: make_cursor("vehicles", f)))
    db["vehicles"].create_index([("plate", 1)])
    try:
        rows.append(measure("T6", "with index", lambda: make_cursor("vehicles", f)))
    finally:
        db["vehicles"].drop_index("plate_1")      # leave the database as generated


if __name__ == "__main__":
    print("Vehicle for T5: %s | plate for T6: %s | driver for T10: %s\n" % (PV, PLATE, DRIVER))
    rows = []
    compare_t5(rows)
    compare_t10(rows)
    compare_t6(rows)

    out_dir = os.path.join(ROOT, "results")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "index_comparison.csv")
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print("\nSaved in", out_path)
