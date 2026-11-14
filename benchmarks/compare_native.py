"""
Native (MongoDB aggregation) vs Spark for A19, A22 and A23.

1) Run spark/compare_spark.py first (it writes results/spark_compare.csv).
2) From the root of the repo:  python benchmarks/compare_native.py
Prints a table and saves results/native_vs_spark.csv.
Native side: 1 warm-up run, then 10 timed runs, median.

The native queries are the ones of the workload:
  A19  queries/btissam_queries/analytics.py  (average speed per vehicle type)
  A22  queries/c_queries/analytics.py        (events per type and day)
  A23  queries/c_queries/analytics.py        (average fuel level per model)
"""
import csv
import importlib.util
import os
import re
import statistics
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "queries", "c_queries"))

import analytics as c          # A22, A23 (Person C); also opens the MongoDB connection


def load_module(name, relative_path):
    """Loads a file under another name: both analytics.py files have the same name,
    so a normal import would give us only one of them."""
    spec = importlib.util.spec_from_file_location(name, os.path.join(ROOT, relative_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


b = load_module("btissam_analytics", os.path.join("queries", "btissam_queries", "analytics.py"))

REPEATS = 10

PAIRS = [
    ("A19", "Average speed per vehicle type (join with vehicles)",
     b.a19_avg_speed_by_vehicle_type),
    ("A22", "Driving events per type and day (no join)",
     c.a22_events_by_type_and_day),
    ("A23", "Average fuel level per model (join with vehicles)",
     c.a23_avg_fuel_by_model),
]


def native_median(func):
    func()                                   # warm-up
    times = []
    rows = 0
    for _ in range(REPEATS):
        start = time.perf_counter()
        rows = len(func())
        times.append((time.perf_counter() - start) * 1000)
    return statistics.median(times), rows


def read_spark(path):
    out = {}
    with open(path, encoding="utf-8-sig") as f:
        lines = [x.strip() for x in f if re.match(r"^(id|A\d\d|S\d\d),", x.strip())]
    for row in csv.DictReader(lines):
        out[row["id"]] = row
    return out


if __name__ == "__main__":
    spark_path = os.path.join(ROOT, "results", "spark_compare.csv")
    spark = read_spark(spark_path) if os.path.exists(spark_path) else {}
    if not spark:
        print("No Spark results: run spark/compare_spark.py first (results/spark_compare.csv).")
    table = []
    print("%-4s %-52s %10s %10s %8s %s" % ("ID", "Query", "Native ms", "Spark ms", "x", "rows (native/Spark)"))
    for qid, label, func in PAIRS:
        nat, nat_rows = native_median(func)
        sp = spark.get(qid)
        sp_ms = float(sp["median_ms"]) if sp else None
        ratio = round(sp_ms / nat, 1) if sp_ms else None
        sp_rows = sp["rows"] if sp else "?"
        print("%-4s %-52s %10.2f %10s %8s %s/%s" % (
            qid, label, nat, "%.2f" % sp_ms if sp_ms else "-", ratio if ratio else "-", nat_rows, sp_rows))
        if sp and str(nat_rows) != str(sp_rows):
            print("     WARNING: %s returns %s rows natively but %s with Spark: the two "
                  "queries do not compute the same thing, do not compare them yet."
                  % (qid, nat_rows, sp_rows))
        table.append([qid, label, round(nat, 2), sp_ms, ratio, nat_rows, sp_rows])
    out_path = os.path.join(ROOT, "results", "native_vs_spark.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "query", "native_ms", "spark_ms", "spark_over_native", "rows_native", "rows_spark"])
        w.writerows(table)
    print("Saved:", out_path)