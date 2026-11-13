"""
Benchmark of the 30 queries (Person B).

Run from the root of the repo (replica set running, data generated):
    python benchmarks/benchmark.py                  # all queries, 10 repetitions
    python benchmarks/benchmark.py --repeats 20
    python benchmarks/benchmark.py --only T5,T6,A25

For every query: 1 warm-up run (not counted), then N timed runs. The script prints
mean / median / p95 / min / max in ms and saves them in results/benchmark_results.csv.
The median is the number to use in docs/queries.md (a single run varies a lot).

Queries of Btissam (T1-T10, A16-A20), Siham (T11-T15, A24, A25) and Person C
(A21-A23, in queries/c_queries/*.py) are loaded automatically. Files in c_queries
must expose a dict:  QUERIES = {"A21": (function, (arguments,)), ...}

Spark queries (S26-S30) run in the Spark container (see spark/spark_queries.py).
Run them first so that results/spark_results.csv exists: this script reads it and
merges the S26-S30 lines in benchmark_results.csv (no p95 for them).
The spark/ folder is never imported here (PySpark is not in the .venv).

Queries that change data (T1, T2, T3, T8, T11, T12, T14) are prepared and cleaned up
around each run, so that the database is back to its original state at the end.
"""
import argparse
import csv
import glob
import importlib.util
import io
import math
import os
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from pymongo import MongoClient

import config

client = MongoClient(config.MONGO_URI)
client.admin.command("ping")
db = client[config.MONGO_DB]

# (path relative to the repo root, owner label): extra files that expose a QUERIES dict,
# in addition to the automatic scan of PERSON_C_PATTERNS below.
OTHER_REGISTRIES = [
    # ("some/other/folder/file.py", "Person C"),
]

# Folders scanned automatically for Person C's files (only files with a QUERIES dict are used).
# spark/ is NOT here: Spark queries run in the container and are merged from a CSV.
PERSON_C_PATTERNS = [
    "queries/c_queries/*.py",
]
IGNORED_FILES = {"common.py", "__init__.py", "config.py"}

SPARK_CSV = os.path.join(ROOT, "results", "spark_results.csv")

OWNERS = {}
for i in range(1, 11):
    OWNERS["T%d" % i] = "Btissam"
for i in range(11, 16):
    OWNERS["T%d" % i] = "Siham"
for i in range(16, 21):
    OWNERS["A%d" % i] = "Btissam"
for i in range(21, 24):
    OWNERS["A%d" % i] = "Person C"
for i in (24, 25):
    OWNERS["A%d" % i] = "Siham"
for i in range(26, 31):
    OWNERS["S%d" % i] = "Person C"
ALL_IDS = ([("T%d" % i) for i in range(1, 16)]
           + [("A%d" % i) for i in range(16, 26)]
           + [("S%d" % i) for i in range(26, 31)])

BENCH_VEHICLE = "V_BENCH"          # fake vehicle used to benchmark T14 (delete)
TEST_MISSION_ID = "M_B_TEST"       # mission used by T2, T3, T8


# ------------------------------------------------------------------ loading
def load_module(name, rel_path):
    """Loads a .py file by its path. Its own folder is added to sys.path while it loads,
    so that 'from common import db, timed' finds the right common.py."""
    path = os.path.join(ROOT, rel_path)
    folder = os.path.dirname(path)
    sys.path.insert(0, folder)
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        sys.path.remove(folder)
        sys.modules.pop("common", None)   # several folders have a common.py: do not mix them
    return module


def find_file(folder, pattern):
    matches = glob.glob(os.path.join(ROOT, folder, pattern))
    if not matches:
        raise FileNotFoundError("%s/%s" % (folder, pattern))
    return os.path.relpath(matches[0], ROOT)


def entry(qid, func, args=(), setup=None, cleanup=None):
    return {"id": qid, "owner": OWNERS.get(qid, "?"), "func": func, "args": args,
            "setup": setup, "cleanup": cleanup}


def add_registry(registry, problems, rel_path, name, owner=None, required=True):
    """Loads a file exposing QUERIES. If required is False, a file without QUERIES is
    silently ignored (used for the automatic scan)."""
    try:
        module = load_module(name, rel_path)
        queries = getattr(module, "QUERIES", None)
        if queries is None:
            if required:
                problems.append("%s: no QUERIES dict found" % rel_path)
            return
        for qid, value in queries.items():
            func, args = value[0], value[1]
            e = entry(qid, func, args)
            if owner:
                e["owner"] = owner
            registry[qid] = e
    except Exception as exc:
        problems.append("%s: %s: %s" % (rel_path, type(exc).__name__, exc))


def add_person_c(registry, problems):
    """Automatic scan of Person C's folders, then the manual OTHER_REGISTRIES."""
    seen = set()
    for pattern in PERSON_C_PATTERNS:
        for path in sorted(glob.glob(os.path.join(ROOT, pattern))):
            if os.path.basename(path) in IGNORED_FILES:
                continue
            rel_path = os.path.relpath(path, ROOT)
            seen.add(rel_path)
            add_registry(registry, problems, rel_path,
                         "c_" + rel_path.replace(os.sep, "_")[:-3],
                         owner="Person C", required=False)
    for rel_path, owner in OTHER_REGISTRIES:
        if rel_path in seen:
            continue
        add_registry(registry, problems, rel_path, "other_" + os.path.basename(rel_path), owner)


def add_siham(registry, problems):
    folder = "queries/siham_queries"
    now = datetime.now(timezone.utc)
    try:
        t11 = load_module("siham_t11", find_file(folder, "t*11_record_maintenance.py"))
        registry["T11"] = entry(
            "T11", t11.run,
            cleanup=lambda inserted_id: db["maintenances"].delete_one({"_id": inserted_id}))

        t12 = load_module("siham_t12", find_file(folder, "t12_*.py"))
        registry["T12"] = entry(
            "T12", t12.run,
            # remove the equipment first, so that every run really adds it
            setup=lambda: db["vehicles"].update_one(
                {"_id": "V0001"}, {"$pull": {"equipment": "temperature_sensor"}}))

        t13 = load_module("siham_t13", find_file(folder, "t13_*.py"))
        registry["T13"] = entry("T13", t13.run)

        # T14 deletes positions: before each run we insert 200 fake positions for a fake
        # vehicle, then T14 deletes them. Real data is never touched.
        t14 = load_module("siham_t14", find_file(folder, "t14_*.py"))
        start, end = now - timedelta(days=2), now

        def t14_setup():
            base = datetime.now(timezone.utc) - timedelta(days=1)
            db["positions"].insert_many([{
                "vehicle_id": BENCH_VEHICLE,
                "ts": base + timedelta(seconds=30 * i),
                "loc": {"type": "Point", "coordinates": [-7.59, 33.57]},
                "speed": 40.0, "heading": 0, "fuel_level": 50.0, "engine_state": "on",
            } for i in range(200)])

        registry["T14"] = entry("T14", t14.run, (BENCH_VEHICLE, start, end), setup=t14_setup)

        t15 = load_module("siham_t15", find_file(folder, "t15_*.py"))
        registry["T15"] = entry("T15", t15.run)
        a24 = load_module("siham_a24", find_file(folder, "a24_*.py"))
        registry["A24"] = entry("A24", a24.run)
        a25 = load_module("siham_a25", find_file(folder, "a25_*.py"))
        registry["A25"] = entry("A25", a25.run)
    except Exception as exc:
        problems.append("Siham's files: %s: %s" % (type(exc).__name__, exc))


def build_registry():
    registry, problems = {}, []
    add_registry(registry, problems, "queries/btissam_queries/transactional.py", "b_transactional")
    add_registry(registry, problems, "queries/btissam_queries/analytics.py", "b_analytics")
    add_siham(registry, problems)
    add_person_c(registry, problems)
    return registry, problems


# ------------------------------------------------------------------ spark results
def read_spark_csv():
    """Reads results/spark_results.csv written by spark_queries.py.
    Handles UTF-8 and UTF-16 (PowerShell '>' often writes UTF-16) and ignores log lines.
    Returns {id: row} (empty dict if the file does not exist)."""
    if not os.path.exists(SPARK_CSV):
        return {}
    with open(SPARK_CSV, "rb") as f:
        raw = f.read()
    text = None
    for encoding in ("utf-8-sig", "utf-16"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeError:
            text = None
    if text is None:
        text = raw.decode("latin-1")
    lines = text.splitlines()
    header_at = next((i for i, l in enumerate(lines) if l.startswith("id,repeats")), None)
    if header_at is None:
        return {}
    kept = [l for l in lines[header_at:] if l.strip() and not l.startswith(" ")]
    reader = csv.DictReader(io.StringIO("\n".join(kept)))
    return {r["id"]: r for r in reader if r.get("id")}


def merge_spark(rows):
    """Fills the S26-S30 lines (status 'missing') with the numbers measured by Spark."""
    spark_rows = read_spark_csv()
    for row in rows:
        s = spark_rows.get(row["id"])
        if not s:
            continue
        try:
            row.update({"status": "ok", "repeats": int(s["repeats"]),
                        "median_ms": float(s["median_ms"]), "mean_ms": float(s["mean_ms"]),
                        "min_ms": float(s["min_ms"]), "max_ms": float(s["max_ms"]),
                        "result": "%s rows" % s["rows"]})
            print("%-4s %-9s %10.2f %10.2f %10s %10.2f %10.2f  %s  (from spark_results.csv)"
                  % (row["id"], row["owner"], row["mean_ms"], row["median_ms"], "-",
                     row["min_ms"], row["max_ms"], row["result"]))
        except (KeyError, ValueError) as exc:
            row["status"] = "error: bad spark_results.csv (%s)" % exc


# ------------------------------------------------------------------ measuring
def one_run(e):
    if e["setup"]:
        e["setup"]()                                   # not timed
    start = time.perf_counter()
    result = e["func"](*e["args"])
    elapsed_ms = (time.perf_counter() - start) * 1000
    if e["cleanup"]:
        e["cleanup"](result)                           # not timed
    return elapsed_ms, result


def summarize(times):
    s = sorted(times)
    return {
        "mean_ms": statistics.mean(s),
        "median_ms": statistics.median(s),
        "p95_ms": s[max(0, math.ceil(0.95 * len(s)) - 1)],
        "min_ms": s[0],
        "max_ms": s[-1],
    }


def describe(result):
    if isinstance(result, (list, tuple)):
        return "%d rows" % len(result)
    if isinstance(result, dict):
        return "1 document"
    if result is None:
        return ""
    return str(result)[:40]


def benchmark(e, repeats):
    one_run(e)                                         # warm-up, not counted
    times, result = [], None
    for _ in range(repeats):
        elapsed, result = one_run(e)
        times.append(elapsed)
    row = {"id": e["id"], "owner": e["owner"], "status": "ok", "repeats": repeats,
           "result": describe(result)}
    row.update(summarize(times))
    return row


# ------------------------------------------------------------------ main
def main():
    parser = argparse.ArgumentParser(description="Benchmark of the 30 queries")
    parser.add_argument("--repeats", type=int, default=10, help="timed runs per query (default 10)")
    parser.add_argument("--only", default="", help="comma-separated ids, e.g. T5,T6,A25")
    args = parser.parse_args()
    wanted = [x.strip().upper() for x in args.only.split(",") if x.strip()] or ALL_IDS

    registry, problems = build_registry()
    started = datetime.now(timezone.utc)
    original = db["vehicles"].find_one({"_id": "V0001"}, {"last_position": 1})

    rows = []
    print("%-4s %-9s %10s %10s %10s %10s %10s  %s"
          % ("ID", "Owner", "mean", "median", "p95", "min", "max", "result"))
    try:
        for qid in wanted:
            if qid not in registry:
                rows.append({"id": qid, "owner": OWNERS.get(qid, "?"), "status": "missing"})
                continue
            try:
                row = benchmark(registry[qid], args.repeats)
                print("%-4s %-9s %10.2f %10.2f %10.2f %10.2f %10.2f  %s"
                      % (qid, row["owner"], row["mean_ms"], row["median_ms"], row["p95_ms"],
                         row["min_ms"], row["max_ms"], row["result"]))
            except Exception as exc:
                row = {"id": qid, "owner": registry[qid]["owner"],
                       "status": "error: %s: %s" % (type(exc).__name__, str(exc)[:80])}
                print("%-4s %-9s ERROR %s" % (qid, row["owner"], row["status"]))
            rows.append(row)
    finally:
        # put the database back as it was
        db["missions"].delete_one({"_id": TEST_MISSION_ID})
        db["positions"].delete_many({"vehicle_id": BENCH_VEHICLE})
        db["positions"].delete_many({"vehicle_id": "V0001", "ts": {"$gte": started}})
        db["vehicles"].update_one({"_id": "V0001"}, {"$pull": {"equipment": "temperature_sensor"}})
        if original is not None:
            db["vehicles"].update_one({"_id": "V0001"},
                                      {"$set": {"last_position": original.get("last_position")}})

    # S26-S30: numbers measured in the Spark container (read from results/spark_results.csv)
    merge_spark([r for r in rows if r["status"] == "missing" and r["id"].startswith("S")])

    out_dir = os.path.join(ROOT, "results")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "benchmark_results.csv")
    fields = ["id", "owner", "status", "repeats", "mean_ms", "median_ms", "p95_ms",
              "min_ms", "max_ms", "result"]
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: (round(v, 3) if isinstance(v, float) else v) for k, v in row.items()})
    print("\nSaved in", out_path)

    missing = [r["id"] for r in rows if r["status"] == "missing"]
    errors = [r["id"] for r in rows if r["status"].startswith("error")]
    if missing:
        hint = ""
        if any(m.startswith("S") for m in missing):
            hint = "  (S26-S30: run spark_queries.py first to create results/spark_results.csv)"
        print("Missing (not written yet, or not registered): " + ", ".join(missing) + hint)
    if errors:
        print("Errors: " + ", ".join(errors))
    for p in problems:
        print("Could not load -> " + p)


if __name__ == "__main__":
    main()