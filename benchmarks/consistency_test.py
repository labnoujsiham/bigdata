"""
Consistency test for MongoDB (replica set of 3 nodes).

Compares:
  - WRITES: write concern w:1  vs  w:"majority"
  - READS : read concern "local" vs "majority", and reading from the primary
            vs from a secondary

Run from the root of the repo (replica set running, data generated):
    python benchmarks/consistency_test.py

Results are printed and saved in results/consistency_results.csv
"""
import csv
import os
import random
import statistics
import sys
import time
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from pymongo import MongoClient, ReadPreference, WriteConcern
from pymongo.read_concern import ReadConcern

import config

N_WRITES = 500
N_READS = 500
random.seed(1)

client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=10000)
db = client[config.MONGO_DB]
client.admin.command("ping")


def summarize(times_ms):
    ordered = sorted(times_ms)
    p95_index = int(len(ordered) * 0.95) - 1
    return (statistics.mean(ordered), statistics.median(ordered),
            ordered[p95_index], ordered[-1])


results = []   # (kind, label, mean, median, p95, max)

# ------------------------------------------------------------------ WRITES
print("WRITE test: %d single inserts per write concern" % N_WRITES)
db.drop_collection("consistency_test")

write_configs = [
    ("w:1", WriteConcern(w=1)),
    ("w:majority", WriteConcern(w="majority")),
]
for label, wc in write_configs:
    coll = db.get_collection("consistency_test", write_concern=wc)
    # warm-up (not measured)
    for i in range(20):
        coll.insert_one({"warmup": True})
    times = []
    for i in range(N_WRITES):
        doc = {
            "vehicle_id": "V%04d" % random.randint(1, 1500),
            "ts": datetime.now(timezone.utc),
            "loc": {"type": "Point", "coordinates": [-7.59, 33.57]},
            "speed": random.uniform(0, 90),
            "label": label,
        }
        start = time.perf_counter()
        coll.insert_one(doc)
        times.append((time.perf_counter() - start) * 1000)
    mean, median, p95, worst = summarize(times)
    results.append(("write", label, mean, median, p95, worst))
    print("  %-12s mean %.2f ms | median %.2f | p95 %.2f | max %.2f"
          % (label, mean, median, p95, worst))

db.drop_collection("consistency_test")

# ------------------------------------------------------------------ READS
print("")
print("READ test: %d 'last known position' queries (like T4) per configuration" % N_READS)
vehicle_ids = db["positions"].distinct("vehicle_id")
queries = [random.choice(vehicle_ids) for i in range(N_READS)]

read_configs = [
    ("primary + local", ReadPreference.PRIMARY, ReadConcern("local")),
    ("primary + majority", ReadPreference.PRIMARY, ReadConcern("majority")),
    ("secondary + local", ReadPreference.SECONDARY, ReadConcern("local")),
]
for label, pref, concern in read_configs:
    coll = db.get_collection("positions", read_preference=pref, read_concern=concern)
    for vid in queries[:20]:      # warm-up (not measured)
        coll.find_one({"vehicle_id": vid}, sort=[("ts", -1)])
    times = []
    for vid in queries:
        start = time.perf_counter()
        coll.find_one({"vehicle_id": vid}, sort=[("ts", -1)])
        times.append((time.perf_counter() - start) * 1000)
    mean, median, p95, worst = summarize(times)
    results.append(("read", label, mean, median, p95, worst))
    print("  %-20s mean %.2f ms | median %.2f | p95 %.2f | max %.2f"
          % (label, mean, median, p95, worst))

# ------------------------------------------------------------------ save
os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
path = os.path.join(ROOT, "results", "consistency_results.csv")
with open(path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["kind", "configuration", "mean_ms", "median_ms", "p95_ms", "max_ms"])
    for row in results:
        writer.writerow([row[0], row[1], round(row[2], 3), round(row[3], 3),
                         round(row[4], 3), round(row[5], 3)])
print("")
print("Saved to", path)