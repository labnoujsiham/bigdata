"""
A16 to A20: analytics queries with aggregation pipelines (Person B).
(A21 to A23 belong to Person C, A24 and A25 to Siham.)

Run from the root of the repo (replica set running, data generated):
    python queries/btissam_queries/analytics.py

The QUERIES dict at the bottom lets benchmark.py call every query the same way.

Field names assumed (check docs/schema.md): driving_events.type / driver_id,
missions.planned_start / planned_end / actual_end / distance_km / depot / status,
positions.speed / vehicle_id, vehicles.type.
"""
import os
import sys
import time
from datetime import datetime, timedelta, timezone

# allow "import config" even though this file lives in queries/btissam_queries/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pymongo import MongoClient

import config

client = MongoClient(config.MONGO_URI)
client.admin.command("ping")   # connect first, so the connection time is not counted
db = client[config.MONGO_DB]

# centre of the Casablanca depot, GeoJSON order: [longitude, latitude]
CASABLANCA = [-7.5898, 33.5731]
SPEEDING_TYPE = "speeding"     # value of driving_events.type for a speeding event


def timed(label, func, *args):
    start = time.perf_counter()
    result = func(*args)
    elapsed_ms = (time.perf_counter() - start) * 1000
    print("%s: %.2f ms" % (label, elapsed_ms))
    return result


# ------------------------------------------------------------------ A16
def a16_missions_per_day(days=30):
    """Number of missions and total distance per day over the last N days."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    pipeline = [
        {"$match": {"planned_start": {"$gte": since}}},
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$planned_start"}},
            "missions": {"$sum": 1},
            "total_km": {"$sum": "$distance_km"},
        }},
        {"$sort": {"_id": 1}},
    ]
    return list(db["missions"].aggregate(pipeline))


# ------------------------------------------------------------------ A17
def a17_top_drivers_speeding(limit=10, event_type=SPEEDING_TYPE):
    """Top drivers by number of speeding events.
    $lookup brings the driver name from the drivers collection."""
    pipeline = [
        {"$match": {"type": event_type}},
        {"$group": {"_id": "$driver_id", "events": {"$sum": 1}}},
        {"$sort": {"events": -1}},
        {"$limit": limit},
        {"$lookup": {"from": "drivers", "localField": "_id",
                     "foreignField": "_id", "as": "driver"}},
        {"$project": {"events": 1,
                      "name": {"$arrayElemAt": ["$driver.name", 0]}}},
    ]
    return list(db["driving_events"].aggregate(pipeline))


# ------------------------------------------------------------------ A18
def a18_late_missions_by_depot():
    """Punctuality per depot: among completed missions, how many ended after their
    planned end, the late rate and the average delay in minutes (negative = early)."""
    pipeline = [
        {"$match": {"status": "completed"}},
        {"$project": {
            "depot": 1,
            "late": {"$cond": [{"$gt": ["$actual_end", "$planned_end"]}, 1, 0]},
            "delay_min": {"$divide": [
                {"$subtract": ["$actual_end", "$planned_end"]}, 60000]},
        }},
        {"$group": {"_id": "$depot",
                    "completed": {"$sum": 1},
                    "late": {"$sum": "$late"},
                    "avg_delay_min": {"$avg": "$delay_min"}}},
        {"$project": {"completed": 1, "late": 1, "avg_delay_min": 1,
                      "late_rate": {"$divide": ["$late", "$completed"]}}},
        {"$sort": {"late_rate": -1}},
    ]
    return list(db["missions"].aggregate(pipeline))


# ------------------------------------------------------------------ A19
def a19_avg_speed_by_vehicle_type():
    """Average speed per vehicle type. Positions are first summed per vehicle (small
    result), then $lookup brings the type from vehicles, then a weighted average
    is computed per type."""
    pipeline = [
        {"$group": {"_id": "$vehicle_id",
                    "speed_sum": {"$sum": "$speed"},
                    "n": {"$sum": 1}}},
        {"$lookup": {"from": "vehicles", "localField": "_id",
                     "foreignField": "_id", "as": "vehicle"}},
        {"$unwind": "$vehicle"},
        {"$group": {"_id": "$vehicle.type",
                    "speed_sum": {"$sum": "$speed_sum"},
                    "positions": {"$sum": "$n"}}},
        {"$project": {"positions": 1,
                      "avg_speed": {"$divide": ["$speed_sum", "$positions"]}}},
        {"$sort": {"_id": 1}},
    ]
    return list(db["positions"].aggregate(pipeline, allowDiskUse=True))


# ------------------------------------------------------------------ A20
def a20_vehicles_near_point(point=CASABLANCA, max_km=5, days=30, limit=10):
    """Vehicles seen within max_km of a point during the last N days, closest first.
    $geoNear needs the 2dsphere index on positions.loc and must be the first stage."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    pipeline = [
        {"$geoNear": {
            "near": {"type": "Point", "coordinates": point},
            "distanceField": "distance_m",
            "maxDistance": max_km * 1000,
            "spherical": True,
            "query": {"ts": {"$gte": since}},
        }},
        {"$group": {"_id": "$vehicle_id", "min_distance_m": {"$min": "$distance_m"},
                    "positions": {"$sum": 1}}},
        {"$sort": {"min_distance_m": 1}},
        {"$limit": limit},
    ]
    return list(db["positions"].aggregate(pipeline))


# For benchmark.py: id -> (function, arguments)
QUERIES = {
    "A16": (a16_missions_per_day, ()),
    "A17": (a17_top_drivers_speeding, ()),
    "A18": (a18_late_missions_by_depot, ()),
    "A19": (a19_avg_speed_by_vehicle_type, ()),
    "A20": (a20_vehicles_near_point, ()),
}


if __name__ == "__main__":
    # python queries/btissam_queries/analytics.py              -> all queries
    # python queries/btissam_queries/analytics.py A17 A20      -> only these ones
    wanted = [a.upper() for a in sys.argv[1:]] or ["A16", "A17", "A18", "A19", "A20"]
    for qid in wanted:
        if qid not in QUERIES:
            print("%s: unknown query (choose from %s)" % (qid, ", ".join(QUERIES)))
            continue
        func, args = QUERIES[qid]
        rows = timed(qid, func, *args)
        print("   ->", len(rows), "rows")
        for row in rows[:3]:
            print("     ", row)