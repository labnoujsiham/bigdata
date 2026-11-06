"""
A16 to A20: analytics queries with aggregation pipelines (Person B).
(A21 to A23 belong to Person C, A24 and A25 to Siham.)

Run from the root of the repo (replica set running, data generated):
    python queries/btissam_queries/analytics.py

The QUERIES dict at the bottom lets benchmark.py call every query the same way.
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


def timed(label, func, *args):
    start = time.perf_counter()
    result = func(*args)
    elapsed_ms = (time.perf_counter() - start) * 1000
    print("%s: %.2f ms" % (label, elapsed_ms))
    return result


# ------------------------------------------------------------------ A16
def a16_fleet_by_depot_and_type():
    """Number of vehicles and average capacity per depot and vehicle type."""
    pipeline = [
        {"$group": {
            "_id": {"depot": "$depot", "type": "$type"},
            "vehicles": {"$sum": 1},
            "avg_capacity": {"$avg": "$capacity"},
        }},
        {"$sort": {"_id.depot": 1, "_id.type": 1}},
    ]
    return list(db["vehicles"].aggregate(pipeline))


# ------------------------------------------------------------------ A17
def a17_top_drivers_by_events(limit=10, event_type=None):
    """Top drivers by number of driving events (all types, or only one type).
    $lookup brings the driver name from the drivers collection."""
    pipeline = []
    if event_type:
        pipeline.append({"$match": {"type": event_type}})
    pipeline += [
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
def a18_missions_by_depot_and_status():
    """Number of missions per depot and status. Uses the index { depot, status }."""
    pipeline = [
        {"$group": {"_id": {"depot": "$depot", "status": "$status"},
                    "missions": {"$sum": 1}}},
        {"$sort": {"_id.depot": 1, "_id.status": 1}},
    ]
    return list(db["missions"].aggregate(pipeline))


# ------------------------------------------------------------------ A19
def a19_late_missions_by_depot():
    """Punctuality per depot: among completed missions, how many ended after their
    planned end, and the average delay in minutes (negative = early)."""
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
    "A16": (a16_fleet_by_depot_and_type, ()),
    "A17": (a17_top_drivers_by_events, ()),
    "A18": (a18_missions_by_depot_and_status, ()),
    "A19": (a19_late_missions_by_depot, ()),
    "A20": (a20_vehicles_near_point, ()),
}


if __name__ == "__main__":
    for qid in ["A16", "A17", "A18", "A19", "A20"]:
        func, args = QUERIES[qid]
        rows = timed(qid, func, *args)
        print("   ->", len(rows), "rows")
        for row in rows[:3]:
            print("     ", row)
