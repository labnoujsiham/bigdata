"""
A21 to A23: analytics queries with aggregation pipelines (Person C).

Run from the root of the repo (replica set running, data generated):
    python queries/c_queries/analytics.py

The QUERIES dict at the bottom lets benchmark.py call every query the same way.
"""
from datetime import timedelta

from common import db, timed


# ------------------------------------------------------------------ A21
def a21_silent_vehicles():
    """Vehicles with no GPS position during the last hour, per depot.

    Reads the embedded copy vehicles.last_position (no join with positions).
    The reference time is the most recent position in the data, not the clock:
    the data is generated once, so with the real clock every vehicle would look
    silent a few hours after the generation.
    """
    newest = list(db["vehicles"].aggregate(
        [{"$group": {"_id": None, "ts": {"$max": "$last_position.ts"}}}]))[0]["ts"]
    cutoff = newest - timedelta(hours=1)
    pipeline = [
        # no position at all, or a last position older than one hour
        {"$match": {"$or": [{"last_position": None},
                            {"last_position.ts": {"$lt": cutoff}}]}},
        {"$group": {"_id": "$depot", "silent_vehicles": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    return list(db["vehicles"].aggregate(pipeline))


# ------------------------------------------------------------------ A22
def a22_events_by_depot_and_type():
    """Driving events per depot (of the vehicle) and event type, with the average value."""
    pipeline = [
        {"$lookup": {"from": "vehicles", "localField": "vehicle_id",
                     "foreignField": "_id", "as": "vehicle"}},
        {"$unwind": "$vehicle"},
        {"$group": {
            "_id": {"depot": "$vehicle.depot", "type": "$type"},
            "events": {"$sum": 1},
            "avg_value": {"$avg": "$value"},   # same unit inside one event type
        }},
        {"$project": {"events": 1, "avg_value": {"$round": ["$avg_value", 1]}}},
        {"$sort": {"_id.depot": 1, "events": -1}},
    ]
    return list(db["driving_events"].aggregate(pipeline))


# ------------------------------------------------------------------ A23
def a23_maintenance_cost_by_vehicle_type():
    """Number of maintenances, total and average cost per vehicle type."""
    pipeline = [
        {"$lookup": {"from": "vehicles", "localField": "vehicle_id",
                     "foreignField": "_id", "as": "vehicle"}},
        {"$unwind": "$vehicle"},
        {"$group": {
            "_id": "$vehicle.type",
            "maintenances": {"$sum": 1},
            "total_cost": {"$sum": "$cost"},
            "avg_cost": {"$avg": "$cost"},
        }},
        {"$project": {"maintenances": 1,
                      "total_cost": {"$round": ["$total_cost", 2]},
                      "avg_cost": {"$round": ["$avg_cost", 2]}}},
        {"$sort": {"total_cost": -1}},
    ]
    return list(db["maintenances"].aggregate(pipeline))


QUERIES = {
    "A21": (a21_silent_vehicles, ()),
    "A22": (a22_events_by_depot_and_type, ()),
    "A23": (a23_maintenance_cost_by_vehicle_type, ()),
}


if __name__ == "__main__":
    for qid, (func, args) in QUERIES.items():
        rows = timed(qid, func, *args)
        print("  %d rows, first: %s" % (len(rows), rows[0] if rows else None))