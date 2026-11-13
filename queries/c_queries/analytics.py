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
    """Vehicles with no GPS position during the last hour (counted per depot).

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
def a22_events_by_type_and_day():
    """Driving events per event type and per day (UTC), with the average value."""
    pipeline = [
        {"$group": {
            "_id": {"day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$ts"}},
                    "type": "$type"},
            "events": {"$sum": 1},
            "avg_value": {"$avg": "$value"},   # same unit inside one event type
        }},
        {"$project": {"events": 1, "avg_value": {"$round": ["$avg_value", 1]}}},
        {"$sort": {"_id.day": 1, "_id.type": 1}},
    ]
    return list(db["driving_events"].aggregate(pipeline))


# ------------------------------------------------------------------ A23
def a23_avg_fuel_by_model():
    """Average fuel level (% of the tank) per vehicle model, from all GPS positions.

    Positions are first reduced to one row per vehicle (sum and count), so the
    $lookup on vehicles only runs on ~1,500 documents, not on every position.
    Sum and count are kept (not an average of averages) so that every position
    weighs the same.
    """
    pipeline = [
        {"$group": {"_id": "$vehicle_id",
                    "fuel_sum": {"$sum": "$fuel_level"},
                    "n": {"$sum": 1}}},
        {"$lookup": {"from": "vehicles", "localField": "_id",
                     "foreignField": "_id", "as": "vehicle"}},
        {"$unwind": "$vehicle"},
        {"$group": {"_id": "$vehicle.model",
                    "brand": {"$first": "$vehicle.brand"},
                    "vehicles": {"$sum": 1},
                    "positions": {"$sum": "$n"},
                    "fuel_sum": {"$sum": "$fuel_sum"}}},
        {"$project": {"brand": 1, "vehicles": 1, "positions": 1,
                      "avg_fuel_level": {"$round": [{"$divide": ["$fuel_sum", "$positions"]}, 1]}}},
        {"$sort": {"avg_fuel_level": 1}},
    ]
    return list(db["positions"].aggregate(pipeline))


QUERIES = {
    "A21": (a21_silent_vehicles, ()),
    "A22": (a22_events_by_type_and_day, ()),
    "A23": (a23_avg_fuel_by_model, ()),
}


if __name__ == "__main__":
    for qid, (func, args) in QUERIES.items():
        rows = timed(qid, func, *args)
        print("  %d rows, first: %s" % (len(rows), rows[0] if rows else None))