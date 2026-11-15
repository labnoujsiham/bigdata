
from datetime import timedelta

from common import db, timed


# ------------------------------------------------------------------ A21
def a21_silent_vehicles():
    
    newest = list(db["vehicles"].aggregate(
        [{"$group": {"_id": None, "ts": {"$max": "$last_position.ts"}}}]))[0]["ts"]
    cutoff = newest - timedelta(hours=1)
    pipeline = [
        
        {"$match": {"$or": [{"last_position": None},
                            {"last_position.ts": {"$lt": cutoff}}]}},
        {"$group": {"_id": "$depot", "silent_vehicles": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    return list(db["vehicles"].aggregate(pipeline))


# ------------------------------------------------------------------ A22
def a22_events_by_type_and_day():
    
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