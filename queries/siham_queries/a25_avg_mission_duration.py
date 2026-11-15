from common import db, timed


def run():
    pipeline = [
        {"$match": {"status": "completed"}},
        {"$group": {
            "_id": "$depot",
            "avg_duration_min": {"$avg": {
                "$divide": [{"$subtract": ["$actual_end", "$actual_start"]}, 60000]
            }},
            "missions": {"$sum": 1},
        }},
        {"$project": {
            "avg_duration_min": {"$round": ["$avg_duration_min", 1]},
            "missions": 1,
        }},
        {"$sort": {"_id": 1}},
    ]
    return list(db["missions"].aggregate(pipeline))


if __name__ == "__main__":
    rows = timed("A25", run)
    for r in rows:
        print("%-12s %6.1f min  (%d missions)" % (r["_id"], r["avg_duration_min"], r["missions"]))