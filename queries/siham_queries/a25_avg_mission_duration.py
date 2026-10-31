from common import db, timed


def run():
    pipeline = [
        # 1. only missions that really finished
        {"$match": {"status": "completed"}},
        # 2. one row per depot: average duration in minutes + number of missions
        {"$group": {
            "_id": "$depot",
            "avg_duration_min": {"$avg": {
                "$divide": [{"$subtract": ["$actual_end", "$actual_start"]}, 60000]
            }},
            "missions": {"$sum": 1},
        }},
        # 3. round the average to 1 decimal
        {"$project": {
            "avg_duration_min": {"$round": ["$avg_duration_min", 1]},
            "missions": 1,
        }},
        # 4. alphabetical order
        {"$sort": {"_id": 1}},
    ]
    return list(db["missions"].aggregate(pipeline))


if __name__ == "__main__":
    rows = timed("A25", run)
    for r in rows:
        print("%-12s %6.1f min  (%d missions)" % (r["_id"], r["avg_duration_min"], r["missions"]))