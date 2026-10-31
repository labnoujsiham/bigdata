from common import db, timed


def run(depot="Casablanca"):
    cursor = db["missions"].find(
        {"depot": depot, "status": "in_progress"},
        {"vehicle_id": 1, "driver_id": 1, "actual_start": 1,
         "planned_end": 1, "distance_km": 1},
    )
    return list(cursor)


def explain(depot="Casablanca"):
    plan = db["missions"].find({"depot": depot, "status": "in_progress"}).explain()
    stats = plan["executionStats"]
    return stats["nReturned"], stats["totalKeysExamined"], stats["totalDocsExamined"]


if __name__ == "__main__":
    missions = timed("T15", run)
    print("Missions in progress in Casablanca:", len(missions))
    for m in missions[:3]:
        print(m)

    returned, keys, docs = explain()
    print("Returned: %d | index keys examined: %d | documents examined: %d"
          % (returned, keys, docs))