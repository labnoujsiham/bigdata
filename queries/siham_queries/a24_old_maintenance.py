from datetime import datetime, timedelta, timezone

from common import db, timed


def run(days=183):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    pipeline = [
        {"$group": {"_id": "$vehicle_id", "last_maintenance": {"$max": "$date"}}},
        {"$match": {"last_maintenance": {"$lt": cutoff}}},
        {"$sort": {"last_maintenance": 1}},
    ]
    return list(db["maintenances"].aggregate(pipeline))


def vehicles_never_maintained():
    maintained = db["maintenances"].distinct("vehicle_id")
    return db["vehicles"].count_documents({"_id": {"$nin": maintained}})


if __name__ == "__main__":
    rows = timed("A24", run)
    print("Vehicles whose last maintenance is more than 6 months old:", len(rows))
    for r in rows[:5]:
        print(r)
    print("Vehicles with no maintenance record at all:", vehicles_never_maintained())