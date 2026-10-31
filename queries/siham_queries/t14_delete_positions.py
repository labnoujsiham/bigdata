from datetime import timedelta

from common import db, timed


def run(vehicle_id, start, end):
    result = db["positions"].delete_many({
        "vehicle_id": vehicle_id,
        "ts": {"$gte": start, "$lt": end},
    })
    return result.deleted_count


def count_in_period(vehicle_id, start, end):
    return db["positions"].count_documents({
        "vehicle_id": vehicle_id,
        "ts": {"$gte": start, "$lt": end},
    })


if __name__ == "__main__":
    # pick a real position to build a realistic example
    sample = db["positions"].find_one({"vehicle_id": {"$ne": "V_TTL_TEST"}})
    vehicle_id = sample["vehicle_id"]
    start = sample["ts"] - timedelta(days=1)
    end = sample["ts"] + timedelta(days=1)

    print("Vehicle:", vehicle_id)
    print("Period :", start, "->", end)
    print("Positions BEFORE:", count_in_period(vehicle_id, start, end))

    deleted = timed("T14", run, vehicle_id, start, end)

    print("Deleted:", deleted)
    print("Positions AFTER :", count_in_period(vehicle_id, start, end))