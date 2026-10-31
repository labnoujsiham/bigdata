from common import db, timed


def run(vehicle_id="V0001", equipment="temperature_sensor"):
    result = db["vehicles"].update_one(
        {"_id": vehicle_id},
        {"$addToSet": {"equipment": equipment}},
    )
    return result.modified_count


if __name__ == "__main__":
    changed = timed("T12", run)
    print("vehicle modified (1 = added, 0 = already there):", changed)
    print(db["vehicles"].find_one({"_id": "V0001"}, {"equipment": 1}))