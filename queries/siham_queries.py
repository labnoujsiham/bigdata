import os
import sys
import time
from datetime import datetime, timezone
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient, ASCENDING

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import MongoClient

import config

client = MongoClient(config.MONGO_URI)
client.admin.command("ping")   # connect first, so the connection time is not counted
db = client[config.MONGO_DB]


def timed(label, func, *args):
    start = time.perf_counter()
    result = func(*args)
    elapsed_ms = (time.perf_counter() - start) * 1000
    print("%s: %.2f ms" % (label, elapsed_ms))
    return result

#t11
def t11_record_maintenance(vehicle_id, mileage_km, maintenance_type, parts, cost):
    doc = {
        "vehicle_id": vehicle_id,
        "date": datetime.now(timezone.utc),
        "mileage_km": mileage_km,
        "type": maintenance_type,
        "parts": parts,
        "cost": cost,
    }
    result = db["maintenances"].insert_one(doc)
    return result.inserted_id

#t12
def t12_add_equipment(vehicle_id, equipment):
    result = db["vehicles"].update_one(
        {"_id": vehicle_id},
        {"$addToSet": {"equipment": equipment}},
    )
    return result.modified_count

#t13
def t13_ensure_ttl_index():
    # does nothing if the index already exists with the same settings
    return db["positions"].create_index([("ts", ASCENDING)], expireAfterSeconds=60 * 86400)


def t13_demo_expiry():
    old_ts = datetime.now(timezone.utc) - timedelta(days=61)
    result = db["positions"].insert_one({
        "vehicle_id": "V_TTL_TEST",
        "ts": old_ts,
        "loc": {"type": "Point", "coordinates": [-7.59, 33.57]},
        "speed": 0.0,
        "heading": 0,
        "fuel_level": 50.0,
        "engine_state": "off",
    })
    doc_id = result.inserted_id
    print("Inserted a position dated 61 days ago. Waiting for MongoDB to delete it...")
    for i in range(18):
        time.sleep(5)
        if db["positions"].count_documents({"_id": doc_id}) == 0:
            print("Deleted automatically after about %d seconds" % ((i + 1) * 5))
            return True
    print("Still there after 90 seconds")
    return False


if __name__ == "__main__":
    new_id = timed("T11", t11_record_maintenance,
                   "V0001", 150000, "oil_change", ["oil filter", "air filter"], 850.0)
    print("inserted:", new_id)
#t12
    changed = timed("T12", t12_add_equipment, "V0001", "temperature_sensor")
    print("vehicle modified (1 = added, 0 = already there):", changed)
    print(db["vehicles"].find_one({"_id": "V0001"}, {"equipment": 1}))

#t13
    timed("T13", t13_ensure_ttl_index)
    print(db["positions"].index_information()["ts_1"])

    if "--ttl-demo" in sys.argv:
        t13_demo_expiry()