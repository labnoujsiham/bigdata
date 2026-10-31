import sys
import time
from datetime import datetime, timedelta, timezone

from pymongo import ASCENDING

from common import db, timed


def run():
    # creates the TTL index (does nothing if it already exists)
    return db["positions"].create_index([("ts", ASCENDING)], expireAfterSeconds=60 * 86400)


def demo_expiry():
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
    timed("T13", run)
    print(db["positions"].index_information()["ts_1"])
    if "--ttl-demo" in sys.argv:
        demo_expiry()