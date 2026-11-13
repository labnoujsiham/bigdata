"""
T1 to T15: transactional queries (Person B).

Run from the root of the repo (replica set running, data generated):
    python queries/btissam_queries/transactional.py

Every query is a function. The QUERIES dict at the bottom lets benchmark.py
call all of them the same way: QUERIES["T5"] = (function, arguments).

Write queries can be run many times without breaking the data:
T6 uses an upsert on a fixed test mission, T7 and T8 only update that mission,
T2 / T12 add test data that cleanup_t2() / cleanup_t12() remove (done in __main__),
T9 / T11 insert one document per run, T14 only touches a test device.

Field names assumed (check docs/schema.md and adjust the constants below if needed):
  positions:      vehicle_id, device_id, ts, loc, speed, heading, fuel_level, engine_state
  driving_events: driver_id, vehicle_id, ts, type, severity
  maintenance:    vehicle_id, date, type, cost
  vehicles:       _id, plate, type, model, depot, equipment, last_position

Run T13 last: it makes MongoDB delete the positions older than 60 days.
"""
import os
import sys
import time
from datetime import datetime, timedelta, timezone

# allow "import config" even though this file lives in queries/btissam_queries/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pymongo import MongoClient, ASCENDING
from pymongo.errors import OperationFailure

import config

client = MongoClient(config.MONGO_URI)
client.admin.command("ping")   # connect first, so the connection time is not counted
db = client[config.MONGO_DB]

TEST_MISSION_ID = "M_B_TEST"
TEST_DEVICE_ID = "DEV_FAULTY_TEST"     # T14 deletes the positions of this device only
BATCH_VEHICLE_ID = "V_BATCH_TEST"      # T2 inserts its 10 000 positions under this id
EQUIPMENT_PREFIX = "gps_backup_"       # T12 adds equipment with this prefix (test data)
SPEEDING_TYPE = "speeding"             # value of driving_events.type for a speeding event


def timed(label, func, *args):
    start = time.perf_counter()
    result = func(*args)
    elapsed_ms = (time.perf_counter() - start) * 1000
    print("%s: %.2f ms" % (label, elapsed_ms))
    return result


def explain_stats(cursor):
    """Short summary of an explain() for the with / without index comparison."""
    plan = cursor.explain()
    stats = plan["executionStats"]
    winning = plan["queryPlanner"]["winningPlan"]
    return {
        "stage": _scan_stage(winning),
        "returned": stats["nReturned"],
        "keys_examined": stats["totalKeysExamined"],
        "docs_examined": stats["totalDocsExamined"],
        "time_ms": stats["executionTimeMillis"],
    }


def _scan_stage(plan):
    # walks down the plan to find COLLSCAN or IXSCAN
    while plan:
        if plan.get("stage") in ("COLLSCAN", "IXSCAN"):
            return plan["stage"]
        plan = plan.get("inputStage")
    return "?"


# ------------------------------------------------------------------ T1
def t1_record_position(vehicle_id="V0001", lon=-7.59, lat=33.57,
                       speed=62.0, fuel_level=70.0):
    """T1: record one GPS position: insert it, then refresh the copy in
    vehicles.last_position (design decision 2 of docs/schema.md)."""
    now = datetime.now(timezone.utc)
    loc = {"type": "Point", "coordinates": [lon, lat]}
    db["positions"].insert_one({
        "vehicle_id": vehicle_id,
        "device_id": "DEV_" + vehicle_id,
        "ts": now,
        "loc": loc,
        "speed": speed,
        "heading": 90,
        "fuel_level": fuel_level,
        "engine_state": "on",
    })
    result = db["vehicles"].update_one(
        {"_id": vehicle_id},
        {"$set": {"last_position": {"ts": now, "loc": loc,
                                    "speed": speed, "fuel_level": fuel_level}}},
    )
    return result.modified_count


# ------------------------------------------------------------------ T2
def t2_bulk_load_positions(n=10000):
    """T2: load n positions in one batch (insert_many, unordered).
    The documents belong to a test vehicle; cleanup_t2() removes them."""
    start = datetime.now(timezone.utc) - timedelta(hours=n // 60 + 1)
    docs = []
    for i in range(n):
        docs.append({
            "vehicle_id": BATCH_VEHICLE_ID,
            "device_id": "DEV_" + BATCH_VEHICLE_ID,
            "ts": start + timedelta(minutes=i),
            "loc": {"type": "Point",
                    "coordinates": [-7.59 + (i % 100) * 0.001, 33.57 + (i % 100) * 0.001]},
            "speed": float(40 + i % 50),
            "heading": i % 360,
            "fuel_level": 80.0 - (i % 600) * 0.1,
            "engine_state": "on",
        })
    result = db["positions"].insert_many(docs, ordered=False)
    return len(result.inserted_ids)


def cleanup_t2():
    """Removes the positions inserted by T2 (not timed)."""
    return db["positions"].delete_many({"vehicle_id": BATCH_VEHICLE_ID}).deleted_count


# ------------------------------------------------------------------ T3
def t3_vehicle_sheet(vehicle_id="V0001"):
    """T3: full record of a vehicle (one read by _id)."""
    return db["vehicles"].find_one({"_id": vehicle_id})


# ------------------------------------------------------------------ T4
def t4_last_position(vehicle_id="V0001"):
    """T4: last known position of a vehicle: one read on the embedded copy, no search
    in the big positions collection."""
    return db["vehicles"].find_one(
        {"_id": vehicle_id}, {"plate": 1, "last_position": 1})


# ------------------------------------------------------------------ T5
def _t5_filter(vehicle_id, days, end=None):
    end = end or datetime.now(timezone.utc)
    return {"vehicle_id": vehicle_id,
            "ts": {"$gte": end - timedelta(days=days), "$lte": end}}


def t5_positions_day(vehicle_id, days=1, end=None):
    """T5: GPS trace of a vehicle over one day, oldest first. The day ends at `end`
    (default: now). benchmark.py passes the vehicle's last position time, so the
    query returns rows even when the generated data is not from today.
    Uses the index { vehicle_id: 1, ts: -1 }."""
    cursor = db["positions"].find(
        _t5_filter(vehicle_id, days, end),
        {"ts": 1, "loc": 1, "speed": 1, "fuel_level": 1},
    ).sort("ts", 1)
    return list(cursor)


def explain_t5(vehicle_id, days=1, end=None):
    return explain_stats(
        db["positions"].find(_t5_filter(vehicle_id, days, end)).sort("ts", 1))


# ------------------------------------------------------------------ T6
def t6_create_mission(vehicle_id="V0001", driver_id="D0001", depot="Casablanca"):
    """T6: create a mission (fixed test id, so it can be run again and again)."""
    start = datetime.now(timezone.utc) + timedelta(hours=2)
    doc = {
        "_id": TEST_MISSION_ID,
        "vehicle_id": vehicle_id,
        "driver_id": driver_id,
        "depot": depot,
        "origin": {"name": "Depot " + depot,
                   "loc": {"type": "Point", "coordinates": [-7.5898, 33.5731]}},
        "destination": {"name": "Client site",
                        "loc": {"type": "Point", "coordinates": [-6.8416, 34.0209]}},
        "planned_start": start,
        "planned_end": start + timedelta(hours=4),
        "actual_start": None,
        "actual_end": None,
        "distance_km": 90.0,
        "status": "planned",
    }
    result = db["missions"].replace_one({"_id": TEST_MISSION_ID}, doc, upsert=True)
    return "inserted" if result.upserted_id is not None else "replaced"


# ------------------------------------------------------------------ T7
def t7_start_then_end_mission(mission_id=TEST_MISSION_ID):
    """T7: start a mission (status in_progress + real start), then end it
    (status completed + real end). Two updates, in this order."""
    start = datetime.now(timezone.utc)
    db["missions"].update_one(
        {"_id": mission_id},
        {"$set": {"status": "in_progress", "actual_start": start}},
    )
    result = db["missions"].update_one(
        {"_id": mission_id},
        {"$set": {"status": "completed",
                  "actual_end": datetime.now(timezone.utc)}},
    )
    return result.modified_count


# ------------------------------------------------------------------ T8
def t8_assign_driver(mission_id=TEST_MISSION_ID, driver_id=None):
    """T8: assign a driver to a mission. The driver alternates between D0002 and
    D0003, so the update really changes the document at every run."""
    if driver_id is None:
        current = db["missions"].find_one({"_id": mission_id}, {"driver_id": 1})
        driver_id = "D0003" if current and current.get("driver_id") == "D0002" else "D0002"
    result = db["missions"].update_one(
        {"_id": mission_id},
        {"$set": {"driver_id": driver_id}},
    )
    return result.modified_count


# ------------------------------------------------------------------ T9
def t9_record_driving_event(driver_id="D0001", vehicle_id="V0001",
                            event_type=SPEEDING_TYPE):
    """T9: record a driving event (speeding, harsh braking, ...)."""
    result = db["driving_events"].insert_one({
        "driver_id": driver_id,
        "vehicle_id": vehicle_id,
        "ts": datetime.now(timezone.utc),
        "type": event_type,
        "severity": 2,
        "loc": {"type": "Point", "coordinates": [-7.59, 33.57]},
    })
    return str(result.inserted_id)


# ------------------------------------------------------------------ T10
def _t10_filter(driver_id, days):
    since = datetime.now(timezone.utc) - timedelta(days=days)
    return {"driver_id": driver_id, "planned_start": {"$gte": since}}


def t10_driver_missions_week(driver_id="D0001", days=7):
    """T10: missions of a driver over the last week, newest first.
    Uses the index { driver_id: 1, planned_start: 1 } (read backwards)."""
    cursor = db["missions"].find(
        _t10_filter(driver_id, days),
        {"planned_start": 1, "status": 1, "depot": 1, "distance_km": 1},
    ).sort("planned_start", -1)
    return list(cursor)


def explain_t10(driver_id="D0001", days=7):
    return explain_stats(
        db["missions"].find(_t10_filter(driver_id, days)).sort("planned_start", -1))


# ------------------------------------------------------------------ T11
def t11_record_maintenance(vehicle_id="V0001", maint_type="oil_change", cost=450.0):
    """T11: record a maintenance operation on a vehicle."""
    result = db["maintenance"].insert_one({
        "vehicle_id": vehicle_id,
        "date": datetime.now(timezone.utc),
        "type": maint_type,
        "cost": cost,
    })
    return str(result.inserted_id)


# ------------------------------------------------------------------ T12
def t12_add_equipment(vehicle_id="V0001", equipment=None):
    """T12: add a piece of equipment to a vehicle ($addToSet: no duplicate).
    The name is unique at every run, so the update really writes something.
    cleanup_t12() removes the test equipment."""
    equipment = equipment or "%s%d" % (EQUIPMENT_PREFIX, time.time_ns())
    result = db["vehicles"].update_one(
        {"_id": vehicle_id},
        {"$addToSet": {"equipment": equipment}},
    )
    return result.modified_count


def cleanup_t12(vehicle_id="V0001"):
    """Removes the test equipment added by T12 (not timed). Also removes the old
    'gps_tracker_backup' value added by the first version of T12."""
    result = db["vehicles"].update_one(
        {"_id": vehicle_id},
        {"$pull": {"equipment": {"$regex": "^(%s|gps_tracker_backup)" % EQUIPMENT_PREFIX}}},
    )
    return result.modified_count


# ------------------------------------------------------------------ T13
def t13_ttl_positions(days=60):
    """T13: automatic expiry of positions after 60 days (TTL index on ts).
    MongoDB deletes the documents itself, in the background (about once a minute).
    If a plain index on ts already exists (ts_1), it is converted to a TTL index
    with collMod, because MongoDB refuses two indexes on the same key.
    Run it last: it deletes the positions older than 60 days."""
    seconds = days * 24 * 3600
    try:
        return db["positions"].create_index(
            [("ts", ASCENDING)], expireAfterSeconds=seconds, name="ts_ttl_60d")
    except OperationFailure:
        db.command({"collMod": "positions",
                    "index": {"keyPattern": {"ts": 1}, "expireAfterSeconds": seconds}})
        return "ts_1 converted to TTL"


# ------------------------------------------------------------------ T14
def t14_delete_device_positions(device_id=TEST_DEVICE_ID, days=7):
    """T14: delete the positions of a faulty GPS device over a period.
    The default device is a test one, so real data is not touched."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    result = db["positions"].delete_many(
        {"device_id": device_id, "ts": {"$gte": since}})
    return result.deleted_count


# ------------------------------------------------------------------ T15
def t15_missions_in_progress(depot="Casablanca"):
    """T15: missions in progress of a depot. Uses the index { depot, status }."""
    cursor = db["missions"].find(
        {"depot": depot, "status": "in_progress"},
        {"vehicle_id": 1, "driver_id": 1, "planned_start": 1, "actual_start": 1},
    )
    return list(cursor)


# ------------------------------------------------------------------ helpers
def _sample_vehicle_with_positions():
    doc = db["positions"].find_one({}, {"vehicle_id": 1})
    return doc["vehicle_id"] if doc else "V0001"


def _sample_driver_with_missions():
    doc = db["missions"].find_one({"status": "completed"}, {"driver_id": 1})
    return doc["driver_id"] if doc else "D0001"


def _last_position_time(vehicle_id):
    doc = db["positions"].find_one({"vehicle_id": vehicle_id},
                                   sort=[("ts", -1)], projection={"ts": 1})
    if not doc:
        return None
    ts = doc["ts"]
    # pymongo returns naive datetimes (UTC): make it timezone-aware
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


# For benchmark.py: id -> (function, arguments)
PV = _sample_vehicle_with_positions()
PV_END = _last_position_time(PV)
DRIVER = _sample_driver_with_missions()

QUERIES = {
    "T1": (t1_record_position, ()),
    "T2": (t2_bulk_load_positions, ()),
    "T3": (t3_vehicle_sheet, ()),
    "T4": (t4_last_position, ()),
    "T5": (t5_positions_day, (PV, 1, PV_END)),
    "T6": (t6_create_mission, ()),
    "T7": (t7_start_then_end_mission, ()),
    "T8": (t8_assign_driver, ()),
    "T9": (t9_record_driving_event, ()),
    "T10": (t10_driver_missions_week, (DRIVER,)),
    "T11": (t11_record_maintenance, ()),
    "T12": (t12_add_equipment, ()),
    "T13": (t13_ttl_positions, ()),
    "T14": (t14_delete_device_positions, ()),
    "T15": (t15_missions_in_progress, ()),
}


if __name__ == "__main__":
    # python queries/btissam_queries/transactional.py          -> all queries
    # python queries/btissam_queries/transactional.py T5 T6    -> only these ones
    # Keep the order T6, T7, T8 when you run them one by one: T7 and T8 work on the
    # mission created by T6.
    wanted = [a.upper() for a in sys.argv[1:]] or list(QUERIES)
    for qid in wanted:
        if qid not in QUERIES:
            print("%s: unknown query (choose from %s)" % (qid, ", ".join(QUERIES)))
            continue
        func, args = QUERIES[qid]
        result = timed(qid, func, *args)
        if qid == "T2":
            print("   ->", result, "positions inserted, cleaned:", cleanup_t2())
        elif qid == "T12":
            print("   ->", result, "(test equipment removed:", cleanup_t12(), ")")
        elif isinstance(result, list):
            print("   ->", len(result), "documents")
        else:
            print("   ->", result)