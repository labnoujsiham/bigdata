"""
T1 to T10: transactional queries (Person B).

Run from the root of the repo (replica set running, data generated):
    python queries/btissam_queries/transactional.py

Every query is a function. The QUERIES dict at the bottom lets benchmark.py
call all of them the same way: QUERIES["T5"] = (function, arguments).

Write queries (T1, T2, T3, T8) can be run many times without breaking the data:
T2 uses an upsert on a fixed test mission, and T3 / T8 only update that mission.
"""
import os
import sys
import time
from datetime import datetime, timedelta, timezone

# allow "import config" even though this file lives in queries/btissam_queries/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pymongo import MongoClient

import config

client = MongoClient(config.MONGO_URI)
client.admin.command("ping")   # connect first, so the connection time is not counted
db = client[config.MONGO_DB]

TEST_MISSION_ID = "M_B_TEST"


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
    """New GPS position: insert it, then refresh the copy in vehicles.last_position
    (design decision 2 of docs/schema.md)."""
    now = datetime.now(timezone.utc)
    loc = {"type": "Point", "coordinates": [lon, lat]}
    db["positions"].insert_one({
        "vehicle_id": vehicle_id,
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
def t2_schedule_mission(vehicle_id="V0001", driver_id="D0001", depot="Casablanca"):
    """Schedule a new mission (fixed test id, so it can be run again and again)."""
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


# ------------------------------------------------------------------ T3
def t3_start_mission(mission_id=TEST_MISSION_ID):
    """A driver starts a mission: status in_progress + real start time."""
    result = db["missions"].update_one(
        {"_id": mission_id},
        {"$set": {"status": "in_progress",
                  "actual_start": datetime.now(timezone.utc)}},
    )
    return result.modified_count


# ------------------------------------------------------------------ T4
def t4_last_position(vehicle_id="V0001"):
    """Last known position of a vehicle: one read on the embedded copy, no search
    in the big positions collection."""
    return db["vehicles"].find_one(
        {"_id": vehicle_id}, {"plate": 1, "last_position": 1})


# ------------------------------------------------------------------ T5
def _t5_filter(vehicle_id, days):
    since = datetime.now(timezone.utc) - timedelta(days=days)
    return {"vehicle_id": vehicle_id, "ts": {"$gte": since}}


def t5_positions_period(vehicle_id, days=60):
    """All positions of a vehicle over the last N days, newest first.
    Uses the index { vehicle_id: 1, ts: -1 }."""
    cursor = db["positions"].find(
        _t5_filter(vehicle_id, days),
        {"ts": 1, "loc": 1, "speed": 1, "fuel_level": 1},
    ).sort("ts", -1)
    return list(cursor)


def explain_t5(vehicle_id, days=60):
    return explain_stats(db["positions"].find(_t5_filter(vehicle_id, days)).sort("ts", -1))


# ------------------------------------------------------------------ T6
def t6_find_by_plate(plate):
    """Find a vehicle by its plate. No index on plate in the base schema, so this is
    a collection scan: a good candidate for the with / without index comparison."""
    return db["vehicles"].find_one({"plate": plate})


def explain_t6(plate):
    return explain_stats(db["vehicles"].find({"plate": plate}))


# ------------------------------------------------------------------ T7
def t7_vehicles_with_equipment(depot="Casablanca", equipment="fridge_unit"):
    """Vehicles of a depot that have a given equipment (array field)."""
    cursor = db["vehicles"].find(
        {"depot": depot, "equipment": equipment},
        {"plate": 1, "type": 1, "equipment": 1},
    )
    return list(cursor)


# ------------------------------------------------------------------ T8
def t8_complete_mission(mission_id=TEST_MISSION_ID):
    """A mission ends: status completed + real end time."""
    result = db["missions"].update_one(
        {"_id": mission_id},
        {"$set": {"status": "completed",
                  "actual_end": datetime.now(timezone.utc)}},
    )
    return result.modified_count


# ------------------------------------------------------------------ T9
def t9_drivers_by_license(depot="Casablanca", category="CE"):
    """Drivers of a depot who hold a given licence category (array field)."""
    cursor = db["drivers"].find(
        {"depot": depot, "license_categories": category},
        {"name": 1, "license_categories": 1},
    )
    return list(cursor)


# ------------------------------------------------------------------ T10
def _t10_filter(driver_id):
    return {"driver_id": driver_id}


def t10_driver_missions(driver_id="D0001", limit=20):
    """The latest missions of a driver, sorted by planned start (newest first).
    Uses the index { driver_id: 1, planned_start: 1 } (read backwards)."""
    cursor = db["missions"].find(
        _t10_filter(driver_id),
        {"planned_start": 1, "status": 1, "depot": 1, "distance_km": 1},
    ).sort("planned_start", -1).limit(limit)
    return list(cursor)


def explain_t10(driver_id="D0001", limit=20):
    return explain_stats(
        db["missions"].find(_t10_filter(driver_id)).sort("planned_start", -1).limit(limit))


# ------------------------------------------------------------------ helpers
def _sample_vehicle_with_positions():
    doc = db["positions"].find_one({}, {"vehicle_id": 1})
    return doc["vehicle_id"] if doc else "V0001"


def _sample_plate():
    doc = db["vehicles"].find_one({}, {"plate": 1})
    return doc["plate"] if doc else "0-A-0"


def _sample_driver_with_missions():
    doc = db["missions"].find_one({"status": "completed"}, {"driver_id": 1})
    return doc["driver_id"] if doc else "D0001"


# For benchmark.py: id -> (function, arguments)
PV = _sample_vehicle_with_positions()
PLATE = _sample_plate()
DRIVER = _sample_driver_with_missions()

QUERIES = {
    "T1": (t1_record_position, ()),
    "T2": (t2_schedule_mission, ()),
    "T3": (t3_start_mission, ()),
    "T4": (t4_last_position, ()),
    "T5": (t5_positions_period, (PV,)),
    "T6": (t6_find_by_plate, (PLATE,)),
    "T7": (t7_vehicles_with_equipment, ()),
    "T8": (t8_complete_mission, ()),
    "T9": (t9_drivers_by_license, ()),
    "T10": (t10_driver_missions, (DRIVER,)),
}


if __name__ == "__main__":
    # keep the order T2, T3, T8: T3 and T8 work on the mission created by T2
    for qid in ["T1", "T2", "T3", "T4", "T5", "T6", "T7", "T8", "T9", "T10"]:
        func, args = QUERIES[qid]
        result = timed(qid, func, *args)
        if isinstance(result, list):
            print("   ->", len(result), "documents")
        else:
            print("   ->", result)
