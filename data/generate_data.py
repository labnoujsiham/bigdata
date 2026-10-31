"""
Data generator for the fleet telematics project (MongoDB).

Run from the root of the repo (with the replica set running):
    python data/generate_data.py

WARNING: it DROPS and recreates the 6 project collections in the database.
"""
import math
import os
import random
import sys
from datetime import datetime, timedelta, timezone

# allow "import config" even though this file lives in data/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from faker import Faker
from pymongo import MongoClient, ASCENDING, DESCENDING, GEOSPHERE

import config

# ---------------------------------------------------------------- settings
SEED = 42
N_VEHICLES = 1500
N_DRIVERS = 2000
N_MISSIONS = 10000
N_EVENTS = 15000
N_MAINTENANCES = 2000
N_TRACED_MISSIONS = 2500   # missions that get a full GPS trace (30 s steps)
N_STOP_MISSIONS = 200      # traced missions with an unplanned stop (S28)
N_THEFT_MISSIONS = 15      # of those, with a suspicious fuel drop (S30)
N_JUMP_MISSIONS = 40       # traced missions with a GPS jump (S29)
N_LIVE_VEHICLES = 1200     # vehicles with a very recent position (A21)
N_IN_PROGRESS = 200
N_PLANNED = 200

random.seed(SEED)
fake = Faker("fr_FR")
Faker.seed(SEED)
now = datetime.now(timezone.utc)

DEPOTS = {
    "Casablanca": (33.5731, -7.5898),
    "Rabat": (34.0209, -6.8416),
    "Tanger": (35.7595, -5.8340),
    "Marrakech": (31.6295, -7.9811),
    "Fes": (34.0331, -5.0003),
    "Agadir": (30.4278, -9.5981),
}
DEPOT_NAMES = list(DEPOTS.keys())

VEHICLE_TYPES = {
    "Truck": (8000, 24000),
    "Van": (800, 1800),
    "Pickup": (500, 1200),
    "Refrigerated": (3000, 12000),
}
BRANDS = {
    "Volvo": ["FH", "FM", "FL"],
    "Mercedes": ["Actros", "Sprinter", "Atego"],
    "Renault": ["Master", "Trafic", "T High"],
    "Iveco": ["Daily", "Eurocargo", "S-Way"],
    "Ford": ["Transit", "Ranger"],
}
EQUIPMENTS = ["gps", "dashcam", "tail_lift", "fridge_unit", "roof_rack",
              "tow_hook", "tachograph", "alarm"]
LICENSES = ["B", "C", "C1", "CE", "D"]
MAINT_TYPES = ["oil_change", "tires", "brakes", "inspection", "engine", "battery"]
PARTS = ["oil filter", "air filter", "brake pads", "tire", "battery",
         "belt", "spark plug", "clutch", "shock absorber", "headlight"]
EVENT_TYPES = ["speeding", "harsh_braking", "long_idle", "out_of_zone"]
EVENT_WEIGHTS = [0.4, 0.3, 0.2, 0.1]


# ---------------------------------------------------------------- helpers
def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = p2 - p1
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlam / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def bearing_deg(lat1, lon1, lat2, lon2):
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlam = math.radians(lon2 - lon1)
    x = math.sin(dlam) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dlam)
    return int((math.degrees(math.atan2(x, y)) + 360) % 360)


def point(lat, lon):
    # GeoJSON: [longitude, latitude]
    return {"type": "Point", "coordinates": [round(lon, 6), round(lat, 6)]}


def random_near(lat, lon, spread):
    return lat + random.uniform(-spread, spread), lon + random.uniform(-spread, spread)


def insert_in_batches(collection, docs, batch_size=10000):
    for i in range(0, len(docs), batch_size):
        collection.insert_many(docs[i:i + batch_size])


# ---------------------------------------------------------------- vehicles
print("Generating vehicles and drivers...")
vehicles = []
for i in range(1, N_VEHICLES + 1):
    vtype = random.choice(list(VEHICLE_TYPES.keys()))
    low, high = VEHICLE_TYPES[vtype]
    brand = random.choice(list(BRANDS.keys()))
    vehicles.append({
        "_id": "V%04d" % i,
        "plate": "%d-%s-%d" % (random.randint(1000, 99999),
                               random.choice("ABDEHJKTW"),
                               random.randint(1, 80)),
        "type": vtype,
        "brand": brand,
        "model": random.choice(BRANDS[brand]),
        "year": random.randint(2012, 2025),
        "capacity": (random.randint(low, high) // 100) * 100,
        "depot": random.choice(DEPOT_NAMES),
        "equipment": random.sample(EQUIPMENTS, random.randint(2, 5)),
        "features": {
            "fuel_type": random.choice(["diesel", "diesel", "diesel", "hybrid"]),
            "axles": random.choice([2, 2, 3, 4]),
            "tank_l": random.choice([80, 120, 200, 300]),
        },
        "last_position": None,
    })

# ---------------------------------------------------------------- drivers
drivers = []
for i in range(1, N_DRIVERS + 1):
    drivers.append({
        "_id": "D%04d" % i,
        "name": fake.name(),
        "license_categories": sorted(random.sample(LICENSES, random.randint(1, 3))),
        "hire_date": now - timedelta(days=random.randint(30, 365 * 18)),
        "depot": random.choice(DEPOT_NAMES),
    })

vehicles_by_depot = {}
drivers_by_depot = {}
for name in DEPOT_NAMES:
    vehicles_by_depot[name] = []
    drivers_by_depot[name] = []
for v in vehicles:
    vehicles_by_depot[v["depot"]].append(v["_id"])
driver_depot = {}
driver_ids = []
driver_weights = []
for d in drivers:
    drivers_by_depot[d["depot"]].append(d["_id"])
    driver_depot[d["_id"]] = d["depot"]
    driver_ids.append(d["_id"])
    # some drivers are much "worse" than others (makes the top 10 meaningful)
    driver_weights.append(random.random() ** 3 + 0.02)

# ---------------------------------------------------------------- missions
print("Generating missions...")
mission_routes = {}   # mission id -> (o_lat, o_lon, d_lat, d_lon, avg_speed)
late_ids = set()


def make_route(depot):
    lat0, lon0 = DEPOTS[depot]
    while True:
        o_lat, o_lon = random_near(lat0, lon0, 0.05)
        d_lat, d_lon = random_near(lat0, lon0, 0.3)
        dist = haversine_km(o_lat, o_lon, d_lat, d_lon)
        if 5 <= dist <= 45:
            return o_lat, o_lon, d_lat, d_lon, round(dist, 1)


def build_mission(index, kind):
    depot = random.choice(DEPOT_NAMES)
    o_lat, o_lon, d_lat, d_lon, dist = make_route(depot)
    avg_speed = random.uniform(40, 65)
    duration = timedelta(minutes=dist / avg_speed * 60)
    mission_id = "M%05d" % index

    planned_start = None
    planned_end = None
    actual_start = None
    actual_end = None
    status = "completed"

    if kind == "past":
        planned_start = now - timedelta(seconds=random.randint(3 * 3600, 30 * 86400))
        planned_end = planned_start + duration + timedelta(minutes=10)
        if random.random() < 0.03:
            status = "cancelled"
        else:
            actual_start = planned_start + timedelta(minutes=random.uniform(0, 3))
            if random.random() < 0.25:
                late_ids.add(mission_id)
                actual_end = actual_start + duration + timedelta(minutes=random.uniform(15, 90))
            else:
                actual_end = actual_start + duration * random.uniform(0.85, 1.05)
    elif kind == "in_progress":
        status = "in_progress"
        actual_start = now - duration * random.uniform(0.2, 0.8)
        planned_start = actual_start - timedelta(minutes=random.uniform(0, 3))
        planned_end = planned_start + duration + timedelta(minutes=10)
    else:
        status = "planned"
        planned_start = now + timedelta(seconds=random.randint(3600, 48 * 3600))
        planned_end = planned_start + duration + timedelta(minutes=10)

    mission_routes[mission_id] = (o_lat, o_lon, d_lat, d_lon, avg_speed)
    return {
        "_id": mission_id,
        "vehicle_id": random.choice(vehicles_by_depot[depot]),
        "driver_id": random.choice(drivers_by_depot[depot]),
        "depot": depot,
        "origin": {"name": fake.street_name(), "loc": point(o_lat, o_lon)},
        "destination": {"name": fake.street_name(), "loc": point(d_lat, d_lon)},
        "planned_start": planned_start,
        "planned_end": planned_end,
        "actual_start": actual_start,
        "actual_end": actual_end,
        "distance_km": dist,
        "status": status,
    }


missions = []
n_past = N_MISSIONS - N_IN_PROGRESS - N_PLANNED
for i in range(1, N_MISSIONS + 1):
    if i <= n_past:
        kind = "past"
    elif i <= n_past + N_IN_PROGRESS:
        kind = "in_progress"
    else:
        kind = "planned"
    missions.append(build_mission(i, kind))

# ---------------------------------------------------------------- positions
print("Generating GPS positions (this is the big one)...")
positions = []


def trace_for_mission(m, with_stop, with_theft, with_jump):
    o_lat, o_lon, d_lat, d_lon, avg_speed = mission_routes[m["_id"]]
    start = m["actual_start"]
    n = int((m["distance_km"] / avg_speed) * 3600 // 30)
    heading = bearing_deg(o_lat, o_lon, d_lat, d_lon)

    steps = []   # [lat, lon, speed, engine]
    for i in range(n + 1):
        t = i / n
        steps.append([
            o_lat + (d_lat - o_lat) * t,
            o_lon + (d_lon - o_lon) * t,
            max(0.0, random.gauss(avg_speed, 8)),
            "on",
        ])

    stop_first = -1
    stop_last = -1
    if with_stop:
        mid = n // 2
        stop_count = random.randint(70, 120)      # 35 to 60 minutes
        s_lat = steps[mid][0]
        s_lon = steps[mid][1]
        stop_steps = []
        for j in range(stop_count):
            stop_steps.append([s_lat + random.uniform(-0.00002, 0.00002),
                               s_lon + random.uniform(-0.00002, 0.00002),
                               0.0, "off"])
        steps = steps[:mid + 1] + stop_steps + steps[mid + 1:]
        stop_first = mid + 1
        stop_last = mid + stop_count

    if with_jump:
        k = random.randint(3, len(steps) - 4)
        steps[k][0] += random.choice([-1, 1]) * random.uniform(0.05, 0.1)

    fuel = random.uniform(55, 100)
    for idx, s in enumerate(steps):
        if s[3] == "on":
            fuel -= 0.06
        if with_theft and stop_first <= idx < stop_first + 16:
            fuel -= 25.0 / 16           # 25 points stolen in 8 minutes
        positions.append({
            "vehicle_id": m["vehicle_id"],
            "ts": start + timedelta(seconds=30 * idx),
            "loc": point(s[0], s[1]),
            "speed": round(s[2], 1),
            "heading": (heading + random.randint(-3, 3)) % 360,
            "fuel_level": round(max(fuel, 1.0), 1),
            "engine_state": s[3],
        })

    # keep the mission consistent with its trace
    m["actual_end"] = start + timedelta(seconds=30 * (len(steps) - 1))


candidates = []
for m in missions:
    if m["status"] == "completed" and m["_id"] not in late_ids:
        candidates.append(m)
random.shuffle(candidates)
traced = []
busy = {}   # vehicle id -> list of (start, end) already traced
for m in candidates:
    if len(traced) == N_TRACED_MISSIONS:
        break
    vid = m["vehicle_id"]
    start = m["actual_start"]
    end = m["actual_end"] + timedelta(minutes=70)   # room for a possible stop
    overlap = False
    if vid in busy:
        for other_start, other_end in busy[vid]:
            if start < other_end and other_start < end:
                overlap = True
    if overlap:
        continue
    if vid not in busy:
        busy[vid] = []
    busy[vid].append((start, end))
    traced.append(m)
stop_missions = random.sample(traced, N_STOP_MISSIONS)
stop_ids = set()
for m in stop_missions:
    stop_ids.add(m["_id"])
theft_ids = set()
for m in random.sample(stop_missions, N_THEFT_MISSIONS):
    theft_ids.add(m["_id"])
jump_ids = set()
for m in random.sample(traced, N_JUMP_MISSIONS):
    jump_ids.add(m["_id"])

for m in traced:
    trace_for_mission(m,
                      m["_id"] in stop_ids,
                      m["_id"] in theft_ids,
                      m["_id"] in jump_ids)

# a few very recent positions (so that A21 "no position for 1 h" is meaningful)
for v in random.sample(vehicles, N_LIVE_VEHICLES):
    lat0, lon0 = DEPOTS[v["depot"]]
    lat, lon = random_near(lat0, lon0, 0.2)
    speed = random.uniform(0, 70)
    if speed < 5:
        speed = 0.0
    positions.append({
        "vehicle_id": v["_id"],
        "ts": now - timedelta(seconds=random.randint(0, 50 * 60)),
        "loc": point(lat, lon),
        "speed": round(speed, 1),
        "heading": random.randint(0, 359),
        "fuel_level": round(random.uniform(20, 100), 1),
        "engine_state": "on" if speed > 0 else "off",
    })

# latest known position stored inside each vehicle
last_pos = {}
for p in positions:
    vid = p["vehicle_id"]
    if vid not in last_pos:
        last_pos[vid] = p
    elif p["ts"] > last_pos[vid]["ts"]:
        last_pos[vid] = p
for v in vehicles:
    if v["_id"] in last_pos:
        p = last_pos[v["_id"]]
        v["last_position"] = {"ts": p["ts"], "loc": p["loc"],
                              "speed": p["speed"], "fuel_level": p["fuel_level"]}

# ---------------------------------------------------------------- driving events
print("Generating driving events and maintenances...")
events = []
for i in range(N_EVENTS):
    driver_id = random.choices(driver_ids, weights=driver_weights)[0]
    depot = driver_depot[driver_id]
    etype = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    if etype == "speeding":
        value = round(random.uniform(85, 140), 1)      # km/h
    elif etype == "harsh_braking":
        value = round(random.uniform(3, 9), 1)         # m/s2
    elif etype == "long_idle":
        value = round(random.uniform(10, 60), 1)       # minutes
    else:
        value = round(random.uniform(0.5, 15), 1)      # km outside the zone
    lat, lon = random_near(DEPOTS[depot][0], DEPOTS[depot][1], 0.3)
    events.append({
        "vehicle_id": random.choice(vehicles_by_depot[depot]),
        "driver_id": driver_id,
        "ts": now - timedelta(seconds=random.randint(0, 30 * 86400)),
        "type": etype,
        "value": value,
        "loc": point(lat, lon),
    })

# ---------------------------------------------------------------- maintenances
maintenances = []
odometer_start = {}
for i in range(N_MAINTENANCES):
    vid = random.choice(vehicles)["_id"]
    if vid not in odometer_start:
        odometer_start[vid] = random.randint(20000, 300000)
    days_ago = random.randint(0, 540)
    maintenances.append({
        "vehicle_id": vid,
        "date": now - timedelta(days=days_ago),
        "mileage_km": odometer_start[vid] + (540 - days_ago) * 110,
        "type": random.choice(MAINT_TYPES),
        "parts": random.sample(PARTS, random.randint(1, 4)),
        "cost": round(random.uniform(200, 15000), 2),
    })

# ---------------------------------------------------------------- write to MongoDB
print("Connecting to MongoDB...")
client = MongoClient(config.MONGO_URI)
db = client[config.MONGO_DB]

names = ["vehicles", "drivers", "positions", "missions", "driving_events", "maintenances"]
for name in names:
    db.drop_collection(name)

print("Inserting documents...")
insert_in_batches(db["vehicles"], vehicles)
insert_in_batches(db["drivers"], drivers)
insert_in_batches(db["missions"], missions)
insert_in_batches(db["driving_events"], events)
insert_in_batches(db["maintenances"], maintenances)
insert_in_batches(db["positions"], positions)

print("Creating indexes...")
db["positions"].create_index([("vehicle_id", ASCENDING), ("ts", DESCENDING)])
db["positions"].create_index([("loc", GEOSPHERE)])
db["positions"].create_index([("ts", ASCENDING)], expireAfterSeconds=60 * 86400)
db["missions"].create_index([("driver_id", ASCENDING), ("planned_start", ASCENDING)])
db["missions"].create_index([("depot", ASCENDING), ("status", ASCENDING)])
db["driving_events"].create_index([("driver_id", ASCENDING), ("type", ASCENDING)])
db["maintenances"].create_index([("vehicle_id", ASCENDING), ("date", DESCENDING)])

print("")
print("Done. Documents per collection:")
total = 0
for name in names:
    count = db[name].count_documents({})
    total += count
    print("  %-15s %d" % (name, count))
print("  %-15s %d" % ("TOTAL", total))