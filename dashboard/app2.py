"""
Streamlit demo of T11, T12 and T13 (maintenance, equipment, TTL expiry).

Run from the root of the repo (replica set running, data generated, .env present):
    streamlit run dashboard/app_t11_t13.py

  T11  record a maintenance           -> insert into `maintenances`
  T12  add an equipment to a vehicle  -> $addToSet on vehicles.equipment
  T13  automatic expiry of positions  -> TTL index on positions.ts (60 days)
"""
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from pymongo import ASCENDING, MongoClient

import config

TTL_SECONDS = 60 * 86400          # 60 days
TTL_TEST_VEHICLE = "V_TTL_TEST"   # test position used by the T13 demo
MAINTENANCE_TYPES = ["oil_change", "brakes", "tires", "inspection", "battery", "engine"]

st.set_page_config(page_title="T11-T13 demo", layout="wide")
st.title("Fleet telematics: T11, T12, T13")


@st.cache_resource
def get_db():
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client[config.MONGO_DB]


try:
    db = get_db()
except Exception as exc:
    st.error("Cannot connect to MongoDB: %s" % exc)
    st.stop()


def timed(func, *args, **kwargs):
    """Runs func and returns (result, milliseconds)."""
    start = time.perf_counter()
    result = func(*args, **kwargs)
    return result, (time.perf_counter() - start) * 1000


def vehicle_exists(vehicle_id):
    return db["vehicles"].count_documents({"_id": vehicle_id}, limit=1) == 1


# ------------------------------------------------------------------ queries
def t11_record_maintenance(vehicle_id, mileage_km, maintenance_type, parts, cost):
    result = db["maintenances"].insert_one({
        "vehicle_id": vehicle_id,
        "date": datetime.now(timezone.utc),
        "mileage_km": mileage_km,
        "type": maintenance_type,
        "parts": parts,
        "cost": cost,
    })
    return result.inserted_id


def t12_add_equipment(vehicle_id, equipment):
    result = db["vehicles"].update_one(
        {"_id": vehicle_id}, {"$addToSet": {"equipment": equipment}})
    return result.modified_count


def t13_create_ttl_index():
    # does nothing if the index already exists with the same settings
    return db["positions"].create_index([("ts", ASCENDING)], expireAfterSeconds=TTL_SECONDS)


def ttl_index_info():
    """The index on ts alone, as MongoDB reports it (None if there is none)."""
    for name, info in db["positions"].index_information().items():
        if info["key"] == [("ts", 1)]:
            return name, info.get("expireAfterSeconds")
    return None, None


# ------------------------------------------------------------------ header
tab11, tab12, tab13 = st.tabs(["T11 Maintenance", "T12 Equipment", "T13 TTL expiry"])

# ------------------------------------------------------------------ T11
with tab11:
    st.subheader("T11 - Record a maintenance")
    st.caption("One document inserted in the `maintenances` collection. "
               "The parts are an embedded array, because they are always read with their maintenance.")
    left, right = st.columns(2)
    with left:
        with st.form("t11_form"):
            vehicle_id = st.text_input("Vehicle id", "V0001").strip()
            maintenance_type = st.selectbox("Type", MAINTENANCE_TYPES)
            mileage = st.number_input("Mileage (km)", min_value=0, value=150000, step=1000)
            parts_text = st.text_input("Parts (separated by commas)", "oil filter, air filter")
            cost = st.number_input("Cost", min_value=0.0, value=850.0, step=50.0)
            go = st.form_submit_button("Record maintenance")
        if go:
            parts = [p.strip() for p in parts_text.split(",") if p.strip()]
            if not vehicle_exists(vehicle_id):
                st.error("Vehicle %s does not exist." % vehicle_id)
            else:
                new_id, ms = timed(t11_record_maintenance, vehicle_id, int(mileage),
                                   maintenance_type, parts, float(cost))
                st.success("Inserted %s in %.2f ms" % (new_id, ms))
    with right:
        shown = st.text_input("Show the maintenances of vehicle", vehicle_id, key="t11_show").strip()
        rows = list(db["maintenances"].find({"vehicle_id": shown}, {"_id": 0})
                    .sort("date", -1).limit(50))
        if rows:
            df = pd.DataFrame(rows)
            st.dataframe(df.head(10), hide_index=True)
            st.plotly_chart(px.bar(df.sort_values("date"), x="date", y="cost", color="type",
                                   title="Maintenance cost over time (%s)" % shown),
                            use_container_width=True)
        else:
            st.info("No maintenance for %s." % shown)

# ------------------------------------------------------------------ T12
with tab12:
    st.subheader("T12 - Add an equipment to a vehicle")
    st.caption("`$addToSet` adds the value only if it is not already in the array "
               "(like a set: no duplicates). modified = 1 means added, 0 means it was already there.")
    left, right = st.columns(2)
    with left:
        with st.form("t12_form"):
            v12 = st.text_input("Vehicle id", "V0001").strip()
            equipment = st.text_input("Equipment", "temperature_sensor").strip()
            c1, c2 = st.columns(2)
            add = c1.form_submit_button("Add equipment")
            remove = c2.form_submit_button("Remove it (clean up the demo)")
        if add or remove:
            if not equipment:
                st.error("Type an equipment name.")
            elif not vehicle_exists(v12):
                st.error("Vehicle %s does not exist." % v12)
            elif add:
                changed, ms = timed(t12_add_equipment, v12, equipment)
                if changed:
                    st.success("Added in %.2f ms (modified = 1)" % ms)
                else:
                    st.warning("Already there, nothing changed (modified = 0), %.2f ms" % ms)
            else:
                db["vehicles"].update_one({"_id": v12}, {"$pull": {"equipment": equipment}})
                st.info("Removed %s from %s." % (equipment, v12))
    with right:
        doc = db["vehicles"].find_one({"_id": v12}, {"plate": 1, "depot": 1, "equipment": 1})
        if doc:
            st.write("**%s** (%s, depot %s)" % (doc["_id"], doc.get("plate", "?"), doc.get("depot", "?")))
            st.write(doc.get("equipment", []))
        counts = list(db["vehicles"].aggregate([
            {"$unwind": "$equipment"},
            {"$group": {"_id": "$equipment", "vehicles": {"$sum": 1}}},
            {"$sort": {"vehicles": -1}}]))
        if counts:
            cdf = pd.DataFrame(counts).rename(columns={"_id": "equipment"})
            st.plotly_chart(px.bar(cdf, x="equipment", y="vehicles",
                                   title="Vehicles per equipment (whole fleet)"),
                            use_container_width=True)

# ------------------------------------------------------------------ T13
with tab13:
    st.subheader("T13 - Positions expire automatically after 60 days")
    st.caption("A TTL index on `positions.ts`: MongoDB deletes the old documents itself, in the "
               "background. Its cleaner wakes up about once a minute, so a deletion is not instant.")

    name, seconds = ttl_index_info()
    left, right = st.columns(2)
    with left:
        if name is None:
            st.warning("No index on ts yet.")
        elif seconds is None:
            st.warning("Index %s exists but is NOT a TTL index." % name)
        else:
            st.success("TTL index %s: expireAfterSeconds = %d (%d days)" % (name, seconds, seconds // 86400))
        if st.button("Create the TTL index (T13)"):
            try:
                res, ms = timed(t13_create_ttl_index)
                st.info("Index %s ready (%.2f ms)." % (res, ms))
            except Exception as exc:
                st.error("MongoDB refused: %s" % exc)

        oldest = db["positions"].find_one({}, {"ts": 1}, sort=[("ts", 1)])
        limit = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=TTL_SECONDS)
        too_old = db["positions"].count_documents({"ts": {"$lt": limit}})
        m1, m2 = st.columns(2)
        m1.metric("Oldest position", oldest["ts"].strftime("%Y-%m-%d") if oldest else "none")
        m2.metric("Positions older than 60 days", too_old)

        st.markdown("**Live demo**")
        st.caption("Inserts one position dated 61 days ago. With the TTL index in place it should "
                   "disappear within about a minute. Press the second button to check.")
        if st.button("1. Insert a position dated 61 days ago"):
            res = db["positions"].insert_one({
                "vehicle_id": TTL_TEST_VEHICLE,
                "ts": datetime.now(timezone.utc) - timedelta(days=61),
                "loc": {"type": "Point", "coordinates": [-7.59, 33.57]},
                "speed": 0.0, "heading": 0, "fuel_level": 50.0, "engine_state": "off",
            })
            st.session_state["ttl_doc"] = res.inserted_id
            st.session_state["ttl_time"] = time.time()
            st.info("Inserted. Wait a little, then press button 2.")
        if st.button("2. Is it deleted yet?"):
            doc_id = st.session_state.get("ttl_doc")
            if doc_id is None:
                st.info("Insert a test position first.")
            elif db["positions"].count_documents({"_id": doc_id}) == 0:
                waited = time.time() - st.session_state["ttl_time"]
                st.success("Deleted by MongoDB, about %d seconds after the insert." % waited)
            else:
                waited = time.time() - st.session_state["ttl_time"]
                st.warning("Still there after %d seconds. Wait a bit and check again "
                           "(and make sure the TTL index exists)." % waited)
        if st.button("Remove leftover test positions"):
            n = db["positions"].delete_many({"vehicle_id": TTL_TEST_VEHICLE}).deleted_count
            st.info("%d test position(s) removed." % n)
    with right:
        per_day = list(db["positions"].aggregate([
            {"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$ts"}},
                        "positions": {"$sum": 1}}},
            {"$sort": {"_id": 1}}], allowDiskUse=True))
        if per_day:
            pdf = pd.DataFrame(per_day).rename(columns={"_id": "day"})
            st.plotly_chart(px.bar(pdf, x="day", y="positions",
                                   title="Positions per day (the TTL keeps the last 60 days)"),
                            use_container_width=True)