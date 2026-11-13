"""
Fleet telematics dashboard (Person C).

Run from the root of the repo (replica set running, data generated):
    streamlit run dashboard/app.py

Tab 1 reads MongoDB directly (A21: silent vehicles, A22: events per type and day,
A23: average fuel level per model). Tab 2 reads the results written by Spark
(collections spark_s26 ... spark_s30, created by: spark_queries.py --save).
Tab 3 reads the timing files in results/.

Spark result columns expected by tab 2:
    S26: vehicle_id, day, km, n_positions
    S27: rank, driver_id, name, km, n_events, penalty_points, score_per_100km
    S28: vehicle_id, driver_id, mission_id, start, duration_min, lat, lon
    S29: vehicle_id, ts, lat, lon, implied_kmh
    S30: vehicle_id, depot, start, duration_min, fuel_drop, litres
"""
import io
import os
import re
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "queries", "c_queries"))

st.set_page_config(page_title="Fleet telematics", layout="wide")
st.title("Fleet telematics dashboard")

try:
    import analytics as native          # A21-A23 (opens the MongoDB connection)
    db = native.db
except Exception as exc:                # MongoDB not reachable
    st.error("Cannot connect to MongoDB: %s" % exc)
    st.stop()

# Columns each Spark query must have (used to detect results from an older version).
SPARK_COLUMNS = {
    "S26": ["vehicle_id", "day", "km"],
    "S27": ["rank", "driver_id", "name", "km", "n_events", "penalty_points", "score_per_100km"],
    "S28": ["vehicle_id", "driver_id", "mission_id", "start", "duration_min", "lat", "lon"],
    "S29": ["vehicle_id", "ts", "lat", "lon", "implied_kmh"],
    "S30": ["vehicle_id", "depot", "start", "duration_min", "fuel_drop", "litres"],
}


# ------------------------------------------------------------------ data access
@st.cache_data(ttl=300)
def counts():
    return {name: db[name].estimated_document_count()
            for name in ("vehicles", "drivers", "missions", "positions")}


@st.cache_data(ttl=300)
def a21():
    df = pd.DataFrame(native.a21_silent_vehicles())
    return df.rename(columns={"_id": "depot"})


@st.cache_data(ttl=300)
def a22():
    df = pd.json_normalize(native.a22_events_by_type_and_day())
    df = df.rename(columns={"_id.day": "day", "_id.type": "type"})
    df["day"] = pd.to_datetime(df["day"])
    return df


@st.cache_data(ttl=300)
def a23():
    df = pd.DataFrame(native.a23_avg_fuel_by_model())
    return df.rename(columns={"_id": "model"})


@st.cache_data(ttl=300)
def spark_df(qid):
    docs = list(db["spark_" + qid.lower()].find({}, {"_id": 0}))
    return pd.DataFrame(docs)


def read_timing_csv(name):
    path = os.path.join(ROOT, "results", name)
    if not os.path.exists(path):
        return pd.DataFrame()
    with open(path, encoding="utf-8-sig") as f:
        lines = [x.strip() for x in f if re.match(r"^(id|[AS]\d\d),", x.strip())]
    if len(lines) < 2:
        return pd.DataFrame()
    return pd.read_csv(io.StringIO("\n".join(lines)))


def need_spark(df, qid):
    """True (and shows a message) if there is nothing usable to display for qid."""
    if df.empty:
        st.info("No data for %s yet. Run spark_queries.py with --save "
                "(see the docstring of spark_queries.py)." % qid)
        return True
    missing = [c for c in SPARK_COLUMNS[qid] if c not in df.columns]
    if missing:
        st.warning("The stored %s results are in an old format (missing: %s). "
                   "Re-run spark_queries.py --only %s --save." % (qid, ", ".join(missing), qid))
        return True
    return False


# ------------------------------------------------------------------ header
c = counts()
k1, k2, k3, k4 = st.columns(4)
k1.metric("Vehicles", "{:,}".format(c["vehicles"]))
k2.metric("Drivers", "{:,}".format(c["drivers"]))
k3.metric("Missions", "{:,}".format(c["missions"]))
k4.metric("GPS positions", "{:,}".format(c["positions"]))

tab1, tab2, tab3 = st.tabs(["Fleet (MongoDB)", "Spark analyses (S26-S30)", "Native vs Spark"])

# ------------------------------------------------------------------ tab 1
with tab1:
    left, right = st.columns(2)
    with left:
        st.subheader("A21 - Vehicles with no position for 1 h")
        st.caption("Per depot. Reference: newest position in the data.")
        df = a21()
        st.metric("Silent vehicles", int(df["silent_vehicles"].sum()))
        st.plotly_chart(px.bar(df, x="depot", y="silent_vehicles", text="silent_vehicles"))
    with right:
        st.subheader("A23 - Average fuel level per model")
        st.caption("Percent of the tank, average over all GPS positions.")
        df = a23()
        st.plotly_chart(px.bar(df.sort_values("avg_fuel_level"), x="model", y="avg_fuel_level",
                               color="brand", hover_data=["vehicles", "positions"]))
    st.subheader("A22 - Driving events per type and per day")
    df = a22()
    st.plotly_chart(px.bar(df, x="day", y="events", color="type", barmode="stack",
                           hover_data=["avg_value"]))

# ------------------------------------------------------------------ tab 2
with tab2:
    # ---- S26 and S27
    left, right = st.columns(2)
    with left:
        st.subheader("S26 - Real distance per vehicle and per day")
        st.caption("Haversine distance between consecutive positions, summed per day (UTC).")
        df = spark_df("S26")
        if not need_spark(df, "S26"):
            df["day"] = pd.to_datetime(df["day"])
            total = (df.groupby("vehicle_id", as_index=False)["km"].sum()
                     .nlargest(10, "km"))
            total["vehicle_id"] = total["vehicle_id"].astype(str)
            st.caption("Top 10 vehicles by total distance")
            st.plotly_chart(px.bar(total, x="vehicle_id", y="km"))

            vehicles = sorted(df["vehicle_id"].astype(str).unique())
            chosen = st.selectbox("Daily distance of vehicle", vehicles, key="s26_vehicle")
            one = df[df["vehicle_id"].astype(str) == chosen].sort_values("day")
            st.plotly_chart(px.bar(one, x="day", y="km", hover_data=["n_positions"]))
    with right:
        st.subheader("S27 - Driving score per driver")
        st.caption("Weighted events per 100 km. Rank 1 = safest driver (lowest score).")
        df = spark_df("S27")
        if not need_spark(df, "S27"):
            which = st.radio("Show", ["Safest 10", "Riskiest 10"], horizontal=True,
                             key="s27_which")
            ranked = df.sort_values(["rank", "driver_id"])
            top = ranked.head(10) if which == "Safest 10" else ranked.tail(10)
            st.plotly_chart(px.bar(top, x="name", y="score_per_100km",
                                   hover_data=["rank", "km", "n_events", "penalty_points"]))
            with st.expander("Full ranking"):
                st.dataframe(ranked, hide_index=True)

    # ---- S28
    st.subheader("S28 - Unplanned stops during a mission (engine off, more than 30 min)")
    df = spark_df("S28")
    if not need_spark(df, "S28"):
        m1, m2 = st.columns(2)
        m1.metric("Stops", len(df))
        m2.metric("Average duration (min)", round(df["duration_min"].mean(), 1))
        st.map(df[["lat", "lon"]].dropna())
        with st.expander("Longest stops"):
            st.dataframe(df.sort_values("duration_min", ascending=False).head(20),
                         hide_index=True)

    # ---- S29 and S30
    left, right = st.columns(2)
    with left:
        st.subheader("S29 - GPS anomalies")
        st.caption("Isolated positions implying more than 200 km/h on arrival and on departure.")
        df = spark_df("S29")
        if not need_spark(df, "S29"):
            st.metric("Anomalies", len(df))
            st.dataframe(df.sort_values("implied_kmh", ascending=False).head(10),
                         hide_index=True)
    with right:
        st.subheader("S30 - Suspected fuel theft")
        st.caption("Engine off, fuel level down by more than 20 points within 10 minutes.")
        df = spark_df("S30")
        if not need_spark(df, "S30"):
            m1, m2 = st.columns(2)
            m1.metric("Suspected thefts", len(df))
            m2.metric("Litres (estimated)", round(df["litres"].sum(), 1))
            by_depot = df.groupby("depot", as_index=False)["litres"].sum()
            st.plotly_chart(px.bar(by_depot, x="depot", y="litres"))

# ------------------------------------------------------------------ tab 3
with tab3:
    st.subheader("Native MongoDB aggregation vs Spark")
    st.caption("Same machine, same data. Median of 10 runs after one warm-up run. "
               "Spark: 2 workers with 1 core each.")
    path = os.path.join(ROOT, "results", "native_vs_spark.csv")
    if os.path.exists(path):
        cmp_df = pd.read_csv(path)
        long = cmp_df.melt(id_vars=["id", "query"], value_vars=["native_ms", "spark_ms"],
                           var_name="engine", value_name="ms")
        long["engine"] = long["engine"].map({"native_ms": "MongoDB native", "spark_ms": "Spark"})
        st.plotly_chart(px.bar(long, x="id", y="ms", color="engine", barmode="group",
                               hover_data=["query"]))
        st.dataframe(cmp_df)
    else:
        st.info("results/native_vs_spark.csv not found: run benchmarks/compare_native.py.")

    st.subheader("Spark queries S26-S30: execution time")
    sp = read_timing_csv("spark_results.csv")
    if sp.empty:
        st.info("results/spark_results.csv not found.")
    else:
        st.plotly_chart(px.bar(sp, x="id", y="median_ms", text="median_ms"))