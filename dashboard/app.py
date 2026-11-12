"""
Fleet telematics dashboard (Person C).

Run from the root of the repo (replica set running, data generated):
    streamlit run dashboard/app.py

Tab 1 reads MongoDB directly (A21-A23). Tab 2 reads the results written by Spark
(collections spark_s26 ... spark_s30, created by: spark_queries.py --save).
Tab 3 reads the timing files in results/.
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
    df = pd.json_normalize(native.a22_events_by_depot_and_type())
    return df.rename(columns={"_id.depot": "depot", "_id.type": "type"})


@st.cache_data(ttl=300)
def a23():
    df = pd.DataFrame(native.a23_maintenance_cost_by_vehicle_type())
    return df.rename(columns={"_id": "vehicle_type"})


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
    if df.empty:
        st.info("No data for %s yet. Run spark_queries.py with --save "
                "(see the docstring of dashboard/app.py)." % qid)
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
        st.subheader("A21 - Silent vehicles per depot")
        st.caption("No GPS position during the last hour (reference: newest position in the data).")
        df = a21()
        st.plotly_chart(px.bar(df, x="depot", y="silent_vehicles", text="silent_vehicles"))
    with right:
        st.subheader("A23 - Maintenance cost per vehicle type")
        df = a23()
        st.plotly_chart(px.bar(df, x="vehicle_type", y="total_cost",
                               hover_data=["maintenances", "avg_cost"]))
    st.subheader("A22 - Driving events per depot and type")
    df = a22()
    st.plotly_chart(px.bar(df, x="depot", y="events", color="type", barmode="group"))

# ------------------------------------------------------------------ tab 2
with tab2:
    left, right = st.columns(2)
    with left:
        st.subheader("S26 - Top 10 vehicles by distance")
        df = spark_df("S26")
        if not need_spark(df, "S26"):
            st.plotly_chart(px.bar(df.sort_values("total_km", ascending=False),
                                   x="vehicle_id", y="total_km", hover_data=["avg_speed_kmh"]))
    with right:
        st.subheader("S27 - Average speed per hour of the day (UTC)")
        df = spark_df("S27")
        if not need_spark(df, "S27"):
            fig = px.line(df.sort_values("hour"), x="hour", y="avg_speed_kmh", markers=True)
            fig.update_yaxes(rangemode="tozero")
            st.plotly_chart(fig)

    st.subheader("S28 - Unplanned stops (engine off, 15 min or more)")
    df = spark_df("S28")
    if not need_spark(df, "S28"):
        m1, m2 = st.columns(2)
        m1.metric("Stops", len(df))
        m2.metric("Average duration (min)", round(df["duration_min"].mean(), 1))
        st.map(df[["lat", "lon"]])

    left, right = st.columns(2)
    with left:
        st.subheader("S29 - GPS anomalies")
        df = spark_df("S29")
        if not need_spark(df, "S29"):
            st.metric("Anomalies", len(df))
            st.dataframe(df.sort_values("implied_kmh", ascending=False).head(10))
    with right:
        st.subheader("S30 - Suspected fuel theft")
        df = spark_df("S30")
        if not need_spark(df, "S30"):
            st.metric("Suspected thefts", len(df))
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