"""
Simple fleet dashboard: only charts.

Run from the root of the repo:
    streamlit run dashboard/app.py

Each chart is drawn by a small function. If the data for a chart is missing
(for example the Spark results were not saved yet), that chart shows a warning
and the rest of the page still works.
"""
import os
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

# so that "import analytics" finds queries/c_queries/analytics.py
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "queries", "c_queries"))

import analytics as native      # A21, A22, A23 (also opens the MongoDB connection)

db = native.db

st.set_page_config(page_title="Fleet telematics", layout="wide")
st.title("Fleet telematics dashboard")


# ------------------------------------------------------------------ helpers
def show(title, make_chart):
    """Writes a title, then draws the chart. make_chart is a function that returns a chart."""
    st.subheader(title)
    try:
        st.plotly_chart(make_chart(), use_container_width=True)
    except Exception as exc:
        st.warning("Cannot draw this chart yet: %s" % exc)


def spark_data(query_id):
    """Reads the results saved by spark_queries.py --save (collection spark_s26 ... spark_s30)."""
    return pd.DataFrame(list(db["spark_" + query_id.lower()].find({}, {"_id": 0})))


# ------------------------------------------------------------------ MongoDB charts
def chart_a21():
    df = pd.DataFrame(native.a21_silent_vehicles()).rename(columns={"_id": "depot"})
    return px.bar(df, x="depot", y="silent_vehicles", text="silent_vehicles")


def chart_a22():
    df = pd.json_normalize(native.a22_events_by_type_and_day())
    df = df.rename(columns={"_id.day": "day", "_id.type": "type"})
    return px.bar(df, x="day", y="events", color="type", barmode="stack")


def chart_a23():
    df = pd.DataFrame(native.a23_avg_fuel_by_model()).rename(columns={"_id": "model"})
    return px.bar(df.sort_values("avg_fuel_level"), x="model", y="avg_fuel_level", color="brand")


show("A21 - Vehicles with no position for 1 hour (per depot)", chart_a21)
show("A22 - Driving events per type and per day", chart_a22)
show("A23 - Average fuel level per model", chart_a23)


# ------------------------------------------------------------------ Spark charts
def chart_s26():
    df = spark_data("S26")
    top10 = df.groupby("vehicle_id", as_index=False)["km"].sum().nlargest(10, "km")
    return px.bar(top10, x="vehicle_id", y="km")


def chart_s27():
    df = spark_data("S27")
    safest = df.sort_values("rank").head(10)
    return px.bar(safest, x="name", y="score_per_100km")


def chart_s28():
    df = spark_data("S28")
    return px.histogram(df, x="duration_min", nbins=20)


def chart_s29():
    df = spark_data("S29")
    worst = df.sort_values("implied_kmh", ascending=False).head(10)
    return px.bar(worst, x="vehicle_id", y="implied_kmh")


def chart_s30():
    df = spark_data("S30")
    per_depot = df.groupby("depot", as_index=False)["litres"].sum()
    return px.bar(per_depot, x="depot", y="litres")


show("S26 - Top 10 vehicles by real distance (km)", chart_s26)
show("S27 - Driving score: 10 safest drivers (lower is better)", chart_s27)
show("S28 - Duration of unplanned stops during a mission (minutes)", chart_s28)
show("S29 - GPS anomalies: 10 highest implied speeds (km/h)", chart_s29)
show("S30 - Suspected fuel theft: litres per depot", chart_s30)


# ------------------------------------------------------------------ timing chart
def chart_native_vs_spark():
    df = pd.read_csv(os.path.join(ROOT, "results", "native_vs_spark.csv"))
    long = df.melt(id_vars=["id"], value_vars=["native_ms", "spark_ms"],
                   var_name="engine", value_name="ms")
    return px.bar(long, x="id", y="ms", color="engine", barmode="group")


show("Native MongoDB vs Spark: execution time (ms)", chart_native_vs_spark)