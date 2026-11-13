"""
S26 to S30: Spark queries on the MongoDB data (Person C).

Run inside the Spark container, from the root of the repo (replica set running,
data generated). Example for all queries, 10 timed runs each:

    docker exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy \
      --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
      /opt/spark-apps/spark_queries.py --repeats 10 > results/spark_results.csv

Only some queries:  ... /opt/spark-apps/spark_queries.py --only S28,S30
The CSV (id, median...) goes to stdout; Spark logs and result samples go to stderr.
For every query: 1 warm-up run (absorbs Spark start-up and schema inference),
then N timed runs, like benchmarks/benchmark.py.
"""

import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]
import os
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.21.9-hotspot"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]
import argparse
import statistics
import sys
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

URI = "mongodb://192.168.141.32:27017,192.168.141.60:27017,192.168.141.171:27017/?replicaSet=rs0"
DB = "fleet"


# ------------------------------------------------------------------ helpers
def load(spark, collection):
    return (spark.read.format("mongodb")
            .option("database", DB).option("collection", collection).load())


def positions(spark):
    """positions with flat lat / lon columns (GeoJSON order is [lon, lat])."""
    return load(spark, "positions").select(
        "vehicle_id", "ts", "speed", "fuel_level", "engine_state",
        F.col("loc.coordinates")[0].alias("lon"),
        F.col("loc.coordinates")[1].alias("lat"))


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dphi = p2 - p1
    dlam = F.radians(lon2 - lon1)
    a = F.sin(dphi / 2) ** 2 + F.cos(p1) * F.cos(p2) * F.sin(dlam / 2) ** 2
    return 2 * 6371.0 * F.asin(F.sqrt(a))


def by_vehicle():
    return Window.partitionBy("vehicle_id").orderBy("ts")


def stop_runs(spark, min_minutes):
    """Periods where a vehicle stays stopped (engine off, speed 0) for at least
    min_minutes. Gaps-and-islands: a new run starts whenever 'stopped' changes."""
    w = by_vehicle()
    p = positions(spark).withColumn(
        "stopped", (F.col("engine_state") == "off") & (F.col("speed") == 0))
    previous = F.lag("stopped").over(w)
    p = p.withColumn("changed",
                     F.when(previous.isNull() | (previous != F.col("stopped")), 1).otherwise(0))
    p = p.withColumn("run_id", F.sum("changed").over(w))
    runs = (p.filter(F.col("stopped"))
            .groupBy("vehicle_id", "run_id")
            .agg(F.min("ts").alias("start"),
                 F.max("ts").alias("end"),
                 F.count("*").alias("n_points"),
                 F.min_by("fuel_level", "ts").alias("fuel_start"),
                 F.max_by("fuel_level", "ts").alias("fuel_end"),
                 F.avg("lat").alias("lat"),
                 F.avg("lon").alias("lon")))
    runs = runs.withColumn(
        "duration_min",
        F.round((F.col("end").cast("long") - F.col("start").cast("long")) / 60, 1))
    return runs.filter(F.col("duration_min") >= min_minutes)


# ------------------------------------------------------------------ S26
def s26(spark):
    """Top 10 vehicles by distance travelled, with their average speed.
    Steps between two positions more than 60 s apart (different missions) and
    GPS jumps (more than 2 km in one step) are ignored."""
    w = by_vehicle()
    p = (positions(spark)
         .withColumn("dt_s", F.col("ts").cast("long") - F.lag(F.col("ts").cast("long")).over(w))
         .withColumn("step_km", haversine_km(F.lag("lat").over(w), F.lag("lon").over(w),
                                             F.col("lat"), F.col("lon"))))
    ok = (F.col("dt_s") <= 60) & (F.col("step_km") < 2)
    return (p.groupBy("vehicle_id")
            .agg(F.round(F.sum(F.when(ok, F.col("step_km")).otherwise(0.0)), 1).alias("total_km"),
                 F.round(F.avg(F.when(F.col("engine_state") == "on", F.col("speed"))), 1)
                 .alias("avg_speed_kmh"),
                 F.count("*").alias("n_positions"))
            .orderBy(F.desc("total_km")).limit(10).collect())


# ------------------------------------------------------------------ S27
def s27(spark):
    """Average speed of the moving fleet by hour of the day (UTC)."""
    return (positions(spark).filter(F.col("engine_state") == "on")
            .groupBy(F.hour("ts").alias("hour"))
            .agg(F.round(F.avg("speed"), 1).alias("avg_speed_kmh"),
                 F.count("*").alias("n_positions"))
            .orderBy("hour").collect())


# ------------------------------------------------------------------ S28
def s28(spark):
    """Unplanned stops: engine off and speed 0 for at least 15 minutes."""
    return (stop_runs(spark, 15)
            .select("vehicle_id", "start", "duration_min", "lat", "lon")
            .orderBy(F.desc("duration_min")).collect())


# ------------------------------------------------------------------ S29
def s29(spark):
    """GPS anomalies: isolated positions (spikes). The speed needed to reach the
    point from the previous one AND to leave it for the next one is above 200 km/h."""
    w = by_vehicle()
    ts = F.col("ts").cast("long")
    p = (positions(spark)
         .withColumn("dt_in", ts - F.lag(ts).over(w))
         .withColumn("dt_out", F.lead(ts).over(w) - ts)
         .withColumn("km_in", haversine_km(F.lag("lat").over(w), F.lag("lon").over(w),
                                           F.col("lat"), F.col("lon")))
         .withColumn("km_out", haversine_km(F.col("lat"), F.col("lon"),
                                            F.lead("lat").over(w), F.lead("lon").over(w)))
         .withColumn("kmh_in", F.col("km_in") / (F.col("dt_in") / 3600))
         .withColumn("kmh_out", F.col("km_out") / (F.col("dt_out") / 3600)))
    return (p.filter((F.col("kmh_in") > 200) & (F.col("kmh_out") > 200)
                     & (F.col("dt_in") <= 300) & (F.col("dt_out") <= 300))
            .select("vehicle_id", "ts", "lat", "lon", F.round("kmh_in").alias("implied_kmh"))
            .orderBy("vehicle_id", "ts").collect())
# ------------------------------------------------------------------ S30
def s30(spark):
    """Suspected fuel theft: during a stop of at least 5 minutes (engine off), the
    fuel level drops by 10 points or more. Litres estimated with the tank size."""
    thefts = (stop_runs(spark, 5)
              .withColumn("fuel_drop", F.round(F.col("fuel_start") - F.col("fuel_end"), 1))
              .filter(F.col("fuel_drop") >= 10))
    vehicles = load(spark, "vehicles").select(
        F.col("_id").alias("vehicle_id"), "depot", F.col("features.tank_l").alias("tank_l"))
    return (thefts.join(vehicles, "vehicle_id")
            .withColumn("litres", F.round(F.col("fuel_drop") * F.col("tank_l") / 100, 1))
            .select("vehicle_id", "depot", "start", "duration_min", "fuel_drop", "litres")
            .orderBy(F.desc("fuel_drop")).collect())


QUERIES = {"S26": s26, "S27": s27, "S28": s28, "S29": s29, "S30": s30}


# ------------------------------------------------------------------ main
def save(spark, qid, rows):
    """Writes the result of a query in MongoDB (collection spark_s26 ... spark_s30),
    so that the Streamlit dashboard can read it."""
    if rows:
        (spark.createDataFrame(rows).write.format("mongodb").mode("overwrite")
         .option("connection.uri", URI).option("database", DB)
         .option("collection", "spark_" + qid.lower()).save())


def main():
    parser = argparse.ArgumentParser(description="Spark queries S26-S30")
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument("--only", default="", help="e.g. S28,S30")
    parser.add_argument("--save", action="store_true",
                        help="also write the results in MongoDB (for the dashboard)")
    args = parser.parse_args()
    wanted = [x.strip().upper() for x in args.only.split(",") if x.strip()] or list(QUERIES)

    spark = (SparkSession.builder.appName("fleet-spark-queries")
         .config("spark.jars.packages",
                 "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0")   # <- add this
         .config("spark.mongodb.read.connection.uri", URI)
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("OFF")

    print("id,repeats,median_ms,mean_ms,min_ms,max_ms,rows")
    for qid in wanted:
        func = QUERIES[qid]
        rows = func(spark)                               # warm-up, not counted
        times = []
        for _ in range(args.repeats):
            start = time.perf_counter()
            rows = func(spark)
            times.append((time.perf_counter() - start) * 1000)
        print("%s,%d,%.2f,%.2f,%.2f,%.2f,%d" % (qid, args.repeats, statistics.median(times),
              statistics.mean(times), min(times), max(times), len(rows)))
        sys.stdout.flush()
        print("%s sample: %s" % (qid, rows[:2]), file=sys.stderr)
        if args.save:
            save(spark, qid, rows)
    spark.stop()


if __name__ == "__main__":
    main()