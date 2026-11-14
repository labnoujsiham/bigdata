"""
Spark side of the native vs Spark comparison: A19, A22 and A23.

The three queries are the same as the MongoDB pipelines (A19: average speed per vehicle
type, A22: events per type and day, A23: average fuel per model), written with
DataFrames and joins.
1 warm-up run, then N timed runs. The CSV goes to stdout:

    docker exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy \
      --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
      /opt/spark-apps/compare_spark.py --repeats 10 | Out-File -Encoding utf8 results\\spark_compare.csv
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

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_queries import URI, load


def a19(spark):
    """Average speed per vehicle type (every position weighs the same)."""
    pos = load(spark, "positions").select("vehicle_id", "speed")
    vehicles = load(spark, "vehicles").select(F.col("_id").alias("vehicle_id"), "type")
    return (pos.join(vehicles, "vehicle_id")
            .groupBy("type")
            .agg(F.round(F.avg("speed"), 1).alias("avg_speed"),
                 F.count("*").alias("positions"))
            .orderBy("type").collect())


def a22(spark):
    """Driving events per event type and per day (UTC), with the average value."""
    return (load(spark, "driving_events")
            .groupBy(F.date_format("ts", "yyyy-MM-dd").alias("day"), "type")
            .agg(F.count("*").alias("events"), F.round(F.avg("value"), 1).alias("avg_value"))
            .orderBy("day", "type").collect())


def a23(spark):
    """Average fuel level per vehicle model (every position weighs the same)."""
    pos = load(spark, "positions").select("vehicle_id", "fuel_level")
    vehicles = load(spark, "vehicles").select(F.col("_id").alias("vehicle_id"), "model", "brand")
    return (pos.join(vehicles, "vehicle_id")
            .groupBy("model", "brand")
            .agg(F.countDistinct("vehicle_id").alias("vehicles"),
                 F.count("*").alias("positions"),
                 F.round(F.avg("fuel_level"), 1).alias("avg_fuel_level"))
            .orderBy("avg_fuel_level").collect())


QUERIES = {"A19": a19, "A22": a22, "A23": a23}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeats", type=int, default=10)
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("fleet-compare-spark")
             .config("spark.jars.packages",
                     "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0")
             .config("spark.mongodb.read.connection.uri", URI)
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    print("id,repeats,median_ms,mean_ms,min_ms,max_ms,rows")
    for qid, func in QUERIES.items():
        rows = func(spark)                                # warm-up, not counted
        times = []
        for _ in range(args.repeats):
            start = time.perf_counter()
            rows = func(spark)
            times.append((time.perf_counter() - start) * 1000)
        print("%s,%d,%.2f,%.2f,%.2f,%.2f,%d" % (qid, args.repeats, statistics.median(times),
              statistics.mean(times), min(times), max(times), len(rows)))
        sys.stdout.flush()
    spark.stop()


if __name__ == "__main__":
    main()