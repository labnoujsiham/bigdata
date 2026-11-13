"""
Spark side of the native vs Spark comparison: A22, A23 and S27.

The three queries are the same as the MongoDB pipelines in queries/c_queries/analytics.py
(A22, A23) and benchmarks/compare_native.py (S27), written with DataFrames and joins.
1 warm-up run, then N timed runs. The CSV goes to stdout:

    docker exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy \
      --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
      /opt/spark-apps/compare_spark.py --repeats 10 | Out-File -Encoding utf8 results\\spark_compare.csv
"""
import argparse
import statistics
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_queries import URI, load, s27


def a22(spark):
    """Driving events per depot (of the vehicle) and event type, with the average value."""
    events = load(spark, "driving_events").select("vehicle_id", "type", "value")
    vehicles = load(spark, "vehicles").select(F.col("_id").alias("vehicle_id"), "depot")
    return (events.join(vehicles, "vehicle_id")
            .groupBy("depot", "type")
            .agg(F.count("*").alias("events"), F.round(F.avg("value"), 1).alias("avg_value"))
            .orderBy("depot", F.desc("events")).collect())


def a23(spark):
    """Number of maintenances, total and average cost per vehicle type."""
    maint = load(spark, "maintenances").select("vehicle_id", "cost")
    vehicles = load(spark, "vehicles").select(F.col("_id").alias("vehicle_id"), "type")
    return (maint.join(vehicles, "vehicle_id")
            .groupBy("type")
            .agg(F.count("*").alias("maintenances"),
                 F.round(F.sum("cost"), 2).alias("total_cost"),
                 F.round(F.avg("cost"), 2).alias("avg_cost"))
            .orderBy(F.desc("total_cost")).collect())


QUERIES = {"A22": a22, "A23": a23, "S27": s27}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeats", type=int, default=10)
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("fleet-compare-spark")
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