# Query tracking

Time (ms) = median of 10 timed runs, measured with `benchmarks/benchmark.py` on one PC
(Docker, 3 MongoDB nodes on the same machine, data generated with `data/generate_data.py`).
Raw numbers: `results/benchmark_results.csv`.

| ID | Description | Owner | File | Time (ms) | Status |
|----|-------------|-------|------|-----------|--------|
| T1 | Record a GPS position and refresh the vehicle's `last_position` | Btissam | queries/btissam_queries/transactional.py | 11.19 | done |
| T2 | Schedule a new mission | Btissam | queries/btissam_queries/transactional.py | 5.66 | done |
| T3 | Start a mission (status `in_progress`) | Btissam | queries/btissam_queries/transactional.py | 4.37 | done |
| T4 | Last known position of a vehicle (embedded copy) | Btissam | queries/btissam_queries/transactional.py | 0.74 | done |
| T5 | Positions of a vehicle over the last 60 days (index `vehicle_id, ts`) | Btissam | queries/btissam_queries/transactional.py | 2.91 | done (212 rows) |
| T6 | Find a vehicle by plate | Btissam | queries/btissam_queries/transactional.py | 0.80 | done |
| T7 | Vehicles of a depot with a given equipment | Btissam | queries/btissam_queries/transactional.py | 2.90 | done (101 rows) |
| T8 | Complete a mission (status `completed`) | Btissam | queries/btissam_queries/transactional.py | 5.46 | done |
| T9 | Drivers of a depot with a given licence category | Btissam | queries/btissam_queries/transactional.py | 4.35 | done (139 rows) |
| T10 | Latest missions of a driver (index `driver_id, planned_start`) | Btissam | queries/btissam_queries/transactional.py | 1.36 | done (8 rows) |
| T11 | Record a maintenance | Siham | queries/siham_queries/t11_record_maintenance.py | 4.92 | done |
| T12 | Add an equipment item to a vehicle (`$addToSet`) | Siham | queries/siham_queries/t12_add_equipment.py | 4.86 | done |
| T13 | Auto-expiry of positions after 60 days (TTL index) | Siham | queries/siham_queries/t13_ttl_expiry.py | 0.90 (index creation) | done |
| T14 | Delete a vehicle's positions over a period | Siham | queries/siham_queries/t14_delete_positions.py | 30.56 | done (200 test positions deleted per run) |
| T15 | Missions in progress for a depot | Siham | queries/siham_queries/t15_missions_in_progress.py | 1.02 | done (26 rows) |
| A16 | Vehicle count and average capacity per depot and type | Btissam | queries/btissam_queries/analytics.py | 3.20 | done (24 rows) |
| A17 | Top 10 drivers by number of driving events | Btissam | queries/btissam_queries/analytics.py | 14.87 | done |
| A18 | Missions per depot and status (index `depot, status`) | Btissam | queries/btissam_queries/analytics.py | 12.68 | done (24 rows) |
| A19 | Late missions per depot (late rate, average delay) | Btissam | queries/btissam_queries/analytics.py | 20.41 | done (6 rows) |
| A20 | Vehicles seen within 5 km of a point (2dsphere index) | Btissam | queries/btissam_queries/analytics.py | 33.42 | done (10 rows) |
| A21 | Vehicles with no GPS position for over 1 hour, per depot (reference: newest position in the data) | Person C | queries/c_queries/analytics.py | 7.05 | done (6 rows) |
| A22 | Driving events per depot and event type, with the average value | Person C | queries/c_queries/analytics.py | 1075.44 | done (24 rows) |
| A23 | Maintenance count, total and average cost per vehicle type | Person C | queries/c_queries/analytics.py | 160.74 | done (4 rows) |
| A24 | Vehicles whose last maintenance is over 6 months old | Siham | queries/siham_queries/a24_old_maintenance.py | 6.16 | done (535 rows) |
| A25 | Average mission duration per depot | Siham | queries/siham_queries/a25_avg_mission_duration.py | 14.65 | done (6 rows, one per depot) |
| S26 | Top 10 vehicles by distance travelled, with average speed (Spark) | Person C | spark/spark_queries.py | 5040.28 | done (10 rows) |
| S27 | Average speed per hour of the day (Spark) | Person C | spark/spark_queries.py | 1945.84 | done (24 rows) |
| S28 | Unplanned stops: engine off and speed 0 for at least 15 minutes (Spark) | Person C | spark/spark_queries.py | 5870.80 | done (200 rows) |
| S29 | GPS anomalies: isolated positions reached and left at over 200 km/h (Spark) | Person C | spark/spark_queries.py | 5781.76 | done (40 rows) |
| S30 | Fuel theft: fuel drop of 10 points or more during a stop (Spark) | Person C | spark/spark_queries.py | 4604.58 | done (15 rows) |

Notes:
- T14 is measured on 200 fake positions inserted just before each run (vehicle `V_BENCH`), so real data is never deleted.
- T13 measures the creation of an index that already exists, not the expiry itself.
- Times change a little from one run to another (and after regenerating the data): compare orders of magnitude, not decimals.
- With / without index comparison (`results/index_comparison.csv`):
  T5 67.28 ms and 161,995 documents examined without index, 4.02 ms and 212 documents with index;
  T10 6.00 ms and 10,001 documents without index, 1.45 ms and 8 documents with index;
  T6 1.86 ms and 1,500 documents without index, 1.06 ms and 1 document with index.
- S26-S30 run on Spark (2 workers, 1 core each): median of 10 timed runs after one warm-up run, measured by `spark/spark_queries.py` (`results/spark_results.csv`), not by `benchmark.py`.
- Native vs Spark comparison for A22, A23 and S27 (same results, same machine): native 1193.51 / 143.57 / 197.22 ms, Spark 2958.79 / 1319.33 / 1696.24 ms. Details: `results/native_vs_spark.csv` (`benchmarks/compare_native.py`).