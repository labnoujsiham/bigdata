# Query tracking

Time (ms) = median of 10 timed runs, measured with `benchmarks/benchmark.py` on one PC
(Docker, 3 MongoDB nodes on the same machine). Raw numbers: `results/benchmark_results.csv`.

| ID | Description | Owner | File | Time (ms) | Status |
|----|-------------|-------|------|-----------|--------|
| T1 | Record a GPS position and refresh the vehicle's `last_position` | Btissam | queries/btissam_queries/transactional.py | 17.98 | done |
| T2 | Schedule a new mission | Btissam | queries/btissam_queries/transactional.py | 6.80 | done |
| T3 | Start a mission (status `in_progress`) | Btissam | queries/btissam_queries/transactional.py | 7.65 | done |
| T4 | Last known position of a vehicle (embedded copy) | Btissam | queries/btissam_queries/transactional.py | 1.89 | done |
| T5 | Positions of a vehicle over the last 60 days (index `vehicle_id, ts`) | Btissam | queries/btissam_queries/transactional.py | 5.91 | done (212 rows) |
| T6 | Find a vehicle by plate | Btissam | queries/btissam_queries/transactional.py | 1.89 | done |
| T7 | Vehicles of a depot with a given equipment | Btissam | queries/btissam_queries/transactional.py | 4.46 | done (101 rows) |
| T8 | Complete a mission (status `completed`) | Btissam | queries/btissam_queries/transactional.py | 6.33 | done |
| T9 | Drivers of a depot with a given licence category | Btissam | queries/btissam_queries/transactional.py | 3.68 | done (139 rows) |
| T10 | Latest missions of a driver (index `driver_id, planned_start`) | Btissam | queries/btissam_queries/transactional.py | 0.89 | done (8 rows) |
| T11 | Record a maintenance | Siham | queries/siham_queries/t11_record_maintenance.py | 5.95 | done |
| T12 | Add an equipment item to a vehicle (`$addToSet`) | Siham | queries/siham_queries/t12_add_equipment.py | 5.69 | done |
| T13 | Auto-expiry of positions after 60 days (TTL index) | Siham | queries/siham_queries/t13_ttl_expiry.py | 1.26 (index creation) | done |
| T14 | Delete a vehicle's positions over a period | Siham | queries/siham_queries/t14_delete_positions.py | 30.99 | done (200 test positions deleted per run) |
| T15 | Missions in progress for a depot | Siham | queries/siham_queries/t15_missions_in_progress.py | 1.45 | done (26 rows) |
| A16 | Vehicle count and average capacity per depot and type | Btissam | queries/btissam_queries/analytics.py | to measure | written |
| A17 | Top 10 drivers by number of driving events | Btissam | queries/btissam_queries/analytics.py | 16.59 | done |
| A18 | Missions per depot and status (index `depot, status`) | Btissam | queries/btissam_queries/analytics.py | 14.82 | done (24 rows) |
| A19 | Late missions per depot (late rate, average delay) | Btissam | queries/btissam_queries/analytics.py | to measure | written |
| A20 | Vehicles seen within 5 km of a point (2dsphere index) | Btissam | queries/btissam_queries/analytics.py | 31.65 | done (10 rows) |
| A21 | (to be filled by Person C) | Person C | | | todo |
| A22 | (to be filled by Person C) | Person C | | | todo |
| A23 | (to be filled by Person C) | Person C | | | todo |
| A24 | Vehicles whose last maintenance is over 6 months old | Siham | queries/siham_queries/a24_old_maintenance.py | 6.07 | done (534 vehicles, plus 410 never maintained) |
| A25 | Average mission duration per depot | Siham | queries/siham_queries/a25_avg_mission_duration.py | 176.30 (single run, to re-measure) | done (about 41 min per depot) |
| S26 | (to be filled by Person C) | Person C | | | todo |
| S27 | (to be filled by Person C) | Person C | | | todo |
| S28 | Unplanned stops | Person C | | | todo |
| S29 | GPS anomalies | Person C | | | todo |
| S30 | Fuel theft | Person C | | | todo |

Notes:
- T14 is measured on 200 fake positions inserted just before each run (vehicle `V_BENCH`), so real data is never deleted.
- T13 measures the creation of an index that already exists, not the expiry itself.
- T1 has a high p95 (145 ms): the first write after a pause is sometimes slow. Use the median.
- With / without index comparison: see `results/index_comparison.csv` (T5: 203 ms without index, 8 ms with).
