| ID | Description | Owner | File | Time (ms) | Status |
|----|-------------|-------|------|-----------|--------|
| T11 | Record a maintenance | Siham | queries/siham_queries/t11_record_maintenance.py | 7.03 | done |
| T12 | Add an equipment item to a vehicle (`$addToSet`) | Siham | queries/siham_queries/t12_add_equipment.py | 11.49 | done |
| T13 | Auto-expiry of positions after 60 days (TTL index) | Siham | queries/siham_queries/t13_ttl_expiry.py | 3.57 (index creation) | done |
| T14 | Delete a vehicle's positions over a period | Siham | queries/siham_queries/t14_delete_positions.py | to measure | written |
| T15 | Missions in progress for a depot | Siham | queries/siham_queries/t15_missions_in_progress.py | 13.19 | done |
| A24 | Vehicles whose last maintenance is over 6 months old | Siham | queries/siham_queries/a24_old_maintenance.py | 62.38 | done (534 vehicles, plus 410 never maintained) |
| A25 | Average mission duration per depot | Siham | queries/siham_queries/a25_avg_mission_duration.py | 176.30 | done (about 41 min per depot) |