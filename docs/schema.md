# Schema (MongoDB, database `fleet`)

Conventions: field names in English, snake_case. Dates are real BSON Dates.
Coordinates are GeoJSON: `{ type: "Point", coordinates: [longitude, latitude] }`.

## vehicles (1,500)
- _id: string (vehicle id, e.g. "V0001")
- plate: string
- type: string (truck, van, ...)
- brand, model: string
- year: int
- capacity: int
- depot: string
- equipment: array of strings (e.g. ["gps", "fridge"])
- features: object (key-value, e.g. { fuel_type: "diesel", axles: 2 })
- last_position: object (copy of the latest position, see decision 2)

## drivers (2,000)
- _id: string (e.g. "D0001")
- name: string
- license_categories: array of strings (e.g. ["B", "C"])
- hire_date: Date
- depot: string

## positions (>= 100,000)
- _id: ObjectId
- vehicle_id: string
- ts: Date
- loc: GeoJSON Point
- speed: double (km/h)
- heading: int (degrees)
- fuel_level: double (percent)
- engine_state: string ("on" / "off")

## missions (>= 10,000)
- _id: string (e.g. "M00001")
- vehicle_id, driver_id: string
- depot: string
- origin: object { name: string, loc: GeoJSON Point }
- destination: object { name: string, loc: GeoJSON Point }
- planned_start, planned_end: Date
- actual_start, actual_end: Date (null until started/finished)
- distance_km: double
- status: string ("planned", "in_progress", "completed", "cancelled")

## driving_events (>= 15,000)
- _id: ObjectId
- vehicle_id, driver_id: string
- ts: Date
- type: string ("speeding", "harsh_braking", "long_idle", "out_of_zone")
- value: double
- loc: GeoJSON Point

## maintenances (>= 2,000)
- _id: ObjectId
- vehicle_id: string
- date: Date
- mileage_km: int
- type: string
- parts: array of strings
- cost: double

## Design decisions (to agree on)
1. Embedded vs referenced: equipment, features, license categories, origin/destination and maintenance parts are embedded (always read with their parent). Positions, missions, events and maintenances are separate collections that reference vehicle_id / driver_id (they grow large or are queried alone).
2. last_position is copied into the vehicle document on every new position (T1 updates it). This makes T4 and A21 fast reads. Trade-off: one extra write per position.
3. Positions are generated only within the last 60 days. Otherwise the TTL index (T13) would delete older ones as soon as it is created.

## Indexes
- positions: { vehicle_id: 1, ts: -1 }  (T4, T5, S26-S30)
- positions: { loc: "2dsphere" }  (A20)
- positions: { ts: 1 } with expireAfterSeconds: 5184000 (60 days) (T13)
- missions: { driver_id: 1, planned_start: 1 }  (T10)
- missions: { depot: 1, status: 1 }  (T15, A18, A25)
- driving_events: { driver_id: 1, type: 1 }  (A17)
- maintenances: { vehicle_id: 1, date: -1 }  (A24)