from datetime import datetime, timezone

from common import db, timed


def run(vehicle_id="V0001", mileage_km=150000, maintenance_type="oil_change",
        parts=None, cost=850.0):
    if parts is None:
        parts = ["oil filter", "air filter"]
    doc = {
        "vehicle_id": vehicle_id,
        "date": datetime.now(timezone.utc),
        "mileage_km": mileage_km,
        "type": maintenance_type,
        "parts": parts,
        "cost": cost,
    }
    result = db["maintenances"].insert_one(doc)
    return result.inserted_id


if __name__ == "__main__":
    new_id = timed("T11", run)
    print("inserted:", new_id)