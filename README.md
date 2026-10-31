# Fleet Telematics: GPS tracking and telemetry (Subject 05)

Big Data mini-project: MongoDB (3-node replica set) + Spark (1 master, 2 workers) + Streamlit dashboard.

## Setup
1. Install Docker and Python 3.
2. `python -m venv .venv` then activate it, then `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` (never commit `.env`).
4. Start the stack: `docker compose -f infra/docker-compose.yml up -d`
5. Generate data: `python data/generate_data.py`

## Structure
- `docs/` schema, query list, report
- `infra/` Docker Compose and replica set scripts
- `data/` data generator
- `queries/` T1-T15 and A16-A25 (MongoDB)
- `spark/` S26-S30
- `benchmarks/` timing of the 30 queries
- `dashboard/` Streamlit app
- `results/` timings and screenshots

## Team
- Infra and experiments: _name_
- Schema, data and MongoDB queries: _name_
- Spark and dashboard: _name_
