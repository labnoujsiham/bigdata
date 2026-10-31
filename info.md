## Local setup (Windows, PowerShell)

### Prerequisites
- Python 3.12 (tick "Add python.exe to PATH" when installing)
- Docker Desktop installed and running
- Git

### 1. Clone and install
git clone <repo-link>
cd fleet-telematics
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env -Force

### 2. Add the Mongo names to the hosts file (once per laptop)
1. Open Notepad as administrator (right-click > Run as administrator).
2. File > Open, paste C:\Windows\System32\drivers\etc\hosts and press Enter
   (set the file type to "All Files" if you can't see it).
3. Add these lines at the very end of the file, below everything else:

   127.0.0.1 mongo1
   127.0.0.1 mongo2
   127.0.0.1 mongo3

4. Save.

### 3. Start MongoDB
docker compose -f infra/docker-compose.yml up -d

Wait about 10 seconds, then create the replica set (only the first time):
Get-Content infra/init-replica.js | docker exec -i mongo1 mongosh --port 27017

### 4. Check it works
docker exec mongo1 mongosh --port 27017 --quiet --eval "rs.status().members.forEach(m => print(m.name + ' -> ' + m.stateStr))"
Expected: one PRIMARY and two SECONDARY.

python -c "import config; from pymongo import MongoClient; c = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000); print(c.admin.command('ping')); print('primary:', c.primary)"
Expected: {'ok': 1.0 ...} and a line starting with "primary:".

### Troubleshooting
- "Name resolution of mongo2 failed": the hosts file lines are missing or have a typo (it's mongo, not mango).
- "Missing setting: MONGO_URI": the .env file doesn't exist; run the copy command from step 1.
- pandas "blocked by Application Control": Smart App Control is on; run pandas code in Docker.