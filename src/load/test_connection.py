import os
from pathlib import Path
import duckdb

# Load token from .env (always overrides shell env)
env_file = Path(__file__).resolve().parents[2] / ".env"
for line in env_file.read_text().splitlines():
    if "=" in line and not line.startswith("#"):
        k, _, v = line.partition("=")
        os.environ[k.strip()] = v.strip()

TOKEN = os.environ["motherduck_token"]
print(f"Token length: {len(TOKEN)}, starts: {TOKEN[:10]}...")

con = duckdb.connect(f"md:dev?motherduck_token={TOKEN}")
print(con.execute("SHOW DATABASES").fetchall())
con.close()
print("Connection OK")
