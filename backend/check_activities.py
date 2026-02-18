import os
import psycopg2
import sqlite3
from dotenv import load_dotenv

# Load env from current directory
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "class_bridge.db")
USE_POSTGRES = os.getenv("USE_POSTGRES", "false").lower() == "true"

print(f"Checking DB: {DATABASE_URL} (Postgres Enabled: {USE_POSTGRES})")

try:
    if USE_POSTGRES and "postgres" in DATABASE_URL:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        query = "SELECT count(*) FROM activities"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        print(f"Total activities in Postgres: {count}")
    else:
        # SQLite
        # If DATABASE_URL is a postgres url but USE_POSTGRES is false, fallback to local sqlite
        if "postgres" in DATABASE_URL and not USE_POSTGRES:
             db_path = "class_bridge.db"
             print(f"Using fallback SQLite DB: {db_path}")
        else:
             db_path = DATABASE_URL
             
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Check if table exists first
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='activities'")
        if not cursor.fetchone():
             print("Table 'activities' does not exist in SQLite DB.")
        else:
             cursor.execute("SELECT count(*) FROM activities")
             count = cursor.fetchone()[0]
             print(f"Total activities in SQLite: {count}")
        
    conn.close()
except Exception as e:
    print(f"Error checking activities: {e}")
