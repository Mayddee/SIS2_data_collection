import sqlite3
import json
import time
from pathlib import Path

DB_FILE = Path("data/output.db")
CLEANED_FILE = Path("data/cleaned_events.json")


def init_db(db_path: Path):
    """Create SQLite DB and define a clear table schema."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Create table WITHOUT the full_price column to keep schema clear
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                category TEXT,
                min_price TEXT,
                partner TEXT,
                views INTEGER,
                date TEXT,
                time TEXT,
                address TEXT,
                description TEXT,
                publication_date TEXT,
                tags TEXT
            );
        """)
    except Exception as err:
        print(f"[db] schema creation failed: {type(err).__name__}")
        conn.close()
        raise err
    
    conn.commit()
    conn.close()
    print("[db] database initialized")


def insert_records(db_path: Path, records: list[dict], retries=3, delay=2):
    """Insert cleaned records into SQLite with retry logic and logging."""
    for attempt in range(1, retries + 1):
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            
            for e in records:
                tags_str = ",".join(e.get("tags", []))  # join list to comma text
                cur.execute("""
                    INSERT OR REPLACE INTO events (
                        id, title, url, category, min_price, partner,
                        views, date, time, address, description,
                        publication_date, tags
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    e["id"],
                    e["title"],
                    e["url"],
                    e.get("category"),
                    e.get("min_price"),
                    e.get("partner"),
                    e.get("views"),
                    e.get("date"),
                    e.get("time"),
                    e.get("address"),
                    e.get("description"),
                    e.get("publication_date"),
                    tags_str
                ))
            
            conn.commit()
            conn.close()
            print("[db] all records inserted successfully")
            break
            
        except Exception as err:
            print(f"[db] insert attempt {attempt} failed: {type(err).__name__}")
            if 'conn' in locals():
                conn.close()
            
            if attempt < retries:
                time.sleep(delay)
            else:
                raise err


def load_cleaned(path: Path):
    """Load cleaned JSON file to insert into database."""
    if not path.exists():
        print("[load] cleaned JSON not found")
        return []
    
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    
    print(f"[load] records ready for DB: {len(data)}")
    return data


def run_loader():
    """
    Main loading function - called by Airflow.
    Returns True on success, raises exception on failure.
    """
    print("[load] starting loading process")
    
    init_db(DB_FILE)
    
    data = load_cleaned(CLEANED_FILE)
    if not data:
        raise ValueError("No cleaned data found to load")
    
    insert_records(DB_FILE, data)
    print(f"[load] successfully loaded {len(data)} records into database")
    
    return True


def main():
    """Manual pipeline: init DB → read cleaned JSON → insert into SQLite."""
    run_loader()


if __name__ == "__main__":
    main()
