import json
from pathlib import Path

RAW_FILE = Path("data/raw_events.json")
CLEAN_FILE = Path("data/cleaned_events.json")


def load_json(path: Path):
    """Read raw scraped JSON data from file."""
    if not path.exists():
        print("[clean] raw JSON not found")
        return []
    
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    
    print(f"[clean] raw records read: {len(data)}")
    return data


def normalize_record(e: dict):
    """Normalize string fields and fix types, remove unwanted fields."""
    # Convert views to integer if stored as string, otherwise keep existing
    if isinstance(e.get("views"), str):
        digits = "".join(c for c in e["views"] if c.isdigit())
        e["views"] = int(digits) if digits else None
    
    # Strip and normalize basic text fields
    for key in ["id", "title", "category", "min_price", "partner", "short_info", "description"]:
        if key in e and e[key]:
            e[key] = str(e[key]).strip()
    
    # Convert price_full to None to later fully drop this column from DB model
    e["price_full"] = None
    
    return e


def clean_records(data: list[dict]):
    """Main cleaning pipeline: dedup → remove invalid rows → normalize."""
    cleaned: list[dict] = []
    seen = set()
    
    for e in data:
        eid = e.get("id")
        
        # Dedup using unique ID
        if eid in seen:
            continue
        seen.add(eid)
        
        # Remove rows where title or short_info are None or empty
        if not e.get("title") or not e.get("short_info"):
            continue
        
        # Normalize text and fix types
        e = normalize_record(e)
        cleaned.append(e)
    
    print(f"[clean] cleaned unique records: {len(cleaned)}")
    print(f"[clean] duplicates removed: {len(data) - len(seen)} discarded by ID set")
    
    return cleaned


def save_json(data, path: Path):
    """Save cleaned data into a new JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("[clean] cleaned JSON saved")


def run_cleaner():
    """
    Main cleaning function - called by Airflow.
    Returns True on success, raises exception on failure.
    """
    print("[clean] starting cleaning process")
    
    data = load_json(RAW_FILE)
    if not data:
        raise ValueError("No raw data found to clean")
    
    cleaned = clean_records(data)
    
    if not cleaned:
        raise ValueError("No records remaining after cleaning")
    
    save_json(cleaned, CLEAN_FILE)
    print(f"[clean] successfully cleaned {len(cleaned)} records")
    
    return True


def main():
    """Execute cleaner pipeline manually for testing."""
    run_cleaner()


if __name__ == "__main__":
    main()
   

