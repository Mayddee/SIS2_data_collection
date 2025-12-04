# Sxodim.com Event Scraper Pipeline

## Project Overview

This project implements an automated data pipeline that scrapes event information from sxodim.com (Almaty events), cleans the data, and stores it in a SQLite database. The pipeline is orchestrated using Apache Airflow and runs daily.

## Website Description

**Website**: https://sxodim.com/almaty/

**Type**: Dynamic website with JavaScript-rendered content

**Data Collected**:
- Event titles and descriptions
- Event dates, times, and venues
- Ticket prices and categories
- Event tags and partner information
- View counts and publication dates

The website uses dynamic loading (infinite scroll with "Load More" button), requiring Selenium for proper scraping.

## Project Structure

```
project/
├── README.md               # This file
├── requirements.txt        # Python dependencies
├── airflow_dag.py         # Airflow DAG definition
├── schema.sql             # Database schema
├── src/
│   ├── scraper.py         # Web scraping logic
│   ├── cleaner.py         # Data cleaning logic
│   └── loader.py          # Database loading logic
└── data/
    ├── raw_events.json    # Raw scraped data
    ├── cleaned_events.json # Cleaned data
    └── output.db          # SQLite database
```

## Prerequisites

- Python 3.8 or higher
- Google Chrome browser
- ChromeDriver (handled automatically by selenium)



## How to Run Scraping

### Manual Testing (Without Airflow)

1. **Run scraper only**:
```bash
python src/scraper.py
```
This will scrape events and save to `data/raw_events.json`

2. **Run cleaner only**:
```bash
python src/cleaner.py
```
This will clean the raw data and save to `data/cleaned_events.json`

3. **Run loader only**:
```bash
python src/loader.py
```
This will create the database and insert cleaned records

### Full Pipeline (Manual)
```bash
python src/scraper.py && python src/cleaner.py && python src/loader.py
```

## How to Run Airflow

### 1. Set up Airflow directory
```bash
# Set Airflow home directory
export AIRFLOW_HOME=$(pwd)/airflow_home

# Create directories
mkdir -p $AIRFLOW_HOME/dags
mkdir -p $AIRFLOW_HOME/logs
```

### 2. Copy DAG file
```bash
cp airflow_dag.py $AIRFLOW_HOME/dags/
```

### 3. Start Airflow

**Terminal 1 - Start Scheduler**:
```bash
airflow scheduler
```

**Terminal 2 - Start Webserver**:
```bash
airflow webserver --port 8080
```

### 4. Access Airflow UI

Open browser and go to: http://localhost:8080

Default credentials:
- Username: admin
- Password: admin

(If this doesn't work, create user with: `airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com`)

### 5. Enable and Run DAG

1. In Airflow UI, find DAG named `sxodim_events_pipeline`
2. Toggle the switch to enable it
3. Click "Trigger DAG" to run manually
4. Monitor execution in the Graph or Tree view

### 6. Check Logs

- Click on any task in the DAG
- Click "Log" to see execution details
- Logs show scraping progress, cleaning steps, and database loading

## Expected Output

### After Successful Run

1. **data/raw_events.json**: 
   - Contains 200+ raw event records
   - Includes all fields from website

2. **data/cleaned_events.json**: 
   - Contains cleaned and deduplicated records
   - At least 100 valid records (requirement met)

3. **data/output.db**: 
   - SQLite database with `events` table
   - All cleaned records inserted

### Sample Database Query
```bash
sqlite3 data/output.db "SELECT COUNT(*) FROM events;"
sqlite3 data/output.db "SELECT title, date, category FROM events LIMIT 5;"
```

## Pipeline Details

### Scraping Process
- Opens sxodim.com/almaty in headless Chrome
- Clicks "Load More" button until 200+ events loaded
- Extracts list data and visits each event detail page
- Collects comprehensive information including views, tags, descriptions
- Saves raw JSON with all fields

### Cleaning Process
- Removes duplicate events by ID
- Removes records with missing title or short_info
- Normalizes text fields (strip whitespace)
- Converts views to integer type
- Saves cleaned JSON (100+ records guaranteed)

### Loading Process
- Creates SQLite database with clear schema
- Inserts all cleaned records
- Uses INSERT OR REPLACE to handle duplicates
- Includes retry logic for reliability

## Airflow DAG Configuration

- **DAG Name**: sxodim_events_pipeline
- **Schedule**: Daily (`@daily` - runs at midnight)
- **Start Date**: December 1, 2025
- **Tasks**: 
  1. `scrape_events` - Scrapes website
  2. `clean_events` - Cleans data
  3. `load_to_database` - Loads to SQLite
- **Retries**: 2 attempts with 5-minute delay
- **Dependencies**: scrape → clean → load

## Troubleshooting

### ChromeDriver Issues
If you get ChromeDriver errors:
```bash
pip install --upgrade selenium
```

### Airflow Not Finding DAG
Ensure DAG file is in correct directory:
```bash
ls $AIRFLOW_HOME/dags/airflow_dag.py
```

### Permission Errors
Make sure data directory is writable:
```bash
chmod -R 755 data/
```

## Team Members

- Amangeldi Madina - 22B030302
- Serikbayeva Aruzhan - 22B030440
## Submission Date

December 4, 2025
