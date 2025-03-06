import subprocess
import os
import pandas as pd
from sqlalchemy import create_engine

# Configuration
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgis:5432/geoapi_db")
SENSEBOX_IDS_FILE = "sensebox_ids.txt"
COMBINED_CSV_FILE = "combined_data.csv"

def run_script(script, args=None):
    """Run a Python script as a subprocess."""
    command = ["python3", script]
    if args:
        command.extend(args)
    print(f"Running: {' '.join(command)}")
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running {script}:\n{result.stderr}")
        exit(result.returncode)
    else:
        print(result.stdout)

def store_csv_in_postgis(csv_file):
    """Load CSV data into PostGIS using SQLAlchemy."""
    print("Connecting to the database...")
    engine = create_engine(DB_URL)

    print("Reading CSV file...")
    df = pd.read_csv(csv_file)

    print("Storing data in PostGIS...")
    df.to_sql("raw_sensebox_data", engine, if_exists="replace", index=False)
    print("Data successfully stored in the database!")

if __name__ == "__main__":

    # Step A: Retrieve senseBox IDs (creates sensebox_ids.txt)
    run_script("atrai_ids.py")

    # Check that the output file was created
    if not os.path.exists(SENSEBOX_IDS_FILE):
        print(f"Error: {SENSEBOX_IDS_FILE} not found after running atrai_ids.py")
        exit(1)

    # Step B: Run main.py, passing senseBox IDs file as input
    # Here we pass the file using the --file argument as expected by main.py
    run_script("main.py", ["--file", SENSEBOX_IDS_FILE])

    # Step C: Combine CSV files into a single CSV (combined_data.csv)
    run_script("combine_csv.py")

    # Step D: Load the combined CSV into the PostGIS database
    store_csv_in_postgis(COMBINED_CSV_FILE)
