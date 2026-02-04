import subprocess
import sys
import os
from datetime import datetime

# Get the directory where this script lives
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Paths relative to this script
ETL_SCRIPT = os.path.join(BASE_DIR, "gosource_ingestion", "gosource_incremental_load.py")
DBT_PROJECT_DIR = os.path.join(BASE_DIR, "gosource_transform")

def run_etl_and_dbt():
    try:
        print(f"Running ETL and dbt tasks at {datetime.now()}...")
       
        # Run ETL Python script
        subprocess.run([sys.executable, ETL_SCRIPT], check=True)
       
        # Run dbt transformations
        subprocess.run(["dbt", "run"], cwd=DBT_PROJECT_DIR, check=True)
       
        print("ETL + dbt finished successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e}")
        sys.exit(1)

# Run once and exit
run_etl_and_dbt()