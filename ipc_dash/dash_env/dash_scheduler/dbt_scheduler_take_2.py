import subprocess
import schedule
import time
import sys
import os
from datetime import datetime, timedelta


ETL_SCRIPT = r"C:/Users/chiso/ipc_dash/dash_env/dash_ingestion/dash_incremental_load.py"
DBT_PROJECT_DIR = r"C:/Users/chiso/ipc_dash/dash_env/dash_transform"

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

# Schedule the task to run every 2 hours, starting immediately
schedule.every(2).hours.do(run_etl_and_dbt)

print(f"Scheduler started. Running immediately and then every 2 hours...")

# Run the first task immediately
run_etl_and_dbt()

# Wait for the scheduled tasks
while True:
    schedule.run_pending()
    time.sleep(60)  # Check every minute
