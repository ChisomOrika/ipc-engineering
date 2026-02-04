from apscheduler.schedulers.blocking import BlockingScheduler
import subprocess

def run_dbt():
    subprocess.run(["dbt", "run"], cwd=r'C:\Users\chiso\ipc_dash\dash_env\dash_transform')

def run_python_script():
    subprocess.run(["python3", r'C:\Users\chiso\ipc_dash\dash_env\dash_ingestion\dash_incremental_load.py'])

scheduler = BlockingScheduler()

# Schedule every 2 hours
scheduler.add_job(run_python_script, 'interval', hours=2)
scheduler.add_job(run_dbt, 'interval', hours=2)


scheduler.start()
