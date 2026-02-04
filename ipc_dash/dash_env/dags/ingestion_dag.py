from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Define the default_args dictionary, which will be used in the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 8),  # Adjust your start date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'my_first_dag',  # Name of the DAG
    default_args=default_args,
    description='A simple DAG to run ingestion and DBT jobs',
    schedule_interval=None,  # Define this if you want a periodic schedule, e.g., '0 0 * * *'
)

# Define your tasks
def run_ingestion():
    print("Ingestion Task is Running...")

def run_dbt():
    print("DBT Task is Running...")

# Ingestion Task: Run a Python function (e.g., ingestion file)
ingestion_task = PythonOperator(
    task_id='run_ingestion_task',
    python_callable=run_ingestion,  # 'Replace this with your ingestion logic'
    dag=dag,
)

# DBT Task: Run a shell command for DBT
dbt_task = BashOperator(
    task_id='run_dbt_task',
    bash_command="dbt run --project-dir /path/to/your/dbt_project",  # 'Replace with actual path'
    dag=dag,
)

# Set up task dependencies
ingestion_task >> dbt_task  # This makes sure the ingestion runs before DBT



