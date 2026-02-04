from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'you',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'python_then_dbt_every_2_hours',
    default_args=default_args,
    schedule_interval='0 */2 * * *',
    catchup=False
)

def run_python_script():
    subprocess.run(["python3", "/path/to/your/script.py"], check=True)

run_python = PythonOperator(
    task_id='run_python_script',
    python_callable=run_python_script,
    dag=dag
)

run_dbt = BashOperator(
    task_id='run_dbt',
    bash_command='cd /path/to/dbt/project && dbt run',
    dag=dag
)

run_python >> run_dbt
