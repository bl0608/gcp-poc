# dag.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from etl_tasks import extract_data_from_sql, load_data_to_gcs  # Import the ETL functions

# Define default arguments
default_args = {
    'owner': 'ksa_poc',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
with DAG(dag_id='sql_to_gcs2', default_args=default_args, schedule_interval=None) as dag:
    extract_task = PythonOperator(
        task_id='extract_data_from_sql',
        python_callable=extract_data_from_sql
    )

    load_task = PythonOperator(
        task_id='load_data_to_gcs',
        python_callable=load_data_to_gcs
    )

    extract_task >> load_task
