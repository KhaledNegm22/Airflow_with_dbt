from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='load_excel_and_dbt_run_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
    description='Load Excel to Postgres and run dbt transformations',
) as dag:

    load_script = BashOperator(
        task_id='load_excel_to_postgres',
        bash_command='python /usr/local/airflow/scripts/load_excel_to_postgres.py'
    )

    run_dbt = BashOperator(
    task_id='run_dbt_project',
    bash_command='cd /usr/local/airflow/my_dbt_project && dbt run',
    env={"DBT_PROFILES_DIR": "/usr/local/airflow/my_dbt_project"},
    )

    load_script >> run_dbt  # ترتيب التنفيذ