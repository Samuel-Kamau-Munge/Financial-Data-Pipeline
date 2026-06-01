from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id='financial_pipeline', start_date=datetime(2023,1,1), schedule_interval='@daily', catchup=False) as dag:
    ingest = BashOperator(
        task_id='run_producer',
        bash_command='python /app/producer.py "C:\\Finance Data\\Accounts-Receivable.xlsx"'
    )

    load = BashOperator(
        task_id='run_consumer',
        bash_command='python /app/consumer.py'
    )

    dbt_run = BashOperator(
        task_id='run_dbt',
        bash_command='cd /opt/dbt && dbt run'
    )

    ingest >> load >> dbt_run
