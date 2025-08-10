from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

def bash(cmd):
    return BashOperator(task_id=cmd.split()[0].replace(' ','_'), bash_command=cmd)

with DAG(
    dag_id="f1_daily_ingest",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    ingest = bash("python /opt/airflow/dags/../src/ingest/ergast_pull.py --from-season 2014 --to-season 2024")
    dbt_build = bash("cd /opt/airflow/dags/../dbt && dbt build --no-color")

    ingest >> dbt_build