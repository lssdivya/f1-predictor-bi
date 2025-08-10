from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="f1_weekend_train_predict",
    start_date=datetime(2024,1,1),
    schedule_interval="0 3 * * MON",  # Monday 03:00 UTC after race weekend
    catchup=False,
) as dag:
    train_top10 = BashOperator(task_id="train_top10", bash_command="python /opt/airflow/dags/../src/models/train.py --target top10")
    predict_top10 = BashOperator(task_id="predict_top10", bash_command="python /opt/airflow/dags/../src/models/predict.py --target top10 --season 2024 --round all")

    train_top10 >> predict_top10