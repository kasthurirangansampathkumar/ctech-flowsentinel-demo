from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from github_notifier import notify_github_on_failure

default_args = {
    "start_date": datetime(2026, 1, 1),
    "on_failure_callback": notify_github_on_failure
}

def deliberate_failure():
    raise RuntimeError("Test execution failure to verify automated GitHub ticket creation.")

with DAG(
    dag_id="example_monitored_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    test_failure_task = PythonOperator(
        task_id="trigger_test_failure",
        python_callable=deliberate_failure
    )