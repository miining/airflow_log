from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_task_deco",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    @task(task_id="task_1")
    def print_context(some_input):
        print(some_input)

    task_1 = print_context("task_1 실행하기")