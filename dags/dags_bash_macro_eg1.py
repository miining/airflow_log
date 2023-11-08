from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
   
    dag_id="dags_bash_macro_eg1", 
    schedule="10 0 L * *",
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    bash_task_1 = BashOperator(
        task_id = 'bask_task_1',
        env={
            'START_DATE' : '{{data_interval_start.in_timezone("Asia/Seoul") | ds }}',
            'END_DATE' : '{{ (data_interval_end.in_timezone("Asia/Seoul")-macros.datautill.relativedelta.relativedelta(days=1)) | ds }}'
        },
        bash_command='echo "START_DATE : $START_DATE" && echo "END_DATE : $END_DATE"'
    )
    