from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator
 
with DAG(
   
    dag_id="dags_bash_template", 
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    bash_t1 = BashOperator(
        task_id ='bash_t1',
        bash_command='echo "data_interval_end: {{data_interval_end}} "'
    )

    bash_t2 = BashOperator(
        task_id ='bash_t2',
        env={
            #key,value 형식 ,ds는 yyyymmdd로 나옴
            'START_DATE' : '{{data_interval_start | ds}}',
            'END_DATE' : '{{data_interval_end | ds}}'

        },
        bash_command = 'echo $START_DATE && echo $END_DATE'
    )

    bash_t1 >> bash_t2