from airflow import DAG
import datetime
import pendulum
from airflow.decorators import task

with DAG(
   
    dag_id="dags_python_time_templ", 
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 11, 4, tz="Asia/Seoul"),
    catchup=True,
    #11월 5일부터 현재까지 task를 모두 수행
) as dag:
    
    @task(task_id ='python_task')
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)
    
    show_templates()
