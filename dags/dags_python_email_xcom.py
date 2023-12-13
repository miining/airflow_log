from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_python_email_xcom",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2023,12,1,tz="Asia/Seoul"),
    catchup=False
)as dag:
    @task(task_id = 'something_task')
    def some_logic(**kwargs):
        from random import choice
        return choice(['Success','Fail'])
    
    send_email = EmailOperator(
        task_id = 'send_email',
        to = 'purione1@naver.com',
        #제목
        subject= '{{data_interval_end.in_timezone("Asia/Seoul") | ds}} some_logic 처리결과',
        html_content='{{data_interval_end.in_timezone("Asia/Seoul") | ds }} 처리결과는 <br> \
            {{ti.xcom_pull(task_ids = "something_task")}} 했습니다. <br>'
    )

    some_logic() >> send_email
