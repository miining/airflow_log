from airflow import DAG
import datetime
import pendulum
from airflow.operators.email import EmailOperator


with DAG(
    dag_id="dags_email_operator", 
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    send_email_task = EmailOperator(
        task_id = 'send_email_task',
        to = 'purione1@naver.com',
        subject='airlfow 테스입니다',
        html_content='airflow 작업이 완료입니다'
    )