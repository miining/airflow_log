import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.operators.bash import BashOperator 
from airflow import DAG

with DAG(
    dag_id="dags_bash_operator", #dag 파일명과 dag_id는 일치시키는게 좋음
    schedule="0 0 * * *", #분,시,일,월,요일
    start_date=pendulum.datetime(2025, 6, 30, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1", #객체명과 task_id도 일치시키는게 좋음!
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME; echo good",
    )

    bash_t3 = BashOperator(
        task_id="bash_t3",
        bash_command="echo $HOSTNAME; echo good",
    )

    bash_t4 = BashOperator(
        task_id="bash_t4",
        bash_command="echo $HOSTNAME; echo good",
    )

    bash_t1 >> bash_t2 >> bash_t3 >> bash_t4