from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dags_bash_select_fruit", 
    schedule="10 0 * * 6#1",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    t1_orange = BashOperator(
        task_id="t1_orange",
        bash_command="/opt/***_log/airflow_log/plugins/shell/select_fruit.sh ORANGE",
        #opt는 옵션을 의미함
    )
    
    t2_banana = BashOperator(
        task_id="t2_banana",
        bash_command="/opt/***_log/airflow_log/plugins/shell/select_fruit.sh BANANA",
    )

    t1_orange >> t2_banana
