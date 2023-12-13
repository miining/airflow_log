from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2023,12,1,tz="Asia/Seoul"),
    catchup=False
)as dag:
    
    #권고하지 않는 방안
    var_value = Variable.get("sample_key")

    bash_var_1 = BashOperator(
        task_id = "bash_var_1",
        bash_command=f"echo variable:{var_value}"
    )

    #권고하는 방안, template변수 이용해 직접꺼냄
    bash_var_2 = BashOperator(
        task_id = "bash_var_2",
        bash_command="echo variable:{{var.value.sample_key}}"
    )