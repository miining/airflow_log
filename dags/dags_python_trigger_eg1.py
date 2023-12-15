from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.decorators import task


import pendulum

with DAG(
    dag_id='dags_python_trigger_eg1',
    start_date=pendulum.datetime(2023,12,1,tz="Asia/Seoul"),
    schedule=None,
    catchup=False
)as dag:
    bash_upstream_1 = BashOperator(
        task_id = 'bash_upstream_1',
        bash_command='echo upstream1'
    )

    @task(task_id = 'python_upstream_1')
    def python_upstream_1():
        raise AirflowException('downstream_1 Exception!!')
    
    #2번 task를 실패하도록 설정을 해봤음
    @task(task_id = 'python_upstream_2')
    def python_upstream_2():
        print('정상처리')


    @task(task_id = 'python_downstream_1',trigger_rule = 'all_done')
    def python_downstream_1():
        print('정상처리')

    [bash_upstream_1,python_upstream_1(),python_upstream_2()] >> python_downstream_1()