from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import task

with DAG(
    dag_id="dags_python_xcom_eg1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),
    catchup=False
)as dag:
    
    @task(task_id = 'python_xcom_push_task1')
    def xcom_push1(**kwagrs):
        ti = kwagrs['ti']
        ti.xcom_push(key="result1", value="value_1")
        ti.xcom_push(key="result2",value=[1,2,3])

    @task(task_id = 'python_xcom_push_task2')
    def xcom_push2(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key="result1", value="value_2")
        ti.xcom_push(key="result2",value=[1,2,3,4])

    @task(taks_id = 'python_xcom_pull_task')
    def xcom_pull(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(key="result1") 
        #value2값을 가져옴, key값이 같을때는 최근에 넣어준거가져옴
        value2 = ti.xcom_pull(key="result2",task_ids= 'python_xcom_push_task1')
        print(value1)
        print(value2)

    xcom_push1() >> xcom_push2() >> xcom_pull()